"""IEC 60870-5-101 master device"""

import asyncio
import collections
import contextlib
import datetime
import functools
import logging

from hat import aio
from hat import json
from hat.drivers import iec101
from hat.drivers import serial
from hat.drivers.iec60870 import link
import hat.event.common

from hat.gateway.devices.iec101 import common


mlog: logging.Logger = logging.getLogger(__name__)

device_type: str = 'iec101_master'

json_schema_id: str = "hat-gateway://iec101.yaml#/definitions/master"

json_schema_repo: json.SchemaRepository = common.json_schema_repo


async def create(conf: common.DeviceConf,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec101MasterDevice':
    device = Iec101MasterDevice()

    device._conf = conf
    device._event_type_prefix = event_type_prefix
    device._event_client = event_client
    device._master = None
    device._conns = {}
    device._send_queue = aio.Queue()
    device._async_group = aio.Group()
    device._remote_enabled = {i['address']: False
                              for i in conf['remote_devices']}
    device._remote_confs = {i['address']: i
                            for i in conf['remote_devices']}
    device._remote_groups = {}

    enable_event_types = [(*event_type_prefix, 'system', 'remote_device',
                           str(i['address']), 'enable')
                          for i in conf['remote_devices']]
    remote_enable_events = await event_client.query(
        hat.event.common.QueryData(event_types=enable_event_types,
                                   unique_type=True))

    for event in remote_enable_events:
        try:
            device._process_event(event)

        except Exception as e:
            mlog.warning('error processing enable event: %s', e, exc_info=e)

    device.async_group.spawn(device._create_link_master_loop)
    device.async_group.spawn(device._event_loop)
    device.async_group.spawn(device._send_loop)

    return device


class Iec101MasterDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _create_link_master_loop(self):
        try:
            while True:
                self._register_status('CONNECTING')

                try:
                    self._master = await link.unbalanced.create_master(
                        port=self._conf['port'],
                        baudrate=self._conf['baudrate'],
                        bytesize=serial.ByteSize[self._conf['bytesize']],
                        parity=serial.Parity[self._conf['parity']],
                        stopbits=serial.StopBits[self._conf['stopbits']],
                        xonxoff=self._conf['flow_control']['xonxoff'],
                        rtscts=self._conf['flow_control']['rtscts'],
                        dsrdtr=self._conf['flow_control']['dsrdtr'],
                        silent_interval=self._conf['silent_interval'],
                        address_size=link.AddressSize[
                            self._conf['device_address_size']])

                except Exception as e:
                    mlog.warning('link master (endpoint) failed to create: %s',
                                 e, exc_info=e)
                    self._register_status('DISCONNECTED')
                    await asyncio.sleep(self._conf['reconnect_delay'])
                    continue

                self._register_status('CONNECTED')

                for address, enabled in self._remote_enabled.items():
                    if enabled:
                        self._enable_remote(address)

                await self._master.wait_closed()
                self._register_status('DISCONNECTED')
                self._master = None

        except Exception as e:
            mlog.error('create link master error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing link master loop')
            self.close()
            with contextlib.suppress(ConnectionError):
                self._register_status('DISCONNECTED')
            self._conns = {}
            if self._master:
                await aio.uncancellable(self._master.async_close())

    async def _event_loop(self):
        try:
            while True:
                events = await self._event_client.receive()

                for event in events:
                    try:
                        self._process_event(event)

                    except Exception as e:
                        mlog.warning('event %s ignored due to: %s',
                                     event, e, exc_info=e)

        except ConnectionError:
            mlog.debug('event client connection closed')

        except Exception as e:
            mlog.error('event loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing device, event loop')
            self.close()

    async def _send_loop(self):
        while True:
            msg, address = await self._send_queue.get()

            conn = self._conns.get(address)
            if not conn or not conn.is_open:
                mlog.warning('msg %s not sent, connection to %s closed',
                             msg, address)
                continue

            try:
                await conn.send([msg])
                mlog.debug('msg sent asdu=%s', msg.asdu_address)

            except ConnectionError:
                mlog.warning('msg %s not sent, connection to %s closed',
                             msg, address)

    async def _connection_loop(self, group, address):
        remote_conf = self._remote_confs[address]
        try:
            while True:
                self._register_rmt_status(address, 'CONNECTING')

                try:
                    master_conn = await self._master.connect(
                        addr=address,
                        response_timeout=remote_conf['response_timeout'],
                        send_retry_count=remote_conf['send_retry_count'],
                        poll_class1_delay=remote_conf['poll_class1_delay'],
                        poll_class2_delay=remote_conf['poll_class2_delay'])

                except Exception as e:
                    mlog.error('connection error to address %s: %s',
                               address, e, exc_info=e)
                    self._register_rmt_status(address, 'DISCONNECTED')
                    await asyncio.sleep(remote_conf['reconnect_delay'])
                    continue

                self._register_rmt_status(address, 'CONNECTED')

                conn = iec101.MasterConnection(
                    conn=master_conn,
                    cause_size=iec101.CauseSize[self._conf['cause_size']],
                    asdu_address_size=iec101.AsduAddressSize[
                        self._conf['asdu_address_size']],
                    io_address_size=iec101.IoAddressSize[
                        self._conf['io_address_size']])
                self._conns[address] = conn
                group.spawn(self._receive_loop, conn, address)

                if remote_conf['time_sync_delay'] is not None:
                    group.spawn(self._time_sync_loop, conn,
                                remote_conf['time_sync_delay'])

                await conn.wait_closed()
                self._register_rmt_status(address, 'DISCONNECTED')
                self._conns.pop(address)

        except Exception as e:
            mlog.error('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing remote device %s', address)
            group.close()
            with contextlib.suppress(ConnectionError):
                self._register_rmt_status(address, 'DISCONNECTED')
            if address in self._conns:
                conn = self._conns.pop(address)
                await aio.uncancellable(conn.async_close())

    async def _receive_loop(self, conn, address):
        try:
            while True:
                try:
                    msgs = await conn.receive()

                except iec101.AsduTypeError as e:
                    mlog.warning("asdu type error: %s", e)
                    continue

                events = collections.deque()
                for msg in msgs:
                    if isinstance(msg, iec101.ClockSyncMsg):
                        continue

                    try:
                        event = _msg_to_event(self._event_type_prefix, address,
                                              msg)
                        events.append(event)

                    except Exception as e:
                        mlog.warning('message %s ignored due to: %s',
                                     msg, e, exc_info=e)

                if not events:
                    continue

                self._event_client.register(list(events))
                for e in events:
                    mlog.debug('registered event %s', e)

        except ConnectionError:
            mlog.debug('connection closed')

        except Exception as e:
            mlog.error('receive loop error: %s', e, exc_info=e)

        finally:
            conn.close()

    async def _time_sync_loop(self, conn, delay):
        try:
            while True:
                time_now = datetime.datetime.now(datetime.timezone.utc)
                time_iec101 = iec101.time_from_datetime(time_now)
                msg = iec101.ClockSyncMsg(
                    is_test=False,
                    originator_address=0,
                    asdu_address={
                        'ONE': 0xFF,
                        'TWO': 0xFFFF}[self._conf['asdu_address_size']],
                    time=time_iec101,
                    is_negative_confirm=False,
                    cause=iec101.ClockSyncReqCause.ACTIVATION)
                await conn.send([msg])
                mlog.debug('time sync sent %s', time_iec101)

                await asyncio.sleep(delay)

        except ConnectionError:
            mlog.debug('connection closed')

        except Exception as e:
            mlog.error('time sync loop error: %s', e, exc_info=e)

        finally:
            conn.close()

    def _process_event(self, event):
        match_type = functools.partial(hat.event.common.matches_query_type,
                                       event.event_type)

        prefix = (*self._event_type_prefix, 'system', 'remote_device', '?')
        if not match_type((*prefix, '*')):
            raise Exception('unexpected event type')

        address = int(event.event_type[len(prefix) - 1])
        suffix = event.event_type[len(prefix):]

        if match_type((*prefix, 'enable')):
            self._process_event_enable(address, event)

        elif match_type((*prefix, 'command', '?', '?', '?')):
            cmd_key = common.CommandKey(
                cmd_type=common.CommandType(suffix[1]),
                asdu_address=int(suffix[2]),
                io_address=int(suffix[3]))
            msg = _command_from_event(cmd_key, event)

            self._send_queue.put_nowait((msg, address))
            mlog.debug('command asdu=%s io=%s prepared for sending',
                       cmd_key.asdu_address, cmd_key.io_address)

        elif match_type((*prefix, 'interrogation', '?')):
            asdu_address = int(suffix[1])
            msg = _interrogation_from_event(asdu_address, event)

            self._send_queue.put_nowait((msg, address))
            mlog.debug("interrogation request asdu=%s prepared for sending",
                       asdu_address)

        elif match_type((*prefix, 'counter_interrogation', '?')):
            asdu_address = int(suffix[1])
            msg = _counter_interrogation_from_event(asdu_address, event)

            self._send_queue.put_nowait((msg, address))
            mlog.debug("counter interrogation request asdu=%s prepared for "
                       "sending", asdu_address)

        else:
            raise Exception('unexpected event type')

    def _process_event_enable(self, address, event):
        if address not in self._remote_enabled:
            raise Exception('invalid remote device address')

        enable = event.payload.data
        if not isinstance(enable, bool):
            raise Exception('invalid enable event payload')

        if address not in self._remote_enabled:
            mlog.warning('received enable for unexpected remote device')
            return

        self._remote_enabled[address] = enable

        if not enable:
            self._disable_remote(address)

        elif not self._master:
            return

        else:
            self._enable_remote(address)

    def _enable_remote(self, address):
        mlog.debug('enabling device %s', address)
        remote_group = self._remote_groups.get(address)
        if remote_group and remote_group.is_open:
            mlog.debug('device %s is already running', address)
            return

        remote_group = self._async_group.create_subgroup()
        self._remote_groups[address] = remote_group
        remote_group.spawn(self._connection_loop, remote_group, address)

    def _disable_remote(self, address):
        mlog.debug('disabling device %s', address)
        if address in self._remote_groups:
            remote_group = self._remote_groups.pop(address)
            remote_group.close()

    def _register_status(self, status):
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=status))
        self._event_client.register([event])
        mlog.debug('device status -> %s', status)

    def _register_rmt_status(self, address, status):
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix,
                        'gateway', 'remote_device', str(address), 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=status))
        self._event_client.register([event])
        mlog.debug('remote device %s status -> %s', address, status)


def _msg_to_event(event_type_prefix, address, msg):
    if isinstance(msg, iec101.DataMsg):
        return _data_to_event(event_type_prefix, address, msg)

    if isinstance(msg, iec101.CommandMsg):
        return _command_to_event(event_type_prefix, address, msg)

    if isinstance(msg, iec101.InterrogationMsg):
        return _interrogation_to_event(event_type_prefix, address, msg)

    if isinstance(msg, iec101.CounterInterrogationMsg):
        return _counter_interrogation_to_event(event_type_prefix, address, msg)

    raise Exception('unsupported message type')


def _data_to_event(event_type_prefix, address, msg):
    data_type = common.get_data_type(msg.data)
    cause = common.cause_to_json(iec101.DataResCause, msg.cause)
    if isinstance(cause, str) and cause.startswith('INTERROGATED_'):
        cause = 'INTERROGATED'
    data = common.data_to_json(msg.data)
    event_type = (*event_type_prefix, 'gateway', 'remote_device', str(address),
                  'data', data_type.value, str(msg.asdu_address),
                  str(msg.io_address))
    source_timestamp = common.time_to_source_timestamp(msg.time)

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'is_test': msg.is_test,
                  'cause': cause,
                  'data': data}))


def _command_to_event(event_type_prefix, address, msg):
    command_type = common.get_command_type(msg.command)
    cause = common.cause_to_json(iec101.CommandResCause, msg.cause)
    command = common.command_to_json(msg.command)
    event_type = (*event_type_prefix, 'gateway', 'remote_device', str(address),
                  'command', command_type.value, str(msg.asdu_address),
                  str(msg.io_address))

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'is_test': msg.is_test,
                  'is_negative_confirm': msg.is_negative_confirm,
                  'cause': cause,
                  'command': command}))


def _interrogation_to_event(event_type_prefix, address, msg):
    cause = common.cause_to_json(iec101.CommandResCause, msg.cause)
    event_type = (*event_type_prefix, 'gateway', 'remote_device', str(address),
                  'interrogation', str(msg.asdu_address))

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'is_test': msg.is_test,
                  'is_negative_confirm': msg.is_negative_confirm,
                  'request': msg.request,
                  'cause': cause}))


def _counter_interrogation_to_event(event_type_prefix, address, msg):
    cause = common.cause_to_json(iec101.CommandResCause, msg.cause)
    event_type = (*event_type_prefix, 'gateway', 'remote_device', str(address),
                  'counter_interrogation', str(msg.asdu_address))

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'is_test': msg.is_test,
                  'is_negative_confirm': msg.is_negative_confirm,
                  'request': msg.request,
                  'freeze': msg.freeze.name,
                  'cause': cause}))


def _command_from_event(cmd_key, event):
    cause = common.cause_from_json(iec101.CommandReqCause,
                                   event.payload.data['cause'])
    command = common.command_from_json(cmd_key.cmd_type,
                                       event.payload.data['command'])

    return iec101.CommandMsg(is_test=event.payload.data['is_test'],
                             originator_address=0,
                             asdu_address=cmd_key.asdu_address,
                             io_address=cmd_key.io_address,
                             command=command,
                             is_negative_confirm=False,
                             cause=cause)


def _interrogation_from_event(asdu_address, event):
    cause = common.cause_from_json(iec101.CommandReqCause,
                                   event.payload.data['cause'])

    return iec101.InterrogationMsg(is_test=event.payload.data['is_test'],
                                   originator_address=0,
                                   asdu_address=asdu_address,
                                   request=event.payload.data['request'],
                                   is_negative_confirm=False,
                                   cause=cause)


def _counter_interrogation_from_event(asdu_address, event):
    freeze = iec101.FreezeCode[event.payload.data['freeze']]
    cause = common.cause_from_json(iec101.CommandReqCause,
                                   event.payload.data['cause'])

    return iec101.CounterInterrogationMsg(
        is_test=event.payload.data['is_test'],
        originator_address=0,
        asdu_address=asdu_address,
        request=event.payload.data['request'],
        freeze=freeze,
        is_negative_confirm=False,
        cause=cause)
