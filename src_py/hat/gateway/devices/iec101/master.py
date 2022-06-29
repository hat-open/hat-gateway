"""IEC 60870-5-101 master device"""

import asyncio
import contextlib
import datetime
import enum
import logging

from hat import aio
from hat import json
from hat.drivers import serial
from hat.drivers.iec60870 import iec101
from hat.drivers.iec60870 import link
from hat.gateway import common
import hat.event.common


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
    device._remote_enabled = {i['address']: False
                              for i in conf['remote_devices']}
    device._remote_confs = {i['address']: i
                            for i in conf['remote_devices']}
    device._remote_groups = {}

    remote_enable_evts = [
        (*event_type_prefix, 'system', 'remote_device', str(i['address']),
            'enable') for i in conf['remote_devices']]
    remote_enable_events = await event_client.query(
        hat.event.common.QueryData(
            event_types=remote_enable_evts, unique_type=True))
    for event in remote_enable_events:
        try:
            device._process_event(event)
        except Exception as e:
            mlog.warning('error processing enable event: %s', e, exc_info=e)

    device._send_queue = aio.Queue()
    device._async_group = aio.Group()
    device._async_group.spawn(device._create_link_master_loop)
    device._async_group.spawn(device._event_loop)
    device._async_group.spawn(device._send_loop)
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
                conn = iec101.Connection(
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
                msgs = await conn.receive()
                events = []
                for msg in msgs:
                    try:
                        event = _msg_to_event(
                            msg, self._event_type_prefix, address)
                    except Exception as e:
                        mlog.warning('message %s ignored due to:%s',
                                     msg, e, exc_info=e)
                        continue
                    if event:
                        events.append(event)
                if events:
                    self._event_client.register(events)
        except ConnectionError:
            mlog.debug('connection closed')
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
        finally:
            conn.close()

    def _process_event(self, event):
        prefix_len = len(self._event_type_prefix)
        if event.event_type[prefix_len + 1] != 'remote_device':
            raise Exception('unexpected event type')
        address = int(event.event_type[prefix_len + 2])
        etype_suffix = event.event_type[prefix_len + 3:]
        if etype_suffix[0] == 'enable':
            self._process_enable(event, address)
        elif etype_suffix[0] == 'command':
            command_type = etype_suffix[1]
            asdu = int(etype_suffix[2])
            io = int(etype_suffix[3])
            self._process_command(event, address, command_type, asdu, io)
        elif etype_suffix[0] == 'interrogation':
            asdu = int(etype_suffix[1])
            self._process_interrogation(event, address, asdu, is_counter=False)
        elif etype_suffix[0] == 'counter_interrogation':
            asdu = int(etype_suffix[1])
            self._process_interrogation(event, address, asdu, is_counter=True)
        else:
            raise Exception('unexpected event type')

    def _process_enable(self, event, address):
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
        remote_group = self._remote_groups.get(address)
        if remote_group and remote_group.is_open:
            return
        remote_group = self._async_group.create_subgroup()
        self._remote_groups[address] = remote_group
        remote_group.spawn(self._connection_loop, remote_group, address)

    def _disable_remote(self, address):
        if address in self._remote_groups:
            remote_group = self._remote_groups.pop(address)
            remote_group.close()

    def _process_command(self, event, address, command_type, asdu, io):
        command = _command_from_event(event, command_type)
        cause = (iec101.CommandReqCause[event.payload.data['cause']]
                 if isinstance(event.payload.data['cause'], str)
                 else event.payload.data['cause'])
        msg = iec101.CommandMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu,
            io_address=io,
            command=command,
            is_negative_confirm=False,
            cause=cause)
        self._send_queue.put_nowait((msg, address))

    def _process_interrogation(self, event, address, asdu, is_counter):
        cause = iec101.CommandReqCause.ACTIVATION
        request = event.payload.data['request']
        if is_counter:
            msg = iec101.CounterInterrogationMsg(
                is_test=False,
                originator_address=0,
                asdu_address=asdu,
                request=request,
                freeze=iec101.FreezeCode[event.payload.data['freeze']],
                is_negative_confirm=False,
                cause=cause)
        else:
            msg = iec101.InterrogationMsg(
                is_test=False,
                originator_address=0,
                asdu_address=asdu,
                request=request,
                is_negative_confirm=False,
                cause=cause)
        self._send_queue.put_nowait((msg, address))

    def _register_status(self, status):
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=status))
        self._event_client.register([event])

    def _register_rmt_status(self, address, status):
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix,
                        'gateway', 'remote_device', str(address), 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=status))
        self._event_client.register([event])


def _msg_to_event(msg, event_type_prefix, address):
    if msg.is_test and isinstance(msg, (iec101.CommandMsg,
                                        iec101.InterrogationMsg,
                                        iec101.CounterInterrogationMsg)):
        mlog.warning('received test response %s', msg)
    if isinstance(msg, iec101.DataMsg):
        return _data_msg_to_event(msg, event_type_prefix, address)
    elif isinstance(msg, iec101.CommandMsg):
        return _command_msg_to_event(msg, event_type_prefix, address)
    elif isinstance(msg, iec101.InterrogationMsg):
        return _interrogation_msg_to_event(
            msg, event_type_prefix, address, is_counter=False)
    elif isinstance(msg, iec101.CounterInterrogationMsg):
        return _interrogation_msg_to_event(
            msg, event_type_prefix, address, is_counter=True)
    elif (isinstance(msg, iec101.ClockSyncMsg) and
          msg.cause == iec101.ClockSyncResCause.ACTIVATION_CONFIRMATION):
        if msg.is_negative_confirm:
            mlog.warning(
                'received negative confirmation on clock sync: %s', msg)
        return
    raise Exception('unexpected message')


def _data_msg_to_event(msg, event_type_prefix, address):
    source_timestamp = hat.event.common.timestamp_from_datetime(
        iec101.time_to_datetime(msg.time)) if msg.time else None
    data_type = _data_type_from_msg(msg)
    payload = _data_payload_from_msg(msg)
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'remote_device',
                    str(address), 'data',
                    data_type, str(msg.asdu_address), str(msg.io_address)),
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))


def _command_msg_to_event(msg, event_type_prefix, address):
    command_type = _command_type_from_msg(msg)
    payload = _command_payload_from_msg(msg)
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'remote_device',
                    str(address), 'command',
                    command_type, str(msg.asdu_address), str(msg.io_address)),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))


def _interrogation_msg_to_event(msg, event_type_prefix, address, is_counter):
    if msg.is_negative_confirm:
        status = 'ERROR'
    elif msg.cause == iec101.CommandResCause.ACTIVATION_CONFIRMATION:
        status = 'START'
    elif msg.cause == iec101.CommandResCause.ACTIVATION_TERMINATION:
        status = 'STOP'
    else:
        status = 'ERROR'
    payload = {'status': status,
               'request': msg.request}
    if is_counter:
        payload['freeze'] = msg.freeze.name
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'remote_device',
                    str(address),
                    'counter_interrogation' if is_counter else 'interrogation',
                    str(msg.asdu_address)),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))


def _data_payload_from_msg(msg):
    if isinstance(msg.cause, enum.Enum):
        cause = ('INTERROGATED' if msg.cause.name.startswith('INTERROGATED')
                 else msg.cause.name)
    else:
        cause = msg.cause
    quality = msg.data.quality._asdict() if msg.data.quality else None
    payload = {'value': _data_value_from_msg(msg),
               'quality': quality,
               'cause': cause,
               'test': msg.is_test}
    if isinstance(msg.data, iec101.ProtectionData):
        payload['elapsed_time'] = msg.data.elapsed_time
    elif isinstance(msg.data, iec101.ProtectionStartData):
        payload['duration_time'] = msg.data.duration_time
    elif isinstance(msg.data, iec101.ProtectionCommandData):
        payload['operating_time'] = msg.data.operating_time
    return payload


def _data_value_from_msg(msg):
    if isinstance(msg.data, (iec101.SingleData,
                             iec101.DoubleData,
                             iec101.ProtectionData)):
        return msg.data.value.name
    elif isinstance(msg.data, (iec101.StepPositionData,
                               iec101.ProtectionStartData,
                               iec101.ProtectionCommandData,
                               iec101.StatusData)):
        return msg.data.value._asdict()
    elif isinstance(msg.data, iec101.BitstringData):
        return list(msg.data.value.value)
    elif isinstance(msg.data, (iec101.NormalizedData,
                               iec101.ScaledData,
                               iec101.FloatingData,
                               iec101.BinaryCounterData)):
        return msg.data.value.value
    raise Exception('unsupported data message')


def _data_type_from_msg(msg):
    if isinstance(msg.data, iec101.SingleData):
        return 'single'
    elif isinstance(msg.data, iec101.DoubleData):
        return 'double'
    elif isinstance(msg.data, iec101.StepPositionData):
        return 'step_position'
    elif isinstance(msg.data, iec101.BitstringData):
        return 'bitstring'
    elif isinstance(msg.data, iec101.NormalizedData):
        return 'normalized'
    elif isinstance(msg.data, iec101.ScaledData):
        return 'scaled'
    elif isinstance(msg.data, iec101.FloatingData):
        return 'floating'
    elif isinstance(msg.data, iec101.BinaryCounterData):
        return 'binary_counter'
    elif isinstance(msg.data, iec101.ProtectionData):
        return 'protection'
    elif isinstance(msg.data, iec101.ProtectionStartData):
        return 'protection_start'
    elif isinstance(msg.data, iec101.ProtectionCommandData):
        return 'protection_command'
    elif isinstance(msg.data, iec101.StatusData):
        return 'status'
    raise Exception('unsupported data message')


def _command_payload_from_msg(msg):
    cause = msg.cause.name if isinstance(msg.cause, enum.Enum) else msg.cause
    payload = {'value': _command_value_from_msg(msg),
               'cause': cause,
               'success': not msg.is_negative_confirm}
    if not isinstance(msg.command, iec101.BitstringCommand):
        payload['select'] = msg.command.select
    if isinstance(msg.command, (iec101.SingleCommand,
                                iec101.DoubleCommand,
                                iec101.RegulatingCommand)):
        payload['qualifier'] = msg.command.qualifier
    return payload


def _command_value_from_msg(msg):
    if isinstance(msg.command, (iec101.SingleCommand,
                                iec101.DoubleCommand,
                                iec101.RegulatingCommand)):
        return msg.command.value.name
    elif isinstance(msg.command, (iec101.NormalizedCommand,
                                  iec101.ScaledCommand,
                                  iec101.FloatingCommand)):
        return msg.command.value.value
    elif isinstance(msg.command, iec101.BitstringCommand):
        return list(msg.command.value.value)
    raise Exception('unsupported command message')


def _command_type_from_msg(msg):
    if isinstance(msg.command, iec101.SingleCommand):
        return 'single'
    elif isinstance(msg.command, iec101.DoubleCommand):
        return 'double'
    elif isinstance(msg.command, iec101.RegulatingCommand):
        return 'regulating'
    elif isinstance(msg.command, iec101.NormalizedCommand):
        return 'normalized'
    elif isinstance(msg.command, iec101.ScaledCommand):
        return 'scaled'
    elif isinstance(msg.command, iec101.FloatingCommand):
        return 'floating'
    elif isinstance(msg.command, iec101.BitstringCommand):
        return 'bitstring'
    raise Exception('unsupported command message')


def _command_from_event(event, command_type):
    if command_type == 'single':
        return iec101.SingleCommand(
            value=iec101.SingleValue[event.payload.data['value']],
            select=event.payload.data['select'],
            qualifier=event.payload.data['qualifier'])
    elif command_type == 'double':
        return iec101.DoubleCommand(
            value=iec101.DoubleValue[event.payload.data['value']],
            select=event.payload.data['select'],
            qualifier=event.payload.data['qualifier'])
    elif command_type == 'regulating':
        return iec101.RegulatingCommand(
            value=iec101.RegulatingValue[event.payload.data['value']],
            select=event.payload.data['select'],
            qualifier=event.payload.data['qualifier'])
    elif command_type == 'normalized':
        return iec101.NormalizedCommand(
            value=iec101.NormalizedValue(
                value=event.payload.data['value']),
            select=event.payload.data['select'])
    elif command_type == 'scaled':
        return iec101.ScaledCommand(
            value=iec101.ScaledValue(
                value=event.payload.data['value']),
            select=event.payload.data['select'])
    elif command_type == 'floating':
        return iec101.FloatingCommand(
            value=iec101.FloatingValue(
                value=event.payload.data['value']),
            select=event.payload.data['select'])
    elif command_type == 'bitstring':
        return iec101.BitstringCommand(
            value=iec101.BitstringValue(
                value=bytes(event.payload.data['value'])))
    raise Exception('command type not supported')
