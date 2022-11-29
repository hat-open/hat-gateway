"""IEC 60870-5-103 master device"""

import asyncio
import contextlib
import datetime
import enum
import functools
import logging

from hat import aio
from hat import json
from hat.drivers import iec103
from hat.drivers import serial
from hat.drivers.iec60870 import link
from hat.gateway import common
import hat.event.common


mlog: logging.Logger = logging.getLogger(__name__)

device_type: str = 'iec103_master'

json_schema_id: str = "hat-gateway://iec103.yaml#/definitions/master"

json_schema_repo: json.SchemaRepository = common.json_schema_repo

command_timeout: float = 100

interrogate_timeout: float = 100


async def create(conf: common.DeviceConf,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec103MasterDevice':
    device = Iec103MasterDevice()

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

    remote_enable_events = await event_client.query(
        hat.event.common.QueryData(
            event_types=[
                (*event_type_prefix, 'system', 'remote_device',
                 str(i['address']), 'enable')
                for i in conf['remote_devices']],
            unique_type=True))
    for event in remote_enable_events:
        try:
            device._process_enable(event)
        except Exception as e:
            mlog.warning('error processing enable event: %s', e, exc_info=e)

    device._async_group = aio.Group()
    device._async_group.spawn(device._create_link_master_loop)
    device._async_group.spawn(device._event_loop)
    return device


class Iec103MasterDevice(common.Device):

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
                            address_size=link.AddressSize.ONE)
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

    async def _connection_loop(self, group, address):
        remote_conf = self._remote_confs[address]
        try:
            while True:
                self._register_rmt_status(address, 'CONNECTING')
                try:
                    conn_link = await self._master.connect(
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
                conn = iec103.MasterConnection(
                    conn=conn_link,
                    data_cb=functools.partial(self._on_data, address),
                    generic_data_cb=None)
                self._conns[address] = conn
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

    async def _time_sync_loop(self, conn, delay):
        try:
            while True:
                await conn.time_sync()
                mlog.debug('time sync')
                await asyncio.sleep(delay)
        except ConnectionError:
            mlog.debug('connection closed')
        finally:
            conn.close()

    def _on_data(self, address, data):
        events = []
        try:
            for event in _events_from_data(data, address,
                                           self._event_type_prefix):
                events.append(event)
        except Exception as e:
            mlog.warning('data %s ignored due to: %s', data, e, exc_info=e)
        if events:
            self._event_client.register(events)

    def _process_event(self, event):
        prefix_len = len(self._event_type_prefix)
        if event.event_type[prefix_len + 1] != 'remote_device':
            raise Exception('unexpected event type')
        address = self._address_from_event(event)
        etype_suffix = event.event_type[prefix_len + 3:]
        if etype_suffix[0] == 'enable':
            self._process_enable(event)
        elif etype_suffix[0] == 'command':
            asdu = int(etype_suffix[1])
            io = iec103.IoAddress(
                function_type=int(etype_suffix[2]),
                information_number=int(etype_suffix[3]))
            self._process_command(event, address, asdu, io)
        elif etype_suffix[0] == 'interrogation':
            asdu = int(etype_suffix[1])
            self._process_interrogation(event, address, asdu)
        else:
            raise Exception('unexpected event type')

    def _process_enable(self, event):
        address = self._address_from_event(event)
        enable = event.payload.data
        if address not in self._remote_enabled:
            raise Exception('invalid remote device address')
        if not isinstance(enable, bool):
            raise Exception('invalid enable event payload')
        self._remote_enabled[address] = enable
        if not self._master:
            return
        if enable:
            self._enable_remote(address)
        else:
            self._disable_remote(address)

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

    def _process_command(self, event, address, asdu, io):
        conn = self._conns.get(address)
        if not conn or not conn.is_open:
            raise Exception('connection closed')
        value = iec103.DoubleValue[event.payload.data['value']]
        session_id = event.payload.data['session_id']
        self._remote_groups[address].spawn(
            self._cmd_req_res, conn, address, asdu, io, value, session_id)

    async def _cmd_req_res(self, conn, address, asdu, io, value, session_id):
        try:
            success = await asyncio.wait_for(
                conn.send_command(asdu, io, value), timeout=command_timeout)
        except ConnectionError:
            mlog.warning('command %s %s %s to %s failed: connection closed',
                         asdu, io, value, address)
            return
        except asyncio.TimeoutError:
            mlog.warning(
                'command %s %s %s to %s timeout', asdu, io, value, address)
            return
        event = _create_event(
            event_type=(*self._event_type_prefix, 'gateway', 'remote_device',
                        str(address), 'command', str(asdu),
                        str(io.function_type), str(io.information_number)),
            payload={'success': success,
                     'session_id': session_id})
        self._event_client.register([event])

    def _process_interrogation(self, event, address, asdu):
        conn = self._conns.get(address)
        if not conn or not conn.is_open:
            mlog.warning("event %s ignored due to connection closed", event)
            return
        self._remote_groups[address].spawn(
            self._interrogate_req_res, conn, address, asdu)

    async def _interrogate_req_res(self, conn, address, asdu):
        try:
            await asyncio.wait_for(
                conn.interrogate(asdu), timeout=interrogate_timeout)
        except ConnectionError:
            mlog.warning('interrogation on %s to %s failed: connection closed',
                         asdu, address)
            return
        except asyncio.TimeoutError:
            mlog.warning('interrogation on %s to %s timeout', asdu, address)
            return
        event = _create_event(
            event_type=(*self._event_type_prefix, 'gateway', 'remote_device',
                        str(address), 'interrogation', str(asdu)),
            payload=None)
        self._event_client.register([event])

    def _register_status(self, status):
        event = _create_event(
            event_type=(*self._event_type_prefix, 'gateway', 'status'),
            payload=status)
        self._event_client.register([event])

    def _register_rmt_status(self, address, status):
        event = _create_event(
            event_type=(*self._event_type_prefix,
                        'gateway', 'remote_device', str(address), 'status'),
            payload=status)
        self._event_client.register([event])

    def _address_from_event(self, event):
        return int(event.event_type[len(self._event_type_prefix) + 2])


def _events_from_data(data, address, event_type_prefix):
    cause = (data.cause.name if isinstance(data.cause, enum.Enum)
             else data.cause)
    if isinstance(data.value, (iec103.DoubleWithTimeValue,
                               iec103.DoubleWithRelativeTimeValue)):
        data_type = 'double'
        payload = {'cause': cause,
                   'value': data.value.value.name}
        source_ts = _time_iec103_to_source_ts(data.value.time)
        event_type = _data_event_type(
            data, address, data_type, event_type_prefix)
        yield _create_event(event_type, payload, source_ts)
    elif isinstance(data.value, iec103.MeasurandValues):
        for meas_type, meas_value in data.value.values.items():
            payload = {'cause': cause,
                       'value': meas_value._asdict()}
            data_type = meas_type.name.lower()
            event_type = _data_event_type(
                data, address, data_type, event_type_prefix)
            yield _create_event(event_type, payload)
    else:
        raise Exception('unsupported data value')


def _data_event_type(data, address, data_type, event_type_prefix):
    return (
        *event_type_prefix, 'gateway', 'remote_device', str(address),
        'data', data_type,
        str(data.asdu_address),
        str(data.io_address.function_type),
        str(data.io_address.information_number))


def _time_iec103_to_source_ts(time_four):
    t_now = datetime.datetime.now(datetime.timezone.utc)
    candidates_now = [t_now - datetime.timedelta(hours=12),
                      t_now,
                      t_now + datetime.timedelta(hours=12)]
    candidates_103 = [_upgrade_time_four_to_seven(
                        time_four, iec103.time_from_datetime(t))
                      for t in candidates_now]
    candidates_dt = [iec103.time_to_datetime(t)
                     for t in candidates_103 if t]
    if not candidates_dt:
        return
    res = min(candidates_dt, key=lambda i: abs(t_now - i))
    return hat.event.common.timestamp_from_datetime(res)


def _upgrade_time_four_to_seven(time_four, time_seven):
    if time_four.summer_time != time_seven.summer_time:
        return
    return time_four._replace(
        day_of_week=time_seven.day_of_week,
        day_of_month=time_seven.day_of_month,
        months=time_seven.months,
        years=time_seven.years,
        size=iec103.TimeSize.SEVEN)


def _create_event(event_type, payload, source_timestamp=None):
    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))
