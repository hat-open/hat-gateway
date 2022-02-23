"""IEC 60870-5-101 master device"""

import asyncio
import contextlib
import datetime
import logging

from hat import aio
from hat import json
from hat import util
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
    device._prefix_len = len(event_type_prefix)
    device._event_client = event_client
    device._master = None
    device._conns = {}
    device._remote_enabled = {i['address']: False
                              for i in conf['remote_devices']}
    device._remote_groups = {}
    device._send_msg_queue = aio.Queue()

    remote_enable_evts = [
        (*event_type_prefix, 'system', 'remote_device', str(i['address']),
            'enable') for i in conf['remote_devices']]
    remote_enable_events = await event_client.query(
        hat.event.common.QueryData(
            event_types=remote_enable_evts, unique_type=True))
    for event in remote_enable_events:
        device._process_enable(event)

    device._async_group = aio.Group()
    device._async_group.spawn(device._create_link_master_loop)
    device._async_group.spawn(device._event_loop)
    return device


class Iec101MasterDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _create_link_master_loop(self):
        try:
            while True:
                self._event_client.register([_status_event('CONNECTING')])
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
                    mlog.error('link master (endpoint) failed to create: %s',
                               e, exc_info=e)
                    self._event_client.register(
                        [_status_event('DISCONNECTED')])
                    await asyncio.sleep(self._conf['reconnect_delay'])
                    continue
                self._event_client.register([_status_event('CONNECTED')])
                for address, enabled in self._remote_enabled.items():
                    if enabled:
                        self._enable_remote(address)
                await self._master.wait_closed()
                self._event_client.register([_status_event('DISCONNECTED')])
                self._master = None
        finally:
            mlog.debug('closing link master loop')
            self.close()
            with contextlib.suppress(ConnectionError):
                self._event_client.register(_status_event('DISCONNECTED'))
            self._conns = {}
            if self._master:
                await aio.uncancellable(self._master.async_close())

    async def _connection_loop(self, group, address):
        remote_conf = util.first(self._conf['remote_devices'],
                                 lambda i: i['address'] == address)
        try:
            while True:
                self._event_client.register([
                    _rmt_status_event(address, 'CONNECTING')])
                try:
                    master_conn = await self._master.connect(addr=address)
                except Exception as e:
                    mlog.error('connection error to address %s: %s',
                               address, e, exc_info=e)
                    self._event_client.register([
                        _rmt_status_event(address, 'DISCONNECTED')])
                    await asyncio.sleep(remote_conf['reconnect_delay'])
                    continue
                self._event_client.register([
                    _rmt_status_event(address, 'CONNECTED')])
                conn = iec101.Connection(
                    conn=master_conn,
                    cause_size=iec101.CauseSize[self._conf['cause_size']],
                    asdu_address_size=iec101.AsduAddressSize[
                        self._conf['asdu_address_size']],
                    io_address_size=iec101.IoAddressSize[
                        self._conf['io_address_size']])
                self._conns[address] = conn
                group.spawn(self._receive_loop, conn, address)
                await conn.wait_closed()
                self._event_client.register([
                    _rmt_status_event(address, 'DISCONNECTED')])
                self._conns.pop(address)
        finally:
            group.close()
            with contextlib.suppress(ConnectionError):
                self._event_client.register([
                    _rmt_status_event(address, 'DISCONNECTED')])
            if address in self._conns:
                conn = self._conns.pop(address)
                await aio.uncancellable(conn.async_close())

    async def _event_loop(self):
        try:
            while True:
                events = await self._event_client.receive()
                for event in events:
                    try:
                        self._process_event(event)
                    except Exception as e:
                        mlog.warning('event %s processing failed %s',
                                     event, e, exc_info=e)
        except ConnectionError:
            mlog.debug('event client connection closed')
        finally:
            self.close()

    async def _receive_loop(self, conn, address):
        try:
            while True:
                msgs = await conn.receive()
                events = []
                for msg in msgs:
                    if msg.is_test:
                        continue
                    try:
                        event = _msg_to_event(
                            msg, self._event_type_prefix, address)
                    except Exception as e:
                        mlog.warning(
                            'message %s error: %s', msg, e, exc_info=e)
                        continue
                    events.append(event)
                if events:
                    self._event_client.register(events)
        except ConnectionError:
            mlog.warning('iec101 connection closed')
        finally:
            conn.close()

    def _send_msg_to_address(self, msg, address, event):
        conn = self._conns.get(address)
        if not conn or not conn.is_open:
            mlog.warning(
                'event %s is ignored: connection to remote device %s closed',
                event, address)
            return
        group = self._remote_groups.get(address)
        if group and group.is_open:
            group.spawn(conn.send, [msg])

    def _process_event(self, event):
        if event.event_type[self._prefix_len + 1] != 'remote_device':
            raise Exception('unexpected event type')
        address = int(event.event_type[self._prefix_len + 2])
        etype_suffix = event.event_type[self._prefix_len + 2:]
        if etype_suffix == ('enable', ):
            self._process_enable(event, address)
        elif etype_suffix[0] == 'command':
            command_type = etype_suffix[1]
            asdu = int(etype_suffix[2])
            io = int(etype_suffix[3])
            self._process_command(event, address, command_type, asdu, io)
        elif etype_suffix[0] == 'interrogation':
            asdu = int(etype_suffix[1])
            self._process_interrogation(event, address, asdu)
        elif etype_suffix[0] == 'counter_interrogation':
            asdu = int(etype_suffix[1])
            self._process_counter_interrogation(event, address, asdu)
        elif etype_suffix[0] == 'time_sync':
            self._process_time_sync(event, address)
        else:
            raise Exception('unexpected event type')

    def _process_enable(self, event, address):
        enable = event.payload.data
        if address not in self._remote_enabled:
            mlog.warning('received enable for unexpected remote device')
            return
        self._remote_enabled[address] = enable
        if not enable:
            self._disable_remote(address)
        elif not self._master:
            mlog.warning('enabling remote %s ignored: endpoint not open')
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
        remote_group = self._remote_groups.pop(address, None)
        if remote_group:
            remote_group.close()

    def _process_command(self, event, address, command_type, asdu, io):
        command = _command_from_event(event, command_type)
        if not command:
            return
        msg = iec101.CommandMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu,
            io_address=io,
            command=command,
            is_negative_confirm=False,
            cause=iec101.CommandReqCause[event.payload.data['cause']])
        self._send_msg_to_address(msg, address, event)

    def _process_interrogation(self, event, address, asdu):
        msg = iec101.InterrogationMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu,
            request=event.payload.data['request'],
            cause=iec101.CommandReqCause.ACTIVATION)
        self._send_msg_to_address(msg, address, event)

    def _process_counter_interrogation(self, event, address, asdu):
        msg = iec101.CounterInterrogationMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu,
            request=event.payload.data['request'],
            freeze=iec101.FreezeCode[event.payload.data['freeze']],
            cause=iec101.CommandReqCause.ACTIVATION)
        self._send_msg_to_address(msg, address, event)

    def _process_time_sync(self, event, address):
        time_now = datetime.datetime.now(datetime.timezone.utc)
        time_iec101 = iec101.time_from_datetime(time_now)
        msg = iec101.ClockSyncMsg(
            is_test=False,
            originator_address=0,
            asdu_address={
                'ONE': 0xFF,
                'TWO': 0xFFFF}[self._conf['asdu_address_size']],
            time=time_iec101,
            cause=iec101.ActivationReqCause.ACTIVATION)
        self._send_msg_to_address(msg, address, event)


def _status_event(self, status):
    return hat.event.common.RegisterEvent(
        event_type=(*self._event_type_prefix, 'gateway', 'status'),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=status))


def _rmt_status_event(self, address, status):
    return hat.event.common.RegisterEvent(
        event_type=(*self._event_type_prefix,
                    'gateway', 'remote_device', str(address), 'status'),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=status))


def _msg_to_event(msg, event_type_prefix, address):
    if isinstance(msg, iec101.DataMsg):
        if not isinstance(msg.cause, iec101.DataResCause):
            raise Exception('data with unexpected cause')
        return _data_msg_to_event(msg, event_type_prefix, address)
    elif isinstance(msg, iec101.CommandMsg):
        if not isinstance(msg.cause, iec101.CommandResCause):
            raise Exception('command with unexpected cause')
        return _command_msg_to_event(msg, event_type_prefix, address)
    elif isinstance(msg, iec101.InterrogationMsg):
        if not isinstance(msg.cause, iec101.CommandResCause):
            raise Exception('interrogation with unexpected cause')
        return _interrogation_msg_to_event(msg, event_type_prefix, address)
    elif isinstance(msg, iec101.CounterInterrogationMsg):
        if not isinstance(msg.cause, iec101.CommandResCause):
            raise Exception('counter interrogation with unexpected cause')
        return _counter_interrogation_msg_to_event(
            msg, event_type_prefix, address)
    raise Exception('unexpected message')


def _data_msg_to_event(msg, event_type_prefix, address):
    source_timestamp = hat.event.common.timestamp_from_datetime(
        iec101.timestamp_to_datetime(msg.time)) if msg.time else None
    data_type, payload = _data_type_payload_from_msg(msg)
    if not payload:
        return
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'remote_device',
                    str(address), 'data',
                    data_type, str(msg.asdu_address), str(msg.io_address)),
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))


def _command_msg_to_event(msg, event_type_prefix, address):
    command_type, payload = _command_type_payload_from_msg(msg)
    if not payload:
        return
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'remote_device',
                    str(address), 'command',
                    command_type, str(msg.asdu_address), str(msg.io_address)),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))


def _interrogation_msg_to_event(msg, event_type_prefix, address):
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'remote_device',
                    str(address), 'interrogation', str(msg.asdu_address)),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=dict(request=msg.request)))


def _counter_interrogation_msg_to_event(msg, event_type_prefix, address):
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'remote_device',
                    str(address), 'counter_interrogation',
                    str(msg.asdu_address)),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=dict(request=msg.request,
                      freeze=msg.freeze.name)))


def _data_type_payload_from_msg(msg):
    cause = ('INTERROGATED' if msg.cause.name.startswith('INTERROGATED')
             else msg.cause.name)
    quality = msg.data.quality._asdict() if msg.data.quality else None
    if isinstance(msg.data, iec101.SingleData):
        return 'single', dict(
            value=msg.data.value.name,
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.DoubleData):
        return 'double', dict(
            value=msg.data.value.name,
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.StepPositionData):
        return 'step_position', dict(
            value=msg.data.value._asdict(),
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.BitstringData):
        return 'bitstring', dict(
            value=list(msg.data.value.value),
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.NormalizedData):
        return 'normalized', dict(
            value=msg.data.value.value,
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.ScaledData):
        return 'scaled', dict(
            value=msg.data.value.value,
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.FloatingData):
        return 'floating', dict(
            value=msg.data.value.value,
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.BinaryCounterData):
        return 'binary_counter', dict(
            value=msg.data.value.value,
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.ProtectionData):
        return 'protection', dict(
            value=msg.data.value.name,
            quality=quality,
            cause=cause,
            elapsed_time=msg.data.elapsed_time)
    elif isinstance(msg.data, iec101.ProtectionStartData):
        return 'protection_start', dict(
            value=msg.data.value._asdict(),
            quality=quality,
            cause=cause,
            duration_time=msg.data.duration_time)
    elif isinstance(msg.data, iec101.ProtectionCommandData):
        return 'protection_command', dict(
            value=msg.data.value._asdict(),
            quality=quality,
            cause=cause)
    elif isinstance(msg.data, iec101.StatusData):
        return 'status', dict(
            value=msg.data.value._asdict(),
            quality=quality,
            cause=cause)

    mlog.warning('unsupported data message %s', msg)
    return None, None


def _command_type_payload_from_msg(msg):
    cause = msg.cause.name
    success = not msg.is_negative_confirm
    if isinstance(msg.command, iec101.SingleCommand):
        return 'single', dict(
                value=msg.command.value.name,
                select=msg.command.select,
                qualifier=msg.command.qualifier,
                cause=cause,
                success=success)
    elif isinstance(msg.command, iec101.DoubleCommand):
        return 'double', dict(
                value=msg.command.value.name,
                select=msg.command.select,
                qualifier=msg.command.qualifier,
                cause=cause,
                success=success)
    elif isinstance(msg.command, iec101.RegulatingCommand):
        return 'regulating', dict(
                value=msg.command.value.name,
                select=msg.command.select,
                qualifier=msg.command.qualifier,
                cause=cause,
                success=success)
    elif isinstance(msg.command, iec101.NormalizedCommand):
        return 'normalized', dict(
                value=msg.command.value.value,
                select=msg.command.select,
                cause=cause,
                success=success)
    elif isinstance(msg.command, iec101.ScaledCommand):
        return 'scaled', dict(
                value=msg.command.value.value,
                select=msg.command.select,
                cause=cause,
                success=success)
    elif isinstance(msg.command, iec101.FloatingCommand):
        return 'floating', dict(
                value=msg.command.value.value,
                select=msg.command.select,
                cause=cause,
                success=success)
    elif isinstance(msg.command, iec101.BitstringCommand):
        return 'bitstring', dict(
                value=msg.command.value.value,
                cause=cause,
                success=success)

    mlog.warning('unsupported command message %s', msg)
    return None, None


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

    mlog.warning('command type %s not supported', command_type)
