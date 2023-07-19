"""IEC 60870-5-104 slave device"""

import collections
import contextlib
import functools
import itertools
import logging
import typing

from hat import aio
from hat import json
from hat.drivers import iec101
from hat.drivers import serial
from hat.drivers.iec60870 import link
import hat.event.common

from hat.gateway.devices.iec101 import common


mlog: logging.Logger = logging.getLogger(__name__)

device_type: str = 'iec101_slave'

json_schema_id: str = "hat-gateway://iec101.yaml#/definitions/slave"

json_schema_repo: json.SchemaRepository = common.json_schema_repo


async def create(conf: common.DeviceConf,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec101SlaveDevice':
    device = Iec101SlaveDevice()
    device._conf = conf
    device._event_client = event_client
    device._event_type_prefix = event_type_prefix
    device._next_conn_ids = itertools.count(1)
    device._conns = {}
    device._buffers = {}
    device._data_msgs = {}
    device._data_buffers = {}

    init_buffers(buffers_conf=conf['buffers'],
                 buffers=device._buffers)

    await init_data(data_conf=conf['data'],
                    data_msgs=device._data_msgs,
                    data_buffers=device._data_buffers,
                    buffers=device._buffers,
                    event_client=event_client,
                    event_type_prefix=event_type_prefix)

    device._slave = await link.unbalanced.create_slave(
        port=conf['port'],
        addrs=conf['addresses'],
        connection_cb=device._on_connection,
        baudrate=conf['baudrate'],
        bytesize=serial.ByteSize[conf['bytesize']],
        parity=serial.Parity[conf['parity']],
        stopbits=serial.StopBits[conf['stopbits']],
        xonxoff=conf['flow_control']['xonxoff'],
        rtscts=conf['flow_control']['rtscts'],
        dsrdtr=conf['flow_control']['dsrdtr'],
        silent_interval=conf['silent_interval'],
        address_size=link.AddressSize[conf['device_address_size']],
        keep_alive_timeout=conf['keep_alive_timeout'])

    try:
        device._register_connections()
        device.async_group.spawn(device._event_loop)

    except BaseException:
        await aio.uncancellable(device.async_close())
        raise

    return device


class Buffer:

    def __init__(self, size: int):
        self._size = size
        self._data = collections.OrderedDict()

    def add(self,
            event_id: hat.event.common.EventId,
            data_msg: iec101.DataMsg):
        self._data[event_id] = data_msg
        while len(self._data) > self._size:
            self._data.popitem(last=False)

    def remove(self, event_id: hat.event.common.EventId):
        self._data.pop(event_id, None)

    def get_event_id_data_msgs(self) -> typing.Iterable[tuple[hat.event.common.EventId,  # NOQA
                                                              iec101.DataMsg]]:
        return self._data.items()


def init_buffers(buffers_conf: json.Data,
                 buffers: dict[str, Buffer]):
    for buffer_conf in buffers_conf:
        buffers[buffer_conf['name']] = Buffer(buffer_conf['size'])


async def init_data(data_conf: json.Data,
                    data_msgs: dict[common.DataKey, iec101.DataMsg],
                    data_buffers: dict[common.DataKey, Buffer],
                    buffers: dict[str, Buffer],
                    event_client: common.DeviceEventClient,
                    event_type_prefix: common.EventTypePrefix):
    for data in data_conf:
        data_key = common.DataKey(data_type=common.DataType[data['data_type']],
                                  asdu_address=data['asdu_address'],
                                  io_address=data['io_address'])
        data_msgs[data_key] = None
        if data['buffer']:
            data_buffers[data_key] = buffers[data['buffer']]

    events = await event_client.query(hat.event.common.QueryData(
        event_types=[(*event_type_prefix, 'system', 'data', '*')],
        unique_type=True))
    for event in events:
        try:
            data_type_str, asdu_address_str, io_address_str = \
                event.event_type[len(event_type_prefix)+2:]
            data_key = common.DataKey(data_type=common.DataType(data_type_str),
                                      asdu_address=int(asdu_address_str),
                                      io_address=int(io_address_str))
            if data_key not in data_msgs:
                raise Exception(f'data {data_key} not configured')

            data_msgs[data_key] = data_msg_from_event(data_key, event)

        except Exception as e:
            mlog.debug('skipping initial data: %s', e, exc_info=e)


class Iec101SlaveDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._slave.async_group

    def _on_connection(self, conn):
        self.async_group.spawn(self._connection_loop, conn)

    async def _connection_loop(self, conn):
        conn_id = next(self._next_conn_ids)

        try:
            conn = iec101.SlaveConnection(
                conn=conn,
                cause_size=iec101.CauseSize[self._conf['cause_size']],
                asdu_address_size=iec101.AsduAddressSize[self._conf['asdu_address_size']],  # NOQA
                io_address_size=iec101.IoAddressSize[self._conf['io_address_size']])  # NOQA

            self._conns[conn_id] = conn
            self._register_connections()

            with contextlib.suppress(Exception):
                for buffer in self._buffers.values():
                    for event_id, data_msg in buffer.get_event_id_data_msgs():
                        self._send_data_msg(conn, buffer, event_id, data_msg)

            while True:
                msgs = await conn.receive()

                for msg in msgs:
                    try:
                        mlog.debug('received message: %s', msg)
                        self._process_msg(conn_id, conn, msg)

                    except Exception as e:
                        mlog.warning('error processing message: %s',
                                     e, exc_info=e)

        except ConnectionError:
            mlog.debug('connection close')

        except Exception as e:
            mlog.warning('connection error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection')
            conn.close()

            with contextlib.suppress(Exception):
                self._conns.pop(conn_id)
                self._register_connections()

    async def _event_loop(self):
        try:
            while True:
                events = await self._event_client.receive()

                for event in events:
                    try:
                        mlog.debug('received event: %s', event)
                        self._process_event(event)

                    except Exception as e:
                        mlog.warning('error processing event: %s',
                                     e, exc_info=e)

        except ConnectionError:
            mlog.debug('event client closed')

        except Exception as e:
            mlog.error('event loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing event loop')
            self.close()

    def _register_connections(self):
        payload = [{'connection_id': conn_id,
                    'address': conn.address}
                   for conn_id, conn in self._conns.items()]

        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'connections'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=payload))

        self._event_client.register([event])

    def _process_event(self, event):
        suffix = event.event_type[len(self._event_type_prefix):]

        if suffix[:2] == ('system', 'data'):
            data_type_str, asdu_address_str, io_address_str = suffix[2:]
            data_key = common.DataKey(data_type=common.DataType(data_type_str),
                                      asdu_address=int(asdu_address_str),
                                      io_address=int(io_address_str))

            self._process_data_event(data_key, event)

        elif suffix[:2] == ('system', 'command'):
            cmd_type_str, asdu_address_str, io_address_str = suffix[2:]
            cmd_key = common.CommandKey(
                cmd_type=common.CommandType(cmd_type_str),
                asdu_address=int(asdu_address_str),
                io_address=int(io_address_str))

            self._process_command_event(cmd_key, event)

        else:
            raise Exception('unsupported event type')

    def _process_data_event(self, data_key, event):
        if data_key not in self._data_msgs:
            raise Exception('data not configured')

        data_msg = data_msg_from_event(data_key, event)
        self._data_msgs[data_key] = data_msg

        buffer = self._data_buffers.get(data_key)
        if buffer:
            buffer.add(event.event_id, data_msg)

        for conn in self._conns.values():
            self._send_data_msg(conn, buffer, event.event_id, data_msg)

    def _process_command_event(self, cmd_key, event):
        cmd_msg = cmd_msg_from_event(cmd_key, event)
        conn_id = event.payload.data['connection_id']
        conn = self._conns[conn_id]
        conn.send([cmd_msg])

    def _process_msg(self, conn_id, conn, msg):
        if isinstance(msg, iec101.CommandMsg):
            self._process_command_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.InterrogationMsg):
            self._process_interrogation_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.CounterInterrogationMsg):
            self._process_counter_interrogation_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.ReadMsg):
            self._process_read_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.ClockSyncMsg):
            self._process_clock_sync_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.TestMsg):
            self._process_test_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.ResetMsg):
            self._process_reset_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.ParameterMsg):
            self._process_parameter_msg(conn_id, conn, msg)

        elif isinstance(msg, iec101.ParameterActivationMsg):
            self._process_parameter_activation_msg(conn_id, conn, msg)

        else:
            raise Exception('unsupported message')

    def _process_command_msg(self, conn_id, conn, msg):
        if isinstance(msg.cause, iec101.CommandReqCause):
            event = cmd_msg_to_event(self._event_type_prefix, conn_id, msg)
            self._event_client.register([event])

        else:
            res = msg._replace(cause=iec101.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            conn.send([res])

    def _process_interrogation_msg(self, conn_id, conn, msg):
        if msg.cause == iec101.CommandReqCause.ACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=False)
            conn.send([res])

            data_msgs = [
                data_msg._replace(
                    is_test=msg.is_test,
                    cause=iec101.DataResCause.INTERROGATED_STATION)
                for data_msg in self._data_msgs.values()
                if (data_msg and
                    (msg.asdu_address == 0xFFFF or
                     msg.asdu_address == data_msg.asdu_address) and
                    not isinstance(data_msg.data, iec101.BinaryCounterData))]
            conn.send(data_msgs)

            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_TERMINATION,
                is_negative_confirm=False)
            conn.send([res])

        elif msg.cause == iec101.CommandReqCause.DEACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.DEACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            conn.send([res])

        else:
            res = msg._replace(cause=iec101.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            conn.send([res])

    def _process_counter_interrogation_msg(self, conn_id, conn, msg):
        if msg.cause == iec101.CommandReqCause.ACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=False)
            conn.send([res])

            data_msgs = [
                data_msg._replace(
                    is_test=msg.is_test,
                    cause=iec101.DataResCause.INTERROGATED_COUNTER)
                for data_msg in self._data_msgs.values()
                if (data_msg and
                    (msg.asdu_address == 0xFFFF or
                     msg.asdu_address == data_msg.asdu_address) and
                    isinstance(data_msg.data, iec101.BinaryCounterData))]
            conn.send(data_msgs)

            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_TERMINATION,
                is_negative_confirm=False)
            conn.send([res])

        elif msg.cause == iec101.CommandReqCause.DEACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.DEACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            conn.send([res])

        else:
            res = msg._replace(cause=iec101.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            conn.send([res])

    def _process_read_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec101.ReadResCause.UNKNOWN_TYPE)
        conn.send([res])

    def _process_clock_sync_msg(self, conn_id, conn, msg):
        if isinstance(msg.cause, iec101.ClockSyncReqCause):
            res = msg._replace(
                cause=iec101.ClockSyncResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            conn.send([res])

        else:
            res = msg._replace(cause=iec101.ClockSyncResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            conn.send([res])

    def _process_test_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec101.ActivationResCause.UNKNOWN_TYPE)
        conn.send([res])

    def _process_reset_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec101.ActivationResCause.UNKNOWN_TYPE)
        conn.send([res])

    def _process_parameter_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec101.ParameterResCause.UNKNOWN_TYPE)
        conn.send([res])

    def _process_parameter_activation_msg(self, conn_id, conn, msg):
        res = msg._replace(
            cause=iec101.ParameterActivationResCause.UNKNOWN_TYPE)
        conn.send([res])

    def _send_data_msg(self, conn, buffer, event_id, data_msg):
        sent_cb = (functools.partial(buffer.remove, event_id)
                   if buffer else None)
        conn.send([data_msg], sent_cb=sent_cb)


def cmd_msg_to_event(event_type_prefix: hat.event.common.EventType,
                     conn_id: int,
                     msg: iec101.CommandMsg
                     ) -> hat.event.common.RegisterEvent:
    command_type = common.get_command_type(msg.command)
    cause = common.cause_to_json(iec101.CommandReqCause, msg.cause)
    command = common.command_to_json(msg.command)
    event_type = (*event_type_prefix, 'gateway', 'command', command_type.value,
                  str(msg.asdu_address), str(msg.io_address))

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'connection_id': conn_id,
                  'is_test': msg.is_test,
                  'cause': cause,
                  'command': command}))


def data_msg_from_event(data_key: common.DataKey,
                        event: hat.event.common.Event
                        ) -> iec101.DataMsg:
    time = common.time_from_source_timestamp(event.source_timestamp)
    if event.payload.data['cause'] == 'INTERROGATED':
        cause = (iec101.DataResCause.INTERROGATED_STATION
                 if data_key.data_type != common.DataType.BINARY_COUNTER
                 else iec101.DataResCause.INTERROGATED_COUNTER)
    else:
        cause = common.cause_from_json(iec101.DataResCause,
                                       event.payload.data['cause'])
    data = common.data_from_json(data_key.data_type,
                                 event.payload.data['data'])

    return iec101.DataMsg(is_test=event.payload.data['is_test'],
                          originator_address=0,
                          asdu_address=data_key.asdu_address,
                          io_address=data_key.io_address,
                          data=data,
                          time=time,
                          cause=cause)


def cmd_msg_from_event(cmd_key: common.CommandKey,
                       event: hat.event.common.Event
                       ) -> iec101.CommandMsg:
    cause = common.cause_from_json(iec101.CommandResCause,
                                   event.payload.data['cause'])
    command = common.command_from_json(cmd_key.cmd_type,
                                       event.payload.data['command'])
    is_negative_confirm = event.payload.data['is_negative_confirm']

    return iec101.CommandMsg(is_test=event.payload.data['is_test'],
                             originator_address=0,
                             asdu_address=cmd_key.asdu_address,
                             io_address=cmd_key.io_address,
                             command=command,
                             is_negative_confirm=is_negative_confirm,
                             cause=cause)
