"""IEC 60870-5-104 slave device"""

from collections.abc import Collection, Iterable
import asyncio
import collections
import contextlib
import functools
import itertools
import logging

from hat import aio
from hat import json
from hat.drivers import iec101
from hat.drivers import serial
from hat.drivers.iec60870 import link
import hat.event.common
import hat.event.eventer

from hat.gateway.devices.iec101 import common


mlog: logging.Logger = logging.getLogger(__name__)


async def create(conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec101SlaveDevice':
    device = Iec101SlaveDevice()
    device._conf = conf
    device._eventer_client = eventer_client
    device._event_type_prefix = event_type_prefix
    device._next_conn_ids = itertools.count(1)
    device._conns = {}
    device._send_queues = {}
    device._buffers = {}
    device._data_msgs = {}
    device._data_buffers = {}

    init_buffers(buffers_conf=conf['buffers'],
                 buffers=device._buffers)

    await init_data(data_conf=conf['data'],
                    data_msgs=device._data_msgs,
                    data_buffers=device._data_buffers,
                    buffers=device._buffers,
                    eventer_client=eventer_client,
                    event_type_prefix=event_type_prefix)

    if conf['link_type'] == 'BALANCED':
        create_link = link.create_balanced_link

    elif conf['link_type'] == 'UNBALANCED':
        create_link = link.create_slave_link

    else:
        raise ValueError('unsupported link type')

    device._link = await create_link(
        port=conf['port'],
        address_size=link.AddressSize[conf['device_address_size']],
        silent_interval=conf['silent_interval'],
        baudrate=conf['baudrate'],
        bytesize=serial.ByteSize[conf['bytesize']],
        parity=serial.Parity[conf['parity']],
        stopbits=serial.StopBits[conf['stopbits']],
        xonxoff=conf['flow_control']['xonxoff'],
        rtscts=conf['flow_control']['rtscts'],
        dsrdtr=conf['flow_control']['dsrdtr'])

    try:
        for device_conf in conf['devices']:
            device.async_group.spawn(device._connection_loop, device_conf)

        await device._register_connections()

    except BaseException:
        await aio.uncancellable(device.async_close())
        raise

    return device


info: common.DeviceInfo = common.DeviceInfo(
    type="iec101_slave",
    create=create,
    json_schema_id="hat-gateway://iec101.yaml#/$defs/slave",
    json_schema_repo=common.json_schema_repo)


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

    def get_event_id_data_msgs(self) -> Iterable[tuple[hat.event.common.EventId,  # NOQA
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
                    eventer_client: hat.event.eventer.Client,
                    event_type_prefix: common.EventTypePrefix):
    for data in data_conf:
        data_key = common.DataKey(data_type=common.DataType[data['data_type']],
                                  asdu_address=data['asdu_address'],
                                  io_address=data['io_address'])
        data_msgs[data_key] = None
        if data['buffer']:
            data_buffers[data_key] = buffers[data['buffer']]

    event_types = [(*event_type_prefix, 'system', 'data', '*')]
    params = hat.event.common.QueryLatestParams(event_types)
    result = await eventer_client.query(params)

    for event in result.events:
        try:
            data_type_str, asdu_address_str, io_address_str = \
                event.type[len(event_type_prefix)+2:]
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
        return self._link.async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        for event in events:
            try:
                await self._process_event(event)

            except Exception as e:
                mlog.warning('error processing event: %s', e, exc_info=e)

    async def _connection_loop(self, device_conf):
        conn = None

        try:
            if self._conf['link_type'] == 'BALANCED':
                conn_args = {
                    'direction': link.Direction[device_conf['direction']],
                    'addr': device_conf['address'],
                    'response_timeout': device_conf['response_timeout'],
                    'send_retry_count': device_conf['send_retry_count'],
                    'test_delay': device_conf['test_delay']}

            elif self._conf['link_type'] == 'UNBALANCED':
                conn_args = {
                    'addr': device_conf['address'],
                    'keep_alive_timeout': device_conf['keep_alive_timeout']}

            else:
                raise ValueError('unsupported link type')

            while True:
                try:
                    conn = await self._link.open_connection(**conn_args)

                except Exception as e:
                    mlog.error('connection error for address %s: %s',
                               device_conf['address'], e, exc_info=e)
                    await asyncio.sleep(device_conf['reconnect_delay'])
                    continue

                conn = iec101.Connection(
                    conn=conn,
                    cause_size=iec101.CauseSize[self._conf['cause_size']],
                    asdu_address_size=iec101.AsduAddressSize[
                        self._conf['asdu_address_size']],
                    io_address_size=iec101.IoAddressSize[
                        self._conf['io_address_size']])

                conn_id = next(self._next_conn_ids)
                self._conns[conn_id] = conn

                send_queue = aio.Queue(1024)
                self._send_queues[conn_id] = send_queue

                try:
                    conn.async_group.spawn(self._connection_send_loop, conn,
                                           send_queue)
                    conn.async_group.spawn(self._connection_receive_loop, conn,
                                           conn_id)

                    await self._register_connections()

                    with contextlib.suppress(Exception):
                        for buffer in self._buffers.values():
                            for event_id, data_msg in buffer.get_event_id_data_msgs():  # NOQA
                                await self._send_data_msg(conn_id, buffer,
                                                          event_id, data_msg)

                    await conn.wait_closed()

                finally:
                    send_queue.close()

                    self._conns.pop(conn_id, None)
                    self._send_queues.pop(conn_id, None)

                    with contextlib.suppress(Exception):
                        await aio.uncancellable(self._register_connections())

                await conn.async_close()

        except Exception as e:
            mlog.warning('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection')
            self.close()

            if conn:
                await aio.uncancellable(conn.async_close())

    async def _connection_send_loop(self, conn, send_queue):
        try:
            while True:
                msgs, sent_cb = await send_queue.get()
                await conn.send(msgs, sent_cb=sent_cb)

        except ConnectionError:
            mlog.debug('connection close')

        except Exception as e:
            mlog.warning('connection send loop error: %s', e, exc_info=e)

        finally:
            conn.close()

    async def _connection_receive_loop(self, conn, conn_id):
        try:
            while True:
                msgs = await conn.receive()

                for msg in msgs:
                    try:
                        mlog.debug('received message: %s', msg)
                        await self._process_msg(conn_id, msg)

                    except Exception as e:
                        mlog.warning('error processing message: %s',
                                     e, exc_info=e)

        except ConnectionError:
            mlog.debug('connection close')

        except Exception as e:
            mlog.warning('connection receive loop error: %s', e, exc_info=e)

        finally:
            conn.close()

    async def _register_connections(self):
        payload = [{'connection_id': conn_id,
                    'address': conn.address}
                   for conn_id, conn in self._conns.items()]

        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'connections'),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson(payload))

        await self._eventer_client.register([event])

    async def _process_event(self, event):
        suffix = event.type[len(self._event_type_prefix):]

        if suffix[:2] == ('system', 'data'):
            data_type_str, asdu_address_str, io_address_str = suffix[2:]
            data_key = common.DataKey(data_type=common.DataType(data_type_str),
                                      asdu_address=int(asdu_address_str),
                                      io_address=int(io_address_str))

            await self._process_data_event(data_key, event)

        elif suffix[:2] == ('system', 'command'):
            cmd_type_str, asdu_address_str, io_address_str = suffix[2:]
            cmd_key = common.CommandKey(
                cmd_type=common.CommandType(cmd_type_str),
                asdu_address=int(asdu_address_str),
                io_address=int(io_address_str))

            await self._process_command_event(cmd_key, event)

        else:
            raise Exception('unsupported event type')

    async def _process_data_event(self, data_key, event):
        if data_key not in self._data_msgs:
            raise Exception('data not configured')

        data_msg = data_msg_from_event(data_key, event)
        self._data_msgs[data_key] = data_msg

        buffer = self._data_buffers.get(data_key)
        if buffer:
            buffer.add(event.id, data_msg)

        for conn_id in self._conns.keys():
            await self._send_data_msg(conn_id, buffer, event.id, data_msg)

    async def _process_command_event(self, cmd_key, event):
        cmd_msg = cmd_msg_from_event(cmd_key, event)
        conn_id = event.payload.data['connection_id']
        await self._send(conn_id, [cmd_msg])

    async def _process_msg(self, conn_id, msg):
        if isinstance(msg, iec101.CommandMsg):
            await self._process_command_msg(conn_id, msg)

        elif isinstance(msg, iec101.InterrogationMsg):
            await self._process_interrogation_msg(conn_id, msg)

        elif isinstance(msg, iec101.CounterInterrogationMsg):
            await self._process_counter_interrogation_msg(conn_id, msg)

        elif isinstance(msg, iec101.ReadMsg):
            await self._process_read_msg(conn_id, msg)

        elif isinstance(msg, iec101.ClockSyncMsg):
            await self._process_clock_sync_msg(conn_id, msg)

        elif isinstance(msg, iec101.TestMsg):
            await self._process_test_msg(conn_id, msg)

        elif isinstance(msg, iec101.ResetMsg):
            await self._process_reset_msg(conn_id, msg)

        elif isinstance(msg, iec101.ParameterMsg):
            await self._process_parameter_msg(conn_id, msg)

        elif isinstance(msg, iec101.ParameterActivationMsg):
            await self._process_parameter_activation_msg(conn_id, msg)

        else:
            raise Exception('unsupported message')

    async def _process_command_msg(self, conn_id, msg):
        if isinstance(msg.cause, iec101.CommandReqCause):
            event = cmd_msg_to_event(self._event_type_prefix, conn_id, msg)
            await self._eventer_client.register([event])

        else:
            res = msg._replace(cause=iec101.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await self._send(conn_id, [res])

    async def _process_interrogation_msg(self, conn_id, msg):
        if msg.cause == iec101.CommandReqCause.ACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=False)
            await self._send(conn_id, [res])

            data_msgs = [
                data_msg._replace(
                    is_test=msg.is_test,
                    cause=iec101.DataResCause.INTERROGATED_STATION)
                for data_msg in self._data_msgs.values()
                if (data_msg and
                    (msg.asdu_address == 0xFFFF or
                     msg.asdu_address == data_msg.asdu_address) and
                    not isinstance(data_msg.data, iec101.BinaryCounterData))]
            await self._send(conn_id, data_msgs)

            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_TERMINATION,
                is_negative_confirm=False)
            await self._send(conn_id, [res])

        elif msg.cause == iec101.CommandReqCause.DEACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.DEACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            await self._send(conn_id, [res])

        else:
            res = msg._replace(cause=iec101.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await self._send(conn_id, [res])

    async def _process_counter_interrogation_msg(self, conn_id, msg):
        if msg.cause == iec101.CommandReqCause.ACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=False)
            await self._send(conn_id, [res])

            data_msgs = [
                data_msg._replace(
                    is_test=msg.is_test,
                    cause=iec101.DataResCause.INTERROGATED_COUNTER)
                for data_msg in self._data_msgs.values()
                if (data_msg and
                    (msg.asdu_address == 0xFFFF or
                     msg.asdu_address == data_msg.asdu_address) and
                    isinstance(data_msg.data, iec101.BinaryCounterData))]
            await self._send(conn_id, data_msgs)

            res = msg._replace(
                cause=iec101.CommandResCause.ACTIVATION_TERMINATION,
                is_negative_confirm=False)
            await self._send(conn_id, [res])

        elif msg.cause == iec101.CommandReqCause.DEACTIVATION:
            res = msg._replace(
                cause=iec101.CommandResCause.DEACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            await self._send(conn_id, [res])

        else:
            res = msg._replace(cause=iec101.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await self._send(conn_id, [res])

    async def _process_read_msg(self, conn_id, msg):
        res = msg._replace(cause=iec101.ReadResCause.UNKNOWN_TYPE)
        await self._send(conn_id, [res])

    async def _process_clock_sync_msg(self, conn_id, msg):
        if isinstance(msg.cause, iec101.ClockSyncReqCause):
            res = msg._replace(
                cause=iec101.ClockSyncResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            await self._send(conn_id, [res])

        else:
            res = msg._replace(cause=iec101.ClockSyncResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await self._send(conn_id, [res])

    async def _process_test_msg(self, conn_id, msg):
        res = msg._replace(cause=iec101.ActivationResCause.UNKNOWN_TYPE)
        await self._send(conn_id, [res])

    async def _process_reset_msg(self, conn_id, msg):
        res = msg._replace(cause=iec101.ActivationResCause.UNKNOWN_TYPE)
        await self._send(conn_id, [res])

    async def _process_parameter_msg(self, conn_id, msg):
        res = msg._replace(cause=iec101.ParameterResCause.UNKNOWN_TYPE)
        await self._send(conn_id, [res])

    async def _process_parameter_activation_msg(self, conn_id, msg):
        res = msg._replace(
            cause=iec101.ParameterActivationResCause.UNKNOWN_TYPE)
        await self._send(conn_id, [res])

    async def _send_data_msg(self, conn_id, buffer, event_id, data_msg):
        sent_cb = (functools.partial(buffer.remove, event_id)
                   if buffer else None)
        await self._send(conn_id, [data_msg], sent_cb=sent_cb)

    async def _send(self, conn_id, msgs, sent_cb=None):
        send_queue = self._send_queues.get(conn_id)
        if send_queue is None:
            return

        with contextlib.suppress(aio.QueueClosedError):
            await send_queue.put((msgs, sent_cb))


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
        type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson({
            'connection_id': conn_id,
            'is_test': msg.is_test,
            'cause': cause,
            'command': command}))


def data_msg_from_event(data_key: common.DataKey,
                        event: hat.event.common.Event
                        ) -> iec101.DataMsg:
    time = common.time_from_source_timestamp(event.source_timestamp)
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
