"""IEC 60870-5-104 slave device"""

from collections.abc import Collection
import contextlib
import functools
import itertools
import logging

from hat import aio
from hat.drivers import iec104
from hat.drivers import tcp
import hat.event.common
import hat.event.eventer

from hat.gateway.devices.iec101 import slave as iec101_slave
from hat.gateway.devices.iec104 import common
from hat.gateway.devices.iec104 import ssl


mlog: logging.Logger = logging.getLogger(__name__)


async def create(conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec104SlaveDevice':
    device = Iec104SlaveDevice()
    device._conf = conf
    device._eventer_client = eventer_client
    device._event_type_prefix = event_type_prefix
    device._max_connections = conf['max_connections']
    device._next_conn_ids = itertools.count(1)
    device._conns = {}
    device._remote_hosts = (set(conf['remote_hosts'])
                            if conf['remote_hosts'] is not None else None)
    device._buffers = {}
    device._data_msgs = {}
    device._data_buffers = {}

    iec101_slave.init_buffers(buffers_conf=conf['buffers'],
                              buffers=device._buffers)

    await iec101_slave.init_data(data_conf=conf['data'],
                                 data_msgs=device._data_msgs,
                                 data_buffers=device._data_buffers,
                                 buffers=device._buffers,
                                 eventer_client=eventer_client,
                                 event_type_prefix=event_type_prefix)

    ssl_ctx = (ssl.create_ssl_ctx(conf['security'], ssl.SslProtocol.TLS_SERVER)
               if conf['security'] else None)

    device._srv = await iec104.listen(
        connection_cb=device._on_connection,
        addr=tcp.Address(host=conf['local_host'],
                         port=conf['local_port']),
        response_timeout=conf['response_timeout'],
        supervisory_timeout=conf['supervisory_timeout'],
        test_timeout=conf['test_timeout'],
        send_window_size=conf['send_window_size'],
        receive_window_size=conf['receive_window_size'],
        ssl=ssl_ctx)

    try:
        await device._register_connections()

    except BaseException:
        await aio.uncancellable(device.async_close())
        raise

    return device


info: common.DeviceInfo = common.DeviceInfo(
    type="iec104_slave",
    create=create,
    json_schema_id="hat-gateway://iec104.yaml#/$defs/slave",
    json_schema_repo=common.json_schema_repo)


class Iec104SlaveDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._srv.async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        for event in events:
            try:
                mlog.debug('received event: %s', event)
                await self._process_event(event)

            except Exception as e:
                mlog.warning('error processing event: %s', e, exc_info=e)

    async def _on_connection(self, conn):
        if (self._max_connections is not None and
                len(self._conns) >= self._max_connections):
            mlog.info('max connections exceeded - rejecting connection')
            conn.close()
            return

        if self._conf['security']:
            try:
                ssl.init_security(self._conf['security'], conn)

            except Exception as e:
                mlog.error('init security error: %s', exc_info=e)
                conn.close()
                return

        conn_id = next(self._next_conn_ids)

        try:
            if self._remote_hosts is not None:
                remote_host = conn.info.remote_addr.host
                if remote_host not in self._remote_hosts:
                    raise Exception(f'remote host {remote_host} not allowed')

            self._conns[conn_id] = conn
            await self._register_connections()

            enabled_cb = functools.partial(self._on_enabled, conn)
            with conn.register_enabled_cb(enabled_cb):
                enabled_cb(conn.is_enabled)

                while True:
                    msgs = await conn.receive()

                    for msg in msgs:
                        try:
                            mlog.debug('received message: %s', msg)
                            await self._process_msg(conn_id, conn, msg)

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
                await aio.uncancellable(self._register_connections())

    def _on_enabled(self, conn, enabled):
        if not enabled:
            return

        with contextlib.suppress(Exception):
            for buffer in self._buffers.values():
                for event_id, data_msg in buffer.get_event_id_data_msgs():
                    self._send_data_msg(conn, buffer, event_id, data_msg)

    async def _register_connections(self):
        payload = [{'connection_id': conn_id,
                    'local': {'host': conn.info.local_addr.host,
                              'port': conn.info.local_addr.port},
                    'remote': {'host': conn.info.remote_addr.host,
                               'port': conn.info.remote_addr.port}}
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

        data_msg = _data_msg_from_event(data_key, event)
        self._data_msgs[data_key] = data_msg

        buffer = self._data_buffers.get(data_key)
        if buffer:
            buffer.add(event.id, data_msg)

        for conn in self._conns.values():
            self._send_data_msg(conn, buffer, event.id, data_msg)

    async def _process_command_event(self, cmd_key, event):
        cmd_msg = _cmd_msg_from_event(cmd_key, event)
        conn_id = event.payload.data['connection_id']
        conn = self._conns[conn_id]
        await conn.send([cmd_msg])

    async def _process_msg(self, conn_id, conn, msg):
        if isinstance(msg, iec104.CommandMsg):
            await self._process_command_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.InterrogationMsg):
            await self._process_interrogation_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.CounterInterrogationMsg):
            await self._process_counter_interrogation_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.ReadMsg):
            await self._process_read_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.ClockSyncMsg):
            await self._process_clock_sync_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.TestMsg):
            await self._process_test_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.ResetMsg):
            await self._process_reset_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.ParameterMsg):
            await self._process_parameter_msg(conn_id, conn, msg)

        elif isinstance(msg, iec104.ParameterActivationMsg):
            await self._process_parameter_activation_msg(conn_id, conn, msg)

        else:
            raise Exception('unsupported message')

    async def _process_command_msg(self, conn_id, conn, msg):
        if isinstance(msg.cause, iec104.CommandReqCause):
            event = _cmd_msg_to_event(self._event_type_prefix, conn_id, msg)
            await self._eventer_client.register([event])

        else:
            res = msg._replace(cause=iec104.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await conn.send([res])

    async def _process_interrogation_msg(self, conn_id, conn, msg):
        if msg.cause == iec104.CommandReqCause.ACTIVATION:
            res = msg._replace(
                cause=iec104.CommandResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=False)
            await conn.send([res])

            data_msgs = [
                data_msg._replace(
                    is_test=msg.is_test,
                    cause=iec104.DataResCause.INTERROGATED_STATION)
                for data_msg in self._data_msgs.values()
                if (data_msg and
                    (msg.asdu_address == 0xFFFF or
                     msg.asdu_address == data_msg.asdu_address) and
                    not isinstance(data_msg.data, iec104.BinaryCounterData))]
            await conn.send(data_msgs)

            res = msg._replace(
                cause=iec104.CommandResCause.ACTIVATION_TERMINATION,
                is_negative_confirm=False)
            await conn.send([res])

        elif msg.cause == iec104.CommandReqCause.DEACTIVATION:
            res = msg._replace(
                cause=iec104.CommandResCause.DEACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            await conn.send([res])

        else:
            res = msg._replace(cause=iec104.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await conn.send([res])

    async def _process_counter_interrogation_msg(self, conn_id, conn, msg):
        if msg.cause == iec104.CommandReqCause.ACTIVATION:
            res = msg._replace(
                cause=iec104.CommandResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=False)
            await conn.send([res])

            data_msgs = [
                data_msg._replace(
                    is_test=msg.is_test,
                    cause=iec104.DataResCause.INTERROGATED_COUNTER)
                for data_msg in self._data_msgs.values()
                if (data_msg and
                    (msg.asdu_address == 0xFFFF or
                     msg.asdu_address == data_msg.asdu_address) and
                    isinstance(data_msg.data, iec104.BinaryCounterData))]
            await conn.send(data_msgs)

            res = msg._replace(
                cause=iec104.CommandResCause.ACTIVATION_TERMINATION,
                is_negative_confirm=False)
            await conn.send([res])

        elif msg.cause == iec104.CommandReqCause.DEACTIVATION:
            res = msg._replace(
                cause=iec104.CommandResCause.DEACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            await conn.send([res])

        else:
            res = msg._replace(cause=iec104.CommandResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await conn.send([res])

    async def _process_read_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec104.ReadResCause.UNKNOWN_TYPE)
        await conn.send([res])

    async def _process_clock_sync_msg(self, conn_id, conn, msg):
        if isinstance(msg.cause, iec104.ClockSyncReqCause):
            res = msg._replace(
                cause=iec104.ClockSyncResCause.ACTIVATION_CONFIRMATION,
                is_negative_confirm=True)
            await conn.send([res])

        else:
            res = msg._replace(cause=iec104.ClockSyncResCause.UNKNOWN_CAUSE,
                               is_negative_confirm=True)
            await conn.send([res])

    async def _process_test_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec104.ActivationResCause.UNKNOWN_TYPE)
        await conn.send([res])

    async def _process_reset_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec104.ActivationResCause.UNKNOWN_TYPE)
        await conn.send([res])

    async def _process_parameter_msg(self, conn_id, conn, msg):
        res = msg._replace(cause=iec104.ParameterResCause.UNKNOWN_TYPE)
        await conn.send([res])

    async def _process_parameter_activation_msg(self, conn_id, conn, msg):
        res = msg._replace(
            cause=iec104.ParameterActivationResCause.UNKNOWN_TYPE)
        await conn.send([res])

    def _send_data_msg(self, conn, buffer, event_id, data_msg):
        self.async_group.spawn(_send_data_msg, conn, buffer, event_id,
                               data_msg)


async def _send_data_msg(conn, buffer, event_id, data_msg):
    try:
        if buffer:
            await conn.send([data_msg], wait_ack=True)
            buffer.remove(event_id)

        else:
            await conn.send([data_msg])

    except ConnectionError:
        pass

    except Exception as e:
        mlog.warning('send data message error: %s', e, exc_info=e)


def _cmd_msg_to_event(event_type_prefix, conn_id, msg):
    event = iec101_slave.cmd_msg_to_event(event_type_prefix, conn_id, msg)
    source_timestamp = common.time_to_source_timestamp(msg.time)
    return event._replace(source_timestamp=source_timestamp)


def _data_msg_from_event(data_key, event):
    return iec101_slave.data_msg_from_event(data_key, event)


def _cmd_msg_from_event(cmd_key, event):
    msg = iec101_slave.cmd_msg_from_event(cmd_key, event)
    time = common.time_from_source_timestamp(event.source_timestamp)
    return iec104.CommandMsg(**msg._asdict(),
                             time=time)
