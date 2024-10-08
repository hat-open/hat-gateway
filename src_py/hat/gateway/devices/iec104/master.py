"""IEC 60870-5-104 master device"""

from collections.abc import Collection
import asyncio
import collections
import contextlib
import datetime
import enum
import logging

from hat import aio
from hat.drivers import iec104
from hat.drivers import tcp
import hat.event.common
import hat.event.eventer

from hat.gateway.devices.iec104 import common
from hat.gateway.devices.iec104 import ssl


mlog: logging.Logger = logging.getLogger(__name__)


class Iec104MasterDevice(common.Device):

    def __init__(self,
                 conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix):
        self._eventer_client = eventer_client
        self._event_type_prefix = event_type_prefix
        self._conn = None
        self._async_group = aio.Group()

        ssl_ctx = (
            ssl.create_ssl_ctx(conf['security'], ssl.SslProtocol.TLS_CLIENT)
            if conf['security'] else None)

        self.async_group.spawn(self._connection_loop, conf, ssl_ctx)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        msgs = collections.deque()
        for event in events:
            try:
                mlog.debug('received event: %s', event)
                msg = _msg_from_event(self._event_type_prefix, event)
                msgs.append(msg)

            except Exception as e:
                mlog.warning('error processing event: %s',
                             e, exc_info=e)
                continue

        if not msgs:
            return

        if not self._conn or not self._conn.is_open:
            mlog.warning('connection closed: %s events ignored',
                         len(msgs))
            return

        try:
            await self._conn.send(msgs)
            mlog.debug('%s messages sent', len(msgs))

        except ConnectionError as e:
            mlog.warning('error sending messages: %s', e, exc_info=e)

    async def _connection_loop(self, conf, ssl_ctx):

        async def cleanup():
            with contextlib.suppress(ConnectionError):
                await self._register_status('DISCONNECTED')

            if self._conn:
                await self._conn.async_close()

        try:
            while True:
                await self._register_status('CONNECTING')
                for address in conf['remote_addresses']:
                    try:
                        self._conn = await iec104.connect(
                            addr=tcp.Address(host=address['host'],
                                             port=address['port']),
                            response_timeout=conf['response_timeout'],
                            supervisory_timeout=conf['supervisory_timeout'],
                            test_timeout=conf['test_timeout'],
                            send_window_size=conf['send_window_size'],
                            receive_window_size=conf['receive_window_size'],
                            ssl=ssl_ctx)

                        if conf['security']:
                            try:
                                ssl.init_security(conf['security'], self._conn)

                            except Exception:
                                await aio.uncancellable(
                                    self._conn.async_close())
                                raise

                        break

                    except Exception as e:
                        mlog.warning('connection failed: %s', e, exc_info=e)

                else:
                    await self._register_status('DISCONNECTED')
                    await asyncio.sleep(conf['reconnect_delay'])
                    continue

                await self._register_status('CONNECTED')
                self.async_group.spawn(self._receive_loop, self._conn)
                if conf['time_sync_delay'] is not None:
                    self.async_group.spawn(self._time_sync_loop, self._conn,
                                           conf['time_sync_delay'])

                await self._conn.wait_closed()
                await self._register_status('DISCONNECTED')
                self._conn = None

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection loop')
            self.close()
            await aio.uncancellable(cleanup())

    async def _receive_loop(self, conn):
        try:
            while True:
                try:
                    msgs = await conn.receive()

                except iec104.AsduTypeError as e:
                    mlog.warning("asdu type error: %s", e)
                    continue

                events = collections.deque()
                for msg in msgs:
                    try:
                        mlog.debug('received message: %s', msg)
                        if isinstance(msg, iec104.ClockSyncMsg):
                            continue

                        event = _msg_to_event(self._event_type_prefix, msg)
                        events.append(event)

                    except Exception as e:
                        mlog.warning('error processing message: %s',
                                     e, exc_info=e)
                        continue

                if not events:
                    continue

                await self._eventer_client.register(events)
                mlog.debug('%s events registered', len(events))

        except ConnectionError:
            mlog.debug('connection closed')

        except Exception as e:
            mlog.error('receive loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing receive loop')
            conn.close()

    async def _time_sync_loop(self, conn, time_sync_delay):
        try:
            while True:
                time_now = datetime.datetime.now(datetime.timezone.utc)
                time_iec104_now = iec104.time_from_datetime(time_now)

                msg = iec104.ClockSyncMsg(
                    is_test=False,
                    originator_address=0,
                    asdu_address=0xFFFF,
                    time=time_iec104_now,
                    is_negative_confirm=False,
                    cause=iec104.ClockSyncReqCause.ACTIVATION)
                await conn.send([msg])

                await conn.drain()
                mlog.debug('time sync sent %s', time_iec104_now)

                await asyncio.sleep(time_sync_delay)

        except ConnectionError:
            mlog.debug('connection closed')

        except Exception as e:
            mlog.error('time sync loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing time sync loop')
            conn.close()

    async def _register_status(self, status):
        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson(status))

        await self._eventer_client.register([event])
        mlog.debug('registered status %s', status)


info: common.DeviceInfo = common.DeviceInfo(
    type="iec104_master",
    create=Iec104MasterDevice,
    json_schema_id="hat-gateway://iec104.yaml#/$defs/master",
    json_schema_repo=common.json_schema_repo)


def _msg_to_event(event_type_prefix, msg):
    if isinstance(msg, iec104.DataMsg):
        return _data_to_event(event_type_prefix, msg)

    if isinstance(msg, iec104.CommandMsg):
        return _command_to_event(event_type_prefix, msg)

    if isinstance(msg, iec104.InterrogationMsg):
        return _interrogation_to_event(event_type_prefix, msg)

    if isinstance(msg, iec104.CounterInterrogationMsg):
        return _counter_interrogation_to_event(event_type_prefix, msg)

    raise Exception('unsupported message type')


def _data_to_event(event_type_prefix, msg):
    data_type = common.get_data_type(msg.data)
    cause = _cause_to_json(iec104.DataResCause, msg.cause)
    data = common.data_to_json(msg.data)
    event_type = (*event_type_prefix, 'gateway', 'data', data_type.value,
                  str(msg.asdu_address), str(msg.io_address))
    source_timestamp = common.time_to_source_timestamp(msg.time)

    return hat.event.common.RegisterEvent(
        type=event_type,
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayloadJson({
            'is_test': msg.is_test,
            'cause': cause,
            'data': data}))


def _command_to_event(event_type_prefix, msg):
    command_type = common.get_command_type(msg.command)
    cause = _cause_to_json(iec104.CommandResCause, msg.cause)
    command = common.command_to_json(msg.command)
    event_type = (*event_type_prefix, 'gateway', 'command', command_type.value,
                  str(msg.asdu_address), str(msg.io_address))
    source_timestamp = common.time_to_source_timestamp(msg.time)

    return hat.event.common.RegisterEvent(
        type=event_type,
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayloadJson({
            'is_test': msg.is_test,
            'is_negative_confirm': msg.is_negative_confirm,
            'cause': cause,
            'command': command}))


def _interrogation_to_event(event_type_prefix, msg):
    cause = _cause_to_json(iec104.CommandResCause, msg.cause)
    event_type = (*event_type_prefix, 'gateway', 'interrogation',
                  str(msg.asdu_address))

    return hat.event.common.RegisterEvent(
        type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson({
            'is_test': msg.is_test,
            'is_negative_confirm': msg.is_negative_confirm,
            'request': msg.request,
            'cause': cause}))


def _counter_interrogation_to_event(event_type_prefix, msg):
    cause = _cause_to_json(iec104.CommandResCause, msg.cause)
    event_type = (*event_type_prefix, 'gateway', 'counter_interrogation',
                  str(msg.asdu_address))

    return hat.event.common.RegisterEvent(
        type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson({
            'is_test': msg.is_test,
            'is_negative_confirm': msg.is_negative_confirm,
            'request': msg.request,
            'freeze': msg.freeze.name,
            'cause': cause}))


def _msg_from_event(event_type_prefix, event):
    suffix = event.type[len(event_type_prefix):]

    if suffix[:2] == ('system', 'command'):
        cmd_type_str, asdu_address_str, io_address_str = suffix[2:]
        cmd_key = common.CommandKey(cmd_type=common.CommandType(cmd_type_str),
                                    asdu_address=int(asdu_address_str),
                                    io_address=int(io_address_str))
        return _command_from_event(cmd_key, event)

    if suffix[:2] == ('system', 'interrogation'):
        asdu_address = int(suffix[2])
        return _interrogation_from_event(asdu_address, event)

    if suffix[:2] == ('system', 'counter_interrogation'):
        asdu_address = int(suffix[2])
        return _counter_interrogation_from_event(asdu_address, event)

    raise Exception('unsupported event type')


def _command_from_event(cmd_key, event):
    time = common.time_from_source_timestamp(event.source_timestamp)
    cause = _cause_from_json(iec104.CommandReqCause,
                             event.payload.data['cause'])
    command = common.command_from_json(cmd_key.cmd_type,
                                       event.payload.data['command'])

    return iec104.CommandMsg(is_test=event.payload.data['is_test'],
                             originator_address=0,
                             asdu_address=cmd_key.asdu_address,
                             io_address=cmd_key.io_address,
                             command=command,
                             is_negative_confirm=False,
                             time=time,
                             cause=cause)


def _interrogation_from_event(asdu_address, event):
    cause = _cause_from_json(iec104.CommandReqCause,
                             event.payload.data['cause'])

    return iec104.InterrogationMsg(is_test=event.payload.data['is_test'],
                                   originator_address=0,
                                   asdu_address=asdu_address,
                                   request=event.payload.data['request'],
                                   is_negative_confirm=False,
                                   cause=cause)


def _counter_interrogation_from_event(asdu_address, event):
    freeze = iec104.FreezeCode[event.payload.data['freeze']]
    cause = _cause_from_json(iec104.CommandReqCause,
                             event.payload.data['cause'])

    return iec104.CounterInterrogationMsg(
        is_test=event.payload.data['is_test'],
        originator_address=0,
        asdu_address=asdu_address,
        request=event.payload.data['request'],
        freeze=freeze,
        is_negative_confirm=False,
        cause=cause)


def _cause_to_json(cls, cause):
    return (cause.name if isinstance(cause, cls) else
            cause.value if isinstance(cause, enum.Enum) else
            cause)


def _cause_from_json(cls, cause):
    return cls[cause] if isinstance(cause, str) else cause
