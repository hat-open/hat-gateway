"""IEC 60870-5-104 master device"""

import asyncio
import collections
import contextlib
import datetime
import enum
import logging

from hat import aio
from hat import json
from hat.drivers import iec104
from hat.drivers import tcp
from hat.gateway.devices.iec104 import common
import hat.event.common


mlog: logging.Logger = logging.getLogger(__name__)

device_type: str = 'iec104_master'

json_schema_id: str = "hat-gateway://iec104.yaml#/definitions/master"

json_schema_repo: json.SchemaRepository = common.json_schema_repo


async def create(conf: common.DeviceConf,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec104MasterDevice':
    device = Iec104MasterDevice()
    device._event_client = event_client
    device._event_type_prefix = event_type_prefix
    device._conn = None
    device._async_group = aio.Group()

    ssl_ctx = (common.create_ssl_ctx(conf['security'],
                                     common.SslProtocol.TLS_CLIENT)
               if conf['security'] else None)

    device.async_group.spawn(device._connection_loop, conf, ssl_ctx)
    device.async_group.spawn(device._event_loop)

    return device


class Iec104MasterDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _connection_loop(self, conf, ssl_ctx):
        try:
            while True:
                self._register_status('CONNECTING')
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
                            ssl_ctx=ssl_ctx)
                        break

                    except Exception as e:
                        mlog.warning('connection failed %s', e, exc_info=e)

                else:
                    self._register_status('DISCONNECTED')
                    await asyncio.sleep(conf['reconnect_delay'])
                    continue

                self._register_status('CONNECTED')
                self.async_group.spawn(self._receive_loop, self._conn)
                if conf['time_sync_delay'] is not None:
                    self.async_group.spawn(self._time_sync_loop, self._conn,
                                           conf['time_sync_delay'])

                await self._conn.wait_closed()
                self._register_status('DISCONNECTED')
                self._conn = None

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection loop')
            self.close()

            with contextlib.suppress(ConnectionError):
                self._register_status('DISCONNECTED')

            if self._conn:
                await aio.uncancellable(self._conn.async_close())

    async def _event_loop(self):
        try:
            while True:
                events = await self._event_client.receive()

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
                    continue

                if not self._conn or not self._conn.is_open:
                    mlog.warning('connection closed: %s events ignored',
                                 len(msgs))

                try:
                    self._conn.send(list(msgs))
                    mlog.debug('%s messages sent', len(msg))

                except ConnectionError as e:
                    mlog.warning('error sending messages: %s', e, exc_info=e)
                    continue

        except ConnectionError:
            mlog.debug('event client closed')

        except Exception as e:
            mlog.error('event loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing event loop')
            self.close()

    async def _receive_loop(self, conn):
        try:
            while True:
                msgs = await conn.receive()

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

                self._event_client.register(list(events))
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
                conn.send([msg])

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

    def _register_status(self, status):
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=status))

        self._event_client.register([event])
        mlog.debug('registered status %s', status)


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
    cause = (msg.cause.name if isinstance(msg.cause, iec104.DataResCause)
             else msg.cause)
    if isinstance(cause, str) and cause.startswith('INTERROGATED_'):
        cause = 'INTERROGATED'
    data = common.data_to_json(msg.data)
    event_type = (*event_type_prefix, 'gateway', 'data', data_type.value,
                  str(msg.asdu_address), str(msg.io_address))
    source_timestamp = common.time_to_source_timestamp(msg.time)

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'is_test': msg.is_test,
                  'cause': cause,
                  'data': data}))


def _command_to_event(event_type_prefix, msg):
    command_type = common.get_command_type(msg.command)
    cause = (msg.cause.name if isinstance(msg.cause, iec104.CommandResCause)
             else msg.cause.value if isinstance(msg.cause, enum.Enum)
             else msg.cause)
    command = common.command_to_json(msg.command)
    event_type = (*event_type_prefix, 'gateway', 'command', command_type.value,
                  str(msg.asdu_address), str(msg.io_address))
    source_timestamp = common.time_to_source_timestamp(msg.time)

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'is_test': msg.is_test,
                  'is_negative_confirm': msg.is_negative_confirm,
                  'cause': cause,
                  'command': command}))


def _interrogation_to_event(event_type_prefix, msg):
    cause = (msg.cause.name if isinstance(msg.cause, iec104.CommandResCause)
             else msg.cause.value if isinstance(msg.cause, enum.Enum)
             else msg.cause)
    event_type = (*event_type_prefix, 'gateway', 'interrogation',
                  str(msg.asdu_address))

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data={'is_test': msg.is_test,
                  'is_negative_confirm': msg.is_negative_confirm,
                  'request': msg.request,
                  'cause': cause}))


def _counter_interrogation_to_event(event_type_prefix, msg):
    cause = (msg.cause.name if isinstance(msg.cause, iec104.CommandResCause)
             else msg.cause.value if isinstance(msg.cause, enum.Enum)
             else msg.cause)
    event_type = (*event_type_prefix, 'gateway', 'counter_interrogation',
                  str(msg.asdu_address))

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


def _msg_from_event(event_type_prefix, event):
    suffix = event.event_type[len(event_type_prefix):]

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
    cause = (iec104.CommandReqCause[event.payload.data['cause']]
             if isinstance(event.payload.data['cause'], str)
             else event.payload.data['cause'])
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
    cause = (iec104.CommandReqCause[event.payload.data['cause']]
             if isinstance(event.payload.data['cause'], str)
             else event.payload.data['cause'])

    return iec104.InterrogationMsg(is_test=event.payload.data['is_test'],
                                   originator_address=0,
                                   asdu_address=asdu_address,
                                   request=event.payload.data['request'],
                                   is_negative_confirm=False,
                                   cause=cause)


def _counter_interrogation_from_event(asdu_address, event):
    freeze = iec104.FreezeCode[event.payload.data['freeze']]
    cause = (iec104.CommandReqCause[event.payload.data['cause']]
             if isinstance(event.payload.data['cause'], str)
             else event.payload.data['cause'])

    return iec104.CounterInterrogationMsg(
        is_test=event.payload.data['is_test'],
        originator_address=0,
        asdu_address=asdu_address,
        request=event.payload.data['request'],
        freeze=freeze,
        is_negative_confirm=False,
        cause=cause)
