from collections.abc import Collection
import asyncio
import contextlib
import logging
import typing

from hat import aio
from hat import util
from hat.drivers import smpp
from hat.drivers import ssl
from hat.drivers import tcp
import hat.event.common
import hat.event.eventer

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)


class SmppClientDevice(common.Device):

    def __init__(self,
                 conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix):
        self._conf = conf
        self._eventer_client = eventer_client
        self._event_type_prefix = event_type_prefix
        self._async_group = aio.Group()
        self._msg_queue = aio.Queue()

        self.async_group.spawn(self._connection_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        for event in events:
            try:
                mlog.debug('received event: %s', event)
                msg = _msg_from_event(
                    event_type_prefix=self._event_type_prefix,
                    message_encoding=self._conf['message_encoding'],
                    event=event)

                if self._msg_queue is None:
                    raise Exception('connection closed')

                await self._msg_queue.put(msg)

            except Exception as e:
                mlog.warning('error processing event: %s',
                             e, exc_info=e)

    async def _connection_loop(self):
        conn = None

        async def cleanup():
            with contextlib.suppress(ConnectionError):
                await self._register_status('DISCONNECTED')

            if conn:
                await conn.async_close()

        try:
            while True:
                await self._register_status('CONNECTING')
                try:
                    ssl_ctx = (ssl.create_ssl_ctx(ssl.SslProtocol.TLS_CLIENT)
                               if self._conf['ssl'] else None)
                    conn = await aio.wait_for(
                        smpp.connect(
                            addr=tcp.Address(
                                host=self._conf['remote_address']['host'],
                                port=self._conf['remote_address']['port']),
                            system_id=self._conf['system_id'],
                            password=self._conf['password'],
                            enquire_link_delay=self._conf['enquire_link_delay'],  # NOQA
                            enquire_link_timeout=self._conf['enquire_link_timeout'],  # NOQA
                            ssl=ssl_ctx),
                        self._conf['connect_timeout'])

                except Exception as e:
                    mlog.warning('connection failed: %s', e, exc_info=e)
                    await asyncio.sleep(self._conf['reconnect_delay'])
                    continue

                except aio.CancelledWithResultError as e:
                    conn = e.result
                    raise

                await self._register_status('CONNECTED')

                try:
                    self._msg_queue = aio.Queue(1024)

                    if conn.is_open:
                        conn.async_group.spawn(self._send_loop, conn,
                                               self._msg_queue)
                        await conn.wait_closing()

                finally:
                    self._msg_queue = None

                await self._register_status('DISCONNECTED')
                await conn.async_close()
                self._conn = None

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection loop')
            self.close()
            await aio.uncancellable(cleanup())

    async def _send_loop(self, conn, msg_queue):
        try:
            short_message = self._conf['short_message']
            priority = smpp.Priority[self._conf['priority']]
            data_coding = smpp.DataCoding[self._conf['data_coding']]

            while True:
                msg = await msg_queue.get()

                mlog.debug('sending message to %s', msg.address)
                msg_id = await conn.send_message(dst_addr=msg.address,
                                                 msg=msg.message,
                                                 short_message=short_message,
                                                 priority=priority,
                                                 data_coding=data_coding)

                mlog.debug('message sent with id %s', msg_id.hex())

        except ConnectionError:
            mlog.debug('connection closed')

        except asyncio.TimeoutError:
            mlog.debug('send timeout')

        except Exception as e:
            mlog.error('send loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing send loop')
            conn.close()

    async def _register_status(self, status):
        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson(status))

        await self._eventer_client.register([event])
        mlog.debug('registered status %s', status)


info: common.DeviceInfo = common.DeviceInfo(
    type="smpp_client",
    create=SmppClientDevice,
    json_schema_id="hat-gateway://smpp.yaml#/$defs/client",
    json_schema_repo=common.json_schema_repo)


class _Msg(typing.NamedTuple):
    address: str
    message: util.Bytes


def _msg_from_event(event_type_prefix, message_encoding, event):
    suffix = event.type[len(event_type_prefix):]
    if suffix != ('system', 'message'):
        raise Exception('unsupported event type')

    if not isinstance(event.payload.data['address'], str):
        raise Exception('invalid address type')

    if not isinstance(event.payload.data['message'], str):
        raise Exception('invalid message type')

    return _Msg(address=event.payload.data['address'],
                message=event.payload.data['message'].encode(message_encoding))
