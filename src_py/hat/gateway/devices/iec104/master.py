"""IEC 60870-5-104 master device"""

import asyncio
import contextlib
import datetime
import logging

from hat import aio
from hat import json
from hat.drivers import tcp
from hat.drivers.iec60870 import apci
from hat.drivers.iec60870 import iec104
from hat.gateway import common
from hat.gateway.devices.iec104.common import msg_to_event, event_to_msg
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

    device._conf = conf
    device._event_type_prefix = event_type_prefix
    device._event_client = event_client
    device._conn = None

    device._async_group = aio.Group()
    device._async_group.spawn(device._connection_loop)
    device._async_group.spawn(device._event_loop)

    return device


class Iec104MasterDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _connection_loop(self):
        try:
            while True:
                self._register_status('CONNECTING')
                try:
                    conn_apci = await apci.connect(
                        addr=tcp.Address(host=self._conf['remote_host'],
                                         port=self._conf['remote_port']),
                        response_timeout=self._conf['response_timeout'],
                        supervisory_timeout=self._conf['supervisory_timeout'],
                        test_timeout=self._conf['test_timeout'],
                        send_window_size=self._conf['send_window_size'],
                        receive_window_size=self._conf['receive_window_size'])
                except Exception as e:
                    mlog.warning('connection failed %s', e, exc_info=e)
                    self._register_status('DISCONNECTED')
                    await asyncio.sleep(self._conf['reconnect_delay'])
                    continue
                self._register_status('CONNECTED')
                self._conn = iec104.Connection(conn_apci)
                self._async_group.spawn(self._receive_loop, self._conn)
                if self._conf['time_sync_delay'] is not None:
                    self._async_group.spawn(self._time_sync_loop, self._conn)
                await self._conn.wait_closed()
                self._register_status('DISCONNECTED')
                self._conn = None
        finally:
            mlog.debug('closing device, connection loop')
            self.close()
            with contextlib.suppress(ConnectionError):
                self._register_status('DISCONNECTED')
            if self._conn:
                await aio.uncancellable(self._conn.async_close())

    async def _event_loop(self):
        try:
            while True:
                events = await self._event_client.receive()
                for event in events:
                    try:
                        msg = event_to_msg(
                            event, self._event_type_prefix, 'master')
                    except Exception as e:
                        mlog.warning('event %s ignored due to: %s',
                                     event, e, exc_info=e)
                        continue
                    if self._conn and self._conn.is_open:
                        self._conn.send([msg])
                        mlog.debug('msg sent %s', msg)
                    else:
                        mlog.warning(
                            'event %s ignored: connection closed', event)
        except ConnectionError:
            mlog.debug('connection to event server closed')
        finally:
            mlog.debug('closing device, event loop')
            self.close()

    async def _receive_loop(self, conn):
        try:
            while True:
                msgs = await conn.receive()
                events = []
                for msg in msgs:
                    try:
                        event = _msg_to_event(msg, self._event_type_prefix)
                    except Exception as e:
                        mlog.warning('message %s ignored:%s',
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

    async def _time_sync_loop(self, conn):
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
                await asyncio.sleep(self._conf['time_sync_delay'])
        except ConnectionError:
            mlog.debug('connection closed')
        finally:
            conn.close()

    def _register_status(self, status):
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=status))
        self._event_client.register([event])


def _msg_to_event(msg, event_type_prefix):
    if msg.is_test and isinstance(msg, (iec104.CommandMsg,
                                        iec104.InterrogationMsg,
                                        iec104.CounterInterrogationMsg)):
        mlog.warning('received test response %s', msg)
    if isinstance(msg, (iec104.DataMsg,
                        iec104.CommandMsg,
                        iec104.InterrogationMsg,
                        iec104.CounterInterrogationMsg)):
        return msg_to_event(msg, event_type_prefix, 'master')
    if (isinstance(msg, iec104.ClockSyncMsg) and
            msg.cause == iec104.ClockSyncResCause.ACTIVATION_CONFIRMATION):
        if msg.is_negative_confirm:
            mlog.warning(
                'received negative confirmation on clock sync: %s', msg)
        return
    raise Exception('message not supported')
