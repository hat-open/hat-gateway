"""IEC 60870-5-104 slave device"""

import contextlib
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

device_type: str = 'iec104_slave'

json_schema_id: str = "hat-gateway://iec104.yaml#/definitions/slave"

json_schema_repo: json.SchemaRepository = common.json_schema_repo


async def create(conf: common.DeviceConf,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec104SlaveDevice':
    device = Iec104SlaveDevice()

    device._conf = conf
    device._event_type_prefix = event_type_prefix
    device._event_client = event_client
    device._conns = {}

    device._async_group = aio.Group()
    srv = await apci.listen(
        connection_cb=device._on_connection,
        addr=tcp.Address(host=conf['local_host'],
                         port=conf['local_port']),
        response_timeout=conf['response_timeout'],
        supervisory_timeout=conf['supervisory_timeout'],
        test_timeout=conf['test_timeout'],
        send_window_size=conf['send_window_size'],
        receive_window_size=conf['receive_window_size'])
    device._async_group.spawn(aio.call_on_cancel, srv.async_close)
    device._async_group.spawn(device._event_loop)
    device._register_connections()

    return device


class Iec104SlaveDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _event_loop(self):
        try:
            while True:
                events = await self._event_client.receive()
                for event in events:
                    try:
                        msg = event_to_msg(
                            event, self._event_type_prefix, 'slave')
                    except Exception as e:
                        mlog.warning('event %s ignored: %s',
                                     event, e, exc_info=e)
                        continue
                    if not self._conns:
                        mlog.warning('event %s ignored: no connection', event)
                    for conn, conn_info in self._conns.items():
                        if conn.is_open:
                            conn.send([msg])
                            mlog.debug("msg %s sent to %s",
                                       msg, conn_info.remote_addr)
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
                    if _is_interrogate_deactivation(msg):
                        conn.send([msg._replace(
                            cause=iec104.CommandResCause.UNKNOWN_CAUSE)])
                        continue
                    if _is_test_request(msg):
                        conn.send([msg._replace(
                            cause=_req_to_resp_cause(msg.cause),
                            is_negative_confirm=True,
                            is_test=False)])
                        continue
                    try:
                        event = _msg_to_event(
                            msg, self._event_type_prefix)
                    except Exception as e:
                        mlog.warning('message %s ignored: %s',
                                     msg, e, exc_info=e)
                        continue
                    if event:
                        events.append(event)
                if events:
                    self._event_client.register(events)
        except ConnectionError:
            mlog.debug('connection close')
        finally:
            self._conns.pop(conn)
            self._register_connections()
            await conn.async_close()

    def _on_connection(self, conn_apci):
        if (self._conf['remote_hosts'] is not None and
                conn_apci.info.remote_addr.host not in
                self._conf['remote_hosts']):
            mlog.warning('remote host %s connected but not allowed, '
                         'will be closed', conn_apci.remote_addr.host)
            conn_apci.close()
            return
        conn = iec104.Connection(conn_apci)
        self._conns[conn] = conn_apci.info
        self._register_connections()
        self._async_group.spawn(self._receive_loop, conn)

    def _register_connections(self):
        payload = [{'local': {'host': conn_info.local_addr.host,
                              'port': conn_info.local_addr.port},
                    'remote': {'host': conn_info.remote_addr.host,
                               'port': conn_info.remote_addr.port}}
                   for conn_info in self._conns.values()]
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'connections'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=payload))
        with contextlib.suppress(ConnectionError):
            self._event_client.register([event])


def _is_test_request(msg):
    return msg.is_test and isinstance(msg, (iec104.CommandMsg,
                                            iec104.InterrogationMsg,
                                            iec104.CounterInterrogationMsg))


def _is_interrogate_deactivation(msg):
    return (isinstance(msg, (iec104.InterrogationMsg,
                             iec104.CounterInterrogationMsg)) and
            msg.cause != iec104.CommandReqCause.ACTIVATION)


def _msg_to_event(msg, event_type_prefix):
    if isinstance(msg, (iec104.CommandMsg,
                        iec104.InterrogationMsg,
                        iec104.CounterInterrogationMsg)):
        return msg_to_event(msg, event_type_prefix, 'slave')
    raise Exception('message not supported')


def _req_to_resp_cause(req_cause):
    return{
        iec104.CommandReqCause.ACTIVATION:
            iec104.CommandResCause.ACTIVATION_CONFIRMATION,
        iec104.CommandReqCause.DEACTIVATION:
            iec104.CommandResCause.DEACTIVATION_CONFIRMATION}.get(
                req_cause, iec104.CommandResCause.UNKNOWN_CAUSE)
