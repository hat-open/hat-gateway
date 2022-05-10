"""SNMP manager device"""

import asyncio
import contextlib
import logging

from hat import aio
from hat import json
from hat.gateway import common
import hat.event.common
from hat.drivers import snmp
from hat.drivers import udp


mlog: logging.Logger = logging.getLogger(__name__)

device_type: str = 'snmp_manager'

json_schema_id: str = "hat-gateway://snmp.yaml#/definitions/manager"

json_schema_repo: json.SchemaRepository = common.json_schema_repo


async def create(conf: common.DeviceConf,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'SnmpManagerDevice':
    device = SnmpManagerDevice()

    device._conf = conf
    device._event_type_prefix = event_type_prefix
    device._event_client = event_client
    device._manager = None
    device._cache = {}
    device._polling_oids = conf['polling_oids'] or ["0.0"]

    device._async_group = aio.Group()
    device._async_group.spawn(device._connection_loop)

    return device


class SnmpManagerDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _connection_loop(self):
        try:
            while True:
                self._register_status('CONNECTING')
                try:
                    self._manager = await snmp.create_manager(
                        context=snmp.Context(
                            engine_id=self._conf['snmp_context']['engine_id'],
                            name=self._conf['snmp_context']['name']),
                        remote_addr=udp.Address(
                            host=self._conf['remote_host'],
                            port=self._conf['remote_port']),
                        version=snmp.Version[self._conf['snmp_version']])
                except Exception as e:
                    mlog.warning('creating manager failed %s', e, exc_info=e)
                if self._manager:
                    self._manager._async_group.spawn(self._polling_loop)
                    await self._manager.wait_closed()
                self._register_status('DISCONNECTED')
                self._manager = None
                await asyncio.sleep(self._conf['connect_delay'])
        finally:
            mlog.debug('closing device')
            with contextlib.suppress(ConnectionError):
                self._register_status('DISCONNECTED')
            self.close()
            if self._manager:
                await aio.uncancellable(self._manager.async_close())

    async def _polling_loop(self):
        try:
            while True:
                for oid in self._polling_oids:
                    req = snmp.GetDataReq(names=[_oid_from_str(oid)])
                    resp = await self._request(req)
                    if resp is None:
                        break
                    if self._status != 'CONNECTED':
                        self._register_status('CONNECTED')
                    try:
                        event = self._response_to_event(resp, oid)
                    except Exception as e:
                        mlog.warning(
                            'response ignored due to: %s', e, exc_info=e)
                        continue
                    if event:
                        self._event_client.register([event])
                await asyncio.sleep(self._conf['polling_delay'])
        finally:
            mlog.warning('closing manager...')
            self._register_status('DISCONNECTED')
            self._manager.close()

    async def _request(self, request):
        for i in range(self._conf['request_retry_count'] + 1):
            try:
                return await asyncio.wait_for(
                    self._manager.send(request),
                    timeout=self._conf['request_timeout'])
            except asyncio.TimeoutError:
                mlog.warning('request %s/%s timeout', i,
                             self._conf['request_retry_count'])
                await asyncio.sleep(self._conf['request_retry_delay'])
        mlog.warning('request retries exceeded')

    def _register_status(self, status):
        if self._status == status:
            return
        event = hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=status))
        self._event_client.register([event])
        self._status = status

    def _response_to_event(self, response, oid):
        if not self._conf['polling_oids']:
            return
        if self._cache.get(oid) == response:
            return
        if oid not in self._cache:
            cause = 'INTERROGATE'
        else:
            cause = 'CHANGE'
        self._cache[oid] = response
        payload = {'session_id': None,
                   'cause': cause,
                   'data': {'type': _type_from_response(response),
                            'value': _value_from_response(response)}}
        return hat.event.common.RegisterEvent(
                event_type=(*self._event_type_prefix, 'gateway', 'read', oid),
                source_timestamp=None,
                payload=hat.event.common.EventPayload(
                    type=hat.event.common.EventPayloadType.JSON,
                    data=payload))


def _type_from_response(response):
    if _is_error_response(response):
        return 'ERROR'
    resp_data = response[0]
    return resp_data.type.name


def _value_from_response(response):
    if isinstance(response, snmp.Error):
        return response.type.name
    resp_data = response[0]
    if _is_error_response(response):
        return resp_data.type.name
    if resp_data.type in [snmp.DataType.IP_ADDRESS,
                          snmp.DataType.OBJECT_ID]:
        return '.'.join(resp_data.value)
    elif resp_data.type == snmp.DataType.ARBITRARY:
        return resp_data.value.hex()
    return resp_data.value


def _is_error_response(response):
    if isinstance(response, snmp.Error):
        return True
    if response and response[0].type in [
            snmp.DataType.EMPTY,  # TODO check EMPTY
            snmp.DataType.UNSPECIFIED,
            snmp.DataType.NO_SUCH_OBJECT,
            snmp.DataType.NO_SUCH_INSTANCE,
            snmp.DataType.END_OF_MIB_VIEW]:
        return True


def _oid_from_str(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))
