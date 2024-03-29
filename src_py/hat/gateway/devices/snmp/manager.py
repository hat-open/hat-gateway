"""SNMP manager device"""

from collections.abc import Collection
import asyncio
import logging

from hat import aio
from hat.drivers import snmp
from hat.drivers import udp
import hat.event.common
import hat.event.eventer

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)


class SnmpManagerDevice(common.Device):

    def __init__(self,
                 conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix):
        self._conf = conf
        self._event_type_prefix = event_type_prefix
        self._eventer_client = eventer_client
        self._manager = None
        self._status = None
        self._cache = {}
        self._polling_oids = conf['polling_oids'] or ["0.0"]
        self._async_group = aio.Group()

        self.async_group.spawn(self._connection_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        for event in events:
            try:
                await self._process_event(event)

            except Exception as e:
                mlog.warning('event processing error: %s', e, exc_info=e)

    async def _connection_loop(self):
        try:
            while True:
                await self._register_status('CONNECTING')
                mlog.debug('connecting to %s:%s',
                           self._conf['remote_host'],
                           self._conf['remote_port'])

                try:
                    try:
                        self._manager = await snmp.create_manager(
                            context=snmp.Context(
                                engine_id=self._conf['snmp_context']['engine_id'],  # NOQA
                                name=self._conf['snmp_context']['name']),
                            remote_addr=udp.Address(
                                host=self._conf['remote_host'],
                                port=self._conf['remote_port']),
                            version=snmp.Version[self._conf['snmp_version']])

                    except Exception as e:
                        mlog.warning('creating manager failed %s', e,
                                     exc_info=e)

                    if self._manager:
                        mlog.debug('connected to %s:%s',
                                   self._conf['remote_host'],
                                   self._conf['remote_port'])
                        self._manager.async_group.spawn(self._polling_loop)
                        await self._manager.wait_closed()

                finally:
                    await self._register_status('DISCONNECTED')

                self._manager = None
                self._cache = {}
                await asyncio.sleep(self._conf['connect_delay'])

        except Exception as e:
            mlog.error('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing device')
            self.close()
            if self._manager:
                await aio.uncancellable(self._manager.async_close())

    async def _polling_loop(self):
        try:
            while True:
                for oid in self._polling_oids:
                    req = snmp.GetDataReq(names=[_oid_from_str(oid)])
                    resp = await self._request(req)
                    await self._register_status('CONNECTED')
                    if (not self._conf['polling_oids'] or
                            self._cache.get(oid) == resp):
                        continue
                    if oid not in self._cache:
                        cause = 'INTERROGATE'
                    else:
                        cause = 'CHANGE'
                    self._cache[oid] = resp
                    mlog.debug('polling oid %s resulted resp %s', oid, resp)
                    try:
                        event = self._event_from_response(resp, oid, cause)
                    except Exception as e:
                        mlog.warning('response %s ignored due to: %s',
                                     resp, e, exc_info=e)
                        continue
                    await self._eventer_client.register([event])
                await asyncio.sleep(self._conf['polling_delay'])
        except Exception as e:
            mlog.warning('polling loop error: %s', e, exc_info=e)
        finally:
            mlog.debug('closing manager')
            self._manager.close()

    async def _process_event(self, event):
        etype_suffix = event.type[len(self._event_type_prefix):]
        if etype_suffix[:2] == ('system', 'read'):
            oid = etype_suffix[2]
            await self._process_read_event(event, oid)
        elif etype_suffix[:2] == ('system', 'write'):
            oid = etype_suffix[2]
            await self._process_write_event(event, oid)
        else:
            raise Exception('event type not supported')

    async def _process_read_event(self, event, oid):
        mlog.debug('read request for oid %s', oid)
        req = snmp.GetDataReq(names=[_oid_from_str(oid)])
        try:
            resp = await self._request(req)
        except Exception:
            self._manager.close()
            raise
        mlog.debug('read response for oid %s: %s', oid, resp)
        session_id = event.payload.data['session_id']
        event = self._event_from_response(resp, oid, 'REQUESTED', session_id)
        await self._eventer_client.register([event])

    async def _process_write_event(self, event, oid):
        set_data = _data_from_event(event, oid)
        mlog.debug('write request for oid %s: %s', oid, set_data)
        try:
            resp = await asyncio.wait_for(
                self._manager.send(snmp.SetDataReq(data=[set_data])),
                timeout=self._conf['request_timeout'])
        except asyncio.TimeoutError:
            mlog.warning('set data request %s timeout', set_data)
            return
        session_id = event.payload.data['session_id']
        success = not _is_error_response(resp)
        mlog.debug('write for oid %s %s, response: %s',
                   oid, 'succeeded' if success else 'failed', resp)
        event = hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway', 'write', oid),
                source_timestamp=None,
                payload=hat.event.common.EventPayloadJson({
                    'session_id': session_id,
                    'success': success}))
        await self._eventer_client.register([event])

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
        raise Exception('request retries exceeded')

    async def _register_status(self, status):
        if self._status == status:
            return
        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson(status))
        await self._eventer_client.register([event])
        mlog.debug("device status %s -> %s", self._status, status)
        self._status = status

    def _event_from_response(self, response, oid, cause, session_id=None):
        payload = {'session_id': session_id,
                   'cause': cause,
                   'data': {'type': _event_type_from_response(response),
                            'value': _event_value_from_response(response)}}
        return hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway', 'read', oid),
                source_timestamp=None,
                payload=hat.event.common.EventPayloadJson(payload))


info = common.DeviceInfo(
    type='snmp_manager',
    create=SnmpManagerDevice,
    json_schema_id="hat-gateway://snmp.yaml#/$defs/manager",
    json_schema_repo=common.json_schema_repo)


def _event_type_from_response(response):
    if _is_error_response(response):
        return 'ERROR'
    resp_data = response[0]
    return resp_data.type.name


def _event_value_from_response(response):
    if isinstance(response, snmp.Error):
        return response.type.name
    resp_data = response[0]
    if _is_error_response(response):
        return resp_data.type.name
    if resp_data.type in [snmp.DataType.IP_ADDRESS,
                          snmp.DataType.OBJECT_ID]:
        return '.'.join(str(i) for i in resp_data.value)
    elif resp_data.type == snmp.DataType.ARBITRARY:
        return resp_data.value.hex()
    return resp_data.value


def _is_error_response(response):
    if isinstance(response, snmp.Error):
        if response.type == snmp.ErrorType.NO_ERROR:
            return False
        return True
    if response and response[0].type in [
            snmp.DataType.EMPTY,
            snmp.DataType.UNSPECIFIED,
            snmp.DataType.NO_SUCH_OBJECT,
            snmp.DataType.NO_SUCH_INSTANCE,
            snmp.DataType.END_OF_MIB_VIEW]:
        return True


def _data_from_event(event, oid):
    return snmp.Data(
        type=snmp.DataType[event.payload.data['data']['type']],
        name=_oid_from_str(oid),
        value=_value_from_event(event))


def _value_from_event(event):
    data = event.payload.data['data']
    if data['type'] in ['INTEGER',
                        'UNSIGNED',
                        'COUNTER',
                        'BIG_COUNTER',
                        'TIME_TICKS',
                        'STRING']:
        return data['value']
    elif data['type'] in ['OBJECT_ID',
                          'IP_ADDRESS']:
        return tuple(int(i) for i in data['value'].split('.'))
    elif data['type'] == 'ARBITRARY':
        return bytes.fromhex(data['value'])
    raise Exception(f"unsupported data type {data['type']} in write event")


def _oid_from_str(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))
