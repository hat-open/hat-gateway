"""SNMP manager device"""

import asyncio
import contextlib
import logging

from hat import aio
from hat import json
from hat.drivers import snmp
from hat.drivers import udp
import hat.event.common

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)

device_type: str = 'snmp_manager'

json_schema_id: str = "hat-gateway://snmp.yaml#/$defs/manager"

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
    device._status = None
    device._cache = {}
    device._polling_oids = conf['polling_oids'] or ["0.0"]

    device._async_group = aio.Group()
    device._async_group.spawn(device._connection_loop)
    device._async_group.spawn(device._event_loop)

    return device


class SnmpManagerDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _connection_loop(self):
        try:
            while True:
                self._register_status('CONNECTING')
                mlog.debug('connecting to %s:%s', self._conf['remote_host'],
                           self._conf['remote_port'])
                try:
                    self._manager = await _create_manager(self._conf)
                except Exception as e:
                    mlog.warning('creating manager failed %s', e, exc_info=e)
                    self._register_status('DISCONNECTED')
                    await asyncio.sleep(self._conf['connect_delay'])
                    continue

                mlog.debug('connected to %s:%s', self._conf['remote_host'],
                           self._conf['remote_port'])
                self._manager.async_group.spawn(self._polling_loop)
                await self._manager.wait_closed()

                self._register_status('DISCONNECTED')
                self._manager = None
                self._cache = {}

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
                    self._register_status('CONNECTED')
                    if (not self._conf['polling_oids'] or
                            self._cache.get(oid) == resp):
                        continue

                    cause = 'CHANGE' if oid in self._cache else 'INTERROGATE'
                    self._cache[oid] = resp
                    mlog.debug('polling oid %s', oid)
                    try:
                        event = self._event_from_response(resp, oid, cause)
                    except Exception as e:
                        mlog.warning('response ignored due to: %s',
                                     e, exc_info=e)
                        continue

                    self._event_client.register([event])
                await asyncio.sleep(self._conf['polling_delay'])

        except Exception as e:
            mlog.warning('polling loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing manager')
            self._manager.close()

    async def _event_loop(self):
        try:
            while True:
                events = await self._event_client.receive()
                self._async_group.spawn(self._process_events, events)
        except ConnectionError:
            mlog.debug('connection to event server closed')
        finally:
            mlog.debug('closing device')
            self.close()

    async def _process_events(self, events):
        for event in events:
            try:
                await self._process_event(event)
            except Exception as e:
                mlog.warning('event processing error: %s', e, exc_info=e)

    async def _process_event(self, event):
        etype_suffix = event.event_type[len(self._event_type_prefix):]
        if etype_suffix[:2] == ('system', 'read'):
            await self._process_read_event(event)
        elif etype_suffix[:2] == ('system', 'write'):
            await self._process_write_event(event)
        else:
            raise Exception('event type not supported')

    async def _process_read_event(self, event):
        if self._manager is None:
            raise Exception('connection not established')

        oid = _oid_from_event(event)
        mlog.debug('read request for oid %s', oid)
        req = snmp.GetDataReq(names=[_oid_from_str(oid)])
        try:
            resp = await self._request(req)
        except Exception:
            self._manager.close()
            raise
        mlog.debug('read response for oid %s: %s', oid, resp)
        session_id = event.payload.data['session_id']

        try:
            event = self._event_from_response(
                resp, oid, 'REQUESTED', session_id)
        except Exception as e:
            mlog.warning('response ignored due to: %s', e, exc_info=e)
            return

        self._event_client.register([event])

    async def _process_write_event(self, event):
        if self._manager is None:
            raise Exception('connection not established')

        oid = _oid_from_event(event)
        set_data = _data_from_write_event(event)
        mlog.debug('write request for oid %s: %s', oid, set_data)
        try:
            resp = await aio.wait_for(
                self._manager.send(snmp.SetDataReq(data=[set_data])),
                timeout=self._conf['request_timeout'])
        except asyncio.TimeoutError:
            mlog.warning('set data request %s timeout', set_data)
            return

        session_id = event.payload.data['session_id']
        success = not _is_error_response(resp)
        mlog.debug('write for oid %s %s',
                   oid, 'succeeded' if success else 'failed')
        event = hat.event.common.RegisterEvent(
                event_type=(*self._event_type_prefix, 'gateway', 'write', oid),
                source_timestamp=None,
                payload=hat.event.common.EventPayload(
                    type=hat.event.common.EventPayloadType.JSON,
                    data={'session_id': session_id,
                          'success': success}))
        self._event_client.register([event])

    async def _request(self, request):
        for i in range(self._conf['request_retry_count'] + 1):
            try:
                return await aio.wait_for(
                    self._manager.send(request),
                    timeout=self._conf['request_timeout'])
            except asyncio.TimeoutError:
                mlog.warning('request %s/%s timeout', i,
                             self._conf['request_retry_count'])
                await asyncio.sleep(self._conf['request_retry_delay'])
        raise Exception('request retries exceeded')

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
        mlog.debug("device status %s -> %s", self._status, status)
        self._status = status

    def _event_from_response(self, response, oid, cause, session_id=None):
        if not response:
            raise Exception('empty response')

        payload = {'session_id': session_id,
                   'cause': cause,
                   'data': {'type': _event_type_from_response(response),
                            'value': _event_value_from_response(response)}}
        return hat.event.common.RegisterEvent(
                event_type=(*self._event_type_prefix, 'gateway', 'read', oid),
                source_timestamp=None,
                payload=hat.event.common.EventPayload(
                    type=hat.event.common.EventPayloadType.JSON,
                    data=payload))


async def _create_manager(conf):
    if conf['version'] == 'V1':
        return await snmp.create_v1_manager(
            remote_addr=udp.Address(
                host=conf['remote_host'],
                port=conf['remote_port']),
            community=conf['community'])

    if conf['version'] == 'V2C':
        return await snmp.create_v2c_manager(
            remote_addr=udp.Address(
                host=conf['remote_host'],
                port=conf['remote_port']),
            community=conf['community'])

    elif conf['version'] == 'V3':
        return await aio.wait_for(
            snmp.create_v3_manager(
                remote_addr=udp.Address(
                    host=conf['remote_host'],
                    port=conf['remote_port']),
                context=snmp.Context(
                    engine_id=bytes.fromhex(conf['context']['engine_id']),
                    name=conf['context']['name']) if conf['context'] else None,
                user=snmp.User(
                    name=conf['user'],
                    auth_type=(snmp.AuthType[conf['authentication']['type']]
                               if ['authentication'] else None),
                    auth_password=(conf['authentication']['password']
                                   if ['authentication'] else None),
                    priv_type=(snmp.PrivType[conf['privacy']['type']]
                               if conf['privacy'] else None),
                    priv_password=(conf['privacy']['password']
                                   if conf['privacy'] else None))),
            timeout=conf['request_timeout'])

    raise Exception('unknown version')


def _event_type_from_response(response):
    if _is_error_response(response):
        return 'ERROR'

    resp_data = response[0]
    if isinstance(resp_data, snmp.IntegerData):
        return 'INTEGER'
    if isinstance(resp_data, snmp.UnsignedData):
        return 'UNSIGNED'
    if isinstance(resp_data, snmp.CounterData):
        return 'COUNTER'
    if isinstance(resp_data, snmp.BigCounterData):
        return 'BIG_COUNTER'
    if isinstance(resp_data, snmp.TimeTicksData):
        return 'TIME_TICKS'
    if isinstance(resp_data, snmp.StringData):
        return 'STRING'
    if isinstance(resp_data, snmp.ObjectIdData):
        return 'OBJECT_ID'
    if isinstance(resp_data, snmp.IpAddressData):
        return 'IP_ADDRESS'
    if isinstance(resp_data, snmp.ArbitraryData):
        return 'ARBITRARY'


def _event_value_from_response(response):
    if _is_error_response(response):
        return _error_name_from_error_response(response)

    resp_data = response[0]
    if isinstance(resp_data, (snmp.ObjectIdData, snmp.IpAddressData)):
        return _oid_to_str(resp_data.value)

    elif isinstance(resp_data, snmp.ArbitraryData):
        return resp_data.value.hex()

    return resp_data.value


def _is_error_response(response):
    if isinstance(response, snmp.Error):
        if response.type == snmp.ErrorType.NO_ERROR:
            return False

        return True

    if not response:
        return False

    if isinstance(response[0],
                  (snmp.EmptyData,
                   snmp.UnspecifiedData,
                   snmp.NoSuchObjectData,
                   snmp.NoSuchInstanceData,
                   snmp.EndOfMibViewData)):
        return True

    return False


def _error_name_from_error_response(response):
    if isinstance(response, snmp.Error):
        return response.type.name

    resp_data = response[0]
    if isinstance(resp_data, snmp.EmptyData):
        return 'EMPTY'
    if isinstance(resp_data, snmp.UnspecifiedData):
        return 'UNSPECIFIED'
    if isinstance(resp_data, snmp.NoSuchObjectData):
        return 'NO_SUCH_OBJECT'
    if isinstance(resp_data, snmp.NoSuchInstanceData):
        return 'NO_SUCH_INSTANCE'
    if isinstance(resp_data, snmp.EndOfMibViewData):
        return 'END_OF_MIB_VIEW'

    raise Exception('unexpected error response type')


def _data_from_write_event(event):
    return _data_class_from_event(event)(
        name=_oid_from_str(_oid_from_event(event)),
        value=_value_from_event(event))


def _data_class_from_event(event):
    return {
        'INTEGER': snmp.IntegerData,
        'UNSIGNED': snmp.UnsignedData,
        'COUNTER': snmp.CounterData,
        'BIG_COUNTER': snmp.BigCounterData,
        'STRING': snmp.StringData,
        'OBJECT_ID': snmp.ObjectIdData,
        'IP_ADDRESS': snmp.IpAddressData,
        'TIME_TICKS':  snmp.TimeTicksData,
        'ARBITRARY': snmp.ArbitraryData}[event.payload.data['data']['type']]


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
        return _oid_from_str(data['value'])

    elif data['type'] == 'ARBITRARY':
        return bytes.fromhex(data['value'])

    raise Exception(f"unsupported data type {data['type']} in write event")


def _oid_from_str(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))


def _oid_to_str(oid):
    return '.'.join(str(i) for i in oid)


def _oid_from_event(event):
    return event.event_type[6]
