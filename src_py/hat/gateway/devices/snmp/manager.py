"""SNMP manager device"""

from collections.abc import Collection
import asyncio
import logging

from hat import aio
from hat import util
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
        self._eventer_client = eventer_client
        self._event_type_prefix = event_type_prefix
        self._manager = None
        self._status = None
        self._cache = {}
        self._polling_oids = ([_oid_from_str(oid_str)
                               for oid_str in conf['polling_oids']]
                              if conf['polling_oids']
                              else [(0, 0)])
        self._string_hex_oids = set(_oid_from_str(oid_str)
                                    for oid_str in conf['string_hex_oids'])
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
                        self._manager = await _create_manager(self._conf)

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
                    connect_delay = (0 if self._status == 'CONNECTED'
                                     else self._conf['connect_delay'])
                    await self._register_status('DISCONNECTED')

                self._manager = None
                self._cache = {}
                await asyncio.sleep(connect_delay)

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
                    req = snmp.GetDataReq(names=[oid])
                    resp = await self._send(req)

                    try:
                        _verify_read_response(resp=resp,
                                              oid=oid)

                    except Exception as e:
                        mlog.warning('connection closing, error response: %s',
                                     e, exc_info=e)
                        return

                    await self._register_status('CONNECTED')
                    if (not self._conf['polling_oids'] or
                            self._cache.get(oid) == resp):
                        continue

                    cause = ('CHANGE' if oid in self._cache
                             else 'INTERROGATE')
                    self._cache[oid] = resp
                    mlog.debug('polling oid %s', oid)
                    try:
                        event = self._response_to_read_event(resp=resp,
                                                             oid=oid,
                                                             cause=cause,
                                                             session_id=None)

                    except Exception as e:
                        mlog.warning('response %s ignored due to: %s',
                                     resp, e, exc_info=e)
                        continue

                    await self._eventer_client.register([event])

                await asyncio.sleep(self._conf['polling_delay'])

        except Exception as e:
            mlog.error('polling loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing manager')
            self._manager.close()

    async def _process_event(self, event):
        if self._manager is None or not self._manager.is_open:
            raise Exception('connection not established')

        etype_suffix = event.type[len(self._event_type_prefix):]

        if etype_suffix[:2] == ('system', 'read'):
            oid = _oid_from_str(etype_suffix[2])
            await self._process_read_event(event=event,
                                           oid=oid)

        elif etype_suffix[:2] == ('system', 'write'):
            oid = _oid_from_str(etype_suffix[2])
            await self._process_write_event(event=event,
                                            oid=oid)

        else:
            raise Exception('event type not supported')

    async def _process_read_event(self, event, oid):
        mlog.debug('read request for oid %s', oid)
        req = snmp.GetDataReq(names=[oid])
        try:
            resp = await self._send(req)

        except Exception:
            self._manager.close()
            raise

        mlog.debug('read response for oid %s: %s', oid, resp)
        session_id = event.payload.data['session_id']

        try:
            event = self._response_to_read_event(resp=resp,
                                                 oid=oid,
                                                 cause='REQUESTED',
                                                 session_id=session_id)

        except Exception as e:
            mlog.warning('response ignored due to: %s', e, exc_info=e)
            return

        await self._eventer_client.register([event])

    async def _process_write_event(self, event, oid):
        set_data = _event_data_to_snmp_data(data=event.payload.data['data'],
                                            oid=oid)
        mlog.debug('write request for oid %s: %s', oid, set_data)
        try:
            resp = await asyncio.wait_for(
                self._manager.send(snmp.SetDataReq(data=[set_data])),
                timeout=self._conf['request_timeout'])

        except asyncio.TimeoutError:
            mlog.warning('set data request %s timeout', set_data)
            return

        session_id = event.payload.data['session_id']
        success = _is_write_response_success(resp=resp,
                                             oid=oid)
        mlog.debug('write for oid %s %s',
                   oid, ('succeeded' if success else 'failed'))
        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'write',
                  _oid_to_str(oid)),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson({
                'session_id': session_id,
                'success': success}))
        await self._eventer_client.register([event])

    async def _send(self, req):
        for i in range(self._conf['request_retry_count'] + 1):
            try:
                return await asyncio.wait_for(
                    self._manager.send(req),
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

    def _response_to_read_event(self, resp, oid, cause, session_id):
        data = _event_data_from_response(resp=resp,
                                         oid=oid,
                                         string_hex_oids=self._string_hex_oids)
        payload = {'session_id': session_id,
                   'cause': cause,
                   'data': data}
        return hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway', 'read',
                      _oid_to_str(oid)),
                source_timestamp=None,
                payload=hat.event.common.EventPayloadJson(payload))


info = common.DeviceInfo(
    type='snmp_manager',
    create=SnmpManagerDevice,
    json_schema_id="hat-gateway://snmp.yaml#/$defs/manager",
    json_schema_repo=common.json_schema_repo)


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

    if conf['version'] == 'V3':
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
                               if conf['authentication'] else None),
                    auth_password=(conf['authentication']['password']
                                   if conf['authentication'] else None),
                    priv_type=(snmp.PrivType[conf['privacy']['type']]
                               if conf['privacy'] else None),
                    priv_password=(conf['privacy']['password']
                                   if conf['privacy'] else None))),
            timeout=conf['request_timeout'])

    raise Exception('unknown version')


def _verify_read_response(resp, oid):
    if isinstance(resp, snmp.Error):
        return

    resp_data = util.first(resp, lambda i: i.name == oid)
    if resp_data:
        return

    if not resp:
        return

    resp_data = resp[0]
    if resp_data.name[:10] in _conn_close_oids:
        raise Exception('unsupported security levels')


def _is_write_response_success(resp, oid):
    if isinstance(resp, snmp.Error):
        if resp.type == snmp.ErrorType.NO_ERROR:
            return True

        return False

    if not resp:
        return True

    resp_data = util.first(resp, lambda i: i.name == oid)
    if not resp_data:
        return False

    if isinstance(resp_data,
                  (snmp.EmptyData,
                   snmp.UnspecifiedData,
                   snmp.NoSuchObjectData,
                   snmp.NoSuchInstanceData,
                   snmp.EndOfMibViewData)):
        return False

    return True


def _event_data_from_response(resp, oid, string_hex_oids):
    if isinstance(resp, snmp.Error):
        if resp.type == snmp.ErrorType.NO_ERROR:
            raise Exception('received unexpected error type NO_ERROR')

        return {'type': 'ERROR',
                'value': resp.type.name}

    data = util.first(resp, lambda i: i.name == oid)
    if data is None:
        if resp and resp[0].name[:10] in _error_oids:
            value = _error_oids[resp[0].name]

        else:
            value = 'GEN_ERR'

        return {'type': 'ERROR',
                'value': value}

    if isinstance(data, snmp.EmptyData):
        return {'type': 'ERROR',
                'value': 'EMPTY'}

    if isinstance(data, snmp.UnspecifiedData):
        return {'type': 'ERROR',
                'value': 'UNSPECIFIED'}

    if isinstance(data, snmp.NoSuchObjectData):
        return {'type': 'ERROR',
                'value': 'NO_SUCH_OBJECT'}

    if isinstance(data, snmp.NoSuchInstanceData):
        return {'type': 'ERROR',
                'value': 'NO_SUCH_INSTANCE'}

    if isinstance(data, snmp.EndOfMibViewData):
        return {'type': 'ERROR',
                'value': 'END_OF_MIB_VIEW'}

    if isinstance(data, snmp.IntegerData):
        return {'type': 'INTEGER',
                'value': data.value}

    if isinstance(data, snmp.UnsignedData):
        return {'type': 'UNSIGNED',
                'value': data.value}

    if isinstance(data, snmp.CounterData):
        return {'type': 'COUNTER',
                'value': data.value}

    if isinstance(data, snmp.BigCounterData):
        return {'type': 'BIG_COUNTER',
                'value': data.value}

    if isinstance(data, snmp.TimeTicksData):
        return {'type': 'TIME_TICKS',
                'value': data.value}

    if isinstance(data, snmp.StringData):
        if oid in string_hex_oids:
            return {'type': 'STRING_HEX',
                    'value': data.value.hex()}

        return {'type': 'STRING',
                'value': str(data.value, encoding='utf-8', errors='replace')}

    if isinstance(data, snmp.ObjectIdData):
        return {'type': 'OBJECT_ID',
                'value': _oid_to_str(data.value)}

    if isinstance(data, snmp.IpAddressData):
        return {'type': 'IP_ADDRESS',
                'value': '.'.join(str(i) for i in data.value)}

    if isinstance(data, snmp.ArbitraryData):
        return {'type': 'ARBITRARY',
                'value': data.value.hex()}

    raise Exception('invalid response data')


def _event_data_to_snmp_data(data, oid):
    data_type = data['type']
    data_value = data['value']

    if data_type == 'INTEGER':
        return snmp.IntegerData(name=oid,
                                value=data_value)

    if data_type == 'UNSIGNED':
        return snmp.UnsignedData(name=oid,
                                 value=data_value)

    if data_type == 'COUNTER':
        return snmp.CounterData(name=oid,
                                value=data_value)

    if data_type == 'BIG_COUNTER':
        return snmp.BigCounterData(name=oid,
                                   value=data_value)

    if data_type == 'STRING':
        return snmp.StringData(name=oid,
                               value=data_value.encode())

    if data_type == 'STRING_HEX':
        return snmp.StringData(name=oid,
                               value=bytes.fromhex(data_value))

    if data_type == 'OBJECT_ID':
        return snmp.ObjectIdData(name=oid,
                                 value=_oid_from_str(data_value))

    if data_type == 'IP_ADDRESS':
        return snmp.IpAddressData(
            name=oid,
            value=tuple(int(i) for i in data_value.split('.')))

    if data_type == 'TIME_TICKS':
        return snmp.TimeTicksData(name=oid,
                                  value=data_value)

    if data_type == 'ARBITRARY':
        return snmp.ArbitraryData(name=oid,
                                  value=bytes.fromhex(data_value))

    raise Exception('invalid data type')


def _oid_from_str(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))


def _oid_to_str(oid):
    return '.'.join(str(i) for i in oid)


_error_oids = {
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 2): 'NOT_IN_TIME_WINDOWS',
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 3): 'UNKNOWN_USER_NAMES',
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 4): 'UNKNOWN_ENGINE_IDS',
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 5): 'WRONG_DIGESTS',
    (1, 3, 6, 1, 6, 3, 15, 1, 1, 6): 'DECRYPTION_ERRORS'}

_conn_close_oids = {(1, 3, 6, 1, 6, 3, 15, 1, 1, 1)}
