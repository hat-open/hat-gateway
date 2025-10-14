import asyncio
import itertools

import pytest

from hat import aio
from hat import json
from hat import util
from hat.drivers import snmp
from hat.drivers import udp
import hat.event.common

from hat.gateway.devices.snmp.manager import info


device_name = 'device_name'
event_type_prefix = ('gateway', info.type, device_name)

next_event_ids = (hat.event.common.EventId(1, 1, instance)
                  for instance in itertools.count(1))


class EventerClient(aio.Resource):

    def __init__(self, event_cb=None, query_cb=None):
        self._event_cb = event_cb
        self._query_cb = query_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    @property
    def status(self):
        raise NotImplementedError()

    async def register(self, events, with_response=False):
        if self._event_cb:
            for event in events:
                await aio.call(self._event_cb, event)

        if not with_response:
            return

        timestamp = hat.event.common.now()
        return [hat.event.common.Event(id=next(next_event_ids),
                                       type=event.type,
                                       timestamp=timestamp,
                                       source_timestamp=event.source_timestamp,
                                       payload=event.payload)
                for event in events]

    async def query(self, params):
        if not self._query_cb:
            return hat.event.common.QueryResult([], False)

        return await aio.call(self._query_cb, params)


def encode_oid(oid):
    return '.'.join(str(i) for i in oid)


def decode_oid(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))


def assert_status_event(event, status):
    assert event.type == (*event_type_prefix, 'gateway', 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def assert_read_event(event, oid, session_id, cause, data):
    assert event.type == (*event_type_prefix, 'gateway', 'read',
                          encode_oid(oid))
    assert event.source_timestamp is None
    assert event.payload.data == {'session_id': session_id,
                                  'cause': cause,
                                  'data': data}


def assert_write_event(event, oid, session_id, success):
    assert event.type == (*event_type_prefix, 'gateway', 'write',
                          encode_oid(oid))
    assert event.source_timestamp is None
    assert event.payload.data == {'session_id': session_id,
                                  'success': success}


def create_event(event_type, payload_data):
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload_data))


def create_read_event(oid, session_id):
    return create_event((*event_type_prefix, 'system', 'read',
                         encode_oid(oid)),
                        {'session_id': session_id})


def create_write_event(oid, session_id, data):
    return create_event((*event_type_prefix, 'system', 'write',
                         encode_oid(oid)),
                        {'session_id': session_id,
                         'data': data})


@pytest.fixture
def port():
    return util.get_unused_udp_port()


@pytest.fixture
def create_conf(port):

    def create_conf(version,
                    connect_delay=0.01,
                    request_timeout=0.1,
                    request_retry_count=1,
                    request_retry_delay=0.01,
                    polling_delay=0.1,
                    polling_oids=[],
                    string_hex_oids=[],
                    community='community name',
                    engine_id='01 23 45 67 89 ab cd ef',
                    context_name='context name',
                    user_name='some user',
                    auth_type='MD5',
                    auth_password='abc',
                    priv_type='DES',
                    priv_password='xyz'):
        if version in (snmp.Version.V1, snmp.Version.V2C):
            version_conf = {'version': version.name,
                            'community': community}

        elif version == snmp.Version.V3:
            version_conf = {
                'version': 'V3',
                'context': {'engine_id': engine_id,
                            'name': context_name},
                'user': user_name,
                'authentication': {
                    'type': auth_type,
                    'password': auth_password},
                'privacy': {
                    'type': priv_type,
                    'password': priv_password}}

        else:
            raise ValueError('unsupported snmp version')

        return {'name': '',
                **version_conf,
                'remote_host': '127.0.0.1',
                'remote_port': port,
                'connect_delay': connect_delay,
                'request_timeout': request_timeout,
                'request_retry_count': request_retry_count,
                'request_retry_delay': request_retry_delay,
                'polling_delay': polling_delay,
                'polling_oids': [encode_oid(i) for i in polling_oids],
                'string_hex_oids': [encode_oid(i) for i in string_hex_oids]}

    return create_conf


@pytest.fixture
async def create_agent(port):

    async def create_agent(v1_request_cb=None,
                           v2c_request_cb=None,
                           v3_request_cb=None,
                           authoritative_engine_id=None,
                           users=[]):
        address = udp.Address('127.0.0.1', port)
        agent = await snmp.create_agent(
            local_addr=address,
            v1_request_cb=v1_request_cb,
            v2c_request_cb=v2c_request_cb,
            v3_request_cb=v3_request_cb,
            authoritative_engine_id=authoritative_engine_id,
            users=users)
        return agent

    return create_agent


@pytest.mark.parametrize('version', snmp.Version)
def test_conf(version, create_conf):
    conf = create_conf(version)
    validator = json.DefaultSchemaValidator(info.json_schema_repo)
    validator.validate(info.json_schema_id, conf)


@pytest.mark.parametrize('version', snmp.Version)
async def test_create(version, create_conf):
    conf = create_conf(version)

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    assert device.is_open

    device.close()
    await device.wait_closing()
    assert device.is_closing
    await device.wait_closed()
    assert device.is_closed

    await eventer_client.async_close()


@pytest.mark.parametrize('version', snmp.Version)
async def test_status(version, create_conf, create_agent):
    conf = create_conf(version)

    if version == snmp.Version.V3:
        user = snmp.User(
            name=conf['user'],
            auth_type=snmp.AuthType[conf['authentication']['type']],
            auth_password=conf['authentication']['password'],
            priv_type=snmp.PrivType[conf['privacy']['type']],
            priv_password=conf['privacy']['password'])
        users = [user]
        engine_id = bytes.fromhex(conf['context']['engine_id'])
    else:
        users = []
        engine_id = b''

    def on_v1_request(addr, comm, req):
        assert comm == conf['community']
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    def on_v2c_request(addr, comm, req):
        assert comm == conf['community']
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    def on_v3_request(addr, usr, ctx, req):
        assert usr == conf['user']
        assert ctx.engine_id == engine_id
        assert ctx.name == conf['context']['name']
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    agent = await create_agent(v1_request_cb=on_v1_request,
                               v2c_request_cb=on_v2c_request,
                               v3_request_cb=on_v3_request,
                               authoritative_engine_id=engine_id,
                               users=users)

    event_queue = aio.Queue()
    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    await agent.async_close()

    event = await event_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    await device.async_close()

    event = await event_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    await eventer_client.async_close()


@pytest.mark.parametrize('version', snmp.Version)
async def test_polling(version, create_conf, create_agent):
    event_queue = aio.Queue()

    int_oid = 1, 2, 3
    str_oid = 1, 3, 2

    conf = create_conf(version,
                       polling_oids=[int_oid, str_oid],
                       request_retry_count=1,
                       request_retry_delay=0.005,
                       polling_delay=0.01)

    if version == snmp.Version.V3:
        user = snmp.User(
            name=conf['user'],
            auth_type=snmp.AuthType[conf['authentication']['type']],
            auth_password=conf['authentication']['password'],
            priv_type=snmp.PrivType[conf['privacy']['type']],
            priv_password=conf['privacy']['password'])
        users = [user]
        engine_id = bytes.fromhex(conf['context']['engine_id'])
    else:
        users = []
        engine_id = b''

    next_values = itertools.count(0)

    def on_request(addr, comm, req):
        if req.names[0] == int_oid:
            return [snmp.IntegerData(name=int_oid,
                                     value=next(next_values))]

        if req.names[0] == str_oid:
            return [snmp.StringData(name=str_oid,
                                    value=str(next(next_values)).encode())]

        raise Exception('unexpected oid')

    def on_v3_request(addr, usr, ctx, req):
        if req.names[0] == int_oid:
            return [snmp.IntegerData(name=int_oid,
                                     value=next(next_values))]

        if req.names[0] == str_oid:
            return [snmp.StringData(name=str_oid,
                                    value=str(next(next_values)).encode())]

        raise Exception('unexpected oid')

    agent = await create_agent(v1_request_cb=on_request,
                               v2c_request_cb=on_request,
                               v3_request_cb=on_v3_request,
                               authoritative_engine_id=engine_id,
                               users=users)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    for i in range(10):
        oid = int_oid if i % 2 == 0 else str_oid
        cause = 'INTERROGATE' if i < 2 else 'CHANGE'
        data_type = 'INTEGER' if i % 2 == 0 else 'STRING'
        data_value = i if i % 2 == 0 else str(i)
        data = {'type': data_type,
                'value': data_value}

        event = await event_queue.get()
        assert_read_event(event, oid, None, cause, data)

    # cause is INTERROGATE on connection restart
    await agent.async_close()
    event = await event_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    agent = await create_agent(v1_request_cb=on_request,
                               v2c_request_cb=on_request,
                               v3_request_cb=on_v3_request,
                               authoritative_engine_id=engine_id,
                               users=users)
    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')
    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    for i in range(10):
        cause = 'INTERROGATE' if i < 2 else 'CHANGE'
        event = await event_queue.get()
        assert event.payload.data['cause'] == cause

    await device.async_close()
    await agent.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize('version', snmp.Version)
async def test_disconnect_on_error_oid(create_conf, create_agent,
                                       version):
    connect_delay = 0.05

    conf = create_conf(version,
                       connect_delay=connect_delay,
                       polling_oids=[(1, 2, 3)])

    event_queue = aio.Queue()
    event_client = EventerClient(event_cb=event_queue.put_nowait)

    error_oid = (1, 3, 6, 1, 6, 3, 15, 1, 1, 1)

    def on_request(addr, comm, req):
        return [snmp.CounterData(name=error_oid,
                                 value=654321)]

    def on_v3_request(addr, usr, ctx, req):
        return [snmp.CounterData(name=error_oid,
                                 value=654321)]

    agent = await create_agent(v1_request_cb=on_request,
                               v2c_request_cb=on_request,
                               v3_request_cb=on_v3_request)

    device = await aio.call(info.create, conf, event_client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    await asyncio.sleep(connect_delay * 0.9)
    assert event_queue.empty()
    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    await device.async_close()
    await agent.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("res, oid, data", [
    ([snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123}),

    ([snmp.UnsignedData(name=(1, 2, 3),
                        value=123)],
     (1, 2, 3),
     {'type': 'UNSIGNED',
      'value': 123}),

    ([snmp.CounterData(name=(1, 2, 3),
                       value=321)],
     (1, 2, 3),
     {'type': 'COUNTER',
      'value': 321}),

    ([snmp.BigCounterData(name=(1, 2, 3),
                          value=123456)],
     (1, 2, 3),
     {'type': 'BIG_COUNTER',
      'value': 123456}),

    ([snmp.StringData(name=(1, 2, 3),
                      value=b'abc')],
     (1, 2, 3),
     {'type': 'STRING',
      'value': 'abc'}),

    ([snmp.StringData(name=(1, 2, 3),
                      value=b'abc')],
     (1, 2, 3),
     {'type': 'STRING_HEX',
      'value': '616263'}),

    ([snmp.ObjectIdData(name=(1, 2, 3),
                        value=(1, 2, 3, 4, 5, 6))],
     (1, 2, 3),
     {'type': 'OBJECT_ID',
      'value': '1.2.3.4.5.6'}),

    ([snmp.IpAddressData(name=(1, 2, 3),
                         value=(192, 168, 0, 1))],
     (1, 2, 3),
     {'type': 'IP_ADDRESS',
      'value': '192.168.0.1'}),

    ([snmp.TimeTicksData(name=(1, 2, 3),
                         value=42)],
     (1, 2, 3),
     {'type': 'TIME_TICKS',
      'value': 42}),

    ([snmp.ArbitraryData(name=(1, 2, 3),
                         value=b'\x01\x02\x03')],
     (1, 2, 3),
     {'type': 'ARBITRARY',
      'value': '010203'}),

    *((snmp.Error(err, 1),
       (1, 2, 3),
       {'type': 'ERROR',
        'value': err.name})
      for err in list(snmp.ErrorType)[1:]),

    ([snmp.UnspecifiedData(name=(1, 2, 3))],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'UNSPECIFIED'}),

    ([snmp.NoSuchObjectData(name=(1, 2, 3))],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'NO_SUCH_OBJECT'}),

    ([snmp.NoSuchInstanceData(name=(1, 2, 3))],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'NO_SUCH_INSTANCE'}),

    ([snmp.EndOfMibViewData(name=(1, 2, 3))],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'END_OF_MIB_VIEW'}),
    # response oid does not correspond to requested
    ([snmp.StringData(name=(1, 2, 4),
                      value=b'abc')],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'GEN_ERR'}),
    # empty response
    ([],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'GEN_ERR'}),
    # error oids - RFC 3414 - Statistics for the User-based Security Model
    ([snmp.CounterData(name=(1, 3, 6, 1, 6, 3, 15, 1, 1, 2),
                       value=54321)],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'NOT_IN_TIME_WINDOWS'}),

    ([snmp.CounterData(name=(1, 3, 6, 1, 6, 3, 15, 1, 1, 6),
                       value=54321)],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'DECRYPTION_ERRORS'}),
])
async def test_read(create_conf, create_agent, res, oid, data):
    version = snmp.Version.V2C
    session_id = 42
    event_queue = aio.Queue()

    string_hex_oids = [oid] if data['type'] == 'STRING_HEX' else []

    conf = create_conf(version,
                       string_hex_oids=string_hex_oids)

    def on_request(addr, comm, req):
        return res

    def on_v3_request(addr, usr, ctx, req):
        return res

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    agent = await create_agent(v1_request_cb=on_request,
                               v2c_request_cb=on_request,
                               v3_request_cb=on_v3_request)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    event = create_read_event(oid, session_id)
    await aio.call(device.process_events, [event])

    event = await event_queue.get()
    assert_read_event(event, oid, session_id, 'REQUESTED', data)

    await device.async_close()
    await agent.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize("req_data, resp_data, oid, data, success", [
    ([snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     snmp.Error(snmp.ErrorType.READ_ONLY, 1),
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123},
     False),

    ([snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     [snmp.IntegerData(name=(1, 2, 4),
                       value=-123)],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123},
     False),

    ([snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     [snmp.NoSuchInstanceData(name=(1, 2, 3))],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123},
     False),

    ([snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     [snmp.UnspecifiedData(name=(1, 2, 3))],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123},
     False),

    ([snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     [],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123},
     True),

    ([snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     [snmp.IntegerData(name=(1, 2, 3),
                       value=-123)],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123},
     True),

    ([snmp.UnsignedData(name=(1, 2, 3),
                        value=123)],
     [snmp.UnsignedData(name=(1, 2, 3),
                        value=123)],
     (1, 2, 3),
     {'type': 'UNSIGNED',
      'value': 123},
     True),

    ([snmp.CounterData(name=(1, 2, 3),
                       value=321)],
     [snmp.CounterData(name=(1, 2, 3),
                       value=321)],
     (1, 2, 3),
     {'type': 'COUNTER',
      'value': 321},
     True),

    ([snmp.BigCounterData(name=(1, 2, 3),
                          value=123456)],
     [snmp.BigCounterData(name=(1, 2, 3),
                          value=123456)],
     (1, 2, 3),
     {'type': 'BIG_COUNTER',
      'value': 123456},
     True),

    ([snmp.StringData(name=(1, 2, 3),
                      value=b'abc')],
     [snmp.StringData(name=(1, 2, 3),
                      value=b'abc')],
     (1, 2, 3),
     {'type': 'STRING',
      'value': 'abc'},
     True),

    ([snmp.StringData(name=(1, 2, 3),
                      value=b'abc')],
     [snmp.StringData(name=(1, 2, 3),
                      value=b'abc')],
     (1, 2, 3),
     {'type': 'STRING_HEX',
      'value': '616263'},
     True),

    ([snmp.ObjectIdData(name=(1, 2, 3),
                        value=(1, 2, 3, 4, 5, 6))],
     [snmp.ObjectIdData(name=(1, 2, 3),
                        value=(1, 2, 3, 4, 5, 6))],
     (1, 2, 3),
     {'type': 'OBJECT_ID',
      'value': '1.2.3.4.5.6'},
     True),

    ([snmp.IpAddressData(name=(1, 2, 3),
                         value=(192, 168, 0, 1))],
     [snmp.IpAddressData(name=(1, 2, 3),
                         value=(192, 168, 0, 1))],
     (1, 2, 3),
     {'type': 'IP_ADDRESS',
      'value': '192.168.0.1'},
     True),

    ([snmp.TimeTicksData(name=(1, 2, 3),
                         value=42)],
     [snmp.TimeTicksData(name=(1, 2, 3),
                         value=42)],
     (1, 2, 3),
     {'type': 'TIME_TICKS',
      'value': 42},
     True),

    ([snmp.ArbitraryData(name=(1, 2, 3),
                         value=b'\x01\x02\x03')],
     [snmp.ArbitraryData(name=(1, 2, 3),
                         value=b'\x01\x02\x03')],
     (1, 2, 3),
     {'type': 'ARBITRARY',
      'value': '010203'},
     True),
])
async def test_write(create_conf, create_agent,
                     req_data, resp_data, oid, data, success):
    version = snmp.Version.V2C
    session_id = 42
    event_queue = aio.Queue()

    conf = create_conf(version)

    def on_request(addr, comm, req):
        if not isinstance(req, snmp.SetDataReq):
            return []

        assert req.data == req_data

        return resp_data

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    agent = await create_agent(v2c_request_cb=on_request)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    event = create_write_event(oid, session_id, data)
    await aio.call(device.process_events, [event])

    event = await event_queue.get()
    assert_write_event(event, oid, session_id, success)

    await device.async_close()
    await agent.async_close()
    await eventer_client.async_close()
