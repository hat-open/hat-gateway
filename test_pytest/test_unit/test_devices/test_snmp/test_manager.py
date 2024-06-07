import itertools

import pytest

from hat import aio
from hat import util
from hat.drivers import snmp
from hat.drivers import udp
import hat.event.common

from hat.gateway import common
from hat.gateway.devices.snmp import manager


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = 'gateway', gateway_name, manager.device_type, device_name


class EventClient(common.DeviceEventClient):

    def __init__(self, query_result=[]):
        self._query_result = query_result
        self._receive_queue = aio.Queue()
        self._register_queue = aio.Queue()
        self._async_group = aio.Group()
        self._async_group.spawn(aio.call_on_cancel, self._receive_queue.close)
        self._async_group.spawn(aio.call_on_cancel, self._register_queue.close)

    @property
    def async_group(self):
        return self._async_group

    @property
    def receive_queue(self):
        return self._receive_queue

    @property
    def register_queue(self):
        return self._register_queue

    async def receive(self):
        try:
            return await self._receive_queue.get()
        except aio.QueueClosedError:
            raise ConnectionError()

    def register(self, events):
        try:
            for event in events:
                self._register_queue.put_nowait(event)
        except aio.QueueClosedError:
            raise ConnectionError()

    async def register_with_response(self, events):
        raise Exception('should not be used')

    async def query(self, data):
        return self._query_result


def encode_oid(oid):
    return '.'.join(str(i) for i in oid)


def decode_oid(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))


def assert_status_event(event, status):
    assert event.event_type == (*event_type_prefix, 'gateway', 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def assert_read_event(event, oid, session_id, cause, data):
    assert event.event_type == (*event_type_prefix, 'gateway', 'read',
                                encode_oid(oid))
    assert event.source_timestamp is None
    assert event.payload.data == {'session_id': session_id,
                                  'cause': cause,
                                  'data': data}


def assert_write_event(event, oid, session_id, success):
    assert event.event_type == (*event_type_prefix, 'gateway', 'write',
                                encode_oid(oid))
    assert event.source_timestamp is None
    assert event.payload.data == {'session_id': session_id,
                                  'success': success}


@pytest.fixture
def port():
    return util.get_unused_udp_port()


@pytest.fixture
def create_base_conf(port):

    def create_base_conf(connect_delay=0.01,
                         request_timeout=0.1,
                         request_retry_count=1,
                         request_retry_delay=0.01,
                         polling_delay=0.1,
                         polling_oids=[]):
        return {'remote_host': '127.0.0.1',
                'remote_port': port,
                'connect_delay': connect_delay,
                'request_timeout': request_timeout,
                'request_retry_count': request_retry_count,
                'request_retry_delay': request_retry_delay,
                'polling_delay': polling_delay,
                'polling_oids': [encode_oid(i) for i in polling_oids]}

    return create_base_conf


@pytest.fixture
def create_agent(port):

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


@pytest.fixture
def create_event():
    instance_ids = itertools.count(1)

    def create_event(event_type, payload_data):
        event_id = hat.event.common.EventId(1, 1, next(instance_ids))
        payload = hat.event.common.EventPayload(
            hat.event.common.EventPayloadType.JSON, payload_data)
        event = hat.event.common.Event(event_id=event_id,
                                       event_type=event_type,
                                       timestamp=hat.event.common.now(),
                                       source_timestamp=None,
                                       payload=payload)
        return event

    return create_event


@pytest.fixture
def create_read_event(create_event):

    def create_read_event(oid, session_id):
        return create_event((*event_type_prefix, 'system', 'read',
                             encode_oid(oid)),
                            {'session_id': session_id})

    return create_read_event


@pytest.fixture
def create_write_event(create_event):

    def create_write_event(oid, session_id, data):
        return create_event((*event_type_prefix, 'system', 'write',
                             encode_oid(oid)),
                            {'session_id': session_id,
                             'data': data})

    return create_write_event


@pytest.mark.parametrize('conf', [
    {'version': 'V1',
     'community': 'abc'},

    {'version': 'V2C',
     'community': 'xyz'},

    {'version': 'V3',
     'context': {'engine_id': '01 23 45 67 89 ab cd ef',
                 'name': 'context name'},
     'user': 'some user',
     'authentication': {'type': 'MD5',
                        'password': 'abc'},
     'privacy': {'type': 'DES',
                 'password': 'xyz'}}
])
def test_conf(create_base_conf, conf):
    conf = {**conf,
            **create_base_conf()}

    manager.json_schema_repo.validate(manager.json_schema_id, conf)


@pytest.mark.parametrize('conf', [
    {'version': 'V1',
     'community': 'abc'},

    {'version': 'V2C',
     'community': 'xyz'},

    {'version': 'V3',
     'context': {'engine_id': '01 23 45 67 89 ab cd ef',
                 'name': 'context name'},
     'user': 'some user',
     'authentication': {'type': 'MD5',
                        'password': 'abc'},
     'privacy': {'type': 'DES',
                 'password': 'xyz'}}
])
async def test_create(create_base_conf, conf):
    conf = {**conf,
            **create_base_conf()}

    event_client = EventClient()
    device = await aio.call(manager.create, conf, event_client,
                            event_type_prefix)

    assert device.is_open

    await device.async_close()
    await event_client.async_close()


@pytest.mark.parametrize('version', snmp.Version)
async def test_status(create_base_conf, create_agent, version):
    community = 'name'
    context = {'engine_id': '01 23 45 67 89 ab cd ef',
               'name': 'context name'}
    username = 'user name'

    user = snmp.User(name=username,
                     auth_type=snmp.AuthType.MD5,
                     auth_password='auth pass',
                     priv_type=snmp.PrivType.DES,
                     priv_password='priv pass')

    engine_id = bytes.fromhex(context['engine_id'])

    if version in (snmp.Version.V1, snmp.Version.V2C):
        conf = {'version': version.name,
                'community': community,
                **create_base_conf()}

    elif version == snmp.Version.V3:
        conf = {'version': version.name,
                'user': username,
                'context': context,
                'authentication': {'type': user.auth_type.name,
                                   'password': user.auth_password},
                'privacy': {'type': user.priv_type.name,
                            'password': user.priv_password},
                **create_base_conf()}

    else:
        raise ValueError('unsupported snmp version')

    event_client = EventClient()

    def on_v1_request(addr, comm, req):
        assert comm == community
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    def on_v2c_request(addr, comm, req):
        assert comm == community
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    def on_v3_request(addr, usr, ctx, req):
        assert usr == user
        assert ctx.engine_id == engine_id
        assert ctx.name == context['name']
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    agent = await create_agent(v1_request_cb=on_v1_request,
                               v2c_request_cb=on_v2c_request,
                               v3_request_cb=on_v3_request,
                               authoritative_engine_id=engine_id,
                               users=[user])
    device = await aio.call(manager.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    await agent.async_close()

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    await device.async_close()

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    await event_client.async_close()


async def test_polling(create_base_conf, create_agent):
    version = snmp.Version.V1
    community = 'name'

    int_oid = 1, 2, 3
    str_oid = 1, 3, 2

    conf = {'version': version.name,
            'community': community,
            **create_base_conf(polling_oids=[int_oid, str_oid])}

    event_client = EventClient()

    next_values = itertools.count(0)

    def on_request(addr, comm, req):
        assert comm == community
        assert len(req.names) == 1

        if req.names[0] == int_oid:
            return [snmp.IntegerData(name=int_oid,
                                     value=next(next_values))]

        if req.names[0] == str_oid:
            return [snmp.StringData(name=str_oid,
                                    value=str(next(next_values)))]

        raise Exception('unexpected oid')

    agent = await create_agent(v1_request_cb=on_request)
    device = await aio.call(manager.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    for i in range(10):
        oid = int_oid if i % 2 == 0 else str_oid
        cause = 'INTERROGATE' if i < 2 else 'CHANGE'
        data_type = 'INTEGER' if i % 2 == 0 else 'STRING'
        data_value = i if i % 2 == 0 else str(i)
        data = {'type': data_type,
                'value': data_value}

        event = await event_client.register_queue.get()
        assert_read_event(event, oid, None, cause, data)

    # cause is INTERROGATE on connection restart
    await agent.async_close()
    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')
    agent = await create_agent(v1_request_cb=on_request)
    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')
    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    for i in range(10):
        cause = 'INTERROGATE' if i < 2 else 'CHANGE'
        event = await event_client.register_queue.get()
        assert event.payload.data['cause'] == cause

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
                      value='abc')],
     (1, 2, 3),
     {'type': 'STRING',
      'value': 'abc'}),

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
      'value': 'END_OF_MIB_VIEW'})
])
async def test_read(create_base_conf, create_agent, create_read_event, res,
                    oid, data):
    version = snmp.Version.V2C
    community = 'name'
    session_id = 42

    conf = {'version': version.name,
            'community': community,
            **create_base_conf()}

    event_client = EventClient()

    def on_request(addr, comm, req):
        return res

    agent = await create_agent(v2c_request_cb=on_request)
    device = await aio.call(manager.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    event = create_read_event(oid, session_id)
    event_client.receive_queue.put_nowait([event])

    event = await event_client.register_queue.get()
    assert_read_event(event, oid, session_id, 'REQUESTED', data)

    await device.async_close()
    await agent.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("req_data, oid, data", [
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
                      value='abc')],
     (1, 2, 3),
     {'type': 'STRING',
      'value': 'abc'}),

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
])
@pytest.mark.parametrize('success', [True, False])
async def test_write(create_base_conf, create_agent, create_write_event,
                     req_data, oid, data, success):
    version = snmp.Version.V2C
    community = 'name'
    session_id = 42

    conf = {'version': version.name,
            'community': community,
            **create_base_conf()}

    event_client = EventClient()

    def on_request(addr, comm, req):
        if not isinstance(req, snmp.SetDataReq):
            return []

        assert req.data == req_data

        if success:
            return req.data

        return snmp.Error(snmp.ErrorType.READ_ONLY, 1)

    agent = await create_agent(v2c_request_cb=on_request)
    device = await aio.call(manager.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    event = create_write_event(oid, session_id, data)
    event_client.receive_queue.put_nowait([event])

    event = await event_client.register_queue.get()
    assert_write_event(event, oid, session_id, success)

    await device.async_close()
    await agent.async_close()
    await event_client.async_close()
