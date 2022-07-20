import itertools

import pytest

from hat import aio
from hat import util
from hat.drivers import snmp
from hat.drivers import udp
from hat.gateway import common
from hat.gateway.devices.snmp import manager
import hat.event.common


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
def create_conf(port):

    def create_conf(version=snmp.Version.V1,
                    context=snmp.Context(None, 'context_name'),
                    connect_delay=0.01,
                    request_timeout=0.1,
                    request_retry_count=1,
                    request_retry_delay=0.01,
                    polling_delay=0.1,
                    polling_oids=[]):
        return {'snmp_version': version.name,
                'snmp_context': {'engine_id': context.engine_id,
                                 'name': context.name},
                'remote_host': '127.0.0.1',
                'remote_port': port,
                'connect_delay': connect_delay,
                'request_timeout': request_timeout,
                'request_retry_count': request_retry_count,
                'request_retry_delay': request_retry_delay,
                'polling_delay': polling_delay,
                'polling_oids': [encode_oid(i) for i in polling_oids]}

    return create_conf


@pytest.fixture
async def create_agent(port):

    async def create_agent(request_cb):
        address = udp.Address('127.0.0.1', port)
        agent = await snmp.create_agent(request_cb, address)
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


def test_conf(create_conf):
    conf = create_conf()
    manager.json_schema_repo.validate(manager.json_schema_id, conf)


async def test_create(create_conf):
    event_client = EventClient()
    conf = create_conf()
    device = await aio.call(manager.create, conf, event_client,
                            event_type_prefix)

    assert device.is_open

    await device.async_close()
    await event_client.async_close()


async def test_status(create_conf, create_agent):
    version = snmp.Version.V2C
    context = snmp.Context(None, 'name')

    event_client = EventClient()
    conf = create_conf(version=version,
                       context=context)

    def on_request(ver, ctx, req):
        assert ver == version
        assert ctx == context
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    agent = await create_agent(on_request)
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


async def test_polling(create_conf, create_agent):
    version = snmp.Version.V1
    context = snmp.Context(None, 'name')

    int_oid = 1, 2, 3
    str_oid = 1, 3, 2

    event_client = EventClient()
    conf = create_conf(version=version,
                       context=context,
                       polling_oids=[int_oid, str_oid])

    next_values = itertools.count(0)

    def on_request(ver, ctx, req):
        assert ver == version
        assert ctx == context
        assert len(req.names) == 1

        if req.names[0] == int_oid:
            return [snmp.Data(type=snmp.DataType.INTEGER,
                              name=int_oid,
                              value=next(next_values))]

        if req.names[0] == str_oid:
            return [snmp.Data(type=snmp.DataType.STRING,
                              name=str_oid,
                              value=str(next(next_values)))]

        raise Exception('unexpected oid')

    agent = await create_agent(on_request)
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
    agent = await create_agent(on_request)
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
    ([snmp.Data(type=snmp.DataType.INTEGER,
                name=(1, 2, 3),
                value=-123)],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123}),

    ([snmp.Data(type=snmp.DataType.UNSIGNED,
                name=(1, 2, 3),
                value=123)],
     (1, 2, 3),
     {'type': 'UNSIGNED',
      'value': 123}),

    ([snmp.Data(type=snmp.DataType.COUNTER,
                name=(1, 2, 3),
                value=321)],
     (1, 2, 3),
     {'type': 'COUNTER',
      'value': 321}),

    ([snmp.Data(type=snmp.DataType.BIG_COUNTER,
                name=(1, 2, 3),
                value=123456)],
     (1, 2, 3),
     {'type': 'BIG_COUNTER',
      'value': 123456}),

    ([snmp.Data(type=snmp.DataType.TIME_TICKS,
                name=(1, 2, 3),
                value=42)],
     (1, 2, 3),
     {'type': 'TIME_TICKS',
      'value': 42}),

    ([snmp.Data(type=snmp.DataType.STRING,
                name=(1, 2, 3),
                value='abc')],
     (1, 2, 3),
     {'type': 'STRING',
      'value': 'abc'}),

    ([snmp.Data(type=snmp.DataType.OBJECT_ID,
                name=(1, 2, 3),
                value=(1, 2, 3, 4, 5, 6))],
     (1, 2, 3),
     {'type': 'OBJECT_ID',
      'value': '1.2.3.4.5.6'}),

    ([snmp.Data(type=snmp.DataType.IP_ADDRESS,
                name=(1, 2, 3),
                value=(192, 168, 0, 1))],
     (1, 2, 3),
     {'type': 'IP_ADDRESS',
      'value': '192.168.0.1'}),

    ([snmp.Data(type=snmp.DataType.ARBITRARY,
                name=(1, 2, 3),
                value=b'\x01\x02\x03')],
     (1, 2, 3),
     {'type': 'ARBITRARY',
      'value': '010203'}),

    *((snmp.Error(err, 1),
       (1, 2, 3),
       {'type': 'ERROR',
        'value': err.name})
      for err in list(snmp.ErrorType)[1:]),

    ([snmp.Data(type=snmp.DataType.UNSPECIFIED,
                name=(1, 2, 3),
                value=None)],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'UNSPECIFIED'}),

    ([snmp.Data(type=snmp.DataType.NO_SUCH_OBJECT,
                name=(1, 2, 3),
                value=None)],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'NO_SUCH_OBJECT'}),

    ([snmp.Data(type=snmp.DataType.NO_SUCH_INSTANCE,
                name=(1, 2, 3),
                value=None)],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'NO_SUCH_INSTANCE'}),

    ([snmp.Data(type=snmp.DataType.END_OF_MIB_VIEW,
                name=(1, 2, 3),
                value=None)],
     (1, 2, 3),
     {'type': 'ERROR',
      'value': 'END_OF_MIB_VIEW'})
])
async def test_read(create_conf, create_agent, create_read_event, res, oid,
                    data):
    version = snmp.Version.V2C
    context = snmp.Context(None, 'name')
    session_id = 42

    event_client = EventClient()
    conf = create_conf(version=version,
                       context=context)

    def on_request(ver, ctx, req):
        return res

    agent = await create_agent(on_request)
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
    ([snmp.Data(type=snmp.DataType.INTEGER,
                name=(1, 2, 3),
                value=-123)],
     (1, 2, 3),
     {'type': 'INTEGER',
      'value': -123}),

    ([snmp.Data(type=snmp.DataType.UNSIGNED,
                name=(1, 2, 3),
                value=123)],
     (1, 2, 3),
     {'type': 'UNSIGNED',
      'value': 123}),

    ([snmp.Data(type=snmp.DataType.COUNTER,
                name=(1, 2, 3),
                value=321)],
     (1, 2, 3),
     {'type': 'COUNTER',
      'value': 321}),

    ([snmp.Data(type=snmp.DataType.BIG_COUNTER,
                name=(1, 2, 3),
                value=123456)],
     (1, 2, 3),
     {'type': 'BIG_COUNTER',
      'value': 123456}),

    ([snmp.Data(type=snmp.DataType.TIME_TICKS,
                name=(1, 2, 3),
                value=42)],
     (1, 2, 3),
     {'type': 'TIME_TICKS',
      'value': 42}),

    ([snmp.Data(type=snmp.DataType.STRING,
                name=(1, 2, 3),
                value='abc')],
     (1, 2, 3),
     {'type': 'STRING',
      'value': 'abc'}),

    ([snmp.Data(type=snmp.DataType.OBJECT_ID,
                name=(1, 2, 3),
                value=(1, 2, 3, 4, 5, 6))],
     (1, 2, 3),
     {'type': 'OBJECT_ID',
      'value': '1.2.3.4.5.6'}),

    ([snmp.Data(type=snmp.DataType.IP_ADDRESS,
                name=(1, 2, 3),
                value=(192, 168, 0, 1))],
     (1, 2, 3),
     {'type': 'IP_ADDRESS',
      'value': '192.168.0.1'}),

    ([snmp.Data(type=snmp.DataType.ARBITRARY,
                name=(1, 2, 3),
                value=b'\x01\x02\x03')],
     (1, 2, 3),
     {'type': 'ARBITRARY',
      'value': '010203'}),
])
@pytest.mark.parametrize('success', [True, False])
async def test_write(create_conf, create_agent, create_write_event, req_data,
                     oid, data, success):
    version = snmp.Version.V2C
    context = snmp.Context(None, 'name')
    session_id = 42

    event_client = EventClient()
    conf = create_conf(version=version,
                       context=context)

    def on_request(ver, ctx, req):
        assert req.data == req_data

        if success:
            return req.data

        return snmp.Error(snmp.ErrorType.READ_ONLY, 1)

    agent = await create_agent(on_request)
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
