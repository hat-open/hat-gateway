import itertools

import pytest

from hat import aio
from hat import util
from hat.drivers import snmp
from hat.drivers import udp
import hat.event.common

import hat.gateway.devices.snmp.manager


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name,
                     hat.gateway.devices.snmp.manager.info.type,
                     device_name)

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


def test_conf(create_conf):
    conf = create_conf()
    hat.gateway.devices.snmp.manager.info.json_schema_repo.validate(
        hat.gateway.devices.snmp.manager.info.json_schema_id, conf)


async def test_create(create_conf):
    conf = create_conf()

    eventer_client = EventerClient()
    device = await aio.call(hat.gateway.devices.snmp.manager.info.create, conf,
                            eventer_client, event_type_prefix)

    assert device.is_open

    await device.async_close()
    await eventer_client.async_close()


async def test_status(create_conf, create_agent):
    version = snmp.Version.V2C
    context = snmp.Context(None, 'name')
    event_queue = aio.Queue()

    conf = create_conf(version=version,
                       context=context)

    async def on_request(ver, ctx, req):
        assert ver == version
        assert ctx == context
        assert req == snmp.GetDataReq([(0, 0)])
        return snmp.Error(snmp.ErrorType.NO_SUCH_NAME, 1)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    agent = await create_agent(on_request)
    device = await aio.call(hat.gateway.devices.snmp.manager.info.create, conf,
                            eventer_client, event_type_prefix)

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


async def test_polling(create_conf, create_agent):
    version = snmp.Version.V1
    context = snmp.Context(None, 'name')
    int_oid = 1, 2, 3
    str_oid = 1, 3, 2
    next_values = itertools.count(0)
    event_queue = aio.Queue()

    conf = create_conf(version=version,
                       context=context,
                       polling_oids=[int_oid, str_oid])

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

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    agent = await create_agent(on_request)
    device = await aio.call(hat.gateway.devices.snmp.manager.info.create, conf,
                            eventer_client, event_type_prefix)

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
    agent = await create_agent(on_request)
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
async def test_read(create_conf, create_agent, res, oid, data):
    version = snmp.Version.V2C
    context = snmp.Context(None, 'name')
    session_id = 42
    event_queue = aio.Queue()

    conf = create_conf(version=version,
                       context=context)

    def on_request(ver, ctx, req):
        return res

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    agent = await create_agent(on_request)
    device = await aio.call(hat.gateway.devices.snmp.manager.info.create, conf,
                            eventer_client, event_type_prefix)

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
async def test_write(create_conf, create_agent, req_data, oid, data, success):
    version = snmp.Version.V2C
    context = snmp.Context(None, 'name')
    session_id = 42
    event_queue = aio.Queue()

    conf = create_conf(version=version,
                       context=context)

    def on_request(ver, ctx, req):
        assert req.data == req_data

        if success:
            return req.data

        return snmp.Error(snmp.ErrorType.READ_ONLY, 1)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    agent = await create_agent(on_request)
    device = await aio.call(hat.gateway.devices.snmp.manager.info.create, conf,
                            eventer_client, event_type_prefix)

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
