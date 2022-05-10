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
event_type_prefix = ('gateway', gateway_name, manager.device_type, device_name)


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
    assert event.event_type == (*event_type_prefix, 'gateway', 'read',
                                encode_oid(oid))
    assert event.source_timestamp is None
    assert event.payload.data == {'session_id': session_id,
                                  'success': success}


@pytest.fixture
def port():
    return util.get_unused_udp_port()


@pytest.fixture
def create_conf(port):

    def create_conf(snmp_version=snmp.Version.V1,
                    snmp_context=snmp.Context(None, 'context_name'),
                    connect_delay=0.01,
                    request_timeout=0.1,
                    request_retry_count=1,
                    request_retry_delay=0.1,
                    polling_delay=0.1,
                    polling_oids=[]):
        return {'snmp_version': snmp_version.name,
                'snmp_context': {'engine_id': snmp_context.engine_id,
                                 'name': snmp_context.name},
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
    instance_ids = itertools.count(0)

    def create_event(event_type, payload_data):
        event_id = hat.event.common.EventId(1, next(instance_ids))
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
