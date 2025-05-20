import itertools

import pytest

from hat import aio
from hat import json
from hat import util
from hat.drivers import smpp
from hat.drivers import tcp
from hat.drivers.smpp import transport
import hat.event.common

from hat.gateway.devices.smpp.client import info


device_name = 'device_name'
event_type_prefix = ('gateway', info.type, device_name)

next_event_ids = (hat.event.common.EventId(1, 1, instance)
                  for instance in itertools.count(1))


class EventerClient(aio.Resource):

    def __init__(self, event_cb=None):
        self._event_cb = event_cb
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
        raise NotImplementedError()


def assert_status_event(event, status):
    assert event.type == (*event_type_prefix, 'gateway', 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def create_event(event_type, payload_data, source_timestamp=None):
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayloadJson(payload_data))


def create_message_event(address, message):
    return create_event((*event_type_prefix, 'system', 'message'),
                        {'address': address,
                         'message': message})


async def create_device(conf, eventer_client):
    return await aio.call(info.create, conf, eventer_client, event_type_prefix)


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def create_conf(port):

    def create_conf(system_id='',
                    password='',
                    enquire_link_delay=None,
                    enquire_link_timeout=1,
                    connect_timeout=0.1,
                    reconnect_delay=0.1,
                    short_message=True,
                    priority=smpp.Priority.BULK,
                    data_coding=smpp.DataCoding.DEFAULT,
                    message_encoding='utf-8',
                    message_timeout=1):
        return {'remote_address': {'host': '127.0.0.1',
                                   'port': port},
                'ssl': False,
                'system_id': system_id,
                'password': password,
                'enquire_link_delay': enquire_link_delay,
                'enquire_link_timeout': enquire_link_timeout,
                'connect_timeout': connect_timeout,
                'reconnect_delay': reconnect_delay,
                'short_message': short_message,
                'priority': priority.name,
                'data_coding': data_coding.name,
                'message_encoding': message_encoding,
                'message_timeout': message_timeout}

    return create_conf


@pytest.fixture
def create_server(port):

    async def create_server(connection_cb=None,
                            submit_sm_req_cb=None,
                            system_id=''):
        next_message_ids = (str(i).encode() for i in itertools.count(1))

        async def on_request(req):
            if isinstance(req, transport.BindReq):
                return transport.BindRes(bind_type=req.bind_type,
                                         system_id=system_id,
                                         optional_params={})

            if isinstance(req, transport.UnbindReq):
                return transport.UnbindRes()

            if isinstance(req, transport.EnquireLinkReq):
                return transport.EnquireLinkRes()

            if isinstance(req, transport.SubmitSmReq):
                if submit_sm_req_cb is not None:
                    await aio.call(submit_sm_req_cb, req)

                return transport.SubmitSmRes(message_id=next(next_message_ids))

            return transport.CommandStatus.ESME_RINVCMDID

        async def on_connection(conn):
            try:
                conn = transport.Connection(conn=conn,
                                            request_cb=on_request,
                                            notification_cb=None)

                if connection_cb is not None:
                    await aio.call(connection_cb, conn)

                await conn.wait_closing()

            finally:
                await aio.uncancellable(conn.async_close())

        return await tcp.listen(connection_cb=on_connection,
                                addr=tcp.Address('127.0.0.1', port),
                                bind_connections=True)

    return create_server


def test_conf(create_conf):
    conf = create_conf()
    validator = json.DefaultSchemaValidator(info.json_schema_repo)
    validator.validate(info.json_schema_id, conf)


async def test_create(create_conf):
    conf = create_conf()

    eventer_client = EventerClient()
    device = await create_device(conf, eventer_client)

    assert device.is_open

    await device.async_close()
    await eventer_client.async_close()


async def test_status(create_conf, create_server):
    conn_queue = aio.Queue()
    event_queue = aio.Queue()

    conf = create_conf()

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    server = await create_server(connection_cb=conn_queue.put_nowait)
    device = await create_device(conf, eventer_client)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    assert event_queue.empty()

    conn = await conn_queue.get()
    assert conn.is_open

    await conn.async_close()

    event = await event_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    conn = await conn_queue.get()
    assert conn.is_open

    await device.async_close()

    event = await event_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    assert event_queue.empty()

    await server.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize('short_message', [True, False])
@pytest.mark.parametrize('priority', list(smpp.Priority)[:2])
@pytest.mark.parametrize('data_coding', list(smpp.DataCoding)[:2])
@pytest.mark.parametrize('message_encoding', ['utf-8'])
async def test_message(create_conf, create_server, short_message, priority,
                       data_coding, message_encoding):
    req_queue = aio.Queue()
    event_queue = aio.Queue()

    conf = create_conf(short_message=short_message,
                       priority=priority,
                       data_coding=data_coding,
                       message_encoding=message_encoding)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    server = await create_server(submit_sm_req_cb=req_queue.put_nowait)
    device = await create_device(conf, eventer_client)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    assert req_queue.empty()

    address = '123456789'
    message = 'abc'

    event = create_message_event(address, message)
    await aio.call(device.process_events, [event])

    req = await req_queue.get()
    assert req.destination_addr == address
    assert req.priority_flag == priority
    assert req.data_coding == data_coding

    if short_message:
        assert req.short_message == message.encode(message_encoding)
        assert transport.OptionalParamTag.MESSAGE_PAYLOAD not in req.optional_params  # NOQA

    else:
        assert req.short_message == b''
        assert req.optional_params[transport.OptionalParamTag.MESSAGE_PAYLOAD] == \
            message.encode(message_encoding)  # NOQA

    await device.async_close()
    await server.async_close()
    await eventer_client.async_close()
