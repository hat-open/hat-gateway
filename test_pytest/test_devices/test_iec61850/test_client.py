import asyncio
import itertools

import pytest

from hat import aio
from hat import json
from hat import util
from hat.drivers import iec61850
from hat.drivers import tcp
import hat.event.common

from hat.gateway.devices.iec61850.client import info

from .server import create_server


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


def assert_status_event(event, status):
    assert event.type == (*event_type_prefix, 'gateway', 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def assert_data_event(event, name, data, reasons):
    assert event.type == (*event_type_prefix, 'gateway', 'data', name)

    # TODO
    # assert event.source_timestamp

    assert event.payload.data == {'data': data,
                                  'reasons': reasons}


def assert_command_event(event, name, session_id, action, success,
                         service_error=None, additional_cause=None,
                         test_error=None):
    assert event.type == (*event_type_prefix, 'gateway', 'command', name)

    # TODO
    # assert event.source_timestamp

    assert event.payload.data['session_id'] == session_id
    assert event.payload.data['action'] == action
    assert event.payload.data['success'] == success

    if service_error is not None:
        assert event.payload.data['service_error'] == service_error

    if additional_cause is not None:
        assert event.payload.data['additional_cause'] == additional_cause

    if test_error is not None:
        assert event.payload.data['test_error'] == test_error


def assert_change_event(event, name, session_id, success, error=None):
    assert event.type == (*event_type_prefix, 'gateway', 'change', name)

    # TODO
    # assert event.source_timestamp

    assert event.payload.data['session_id'] == session_id
    assert event.payload.data['success'] == success

    if error is not None:
        assert event.payload.data['error'] == error


def assert_entry_id_event(event, name, entry_id):
    assert event.type == (*event_type_prefix, 'gateway', 'entry_id', name)

    # TODO
    # assert event.source_timestamp

    assert event.payload.data == entry_id


def create_event(event_type, payload_data, source_timestamp=None):
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayloadJson(payload_data))


def create_command_event(name, session_id, action, value, origin, test,
                         checks):
    return create_event((*event_type_prefix, 'system', 'command', name),
                        {'session_id': session_id,
                         'action': action,
                         'value': value,
                         'origin': origin,
                         'test': test,
                         'checks': checks})


def create_change_event(name, session_id, value):
    return create_event((*event_type_prefix, 'system', 'change', name),
                        {'session_id': session_id,
                         'value': value})


def create_entry_id_event(name, entry_id):
    return create_event((*event_type_prefix, 'gateway', 'entry_id', name),
                        entry_id)


def create_queue_cb(queue):
    return lambda *args: queue.put_nowait(args)


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def addr(port):
    return tcp.Address('127.0.0.1', port)


@pytest.fixture
def create_conf(port):

    def create_conf(connect_timeout=0.1,
                    reconnect_delay=0.01,
                    response_timeout=0.1,
                    status_delay=1,
                    status_timeout=0.1,
                    value_types=[],
                    datasets=[],
                    rcbs=[],
                    data=[],
                    commands=[],
                    changes=[]):
        return {'connection': {'host': '127.0.0.1',
                               'port': port,
                               'connect_timeout': connect_timeout,
                               'reconnect_delay': reconnect_delay,
                               'response_timeout': response_timeout,
                               'status_delay': status_delay,
                               'status_timeout': status_timeout},
                'value_types': value_types,
                'datasets': datasets,
                'rcbs': rcbs,
                'data': data,
                'commands': commands,
                'changes': changes}

    return create_conf


def test_conf(create_conf):
    conf = create_conf()
    validator = json.DefaultSchemaValidator(info.json_schema_repo)
    validator.validate(info.json_schema_id, conf)


async def test_create(create_conf):
    conf = create_conf()
    client = EventerClient()

    device = await aio.call(info.create, conf, client, event_type_prefix)

    await asyncio.sleep(0.01)

    assert device.is_open

    await device.async_close()
    await client.async_close()


async def test_status_without_server(create_conf):
    event_queue = aio.Queue()

    conf = create_conf()
    client = EventerClient(event_cb=event_queue.put_nowait)

    device = await aio.call(info.create, conf, client, event_type_prefix)

    for _ in range(5):
        for status in ['CONNECTING', 'DISCONNECTED']:
            event = await event_queue.get()
            assert_status_event(event, status)

    await device.async_close()
    await client.async_close()


async def test_status(addr, create_conf):
    event_queue = aio.Queue()

    conf = create_conf()
    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(addr=addr)
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    await asyncio.sleep(0.1)
    assert event_queue.empty()

    await server.async_close()

    for _ in range(5):
        for status in ['DISCONNECTED', 'CONNECTING']:
            event = await event_queue.get()
            assert_status_event(event, status)

    await device.async_close()
    await client.async_close()


@pytest.mark.parametrize("ref_conf, dataset_ref", [
    ('ds',
     iec61850.NonPersistedDatasetRef('ds')),

    ({'logical_device': 'ld',
      'logical_node': 'ln',
      'name': 'ds'},
     iec61850.PersistedDatasetRef(logical_device='ld',
                                  logical_node='ln',
                                  name='ds')),
])
@pytest.mark.parametrize("values_conf, data_refs", [
    ([],
     []),

    ([{'logical_device': 'ld',
       'logical_node': 'ln',
       'fc': 'fc',
       'names': ['a', 'b', 123]}],
     [iec61850.DataRef(logical_device='ld',
                       logical_node='ln',
                       fc='fc',
                       names=('a', 'b', 123))]),
])
async def test_dynamic_dataset(addr, create_conf, ref_conf, dataset_ref,
                               values_conf, data_refs):
    dataset_queue = aio.Queue()

    conf = create_conf(datasets=[{'ref': ref_conf,
                                  'values': values_conf,
                                  'dynamic': True}])
    client = EventerClient()

    server = await create_server(addr=addr,
                                 dataset_cb=create_queue_cb(dataset_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    result_dataset_ref, result_data_refs = await dataset_queue.get()

    assert result_dataset_ref == dataset_ref
    assert list(result_data_refs) == data_refs

    await device.async_close()
    await server.async_close()
    await client.async_close()


async def test_dynamic_dataset_changed_values(addr, create_conf):
    dataset_queue = aio.Queue()

    dataset_ref = iec61850.PersistedDatasetRef(logical_device='ld',
                                               logical_node='ln',
                                               name='ds')
    data_ref_a = iec61850.DataRef(logical_device='ld',
                                  logical_node='ln',
                                  fc='fc',
                                  names=('a', ))
    data_ref_b = iec61850.DataRef(logical_device='ld',
                                  logical_node='ln',
                                  fc='fc',
                                  names=('b', ))

    conf = create_conf(datasets=[{'ref': {'logical_device': 'ld',
                                          'logical_node': 'ln',
                                          'name': 'ds'},
                                  'values': [{'logical_device': 'ld',
                                              'logical_node': 'ln',
                                              'fc': 'fc',
                                              'names': ['a']}],
                                  'dynamic': True}])
    client = EventerClient()

    server = await create_server(addr=addr,
                                 datasets={dataset_ref: [data_ref_b]},
                                 dataset_cb=create_queue_cb(dataset_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    result_dataset_ref, result_data_refs = await dataset_queue.get()

    assert result_dataset_ref == dataset_ref
    assert result_data_refs is None

    result_dataset_ref, result_data_refs = await dataset_queue.get()

    assert result_dataset_ref == dataset_ref
    assert list(result_data_refs) == [data_ref_a]

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
async def test_rcb_enable(addr, create_conf, rcb_type):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': []}])
    client = EventerClient()

    server = await create_server(addr=addr,
                                 rcbs={rcb_ref: {}},
                                 rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    result_rcb_ref, attr_type, attr_value = await rcb_queue.get()
    assert result_rcb_ref == rcb_ref
    assert attr_type == iec61850.RcbAttrType.REPORT_ENABLE
    assert attr_value is False

    async def wait_enable_true():
        while True:
            result_rcb_ref, attr_type, attr_value = await rcb_queue.get()
            assert result_rcb_ref == rcb_ref

            if attr_type != iec61850.RcbAttrType.REPORT_ENABLE:
                continue

            assert attr_value is True
            break

    await aio.wait_for(wait_enable_true(), 0.1)

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
async def test_rcb_gi(addr, create_conf, rcb_type):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': []}])
    client = EventerClient()

    server = await create_server(addr=addr,
                                 rcbs={rcb_ref: {}},
                                 rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi_true():
        while True:
            result_rcb_ref, attr_type, attr_value = await rcb_queue.get()
            assert result_rcb_ref == rcb_ref

            if attr_type != iec61850.RcbAttrType.GI:
                continue

            assert attr_value is True
            break

    await aio.wait_for(wait_gi_true(), 0.1)

    await asyncio.sleep(0.05)
    assert rcb_queue.empty()

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
async def test_rcb_conf_revision(addr, create_conf, rcb_type):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': [],
                              'conf_revision': 123}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={rcb_ref: {iec61850.RcbAttrType.CONF_REVISION: 123}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
async def test_rcb_conf_revision_invalid(addr, create_conf, rcb_type):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': [],
                              'conf_revision': 321}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={rcb_ref: {iec61850.RcbAttrType.CONF_REVISION: 123}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(wait_gi(), 0.1)

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('trigger_options', [set(),
                                             set(iec61850.TriggerCondition)])
async def test_rcb_trigger_options(addr, create_conf, rcb_type,
                                   trigger_options):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': [i.name
                                                  for i in trigger_options]}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={rcb_ref: {}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    assert server.rcbs[rcb_ref][iec61850.RcbAttrType.TRIGGER_OPTIONS] == \
        trigger_options

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('buffer_time', [None, 123])
async def test_rcb_buffer_time(addr, create_conf, rcb_type, buffer_time):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': [],
                              **({'buffer_time': buffer_time}
                                 if buffer_time is not None else {})}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={rcb_ref: {}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if buffer_time is None:
        assert iec61850.RcbAttrType.BUFFER_TIME not in server.rcbs[rcb_ref]

    else:
        assert server.rcbs[rcb_ref][iec61850.RcbAttrType.BUFFER_TIME] == \
            buffer_time

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('integrity_period', [None, 123])
async def test_rcb_integrity_period(addr, create_conf, rcb_type,
                                    integrity_period):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': [],
                              **({'integrity_period': integrity_period}
                                 if integrity_period is not None else {})}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={rcb_ref: {}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if integrity_period is None:
        assert iec61850.RcbAttrType.INTEGRITY_PERIOD not in server.rcbs[rcb_ref]  # NOQA

    else:
        assert server.rcbs[rcb_ref][iec61850.RcbAttrType.INTEGRITY_PERIOD] == \
            integrity_period

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('reservation_time', [None, 123])
async def test_rcb_reservation_time(addr, create_conf, rcb_type,
                                    reservation_time):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': [],
                              'reservation_time': reservation_time}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={rcb_ref: {}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if rcb_type == iec61850.RcbType.UNBUFFERED or reservation_time is None:
        assert iec61850.RcbAttrType.RESERVATION_TIME not in server.rcbs[rcb_ref]  # NOQA

    else:
        assert server.rcbs[rcb_ref][iec61850.RcbAttrType.RESERVATION_TIME] == \
            reservation_time

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
async def test_rcb_reserve(addr, create_conf, rcb_type):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds',
                              'trigger_options': []}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={rcb_ref: {}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if rcb_type == iec61850.RcbType.BUFFERED:
        assert iec61850.RcbAttrType.RESERVE not in server.rcbs[rcb_ref]

    else:
        assert server.rcbs[rcb_ref][iec61850.RcbAttrType.RESERVE] is True

    await device.async_close()
    await server.async_close()
    await client.async_close()
