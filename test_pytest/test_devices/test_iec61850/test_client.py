from collections.abc import Callable, Collection, Iterable
import asyncio
import datetime
import itertools
import typing

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
operate_time = iec61850.Timestamp(
    value=datetime.datetime.fromtimestamp(0, datetime.timezone.utc),
    leap_second=False,
    clock_failure=False,
    not_synchronized=False,
    accuracy=0)


EventCb: typing.TypeAlias = aio.AsyncCallable[[hat.event.common.Event], None]

QueryCb: typing.TypeAlias = aio.AsyncCallable[[hat.event.common.QueryParams],
                                              hat.event.common.QueryResult]


class EventerClient(aio.Resource):

    def __init__(self,
                 event_cb: EventCb | None = None,
                 query_cb: QueryCb | None = None):
        self._event_cb = event_cb
        self._query_cb = query_cb
        self._async_group = aio.Group()

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    @property
    def status(self) -> hat.event.common.Status:
        raise NotImplementedError()

    async def register(self,
                       events: Collection[hat.event.common.RegisterEvent],
                       with_response: bool = False
                       ) -> Collection[hat.event.common.Event] | None:
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

    async def query(self,
                    params: hat.event.common.QueryParams
                    ) -> hat.event.common.QueryResult:
        if not self._query_cb:
            return hat.event.common.QueryResult([], False)

        return await aio.call(self._query_cb, params)


def assert_status_event(event: hat.event.common.Event,
                        status: str):
    assert event.type == (*event_type_prefix, 'gateway', 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def assert_data_event(event: hat.event.common.Event,
                      name: str,
                      data: json.Data,
                      reasons: Iterable[iec61850.Reason]):
    assert event.type == (*event_type_prefix, 'gateway', 'data', name)

    # TODO
    # assert event.source_timestamp

    assert event.payload.data == {'data': data,
                                  'reasons': [reason.name
                                              for reason in reasons]}


def assert_command_event(event: hat.event.common.Event,
                         name: str,
                         session_id: str,
                         action: str,
                         success: bool,
                         service_error: str | None = None,
                         additional_cause: str | None = None,
                         test_error: str | None = None):
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


def assert_change_event(event: hat.event.common.Event,
                        name: str,
                        session_id: str,
                        success: bool,
                        error: str = None):
    assert event.type == (*event_type_prefix, 'gateway', 'change', name)

    # TODO
    # assert event.source_timestamp

    assert event.payload.data['session_id'] == session_id
    assert event.payload.data['success'] == success

    if error is not None:
        assert event.payload.data['error'] == error


def assert_entry_id_event(event: hat.event.common.Event,
                          report_id: str,
                          entry_id: util.Bytes):
    assert event.type == (*event_type_prefix, 'gateway', 'entry_id', report_id)

    # TODO
    # assert event.source_timestamp

    assert event.payload.data == entry_id.hex()


def assert_equal_timestamp(t1: float, t2: float):
    assert round(t1 * 1000) == round(t2 * 1000)


def create_event(event_type: hat.event.common.EventType,
                 payload_data: json.Data,
                 source_timestamp: hat.event.common.Timestamp | None = None
                 ) -> hat.event.common.Event:
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayloadJson(payload_data))


def create_command_event(name: str,
                         session_id: str,
                         action: str,
                         value: json.Data,
                         origin: iec61850.Origin,
                         test: bool,
                         checks: Iterable[iec61850.Check]
                         ) -> hat.event.common.Event:
    return create_event(
        (*event_type_prefix, 'system', 'command', name),
        {'session_id': session_id,
         'action': action,
         'value': value,
         'origin': {'category': origin.category.name,
                    'identification': origin.identification.decode()},
         'test': test,
         'checks': [check.name for check in checks]})


def create_change_event(name: str,
                        session_id: str,
                        value: json.Data
                        ) -> hat.event.common.Event:
    return create_event((*event_type_prefix, 'system', 'change', name),
                        {'session_id': session_id,
                         'value': value})


def create_entry_id_event(report_id: str,
                          entry_id: util.Bytes
                          ) -> hat.event.common.Event:
    return create_event((*event_type_prefix, 'gateway', 'entry_id', report_id),
                        entry_id.hex())


def create_queue_cb(queue: aio.Queue) -> Callable:
    return lambda *args: queue.put_nowait(args)


def value_type_to_json(value_type: iec61850.ValueType) -> json.Data:
    if isinstance(value_type, iec61850.BasicValueType):
        return value_type.value

    if isinstance(value_type, iec61850.AcsiValueType):
        return value_type.value

    if isinstance(value_type, iec61850.ArrayValueType):
        return {'type': 'ARRAY',
                'element_type': value_type_to_json(value_type.type)}

    if isinstance(value_type, iec61850.StructValueType):
        return {
            'type': 'STRUCT',
            'elements': [
                {'name': element_name,
                 'type': value_type_to_json(element_type)}
                for element_name, element_type in value_type.elements]}

    raise TypeError('unsupported value type')


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
                              'dataset': 'ds'}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
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
                              'dataset': 'ds'}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
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
async def test_rcb_report_id_invalid(addr, create_conf, rcb_type):
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
                              'report_id': 'report id 1',
                              'dataset': 'ds'}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id 2',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
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
async def test_rcb_dataset_invalid(addr, create_conf, rcb_type):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(datasets=[{'ref': 'ds 1',
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': 'ds 1'}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds 2')}},
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
                              'conf_revision': 123}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds'),
                iec61850.RcbAttrType.CONF_REVISION: 123}},
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
                              'conf_revision': 321}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds'),
                iec61850.RcbAttrType.CONF_REVISION: 123}},
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
@pytest.mark.parametrize('trigger_options', [None,
                                             set(),
                                             set(iec61850.TriggerCondition)])
async def test_rcb_trigger_options(addr, create_conf, rcb_type,
                                   trigger_options):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(
        datasets=[{'ref': 'ds',
                   'values': [],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': 'ld',
                       'logical_node': 'ln',
                       'type': rcb_type.name,
                       'name': 'rcb'},
               'report_id': 'report id',
               'dataset': 'ds',
               **({'trigger_options': [i.name for i in trigger_options]}
                  if trigger_options is not None else {})}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if trigger_options is None:
        assert iec61850.RcbAttrType.TRIGGER_OPTIONS not in server.rcbs[rcb_ref]

    else:
        assert server.rcbs[rcb_ref][iec61850.RcbAttrType.TRIGGER_OPTIONS] == \
            trigger_options

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('optional_fields', [None,
                                             set(),
                                             set(iec61850.OptionalField)])
async def test_rcb_optional_fields(addr, create_conf, rcb_type,
                                   optional_fields):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    conf = create_conf(
        datasets=[{'ref': 'ds',
                   'values': [],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': 'ld',
                       'logical_node': 'ln',
                       'type': rcb_type.name,
                       'name': 'rcb'},
               'report_id': 'report id',
               'dataset': 'ds',
               **({'optional_fields': [i.name for i in optional_fields]}
                  if optional_fields is not None else {})}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if optional_fields is None:
        assert iec61850.RcbAttrType.OPTIONAL_FIELDS not in server.rcbs[rcb_ref]

    else:
        assert server.rcbs[rcb_ref][iec61850.RcbAttrType.OPTIONAL_FIELDS] == \
            optional_fields

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
                              **({'buffer_time': buffer_time}
                                 if buffer_time is not None else {})}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
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
                              **({'integrity_period': integrity_period}
                                 if integrity_period is not None else {})}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
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
                              **({'reservation_time': reservation_time}
                                 if reservation_time is not None else {})}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
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
                              'dataset': 'ds'}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
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


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('purge_buffer', [True, False])
async def test_rcb_purge_buffer_without_entry_id(addr, create_conf, rcb_type,
                                                 purge_buffer):
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
                              'purge_buffer': purge_buffer}])
    client = EventerClient()

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if rcb_type == iec61850.RcbType.BUFFERED:
        assert server.rcbs[rcb_ref][iec61850.RcbAttrType.PURGE_BUFFER] is True

    else:
        assert iec61850.RcbAttrType.PURGE_BUFFER not in server.rcbs[rcb_ref]

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('purge_buffer', [True, False])
async def test_rcb_purge_buffer_with_entry_id(addr, create_conf, rcb_type,
                                              purge_buffer):
    rcb_queue = aio.Queue()

    entry_id = b'123'
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
                              'purge_buffer': purge_buffer}])

    def on_query(params):
        assert isinstance(params, hat.event.common.QueryLatestParams)
        assert set(params.event_types) == {(*event_type_prefix, 'gateway',
                                            'entry_id', 'report id')}

        return hat.event.common.QueryResult(
            events=[create_entry_id_event(report_id='report id',
                                          entry_id=entry_id)],
            more_follows=False)

    client = EventerClient(query_cb=on_query)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: 'report id',
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    'ds')}},
        rcb_cb=create_queue_cb(rcb_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    await aio.wait_for(wait_gi(), 0.1)

    if rcb_type == iec61850.RcbType.BUFFERED:
        if purge_buffer:
            assert server.rcbs[rcb_ref][iec61850.RcbAttrType.PURGE_BUFFER] is True  # NOQA
            assert iec61850.RcbAttrType.ENTRY_ID not in server.rcbs[rcb_ref]

        else:
            assert iec61850.RcbAttrType.PURGE_BUFFER not in server.rcbs[rcb_ref]  # NOQA
            assert server.rcbs[rcb_ref][iec61850.RcbAttrType.ENTRY_ID] == entry_id  # NOQA

    else:
        assert iec61850.RcbAttrType.PURGE_BUFFER not in server.rcbs[rcb_ref]
        assert iec61850.RcbAttrType.ENTRY_ID not in server.rcbs[rcb_ref]

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('model', ['DIRECT_WITH_NORMAL_SECURITY',
                                   'SBO_WITH_NORMAL_SECURITY',
                                   'DIRECT_WITH_ENHANCED_SECURITY',
                                   'SBO_WITH_ENHANCED_SECURITY'])
@pytest.mark.parametrize('with_operate_time', [True, False])
@pytest.mark.parametrize('value_type, iec61850_value, json_value', [
    (iec61850.BasicValueType.BOOLEAN,
     True,
     True),

    (iec61850.BasicValueType.INTEGER,
     123,
     123),

    (iec61850.BasicValueType.BINARY_CONTROL,
     iec61850.BinaryControl.HIGHER,
     'HIGHER'),

    (iec61850.BasicValueType.ANALOGUE,
     iec61850.Analogue(i=123),
     {'i': 123})
])
async def test_command_success(addr, create_conf, model, with_operate_time,
                               value_type, iec61850_value, json_value):
    event_queue = aio.Queue()
    select_queue = aio.Queue()
    cancel_queue = aio.Queue()
    operate_queue = aio.Queue()

    name = 'cmd name'
    session_id = 'session id'
    origin = iec61850.Origin(category=iec61850.OriginCategory.REMOTE_CONTROL,
                             identification=b'xyz')
    test = False
    checks = set(iec61850.Check)
    cmd_ref = iec61850.CommandRef(logical_device='ld',
                                  logical_node='ln',
                                  name='cmd')

    conf = create_conf(value_types=[{'logical_device': cmd_ref.logical_device,
                                     'logical_node': cmd_ref.logical_node,
                                     'fc': 'CO',
                                     'name': cmd_ref.name,
                                     'type': value_type_to_json(value_type)}],
                       commands=[{'name': name,
                                  'model': model,
                                  'ref': cmd_ref._asdict(),
                                  'with_operate_time': with_operate_time}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        cmd_value_types={cmd_ref: value_type},
        select_cb=create_queue_cb(select_queue),
        cancel_cb=create_queue_cb(cancel_queue),
        operate_cb=create_queue_cb(operate_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    if model in ['SBO_WITH_NORMAL_SECURITY', 'SBO_WITH_ENHANCED_SECURITY']:
        event = create_command_event(name=name,
                                     session_id=session_id,
                                     action='SELECT',
                                     value=json_value,
                                     origin=origin,
                                     test=test,
                                     checks=checks)
        await aio.call(device.process_events, [event])

        result_cmd_ref, cmd = await select_queue.get()
        assert result_cmd_ref == cmd_ref

        if model == 'SBO_WITH_NORMAL_SECURITY':
            assert cmd is None

        else:
            assert cmd.value == iec61850_value
            assert cmd.operate_time == (operate_time if with_operate_time
                                        else None)
            assert cmd.origin == origin
            assert_equal_timestamp(
                hat.event.common.timestamp_to_float(event.timestamp),
                cmd.t.value.timestamp())
            assert cmd.t.clock_failure is False
            assert cmd.t.not_synchronized is False
            assert cmd.test == test
            assert cmd.checks == checks

        event = await event_queue.get()
        assert_command_event(event=event,
                             name=name,
                             session_id=session_id,
                             action='SELECT',
                             success=True)

        event = create_command_event(name=name,
                                     session_id=session_id,
                                     action='CANCEL',
                                     value=json_value,
                                     origin=origin,
                                     test=test,
                                     checks=checks)
        await aio.call(device.process_events, [event])

        result_cmd_ref, cmd = await cancel_queue.get()
        assert result_cmd_ref == cmd_ref
        assert cmd.value == iec61850_value
        assert cmd.operate_time == (operate_time if with_operate_time
                                    else None)
        assert cmd.origin == origin
        assert_equal_timestamp(
            hat.event.common.timestamp_to_float(event.timestamp),
            cmd.t.value.timestamp())
        assert cmd.t.clock_failure is False
        assert cmd.t.not_synchronized is False
        assert cmd.test == test
        assert cmd.checks == checks

        event = await event_queue.get()
        assert_command_event(event=event,
                             name=name,
                             session_id=session_id,
                             action='CANCEL',
                             success=True)

        event = create_command_event(name=name,
                                     session_id=session_id,
                                     action='SELECT',
                                     value=json_value,
                                     origin=origin,
                                     test=test,
                                     checks=checks)
        await aio.call(device.process_events, [event])

        await select_queue.get()

    event = create_command_event(name=name,
                                 session_id=session_id,
                                 action='OPERATE',
                                 value=json_value,
                                 origin=origin,
                                 test=test,
                                 checks=checks)
    await aio.call(device.process_events, [event])

    result_cmd_ref, cmd = await operate_queue.get()
    assert result_cmd_ref == cmd_ref
    assert cmd.value == iec61850_value
    assert cmd.operate_time == (operate_time if with_operate_time
                                else None)
    assert cmd.origin == origin
    assert_equal_timestamp(
        hat.event.common.timestamp_to_float(event.timestamp),
        cmd.t.value.timestamp())
    assert cmd.t.clock_failure is False
    assert cmd.t.not_synchronized is False
    assert cmd.test == test
    assert cmd.checks == checks

    event = await event_queue.get()
    assert_command_event(event=event,
                         name=name,
                         session_id=session_id,
                         action='OPERATE',
                         success=True)

    if model == 'SBO_WITH_ENHANCED_SECURITY':
        # TODO termination
        pass

    await device.async_close()
    await server.async_close()
    await client.async_close()
