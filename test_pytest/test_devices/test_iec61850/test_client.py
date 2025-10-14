from collections.abc import Callable, Collection, Iterable
import asyncio
import datetime
import itertools
import math
import typing

import pytest

from hat import aio
from hat import json
from hat import util
from hat.drivers import iec61850
from hat.drivers import mms
from hat.drivers import tcp
import hat.event.common

from hat.gateway.devices.iec61850.client import info

from .server import DataDef, create_server


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
                      reasons: Iterable[iec61850.Reason] = [],
                      value: json.Data | None = None,
                      quality: iec61850.Quality | None = None,
                      timestamp: iec61850.Timestamp | None = None,
                      selected: bool | None = None):
    assert event.type == (*event_type_prefix, 'gateway', 'data', name)
    assert event.source_timestamp is None
    assert event.payload.data['reasons'] == [reason.name for reason in reasons]

    if value is not None:
        if isinstance(value, float):
            math.isclose(event.payload.data['value'], value)
        else:
            assert event.payload.data['value'] == value

    if quality is not None:
        assert event.payload.data['quality'] == {
            'validity': quality.validity.name,
            'details': [i.name for i in quality.details],
            'source': quality.source.name,
            'test': quality.test,
            'operator_blocked': quality.operator_blocked}

    if timestamp is not None:
        assert math.isclose(event.payload.data['timestamp']['value'],
                            timestamp.value.timestamp())
        assert (event.payload.data['timestamp']['leap_second'] ==
                timestamp.leap_second)
        assert (event.payload.data['timestamp']['clock_failure'] ==
                timestamp.clock_failure)
        assert (event.payload.data['timestamp']['not_synchronized'] ==
                timestamp.not_synchronized)

        if timestamp.accuracy is not None:
            assert (event.payload.data['timestamp']['accuracy'] ==
                    timestamp.accuracy)

    if selected is not None:
        assert event.payload.data['selected'] == selected


def assert_command_event(event: hat.event.common.Event,
                         name: str,
                         session_id: str,
                         action: str,
                         success: bool,
                         service_error: str | None = None,
                         additional_cause: str | None = None,
                         test_error: str | None = None):
    assert event.type == (*event_type_prefix, 'gateway', 'command', name)
    assert event.source_timestamp is None
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
    assert event.source_timestamp is None
    assert event.payload.data['session_id'] == session_id
    assert event.payload.data['success'] == success

    if error is not None:
        assert event.payload.data['error'] == error


def assert_entry_id_event(event: hat.event.common.Event,
                          report_id: str,
                          entry_id: util.Bytes):
    assert event.type == (*event_type_prefix, 'gateway', 'entry_id', report_id)
    assert event.source_timestamp is None
    assert event.payload.data == (entry_id.hex() if entry_id is not None
                                  else None)


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
                          entry_id: util.Bytes | None
                          ) -> hat.event.common.Event:
    return create_event((*event_type_prefix, 'gateway', 'entry_id', report_id),
                        (entry_id.hex() if entry_id is not None else None))


def create_queue_cb(queue: aio.Queue) -> Callable:
    return lambda *args: queue.put_nowait(args)


def value_type_to_json(value_type: iec61850.ValueType) -> json.Data:
    if isinstance(value_type, iec61850.BasicValueType):
        return value_type.value

    if isinstance(value_type, iec61850.AcsiValueType):
        return value_type.value

    if isinstance(value_type, iec61850.ArrayValueType):
        return {'type': 'ARRAY',
                'element_type': value_type_to_json(value_type.type),
                'length': value_type.length}

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
        return {'name': '',
                'connection': {'host': '127.0.0.1',
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
    rcb_queue = aio.Queue()

    dataset_ref = iec61850.PersistedDatasetRef(logical_device='ld',
                                               logical_node='ln',
                                               name='ds')
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
                                 rcb_cb=create_queue_cb(rcb_queue),
                                 dataset_cb=create_queue_cb(dataset_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    async def wait_gi():
        while True:
            _, attr_type, __ = await rcb_queue.get()
            if attr_type == iec61850.RcbAttrType.GI:
                return

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(wait_gi(), 0.1)

    assert dataset_queue.empty()

    await device.async_close()
    await server.async_close()
    await client.async_close()


async def test_dynamic_dataset_same_values(addr, create_conf):
    dataset_queue = aio.Queue()

    logical_device = 'ld'
    logical_node = 'ln'

    dataset_ref = iec61850.PersistedDatasetRef(logical_device=logical_device,
                                               logical_node=logical_node,
                                               name='ds')
    data_ref = iec61850.DataRef(logical_device=logical_device,
                                logical_node=logical_node,
                                fc='fc',
                                names=('a', 'b', 'c'))

    conf = create_conf(datasets=[{'ref': {'logical_device': logical_device,
                                          'logical_node': logical_node,
                                          'name': dataset_ref.name},
                                  'values': [{'logical_device': logical_device,
                                              'logical_node': logical_node,
                                              'fc': data_ref.fc,
                                              'names': list(data_ref.names)}],
                                  'dynamic': True}])
    client = EventerClient()

    server = await create_server(addr=addr,
                                 datasets={dataset_ref: [data_ref]},
                                 dataset_cb=create_queue_cb(dataset_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(dataset_queue.get(), 0.1)

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

    async def wait_enable(value):
        while True:
            result_rcb_ref, attr_type, attr_value = await rcb_queue.get()
            assert result_rcb_ref == rcb_ref

            if attr_type != iec61850.RcbAttrType.REPORT_ENABLE:
                continue

            assert attr_value is value
            break

    await aio.wait_for(wait_enable(False), 0.1)

    await aio.wait_for(wait_enable(True), 0.1)

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
                                  'dynamic': False}],
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


@pytest.mark.parametrize('dyn_dataset_ref', [
    iec61850.NonPersistedDatasetRef('ds 1'),
    iec61850.PersistedDatasetRef(logical_device='ld1',
                                 logical_node='ln1',
                                 name='ds xyz')])
@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
async def test_rcb_set_dynamic_dataset(addr, create_conf, rcb_type,
                                       dyn_dataset_ref):
    rcb_queue = aio.Queue()

    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')

    if isinstance(dyn_dataset_ref, iec61850.NonPersistedDatasetRef):
        ds_ref_conf = dyn_dataset_ref.name
    else:
        ds_ref_conf = dyn_dataset_ref._asdict()
    conf = create_conf(datasets=[{'ref': ds_ref_conf,
                                  'values': [],
                                  'dynamic': True}],
                       rcbs=[{'ref': {'logical_device': 'ld',
                                      'logical_node': 'ln',
                                      'type': rcb_type.name,
                                      'name': 'rcb'},
                              'report_id': 'report id',
                              'dataset': ds_ref_conf}])
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

    async def wait_set_dataset():
        while True:
            result_rcb_ref, attr_type, attr_value = await rcb_queue.get()
            if attr_type != iec61850.RcbAttrType.DATASET:
                continue

            assert result_rcb_ref == rcb_ref
            assert attr_value == dyn_dataset_ref
            return

    await aio.wait_for(wait_set_dataset(), 0.1)

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

    if rcb_type == iec61850.RcbType.BUFFERED and reservation_time is not None:
        _, attr_type, attr_value = await rcb_queue.get()
        assert attr_type == iec61850.RcbAttrType.RESERVATION_TIME
        assert attr_value == reservation_time

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

    if rcb_type == iec61850.RcbType.UNBUFFERED:
        _, attr_type, attr_value = await rcb_queue.get()
        assert attr_type == iec61850.RcbAttrType.RESERVE
        assert attr_value is True

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
        if rcb_type == iec61850.RcbType.UNBUFFERED:
            raise Exception('invalid query for unbuffered')

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


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
async def test_rcb_null_entry_id(addr, create_conf, rcb_type):
    rcb_queue = aio.Queue()

    entry_id = None
    report_id = 'report id'
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
                              'report_id': report_id,
                              'dataset': 'ds'}])

    def on_query(params):
        if rcb_type == iec61850.RcbType.UNBUFFERED:
            raise Exception('invalid query for unbuffered')

        assert isinstance(params, hat.event.common.QueryLatestParams)
        assert set(params.event_types) == {(*event_type_prefix, 'gateway',
                                            'entry_id', report_id)}

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
        assert iec61850.RcbAttrType.PURGE_BUFFER in server.rcbs[rcb_ref]
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

    (iec61850.AcsiValueType.BINARY_CONTROL,
     iec61850.BinaryControl.HIGHER,
     'HIGHER'),

    (iec61850.AcsiValueType.ANALOGUE,
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

    def _create_command_event(action):
        return create_command_event(name=name,
                                    session_id=session_id,
                                    action=action,
                                    value=json_value,
                                    origin=origin,
                                    test=test,
                                    checks=checks)

    def _assert_command_event(event, action):
        assert_command_event(event=event,
                             name=name,
                             session_id=session_id,
                             action=action,
                             success=True)

    def assert_command(cmd, is_cancel=False):
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
        assert cmd.checks == (checks if not is_cancel else set())

    conf = create_conf(
        value_types=[
            {'logical_device': cmd_ref.logical_device,
             'logical_node': cmd_ref.logical_node,
             'fc': 'CO',
             'name': cmd_ref.name,
             'type': {'type': 'STRUCT',
                      'elements': [
                        {'name': 'Oper',
                         'type': {
                            'type': 'STRUCT',
                            'elements': [{'name': 'ctlVal',
                                          'type': value_type_to_json(
                                             value_type)}]}}]}}],
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
        event = _create_command_event('SELECT')
        await aio.call(device.process_events, [event])

        result_cmd_ref, cmd = await select_queue.get()
        assert result_cmd_ref == cmd_ref

        if model == 'SBO_WITH_NORMAL_SECURITY':
            assert cmd is None

        else:
            assert_command(cmd)

        event = await event_queue.get()
        _assert_command_event(event, 'SELECT')

        event = _create_command_event('CANCEL')
        await aio.call(device.process_events, [event])

        result_cmd_ref, cmd = await cancel_queue.get()
        assert result_cmd_ref == cmd_ref
        assert_command(cmd, is_cancel=True)

        event = await event_queue.get()
        _assert_command_event(event, 'CANCEL')

        event = _create_command_event('SELECT')
        await aio.call(device.process_events, [event])

        await select_queue.get()
        await event_queue.get()

    event = _create_command_event('OPERATE')
    await aio.call(device.process_events, [event])

    result_cmd_ref, cmd = await operate_queue.get()
    assert result_cmd_ref == cmd_ref
    assert_command(cmd)

    event = await event_queue.get()
    _assert_command_event(event, 'OPERATE')

    if model == 'SBO_WITH_ENHANCED_SECURITY':
        await server.send_termination(cmd_ref, cmd)

        event = await event_queue.get()
        _assert_command_event(event, 'TERMINATION')

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('model', ['DIRECT_WITH_NORMAL_SECURITY',
                                   'SBO_WITH_NORMAL_SECURITY',
                                   'DIRECT_WITH_ENHANCED_SECURITY',
                                   'SBO_WITH_ENHANCED_SECURITY'])
@pytest.mark.parametrize('with_operate_time', [True, False])
@pytest.mark.parametrize('value_type, json_value', [
    (iec61850.BasicValueType.BOOLEAN,
     True),

    (iec61850.BasicValueType.INTEGER,
     123),

    (iec61850.AcsiValueType.BINARY_CONTROL,
     'HIGHER'),

    (iec61850.AcsiValueType.ANALOGUE,
     {'i': 123})
])
async def test_command_error(addr, create_conf, model, with_operate_time,
                             value_type, json_value):
    event_queue = aio.Queue()

    name = 'cmd name'
    session_id = 'session id'
    origin = iec61850.Origin(category=iec61850.OriginCategory.REMOTE_CONTROL,
                             identification=b'xyz')
    test = False
    checks = set(iec61850.Check)
    cmd_ref = iec61850.CommandRef(logical_device='ld',
                                  logical_node='ln',
                                  name='cmd')

    def _create_command_event(action):
        return create_command_event(name=name,
                                    session_id=session_id,
                                    action=action,
                                    value=json_value,
                                    origin=origin,
                                    test=test,
                                    checks=checks)

    def _assert_command_event(event, action):
        assert_command_event(event=event,
                             name=name,
                             session_id=session_id,
                             action=action,
                             success=False)

    def on_command(cmd_ref, cmd):
        return mms.DataAccessError.OBJECT_UNDEFINED

    conf = create_conf(
        value_types=[
            {'logical_device': cmd_ref.logical_device,
             'logical_node': cmd_ref.logical_node,
             'fc': 'CO',
             'name': cmd_ref.name,
             'type': {'type': 'STRUCT',
                      'elements': [
                        {'name': 'Oper',
                         'type': {
                            'type': 'STRUCT',
                            'elements': [{'name': 'ctlVal',
                                          'type': value_type_to_json(
                                            value_type)}]}}]}}],
        commands=[{'name': name,
                   'model': model,
                   'ref': cmd_ref._asdict(),
                   'with_operate_time': with_operate_time}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        cmd_value_types={cmd_ref: value_type},
        select_cb=on_command,
        cancel_cb=on_command,
        operate_cb=on_command)
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    if model in ['SBO_WITH_NORMAL_SECURITY', 'SBO_WITH_ENHANCED_SECURITY']:
        event = _create_command_event('SELECT')
        await aio.call(device.process_events, [event])

        event = await event_queue.get()
        _assert_command_event(event, 'SELECT')

        event = _create_command_event('CANCEL')
        await aio.call(device.process_events, [event])

        event = await event_queue.get()
        _assert_command_event(event, 'CANCEL')

    event = _create_command_event('OPERATE')
    await aio.call(device.process_events, [event])

    event = await event_queue.get()
    _assert_command_event(event, 'OPERATE')

    await device.async_close()
    await server.async_close()
    await client.async_close()


# TODO test termination error


@pytest.mark.parametrize('value_type, mms_data, json_value', [
    (iec61850.BasicValueType.BOOLEAN,
     mms.BooleanData(True),
     True),

    (iec61850.BasicValueType.INTEGER,
     mms.IntegerData(123),
     123),

    (iec61850.BasicValueType.UNSIGNED,
     mms.UnsignedData(123),
     123),

    (iec61850.BasicValueType.FLOAT,
     mms.FloatingPointData(123.5),
     123.5),

    (iec61850.BasicValueType.BIT_STRING,
     mms.BitStringData([True, False, True]),
     [True, False, True]),

    (iec61850.BasicValueType.OCTET_STRING,
     mms.OctetStringData(b'abc'),
     b'abc'.hex()),

    (iec61850.BasicValueType.VISIBLE_STRING,
     mms.VisibleStringData('abc'),
     'abc'),

    (iec61850.BasicValueType.MMS_STRING,
     mms.MmsStringData('abc'),
     'abc'),

    (iec61850.AcsiValueType.QUALITY,
     mms.BitStringData([False] * 13),
     {'validity': 'GOOD',
      'details': [],
      'source': 'PROCESS',
      'test': False,
      'operator_blocked': False}),

    (iec61850.AcsiValueType.TIMESTAMP,
     mms.UtcTimeData(
        value=datetime.datetime.fromtimestamp(0, datetime.timezone.utc),
        leap_second=False,
        clock_failure=False,
        not_synchronized=False,
        accuracy=None),
     {'value': 0,
      'leap_second': False,
      'clock_failure': False,
      'not_synchronized': False}),

    (iec61850.AcsiValueType.DOUBLE_POINT,
     mms.BitStringData([False] * 2),
     'INTERMEDIATE'),

    (iec61850.AcsiValueType.DIRECTION,
     mms.IntegerData(1),
     'FORWARD'),

    (iec61850.AcsiValueType.SEVERITY,
     mms.IntegerData(2),
     'MAJOR'),

    (iec61850.AcsiValueType.ANALOGUE,
     mms.StructureData([mms.FloatingPointData(123.5)]),
     {'f': 123.5}),

    (iec61850.AcsiValueType.VECTOR,
     mms.StructureData([mms.StructureData([mms.IntegerData(123)])]),
     {'magnitude': {'i': 123}}),

    (iec61850.AcsiValueType.STEP_POSITION,
     mms.StructureData([mms.IntegerData(123)]),
     {'value': 123}),

    (iec61850.AcsiValueType.BINARY_CONTROL,
     mms.BitStringData([False] * 2),
     'STOP'),

    (iec61850.ArrayValueType(type=iec61850.BasicValueType.INTEGER,
                             length=3),
     mms.ArrayData([mms.IntegerData(1),
                    mms.IntegerData(2),
                    mms.IntegerData(3)]),
     [1, 2, 3]),

    (iec61850.StructValueType([('a', iec61850.BasicValueType.INTEGER),
                               ('b', iec61850.BasicValueType.INTEGER),
                               ('c', iec61850.BasicValueType.INTEGER)]),
     mms.StructureData([mms.IntegerData(1),
                        mms.IntegerData(2),
                        mms.IntegerData(3)]),
     {'a': 1, 'b': 2, 'c': 3}),
])
async def test_change_success(addr, create_conf, value_type, mms_data,
                              json_value):
    write_queue = aio.Queue()
    event_queue = aio.Queue()

    name = 'data name'
    session_id = 'session id'
    data_ref = iec61850.DataRef(logical_device='ld',
                                logical_node='ln',
                                fc='fc',
                                names=('a', 123, 'b'))

    conf = create_conf(
        value_types=[{
            'logical_device': data_ref.logical_device,
            'logical_node': data_ref.logical_node,
            'fc': data_ref.fc,
            'name': 'a',
            'type': value_type_to_json(
                iec61850.ArrayValueType(
                    type=iec61850.StructValueType([('b', value_type)]),
                    length=124))}],
        changes=[{'name': name,
                  'ref': {'logical_device': data_ref.logical_device,
                          'logical_node': data_ref.logical_node,
                          'fc': data_ref.fc,
                          'names': list(data_ref.names)}}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(addr=addr,
                                 write_cb=create_queue_cb(write_queue))
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    event = create_change_event(name=name,
                                session_id=session_id,
                                value=json_value)
    await aio.call(device.process_events, [event])

    result_data_ref, result_mms_data = await write_queue.get()
    assert result_data_ref == data_ref
    assert result_mms_data == mms_data

    event = await event_queue.get()
    assert_change_event(event=event,
                        name=name,
                        session_id=session_id,
                        success=True)

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('value_type, json_value', [
    (iec61850.BasicValueType.BOOLEAN,
     True),

    (iec61850.BasicValueType.INTEGER,
     123),

    (iec61850.BasicValueType.UNSIGNED,
     123),

    (iec61850.BasicValueType.FLOAT,
     123.5),

    (iec61850.BasicValueType.BIT_STRING,
     [True, False, True]),

    (iec61850.BasicValueType.OCTET_STRING,
     b'abc'.hex()),

    (iec61850.BasicValueType.VISIBLE_STRING,
     'abc'),

    (iec61850.BasicValueType.MMS_STRING,
     'abc'),

    (iec61850.AcsiValueType.QUALITY,
     {'validity': 'GOOD',
      'details': [],
      'source': 'PROCESS',
      'test': False,
      'operator_blocked': False}),

    (iec61850.AcsiValueType.TIMESTAMP,
     {'value': 0,
      'leap_second': False,
      'clock_failure': False,
      'not_synchronized': False}),

    (iec61850.AcsiValueType.DOUBLE_POINT,
     'INTERMEDIATE'),

    (iec61850.AcsiValueType.DIRECTION,
     'FORWARD'),

    (iec61850.AcsiValueType.SEVERITY,
     'MAJOR'),

    (iec61850.AcsiValueType.ANALOGUE,
     {'f': 123.5}),

    (iec61850.AcsiValueType.VECTOR,
     {'magnitude': {'i': 123}}),

    (iec61850.AcsiValueType.STEP_POSITION,
     {'value': 123}),

    (iec61850.AcsiValueType.BINARY_CONTROL,
     'STOP'),

    (iec61850.ArrayValueType(type=iec61850.BasicValueType.INTEGER,
                             length=3),
     [1, 2, 3]),

    (iec61850.StructValueType([('a', iec61850.BasicValueType.INTEGER),
                               ('b', iec61850.BasicValueType.INTEGER),
                               ('c', iec61850.BasicValueType.INTEGER)]),
     {'a': 1, 'b': 2, 'c': 3}),
])
async def test_change_error(addr, create_conf, value_type, json_value):
    event_queue = aio.Queue()

    name = 'data name'
    session_id = 'session id'
    data_ref = iec61850.DataRef(logical_device='ld',
                                logical_node='ln',
                                fc='fc',
                                names=('a', 123, 'b'))

    def on_write(data_ref, mms_data):
        return mms.DataAccessError.OBJECT_UNDEFINED

    conf = create_conf(
        value_types=[{
            'logical_device': data_ref.logical_device,
            'logical_node': data_ref.logical_node,
            'fc': data_ref.fc,
            'name': 'a',
            'type': value_type_to_json(
                iec61850.ArrayValueType(
                    type=iec61850.StructValueType([('b', value_type)]),
                    length=124))}],
        changes=[{'name': name,
                  'ref': {'logical_device': data_ref.logical_device,
                          'logical_node': data_ref.logical_node,
                          'fc': data_ref.fc,
                          'names': list(data_ref.names)}}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(addr=addr,
                                 write_cb=on_write)
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    event = create_change_event(name=name,
                                session_id=session_id,
                                value=json_value)
    await aio.call(device.process_events, [event])

    event = await event_queue.get()
    assert_change_event(event=event,
                        name=name,
                        session_id=session_id,
                        success=False)

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('entry_id', [None, b'xyz'])
async def test_data_empty_report(addr, create_conf, rcb_type, entry_id):
    event_queue = aio.Queue()

    name = 'data name'
    value_type = iec61850.BasicValueType.BOOLEAN
    report_id = 'report id'
    dataset = 'ds'
    data_ref = iec61850.DataRef(logical_device='ld',
                                logical_node='ln',
                                fc='fc',
                                names=('a', 123, 'b'))
    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')
    data_defs = [DataDef(ref=data_ref,
                         value_type=value_type)]

    conf = create_conf(
        value_types=[{
            'logical_device': data_ref.logical_device,
            'logical_node': data_ref.logical_node,
            'fc': data_ref.fc,
            'name': 'a',
            'type': value_type_to_json(
                iec61850.ArrayValueType(
                    type=iec61850.StructValueType([('b', value_type)]),
                    length=124))}],
        datasets=[{'ref': dataset,
                   'values': [{'logical_device': data_ref.logical_device,
                               'logical_node': data_ref.logical_node,
                               'fc': data_ref.fc,
                               'names': list(data_ref.names)}],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': rcb_ref.logical_device,
                       'logical_node': rcb_ref.logical_node,
                       'type': rcb_type.name,
                       'name': rcb_ref.name},
               'report_id': report_id,
               'dataset': dataset}],
        data=[{'name': name,
               'report_id': report_id,
               'value': {'logical_device': data_ref.logical_device,
                         'logical_node': data_ref.logical_node,
                         'fc': data_ref.fc,
                         'names': list(data_ref.names)}}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: report_id,
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    dataset)}})
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    report = iec61850.Report(report_id=report_id,
                             sequence_number=None,
                             subsequence_number=None,
                             more_segments_follow=None,
                             dataset=None,
                             buffer_overflow=None,
                             conf_revision=None,
                             entry_time=None,
                             entry_id=entry_id,
                             data=[])
    await server.send_report(report, data_defs)

    if rcb_type == iec61850.RcbType.BUFFERED:
        event = await event_queue.get()
        assert_entry_id_event(event=event,
                              report_id=report_id,
                              entry_id=entry_id)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(event_queue.get(), 0.01)

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize('rcb_type', iec61850.RcbType)
@pytest.mark.parametrize('value_type, iec61850_value, json_value', [
    (iec61850.BasicValueType.BOOLEAN,
     True,
     True),

    (iec61850.BasicValueType.INTEGER,
     123,
     123),

    (iec61850.BasicValueType.UNSIGNED,
     123,
     123),

    (iec61850.BasicValueType.FLOAT,
     123.5,
     123.5),

    (iec61850.BasicValueType.BIT_STRING,
     [True, False, True],
     [True, False, True]),

    (iec61850.BasicValueType.OCTET_STRING,
     b'abc',
     b'abc'.hex()),

    (iec61850.BasicValueType.VISIBLE_STRING,
     'abc',
     'abc'),

    (iec61850.BasicValueType.MMS_STRING,
     'abc',
     'abc'),

    (iec61850.AcsiValueType.QUALITY,
     iec61850.Quality(validity=iec61850.QualityValidity.GOOD,
                      details=set(),
                      source=iec61850.QualitySource.PROCESS,
                      test=False,
                      operator_blocked=False),
     {'validity': 'GOOD',
      'details': [],
      'source': 'PROCESS',
      'test': False,
      'operator_blocked': False}),

    (iec61850.AcsiValueType.TIMESTAMP,
     iec61850.Timestamp(
        value=datetime.datetime.fromtimestamp(0, datetime.timezone.utc),
        leap_second=False,
        clock_failure=False,
        not_synchronized=False,
        accuracy=None),
     {'value': 0,
      'leap_second': False,
      'clock_failure': False,
      'not_synchronized': False}),

    (iec61850.AcsiValueType.DOUBLE_POINT,
     iec61850.DoublePoint.INTERMEDIATE,
     'INTERMEDIATE'),

    (iec61850.AcsiValueType.DIRECTION,
     iec61850.Direction.FORWARD,
     'FORWARD'),

    (iec61850.AcsiValueType.SEVERITY,
     iec61850.Severity.MAJOR,
     'MAJOR'),

    (iec61850.AcsiValueType.ANALOGUE,
     iec61850.Analogue(f=123.5),
     {'f': 123.5}),

    (iec61850.AcsiValueType.VECTOR,
     iec61850.Vector(magnitude=iec61850.Analogue(i=123),
                     angle=None),
     {'magnitude': {'i': 123}}),

    (iec61850.AcsiValueType.STEP_POSITION,
     iec61850.StepPosition(value=123,
                           transient=None),
     {'value': 123}),

    (iec61850.AcsiValueType.BINARY_CONTROL,
     iec61850.BinaryControl.STOP,
     'STOP'),

    (iec61850.ArrayValueType(type=iec61850.BasicValueType.INTEGER,
                             length=3),
     [1, 2, 3],
     [1, 2, 3]),

    (iec61850.StructValueType([('a', iec61850.BasicValueType.INTEGER),
                               ('b', iec61850.BasicValueType.INTEGER),
                               ('c', iec61850.BasicValueType.INTEGER)]),
     {'a': 1, 'b': 2, 'c': 3},
     {'a': 1, 'b': 2, 'c': 3}),
])
async def test_data_report(addr, create_conf, rcb_type, value_type,
                           iec61850_value, json_value):
    event_queue = aio.Queue()

    name = 'data name'
    report_id = 'report id'
    dataset = 'ds'
    data_ref = iec61850.DataRef(logical_device='ld',
                                logical_node='ln',
                                fc='fc',
                                names=('a', ))
    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')
    data_defs = [DataDef(ref=data_ref,
                         value_type=value_type)]

    conf = create_conf(
        value_types=[{'logical_device': data_ref.logical_device,
                      'logical_node': data_ref.logical_node,
                      'fc': data_ref.fc,
                      'name': 'a',
                      'type': value_type_to_json(value_type)}],
        datasets=[{'ref': dataset,
                   'values': [{'logical_device': data_ref.logical_device,
                               'logical_node': data_ref.logical_node,
                               'fc': data_ref.fc,
                               'names': list(data_ref.names)}],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': rcb_ref.logical_device,
                       'logical_node': rcb_ref.logical_node,
                       'type': rcb_type.name,
                       'name': rcb_ref.name},
               'report_id': report_id,
               'dataset': dataset}],
        data=[{'name': name,
               'report_id': report_id,
               'value': {'logical_device': data_ref.logical_device,
                         'logical_node': data_ref.logical_node,
                         'fc': data_ref.fc,
                         'names': list(data_ref.names)}}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: report_id,
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    dataset)}})
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    report = iec61850.Report(report_id=report_id,
                             sequence_number=None,
                             subsequence_number=None,
                             more_segments_follow=None,
                             dataset=None,
                             buffer_overflow=None,
                             conf_revision=None,
                             entry_time=None,
                             entry_id=None,
                             data=[iec61850.ReportData(ref=data_ref,
                                                       value=iec61850_value,
                                                       reasons=None)])
    await server.send_report(report, data_defs)

    entry_id_event = None
    data_event = None
    while (data_event is None or
            (rcb_type == iec61850.RcbType.BUFFERED and entry_id_event is None)):  # NOQA
        event = await event_queue.get()

        if event.type[-2] == 'entry_id':
            assert entry_id_event is None
            entry_id_event = event

        elif event.type[-2] == 'data':
            assert data_event is None
            data_event = event

        else:
            raise Exception('invalid event')

    if rcb_type == iec61850.RcbType.BUFFERED:
        assert_entry_id_event(event=entry_id_event,
                              report_id=report_id,
                              entry_id=None)

    else:
        assert entry_id_event is None

    assert_data_event(event=data_event,
                      name=name,
                      value=json_value)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(event_queue.get(), 0.01)

    await device.async_close()
    await server.async_close()
    await client.async_close()


async def test_data_report_ids(addr, create_conf):
    event_queue = aio.Queue()

    value_type = iec61850.BasicValueType.BOOLEAN
    value = True
    rcb_type = iec61850.RcbType.UNBUFFERED
    name = 'data name'
    report_id = 'report id'
    dataset = 'ds'
    data_ref = iec61850.DataRef(logical_device='ld',
                                logical_node='ln',
                                fc='fc',
                                names=('a', ))
    rcb_ref = iec61850.RcbRef(logical_device='ld',
                              logical_node='ln',
                              type=rcb_type,
                              name='rcb')
    data_defs = [DataDef(ref=data_ref,
                         value_type=value_type)]

    conf = create_conf(
        value_types=[{'logical_device': data_ref.logical_device,
                      'logical_node': data_ref.logical_node,
                      'fc': data_ref.fc,
                      'name': 'a',
                      'type': value_type_to_json(value_type)}],
        datasets=[{'ref': dataset,
                   'values': [{'logical_device': data_ref.logical_device,
                               'logical_node': data_ref.logical_node,
                               'fc': data_ref.fc,
                               'names': list(data_ref.names)}],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': rcb_ref.logical_device,
                       'logical_node': rcb_ref.logical_node,
                       'type': rcb_type.name,
                       'name': rcb_ref.name},
               'report_id': report_id,
               'dataset': dataset}],
        data=[{'name': name,
               'report_id': report_id,
               'value': {'logical_device': data_ref.logical_device,
                         'logical_node': data_ref.logical_node,
                         'fc': data_ref.fc,
                         'names': list(data_ref.names)}}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: report_id,
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    dataset)}})
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    report = iec61850.Report(report_id=report_id,
                             sequence_number=None,
                             subsequence_number=None,
                             more_segments_follow=None,
                             dataset=None,
                             buffer_overflow=None,
                             conf_revision=None,
                             entry_time=None,
                             entry_id=None,
                             data=[iec61850.ReportData(ref=data_ref,
                                                       value=value,
                                                       reasons=None)])
    await server.send_report(report, data_defs)

    data_event = await event_queue.get()
    assert_data_event(event=data_event,
                      name=name,
                      value=value)

    report = iec61850.Report(report_id=f'not {report_id}',
                             sequence_number=None,
                             subsequence_number=None,
                             more_segments_follow=None,
                             dataset=None,
                             buffer_overflow=None,
                             conf_revision=None,
                             entry_time=None,
                             entry_id=None,
                             data=[iec61850.ReportData(ref=data_ref,
                                                       value=value,
                                                       reasons=None)])
    await server.send_report(report, data_defs)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(event_queue.get(), 0.01)

    await device.async_close()
    await server.async_close()
    await client.async_close()


async def test_data_subset_report(addr, create_conf):
    event_queue = aio.Queue()

    name = 'data name'
    report_id = 'report id'
    dataset = 'ds'
    logical_device = 'ld'
    logical_node = 'ln'
    rcb_type = iec61850.RcbType.BUFFERED
    value_type = iec61850.BasicValueType.INTEGER
    value_data_ref = iec61850.DataRef(logical_device=logical_device,
                                      logical_node=logical_node,
                                      fc='fc1',
                                      names=('a', 'value'))
    quality_data_ref = iec61850.DataRef(logical_device=logical_device,
                                        logical_node=logical_node,
                                        fc='fc2',
                                        names=('a', 'quality'))
    timestamp_data_ref = iec61850.DataRef(logical_device=logical_device,
                                          logical_node=logical_node,
                                          fc='fc3',
                                          names=('a', 'timestamp'))
    selected_data_ref = iec61850.DataRef(logical_device=logical_device,
                                         logical_node=logical_node,
                                         fc='fc4',
                                         names=('a', 'selected'))
    rcb_ref = iec61850.RcbRef(logical_device=logical_device,
                              logical_node=logical_node,
                              type=rcb_type,
                              name='rcb')
    data_defs = [DataDef(ref=value_data_ref,
                         value_type=value_type),
                 DataDef(ref=quality_data_ref,
                         value_type=iec61850.AcsiValueType.QUALITY),
                 DataDef(ref=timestamp_data_ref,
                         value_type=iec61850.AcsiValueType.TIMESTAMP),
                 DataDef(ref=selected_data_ref,
                         value_type=iec61850.BasicValueType.BOOLEAN)]

    conf = create_conf(
        value_types=[
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': value_data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(
                iec61850.StructValueType([('value', value_type)]))},
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': quality_data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(
                iec61850.StructValueType([('quality',
                                           iec61850.AcsiValueType.QUALITY)]))},
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': timestamp_data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(
                iec61850.StructValueType([('timestamp',
                                           iec61850.AcsiValueType.TIMESTAMP)]))},  # NOQA
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': selected_data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(
                iec61850.StructValueType([('selected',
                                           iec61850.BasicValueType.BOOLEAN)]))}],  # NOQA
        datasets=[{'ref': dataset,
                   'values': [{'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': value_data_ref.fc,
                               'names': list(value_data_ref.names)},
                              {'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': quality_data_ref.fc,
                               'names': list(quality_data_ref.names)},
                              {'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': timestamp_data_ref.fc,
                               'names': list(timestamp_data_ref.names)},
                              {'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': selected_data_ref.fc,
                               'names': list(selected_data_ref.names)}],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': logical_device,
                       'logical_node': logical_node,
                       'type': rcb_type.name,
                       'name': rcb_ref.name},
               'report_id': report_id,
               'dataset': dataset}],
        data=[{'name': name,
               'report_id': report_id,
               'value': {'logical_device': logical_device,
                         'logical_node': logical_node,
                         'fc': value_data_ref.fc,
                         'names': list(value_data_ref.names)},
               'quality': {'logical_device': logical_device,
                           'logical_node': logical_node,
                           'fc': quality_data_ref.fc,
                           'names': list(quality_data_ref.names)},
               'timestamp': {'logical_device': logical_device,
                             'logical_node': logical_node,
                             'fc': timestamp_data_ref.fc,
                             'names': list(timestamp_data_ref.names)},
               'selected': {'logical_device': logical_device,
                            'logical_node': logical_node,
                            'fc': selected_data_ref.fc,
                            'names': list(selected_data_ref.names)}}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: report_id,
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    dataset)}})
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    value = 1
    quality = iec61850.Quality(
        validity=iec61850.QualityValidity.GOOD,
        details=set(),
        source=iec61850.QualitySource.PROCESS,
        test=False,
        operator_blocked=False)
    timestamp = iec61850.Timestamp(
        value=datetime.datetime.now(datetime.timezone.utc),
        leap_second=False,
        clock_failure=False,
        not_synchronized=False,
        accuracy=0)
    selected = False

    report = iec61850.Report(
        report_id=report_id,
        sequence_number=None,
        subsequence_number=None,
        more_segments_follow=None,
        dataset=None,
        buffer_overflow=None,
        conf_revision=None,
        entry_time=None,
        entry_id=None,
        data=[iec61850.ReportData(ref=value_data_ref,
                                  value=1,
                                  reasons={iec61850.Reason.DATA_CHANGE}),
              iec61850.ReportData(ref=quality_data_ref,
                                  value=quality,
                                  reasons={iec61850.Reason.QUALITY_CHANGE}),
              iec61850.ReportData(ref=timestamp_data_ref,
                                  value=timestamp,
                                  reasons=None),
              iec61850.ReportData(ref=selected_data_ref,
                                  value=selected,
                                  reasons=None)])
    await server.send_report(report, data_defs)

    entry_id_event = None
    data_event = None
    while (data_event is None or
            (rcb_type == iec61850.RcbType.BUFFERED and entry_id_event is None)):  # NOQA
        event = await event_queue.get()

        if event.type[-2] == 'entry_id':
            assert entry_id_event is None
            entry_id_event = event

        elif event.type[-2] == 'data':
            assert data_event is None
            data_event = event

        else:
            raise Exception('invalid event')

    if rcb_type == iec61850.RcbType.BUFFERED:
        assert_entry_id_event(event=entry_id_event,
                              report_id=report_id,
                              entry_id=None)

    else:
        assert entry_id_event is None

    assert_data_event(event=data_event,
                      name=name,
                      reasons={iec61850.Reason.DATA_CHANGE,
                               iec61850.Reason.QUALITY_CHANGE},
                      value=value,
                      quality=quality,
                      timestamp=timestamp,
                      selected=selected)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(event_queue.get(), 0.01)

    await device.async_close()
    await server.async_close()
    await client.async_close()


@pytest.mark.parametrize(
    'value_type, data_ref, report_data_value, value_names, quality_names, '
    'timestamp_names, selected_names, value, quality, timestamp, selected', [
     (iec61850.StructValueType([
        ('value', iec61850.BasicValueType.INTEGER),
        ('quality', iec61850.AcsiValueType.QUALITY),
        ('timestamp', iec61850.AcsiValueType.TIMESTAMP),
        ('selected', iec61850.BasicValueType.BOOLEAN)]),
      iec61850.DataRef(logical_device='ld',
                       logical_node='ln',
                       fc='fc',
                       names=('a', )),
      {'value': 1,
       'quality': iec61850.Quality(
            validity=iec61850.QualityValidity.GOOD,
            details=set(),
            source=iec61850.QualitySource.PROCESS,
            test=False,
            operator_blocked=False),
       'timestamp': iec61850.Timestamp(
            value=datetime.datetime.now(datetime.timezone.utc),
            leap_second=False,
            clock_failure=False,
            not_synchronized=False,
            accuracy=0),
       'selected': False},
      ['value'],
      ['quality'],
      ['timestamp'],
      ['selected'],
      1,
      iec61850.Quality(
        validity=iec61850.QualityValidity.GOOD,
        details=set(),
        source=iec61850.QualitySource.PROCESS,
        test=False,
        operator_blocked=False),
      iec61850.Timestamp(
        value=datetime.datetime.now(datetime.timezone.utc),
        leap_second=False,
        clock_failure=False,
        not_synchronized=False,
        accuracy=0),
      False),
     (iec61850.AcsiValueType.VECTOR,
      iec61850.DataRef(logical_device='ld',
                       logical_node='ln',
                       fc='fc',
                       names=('a', )),
      iec61850.Vector(magnitude=iec61850.Analogue(f=123.456, i=None),
                      angle=None),
      ['mag', 'f'],
      None,
      None,
      None,
      123.456,
      None,
      None,
      None),
     (iec61850.AcsiValueType.STEP_POSITION,
      iec61850.DataRef(logical_device='ld',
                       logical_node='ln',
                       fc='fc',
                       names=('a', )),
      iec61850.StepPosition(value=234,
                            transient=True),
      ['posVal'],
      None,
      None,
      None,
      234,
      None,
      None,
      None),
     (iec61850.AcsiValueType.STEP_POSITION,
      iec61850.DataRef(logical_device='ld',
                       logical_node='ln',
                       fc='fc',
                       names=('a', )),
      iec61850.StepPosition(value=234,
                            transient=True),
      ['transInd'],
      None,
      None,
      None,
      True,
      None,
      None,
      None),
    ])
async def test_data_superset_report(addr, create_conf, value_type,
                                    data_ref, report_data_value, value_names,
                                    quality_names, timestamp_names,
                                    selected_names, value, quality, timestamp,
                                    selected):
    event_queue = aio.Queue()

    name = 'data name'
    report_id = 'report id'
    dataset = 'ds'
    logical_device = data_ref.logical_device
    logical_node = data_ref.logical_node
    rcb_type = iec61850.RcbType.BUFFERED
    rcb_ref = iec61850.RcbRef(logical_device=logical_device,
                              logical_node=logical_node,
                              type=rcb_type,
                              name='rcb')
    data_defs = [DataDef(ref=data_ref,
                         value_type=value_type)]

    data_conf = {'name': name,
                 'report_id': report_id,
                 'value': {'logical_device': logical_device,
                           'logical_node': logical_node,
                           'fc': data_ref.fc,
                           'names': [*data_ref.names, *value_names]}}
    if quality_names:
        data_conf['quality'] = {'logical_device': logical_device,
                                'logical_node': logical_node,
                                'fc': data_ref.fc,
                                'names': [*data_ref.names, *quality_names]}
    if timestamp_names:
        data_conf['timestamp'] = {'logical_device': logical_device,
                                  'logical_node': logical_node,
                                  'fc': data_ref.fc,
                                  'names': [*data_ref.names, *timestamp_names]}
    if selected_names:
        data_conf['selected'] = {'logical_device': logical_device,
                                 'logical_node': logical_node,
                                 'fc': data_ref.fc,
                                 'names': [*data_ref.names, *selected_names]}
    conf = create_conf(
        value_types=[
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(value_type)}],
        datasets=[{'ref': dataset,
                   'values': [{'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': data_ref.fc,
                               'names': list(data_ref.names)}],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': logical_device,
                       'logical_node': logical_node,
                       'type': rcb_type.name,
                       'name': rcb_ref.name},
               'report_id': report_id,
               'dataset': dataset}],
        data=[data_conf])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: report_id,
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    dataset)}})
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    report = iec61850.Report(
        report_id=report_id,
        sequence_number=None,
        subsequence_number=None,
        more_segments_follow=None,
        dataset=None,
        buffer_overflow=None,
        conf_revision=None,
        entry_time=None,
        entry_id=None,
        data=[iec61850.ReportData(
            ref=data_ref,
            value=report_data_value,
            reasons={iec61850.Reason.DATA_CHANGE})])
    await server.send_report(report, data_defs)

    entry_id_event = None
    data_event = None
    while (data_event is None or
            (rcb_type == iec61850.RcbType.BUFFERED and entry_id_event is None)):  # NOQA
        event = await event_queue.get()

        if event.type[-2] == 'entry_id':
            assert entry_id_event is None
            entry_id_event = event

        elif event.type[-2] == 'data':
            assert data_event is None
            data_event = event

        else:
            raise Exception('invalid event')

    if rcb_type == iec61850.RcbType.BUFFERED:
        assert_entry_id_event(event=entry_id_event,
                              report_id=report_id,
                              entry_id=None)

    else:
        assert entry_id_event is None

    assert_data_event(event=data_event,
                      name=name,
                      reasons={iec61850.Reason.DATA_CHANGE},
                      value=value,
                      quality=quality,
                      timestamp=timestamp,
                      selected=selected)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(event_queue.get(), 0.01)

    await device.async_close()
    await server.async_close()
    await client.async_close()


async def test_multiple_data_superset_report(addr, create_conf):
    event_queue = aio.Queue()

    report_id = 'report id'
    dataset_ref = 'ds'
    logical_device = 'ld'
    logical_node = 'ln'
    rcb_type = iec61850.RcbType.UNBUFFERED
    value_ref = iec61850.DataRef(logical_device=logical_device,
                                 logical_node=logical_node,
                                 fc='fc',
                                 names=('a', ))
    rcb_ref = iec61850.RcbRef(logical_device=logical_device,
                              logical_node=logical_node,
                              type=rcb_type,
                              name='rcb')

    data_names = ['d1', 'd2', 'd3']
    data_values = [1, 2, 3]
    data_qualities = [
        iec61850.Quality(
            validity=iec61850.QualityValidity.GOOD,
            details=set(),
            source=iec61850.QualitySource.PROCESS,
            test=False,
            operator_blocked=False) for _ in range(3)]
    data_timestamps = [
        iec61850.Timestamp(
            value=datetime.datetime.now(datetime.timezone.utc),
            leap_second=False,
            clock_failure=False,
            not_synchronized=False,
            accuracy=0) for _ in range(3)]
    data_value_names = [['phsA', 'cVal', 'mag', 'f'],
                        ['phsB', 'cVal', 'mag', 'f'],
                        ['phsC', 'cVal', 'mag', 'f']]
    data_quality_names = [['phsA', 'q'],
                          ['phsB', 'q'],
                          ['phsC', 'q']]
    data_timestamp_names = [['phsA', 't'],
                            ['phsB', 't'],
                            ['phsC', 't']]

    value_type = iec61850.StructValueType([
        ('phsA', iec61850.StructValueType([
            ('cVal', iec61850.AcsiValueType.VECTOR),
            ('q', iec61850.AcsiValueType.QUALITY),
            ('t', iec61850.AcsiValueType.TIMESTAMP)])),
        ('phsB', iec61850.StructValueType([
            ('cVal', iec61850.AcsiValueType.VECTOR),
            ('q', iec61850.AcsiValueType.QUALITY),
            ('t', iec61850.AcsiValueType.TIMESTAMP)])),
        ('phsC', iec61850.StructValueType([
            ('cVal', iec61850.AcsiValueType.VECTOR),
            ('q', iec61850.AcsiValueType.QUALITY),
            ('t', iec61850.AcsiValueType.TIMESTAMP)]))])

    report_value = {
        'phsA': {
            'cVal': iec61850.Vector(
               magnitude=iec61850.Analogue(f=data_values[0], i=None),
               angle=None),
            'q': data_qualities[0],
            't': data_timestamps[0]},
        'phsB': {
            'cVal': iec61850.Vector(
               magnitude=iec61850.Analogue(f=data_values[1], i=None),
               angle=None),
            'q': data_qualities[1],
            't': data_timestamps[1]},
        'phsC': {
            'cVal': iec61850.Vector(
               magnitude=iec61850.Analogue(f=data_values[2], i=None),
               angle=None),
            'q': data_qualities[2],
            't': data_timestamps[2]}}

    data_defs = [DataDef(ref=value_ref,
                         value_type=value_type)]

    data_conf = []
    for (data_name,
         value_names,
         quality_names,
         timestamp_names) in zip(data_names,
                                 data_value_names,
                                 data_quality_names,
                                 data_timestamp_names):
        data_conf.append(
            {'name': data_name,
             'report_id': report_id,
             'value': {'logical_device': logical_device,
                       'logical_node': logical_node,
                       'fc': value_ref.fc,
                       'names': [*value_ref.names, *value_names]},
             'quality': {'logical_device': logical_device,
                         'logical_node': logical_node,
                         'fc': value_ref.fc,
                         'names': [*value_ref.names, *quality_names]},
             'timestamp': {'logical_device': logical_device,
                           'logical_node': logical_node,
                           'fc': value_ref.fc,
                           'names': [*value_ref.names, *timestamp_names]}})

    conf = create_conf(
        value_types=[
            {'logical_device': value_ref.logical_device,
             'logical_node': value_ref.logical_node,
             'fc': value_ref.fc,
             'name': value_ref.names[0],
             'type': value_type_to_json(value_type)}],
        datasets=[{'ref': dataset_ref,
                   'values': [{'logical_device': value_ref.logical_device,
                               'logical_node': value_ref.logical_node,
                               'fc': value_ref.fc,
                               'names': list(value_ref.names)}],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': logical_device,
                       'logical_node': logical_node,
                       'type': rcb_type.name,
                       'name': rcb_ref.name},
               'report_id': report_id,
               'dataset': dataset_ref}],
        data=data_conf)

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: report_id,
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    dataset_ref)}})
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    report = iec61850.Report(
        report_id=report_id,
        sequence_number=None,
        subsequence_number=None,
        more_segments_follow=None,
        dataset=None,
        buffer_overflow=None,
        conf_revision=None,
        entry_time=None,
        entry_id=None,
        data=[iec61850.ReportData(
            ref=value_ref,
            value=report_value,
            reasons={iec61850.Reason.DATA_CHANGE})])
    await server.send_report(report, data_defs)

    for (name,
         value,
         quality,
         timestamp) in zip(data_names,
                           data_values,
                           data_qualities,
                           data_timestamps):
        data_event = await event_queue.get()
        assert_data_event(event=data_event,
                          name=name,
                          reasons={iec61850.Reason.DATA_CHANGE},
                          value=value,
                          quality=quality,
                          timestamp=timestamp)

    await device.async_close()
    await server.async_close()
    await client.async_close()


async def test_data_report_segmentation(addr, create_conf):
    event_queue = aio.Queue()

    name = 'data name'
    report_id = 'report id'
    dataset = 'ds'
    logical_device = 'ld'
    logical_node = 'ln'
    rcb_type = iec61850.RcbType.BUFFERED
    value_type = iec61850.BasicValueType.INTEGER
    value_data_ref = iec61850.DataRef(logical_device=logical_device,
                                      logical_node=logical_node,
                                      fc='fc1',
                                      names=('a', 'value'))
    quality_data_ref = iec61850.DataRef(logical_device=logical_device,
                                        logical_node=logical_node,
                                        fc='fc2',
                                        names=('a', 'quality'))
    timestamp_data_ref = iec61850.DataRef(logical_device=logical_device,
                                          logical_node=logical_node,
                                          fc='fc3',
                                          names=('a', 'timestamp'))
    rcb_ref = iec61850.RcbRef(logical_device=logical_device,
                              logical_node=logical_node,
                              type=rcb_type,
                              name='rcb')
    data_defs = [DataDef(ref=value_data_ref,
                         value_type=value_type),
                 DataDef(ref=quality_data_ref,
                         value_type=iec61850.AcsiValueType.QUALITY),
                 DataDef(ref=timestamp_data_ref,
                         value_type=iec61850.AcsiValueType.TIMESTAMP)]
    sequence_number = 123

    conf = create_conf(
        value_types=[
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': value_data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(
                iec61850.StructValueType([('value', value_type)]))},
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': quality_data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(
                iec61850.StructValueType([('quality',
                                           iec61850.AcsiValueType.QUALITY)]))},
            {'logical_device': logical_device,
             'logical_node': logical_node,
             'fc': timestamp_data_ref.fc,
             'name': 'a',
             'type': value_type_to_json(
                iec61850.StructValueType([('timestamp',
                                           iec61850.AcsiValueType.TIMESTAMP)]))}],  # NOQA
        datasets=[{'ref': dataset,
                   'values': [{'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': value_data_ref.fc,
                               'names': list(value_data_ref.names)},
                              {'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': quality_data_ref.fc,
                               'names': list(quality_data_ref.names)},
                              {'logical_device': logical_device,
                               'logical_node': logical_node,
                               'fc': timestamp_data_ref.fc,
                               'names': list(timestamp_data_ref.names)}],
                   'dynamic': True}],
        rcbs=[{'ref': {'logical_device': logical_device,
                       'logical_node': logical_node,
                       'type': rcb_type.name,
                       'name': rcb_ref.name},
               'report_id': report_id,
               'dataset': dataset}],
        data=[{'name': name,
               'report_id': report_id,
               'value': {'logical_device': logical_device,
                         'logical_node': logical_node,
                         'fc': value_data_ref.fc,
                         'names': list(value_data_ref.names)},
               'quality': {'logical_device': logical_device,
                           'logical_node': logical_node,
                           'fc': quality_data_ref.fc,
                           'names': list(quality_data_ref.names)},
               'timestamp': {'logical_device': logical_device,
                             'logical_node': logical_node,
                             'fc': timestamp_data_ref.fc,
                             'names': list(timestamp_data_ref.names)}}])

    client = EventerClient(event_cb=event_queue.put_nowait)

    server = await create_server(
        addr=addr,
        rcbs={
            rcb_ref: {
                iec61850.RcbAttrType.REPORT_ID: report_id,
                iec61850.RcbAttrType.DATASET: iec61850.NonPersistedDatasetRef(
                    dataset)}})
    device = await aio.call(info.create, conf, client, event_type_prefix)

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_queue.get()
    assert_status_event(event, 'CONNECTED')

    value = 1
    quality = iec61850.Quality(
        validity=iec61850.QualityValidity.GOOD,
        details=set(),
        source=iec61850.QualitySource.PROCESS,
        test=False,
        operator_blocked=False)
    timestamp = iec61850.Timestamp(
        value=datetime.datetime.now(datetime.timezone.utc),
        leap_second=False,
        clock_failure=False,
        not_synchronized=False,
        accuracy=0)

    for i, (data_ref, v) in enumerate([(value_data_ref, value),
                                       (quality_data_ref, quality),
                                       (timestamp_data_ref, timestamp)]):
        report = iec61850.Report(
            report_id=report_id,
            sequence_number=sequence_number,
            subsequence_number=i,
            more_segments_follow=True,
            dataset=None,
            buffer_overflow=None,
            conf_revision=None,
            entry_time=None,
            entry_id=None,
            data=[iec61850.ReportData(ref=data_ref,
                                      value=v,
                                      reasons=None)])
        await server.send_report(report, data_defs)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(event_queue.get(), 0.01)

    report = iec61850.Report(
        report_id=report_id,
        sequence_number=sequence_number,
        subsequence_number=i + 1,
        more_segments_follow=False,
        dataset=None,
        buffer_overflow=None,
        conf_revision=None,
        entry_time=None,
        entry_id=None,
        data=[])
    await server.send_report(report, data_defs)

    entry_id_event = None
    data_event = None
    while (data_event is None or
            (rcb_type == iec61850.RcbType.BUFFERED and entry_id_event is None)):  # NOQA
        event = await event_queue.get()

        if event.type[-2] == 'entry_id':
            assert entry_id_event is None
            entry_id_event = event

        elif event.type[-2] == 'data':
            assert data_event is None
            data_event = event

        else:
            raise Exception('invalid event')

    if rcb_type == iec61850.RcbType.BUFFERED:
        assert_entry_id_event(event=entry_id_event,
                              report_id=report_id,
                              entry_id=None)

    else:
        assert entry_id_event is None

    assert_data_event(event=data_event,
                      name=name,
                      value=value,
                      quality=quality,
                      timestamp=timestamp)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(event_queue.get(), 0.01)

    await device.async_close()
    await server.async_close()
    await client.async_close()
