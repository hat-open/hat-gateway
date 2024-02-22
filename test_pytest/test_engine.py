import asyncio
import collections
import itertools
import sys
import types

import pytest

from hat import aio
import hat.event.common

from hat.gateway import common
import hat.gateway.engine


gateway_name = 'gateway_name'
device_type = 'device_type'

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


def create_event(event_type, payload_data):
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload_data))


def create_enable_event(device_name, is_enabled):
    event_type = ('gateway', gateway_name, device_type, device_name,
                  'system', 'enable')
    return create_event(event_type, is_enabled)


def assert_running_event(event, device_name, is_running):
    assert event.type == ('gateway', gateway_name, device_type, device_name,
                          'gateway', 'running')
    assert event.payload.data == is_running


@pytest.fixture
def create_device_module():
    module_names = collections.deque()

    def create_device_module(device_cb=None, process_events_cb=None):
        module_name = f'test_{device_type}_{len(module_names)}'
        module_names.append(module_name)

        class Device(common.Device):

            def __init__(self):
                self._async_group = aio.Group()

            @property
            def async_group(self):
                return self._async_group

            async def process_events(self, events):
                if process_events_cb:
                    await aio.call(process_events_cb, events)

        async def create(conf, eventer_client, event_type_prefix):
            assert event_type_prefix == ('gateway', gateway_name, device_type,
                                         conf['name'])

            device = Device()
            if device_cb:
                await aio.call(device_cb, device)

            return device

        module = types.ModuleType(module_name)
        module.info = common.DeviceInfo(type=device_type,
                                        create=create)
        sys.modules[module_name] = module

        return module_name

    try:
        yield create_device_module

    finally:
        for module_name in module_names:
            del sys.modules[module_name]


async def test_empty_engine():
    conf = {'gateway_name': gateway_name,
            'devices': []}

    eventer_client = EventerClient()
    engine = hat.gateway.engine.Engine(conf, eventer_client)

    await asyncio.sleep(0.001)

    assert engine.is_open

    await engine.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize("device_count", [1, 2, 5])
async def test_create_engine_with_disabled_devices(device_count,
                                                   create_device_module):
    device_queue = aio.Queue()
    query_queue = aio.Queue()

    device_names = [f'name{i}' for i in range(device_count)]
    device_modules = [create_device_module(device_cb=device_queue.put_nowait)
                      for _ in device_names]
    conf = {'gateway_name': gateway_name,
            'devices': [{'module': device_module,
                         'name': device_name}
                        for device_name, device_module in zip(device_names,
                                                              device_modules)]}

    def on_query(params):
        assert isinstance(params, hat.event.common.QueryLatestParams)
        query_queue.put_nowait(params)

        return hat.event.common.QueryResult([], False)

    eventer_client = EventerClient(query_cb=on_query)
    engine = hat.gateway.engine.Engine(conf, eventer_client)

    params = await query_queue.get()
    subscription = hat.event.common.Subscription(params.event_types)
    for device_name in device_names:
        event_type = ('gateway', gateway_name, device_type, device_name,
                      'system', 'enable')
        assert subscription.matches(event_type)

    assert device_queue.empty()

    await engine.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize("device_count", [1, 2, 5])
async def test_create_engine_with_enabled_devices(device_count,
                                                  create_device_module):
    device_queue = aio.Queue()

    device_names = [f'name{i}' for i in range(device_count)]
    device_modules = [create_device_module(device_cb=device_queue.put_nowait)
                      for _ in device_names]
    conf = {'gateway_name': gateway_name,
            'devices': [{'module': device_module,
                         'name': device_name}
                        for device_name, device_module in zip(device_names,
                                                              device_modules)]}

    def on_query(params):
        assert isinstance(params, hat.event.common.QueryLatestParams)

        return hat.event.common.QueryResult(
            [create_enable_event(device_name, True)
             for device_name in device_names],
            False)

    eventer_client = EventerClient(query_cb=on_query)
    engine = hat.gateway.engine.Engine(conf, eventer_client)

    devices = collections.deque()
    for _ in device_names:
        device = await device_queue.get()
        devices.append(device)

    assert len(devices) == len(device_names)
    assert device_queue.empty()

    await engine.async_close()

    for device in devices:
        assert device.is_closed

    await eventer_client.async_close()


async def test_close_engine_when_device_closed(create_device_module):
    device_queue = aio.Queue()

    device_name = 'device name'
    device_module = create_device_module(device_cb=device_queue.put_nowait)
    conf = {'gateway_name': gateway_name,
            'devices': [{'module': device_module,
                         'name': device_name}]}

    def on_query(params):
        return hat.event.common.QueryResult(
            [create_enable_event(device_name, True)],
            False)

    eventer_client = EventerClient(query_cb=on_query)
    engine = hat.gateway.engine.Engine(conf, eventer_client)

    device = await device_queue.get()

    assert device.is_open
    assert engine.is_open

    device.close()

    await engine.wait_closed()
    await eventer_client.async_close()


async def test_enable_disable(create_device_module):
    device_queue = aio.Queue()

    device_name = 'device name'
    device_module = create_device_module(device_cb=device_queue.put_nowait)
    conf = {'gateway_name': gateway_name,
            'devices': [{'module': device_module,
                         'name': device_name}]}

    eventer_client = EventerClient()
    engine = hat.gateway.engine.Engine(conf, eventer_client)
    assert device_queue.empty()

    await engine.process_events([create_enable_event(device_name, False)])
    assert device_queue.empty()

    await engine.process_events([create_enable_event(device_name, True)])
    device = await device_queue.get()
    assert device.is_open

    await engine.process_events([create_enable_event(device_name, True)])
    assert device.is_open

    await engine.process_events([create_enable_event(device_name, False)])
    await device.wait_closed()

    await engine.process_events([create_enable_event(device_name, False)])
    assert device_queue.empty()

    await engine.process_events([create_enable_event(device_name, True)])
    device = await device_queue.get()
    assert device.is_open

    await engine.async_close()
    await eventer_client.async_close()


async def test_running_event(create_device_module):
    event_queue = aio.Queue()

    device_name = 'device name'
    device_module = create_device_module()
    conf = {'gateway_name': gateway_name,
            'devices': [{'module': device_module,
                         'name': device_name}]}

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    engine = hat.gateway.engine.Engine(conf, eventer_client)
    assert event_queue.empty()

    await engine.process_events([create_enable_event(device_name, True)])
    event = await event_queue.get()
    assert_running_event(event, device_name, True)

    await engine.process_events([create_enable_event(device_name, True)])
    assert event_queue.empty()

    await engine.process_events([create_enable_event(device_name, False)])
    event = await event_queue.get()
    assert_running_event(event, device_name, False)

    await engine.process_events([create_enable_event(device_name, False)])
    assert event_queue.empty()

    await engine.process_events([create_enable_event(device_name, True)])
    event = await event_queue.get()
    assert_running_event(event, device_name, True)

    await engine.async_close()
    await eventer_client.async_close()


async def test_process_events(create_device_module):
    device_queue = aio.Queue()
    process_queue = aio.Queue()

    device_name = 'device name'
    device_module = create_device_module(
        device_cb=device_queue.put_nowait,
        process_events_cb=process_queue.put_nowait)
    conf = {'gateway_name': gateway_name,
            'devices': [{'module': device_module,
                         'name': device_name}]}

    eventer_client = EventerClient()
    engine = hat.gateway.engine.Engine(conf, eventer_client)

    await engine.process_events([create_enable_event(device_name, True)])
    await device_queue.get()

    assert process_queue.empty()

    event_type = ('gateway', gateway_name, device_type, device_name,
                  'system', 'abc')
    payload_data = 123
    await engine.process_events([create_event(event_type, payload_data)])
    events = await process_queue.get()
    assert len(events) == 1
    assert events[0].type == event_type
    assert events[0].payload.data == payload_data

    event_type = ('gateway', gateway_name, device_type, f'not {device_name}',
                  'system', 'abc')
    payload_data = 123
    await engine.process_events([create_event(event_type, payload_data)])
    assert process_queue.empty()

    event_types = [('gateway', gateway_name, device_type, f'not {device_name}',
                    'system', 'abc'),
                   ('gateway', gateway_name, device_type, device_name,
                    'system', 'abc')]
    payload_data = 123
    await engine.process_events([create_event(event_type, payload_data)
                                 for event_type in event_types])
    events = await process_queue.get()
    assert len(events) == 1
    assert events[0].type == event_types[1]
    assert events[0].payload.data == payload_data

    await engine.async_close()
    await eventer_client.async_close()
