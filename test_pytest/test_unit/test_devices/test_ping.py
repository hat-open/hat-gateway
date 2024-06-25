import asyncio
import itertools
import contextlib

import pytest

from hat import aio
from hat import util
from hat.drivers import icmp

from hat.gateway import common
from hat.gateway.devices import ping


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = 'gateway', gateway_name, ping.device_type, device_name


class EventClient(common.DeviceEventClient):

    def __init__(self):
        self._register_queue = aio.Queue()
        self._async_group = aio.Group()
        self._async_group.spawn(aio.call_on_cancel, self._register_queue.close)

    @property
    def async_group(self):
        return self._async_group

    @property
    def register_queue(self):
        return self._register_queue

    async def receive(self):
        raise Exception('should not be used')

    def register(self, events):
        try:
            for event in events:
                self._register_queue.put_nowait(event)

        except aio.QueueClosedError:
            raise ConnectionError()

    async def register_with_response(self, events):
        raise Exception('should not be used')

    async def query(self, data):
        raise Exception('should not be used')


class Endpoint(aio.Resource):

    def __init__(self, ping_cb):
        self._async_group = aio.Group()
        self._ping_cb = ping_cb

    @property
    def async_group(self):
        return self._async_group

    async def ping(self, remote_host):
        if not self._ping_cb:
            return

        await aio.call(self._ping_cb, remote_host)


def assert_status_event(event, name, status):
    assert event.event_type == (*event_type_prefix, 'gateway', 'status', name)
    assert event.source_timestamp is None
    assert event.payload.data == status


@pytest.fixture
async def patch_endpoint(monkeypatch):

    @contextlib.contextmanager
    def patch_endpoint(create_cb=None, ping_cb=None):

        async def create_endpoint(local_addr='0.0.0.0'):
            if create_cb:
                await aio.call(create_cb, local_addr)

            return Endpoint(ping_cb)

        with monkeypatch.context() as ctx:
            ctx.setattr(icmp, 'create_endpoint', create_endpoint)
            yield

    return patch_endpoint


async def test_create(patch_endpoint):
    conf = {'remote_devices': []}

    with patch_endpoint():
        event_client = EventClient()
        device = await aio.call(ping.create, conf, event_client,
                                event_type_prefix)

        assert device.is_open

        await device.async_close()
        await event_client.async_close()


@pytest.mark.parametrize('remote_device_count', [1, 2, 5])
async def test_status_available(patch_endpoint, remote_device_count):
    ping_queue = aio.Queue()
    conf = {'remote_devices': [{'name': f'name {i}',
                                'host': f'host {i}',
                                'ping_delay': 0.01,
                                'ping_timeout': 1,
                                'retry_count': 1,
                                'retry_delay': 0}
                               for i in range(remote_device_count)]}

    def on_create(local_addr):
        assert local_addr == '0.0.0.0'

    async def on_ping(remote_host):
        await asyncio.sleep(0)
        ping_queue.put_nowait(remote_host)

    with patch_endpoint(create_cb=on_create, ping_cb=on_ping):
        event_client = EventClient()
        device = await aio.call(ping.create, conf, event_client,
                                event_type_prefix)

        names = set(i['name'] for i in conf['remote_devices'])
        while names:
            event = await event_client.register_queue.get()
            name = event.event_type[len(event_type_prefix) + 2]
            assert_status_event(event, name, 'NOT_AVAILABLE')

            names.remove(name)

        hosts = set(i['host'] for i in conf['remote_devices'])
        while hosts:
            host = await ping_queue.get()
            remote_device = util.first(conf['remote_devices'],
                                       lambda i: i['host'] == host)
            name = remote_device['name']

            event = await event_client.register_queue.get()
            assert_status_event(event, name, 'AVAILABLE')

            hosts.remove(host)

        for _ in range(3):
            hosts = set(i['host'] for i in conf['remote_devices'])
            while hosts:
                host = await ping_queue.get()
                hosts.remove(host)

        assert event_client.register_queue.empty()

        await device.async_close()
        await event_client.async_close()


async def test_status_change(patch_endpoint):
    ping_counter = itertools.count(0)
    name = 'name'
    host = 'host'
    conf = {'remote_devices': [{'name': name,
                                'host': host,
                                'ping_delay': 0.01,
                                'ping_timeout': 1,
                                'retry_count': 1,
                                'retry_delay': 0}]}

    def on_create(local_addr):
        assert local_addr == '0.0.0.0'

    async def on_ping(remote_host):
        await asyncio.sleep(0)

        assert host == remote_host

        if next(ping_counter) % 2:
            raise Exception()

    with patch_endpoint(create_cb=on_create, ping_cb=on_ping):
        event_client = EventClient()
        device = await aio.call(ping.create, conf, event_client,
                                event_type_prefix)

        event = await event_client.register_queue.get()
        assert_status_event(event, name, 'NOT_AVAILABLE')

        for _ in range(3):
            for status in ['AVAILABLE', 'NOT_AVAILABLE']:
                event = await event_client.register_queue.get()
                assert_status_event(event, name, status)

        await device.async_close()
        await event_client.async_close()


async def test_ping_timeout(patch_endpoint):
    name = 'name'
    ping_timeout = 0.01
    conf = {'remote_devices': [{'name': name,
                                'host': 'host',
                                'ping_delay': 0.01,
                                'ping_timeout': ping_timeout,
                                'retry_count': 1,
                                'retry_delay': 0}]}

    def on_create(local_addr):
        assert local_addr == '0.0.0.0'

    async def on_ping(remote_host):
        await asyncio.sleep(ping_timeout * 2)

    with patch_endpoint(create_cb=on_create, ping_cb=on_ping):
        event_client = EventClient()
        device = await aio.call(ping.create, conf, event_client,
                                event_type_prefix)

        event = await event_client.register_queue.get()
        assert_status_event(event, name, 'NOT_AVAILABLE')

        await asyncio.sleep(ping_timeout * 10)

        assert event_client.register_queue.empty()

        await device.async_close()
        await event_client.async_close()
