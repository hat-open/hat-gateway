"""Gateway engine"""

import collections
import contextlib
import importlib
import logging

from hat import aio
from hat import json
from hat.gateway import common
import hat.event.eventer_client
import hat.event.common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


async def create_engine(conf: json.Data,
                        client: hat.event.eventer_client.EventerClient
                        ) -> 'Engine':
    """Create gateway engine"""
    engine = Engine()
    engine._client = client
    engine._devices = {}

    try:
        for device_conf in conf['devices']:
            device = _DeviceProxy(device_conf, client, conf['gateway_name'])
            if device.event_type_prefix in engine._devices:
                raise Exception(f'duplicate device identifier: '
                                f'{device.event_type_prefix}')
            engine._devices[device.event_type_prefix] = device

        engine.async_group.spawn(engine._read_loop)

    except BaseException:
        await aio.uncancellable(engine.async_close())
        raise

    return engine


class Engine(aio.Resource):
    """Gateway engine"""

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._client.async_group

    async def _read_loop(self):
        try:
            events = []

            if self._devices:
                event_types = [(*device.event_type_prefix, 'system', 'enable')
                               for device in self._devices.values()]
                query = hat.event.common.QueryData(event_types=event_types,
                                                   unique_type=True)
                events = await self._client.query(query)

            while True:
                device_events = {}

                for event in events:
                    event_type_prefix = event.event_type[:4]
                    device = self._devices.get(event_type_prefix)
                    if not device:
                        continue

                    if device not in device_events:
                        device_events[device] = collections.deque()
                    device_events[device].append(event)

                for device, events in device_events.items():
                    device.receive_queue.put_nowait(events)

                events = await self._client.receive()

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error('read loop error: %s', e, exc_info=e)

        finally:
            self.close()


class _DeviceProxy(aio.Resource):

    def __init__(self, conf, client, gateway_name):
        self._conf = conf
        self._client = client
        self._receive_queue = aio.Queue()
        self._events_queues_queue = aio.Queue()
        self._device_module = importlib.import_module(conf['module'])
        self._event_type_prefix = ('gateway', gateway_name,
                                   self._device_module.device_type,
                                   conf['name'])

        self.async_group.spawn(self._read_loop)
        self.async_group.spawn(self._device_loop)

    @property
    def async_group(self):
        return self._client.async_group

    @property
    def event_type_prefix(self):
        return self._event_type_prefix

    @property
    def receive_queue(self):
        return self._receive_queue

    async def _read_loop(self):
        try:
            events = collections.deque()
            enabled = False

            while True:
                while True:
                    while events and not enabled:
                        event = events.popleft()
                        if event.event_type[5:] == ('enable', ):
                            enabled = _is_enable_event(event)

                    if enabled:
                        break

                    events = await self._receive_queue.get()

                events_queue = aio.Queue()
                try:
                    self._events_queues_queue.put_nowait(events_queue)

                    while True:
                        filtered_events = collections.deque()

                        while events and enabled:
                            event = events.popleft()
                            if event.event_type[5:] == ('enable', ):
                                enabled = _is_enable_event(event)
                            else:
                                filtered_events.append(event)

                        if filtered_events and not events_queue.is_closed:
                            events_queue.put_nowait(list(filtered_events))

                        if not enabled:
                            break

                        events = await self._receive_queue.get()

                finally:
                    events_queue.close()

        except Exception as e:
            mlog.error('read loop error: %s', e, exc_info=e)

        finally:
            self.close()

    async def _device_loop(self):
        try:
            self._register_running(False)
            while True:
                events_queue = await self._events_queues_queue.get()
                if events_queue.is_closed:
                    continue

                async with self.async_group.create_subgroup() as subgroup:
                    client = _DeviceEventClient(self._client)
                    device = await aio.call(self._device_module.create,
                                            self._conf, client,
                                            self._event_type_prefix)

                    subgroup.spawn(aio.call_on_cancel, device.async_close)
                    subgroup.spawn(aio.call_on_cancel, events_queue.close)
                    subgroup.spawn(aio.call_on_done, device.wait_closing(),
                                   subgroup.close)

                    try:
                        self._register_running(True)
                        while True:
                            events = await events_queue.get()
                            client.receive_queue.put_nowait(events)

                    except aio.QueueClosedError:
                        if not device.is_open:
                            break

                    finally:
                        with contextlib.suppress(ConnectionError):
                            self._register_running(False)

        except Exception as e:
            mlog.error('device loop error: %s', e, exc_info=e)

        finally:
            self.close()

    def _register_running(self, is_running):
        self._client.register([hat.event.common.RegisterEvent(
            event_type=(*self._event_type_prefix, 'gateway', 'running'),
            source_timestamp=hat.event.common.now(),
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=is_running))])


class _DeviceEventClient(common.DeviceEventClient):

    def __init__(self, client):
        self._client = client
        self._receive_queue = aio.Queue()

        self.async_group.spawn(aio.call_on_cancel, self._receive_queue.close)

    @property
    def async_group(self):
        return self._client.async_group

    @property
    def receive_queue(self):
        return self._receive_queue

    async def receive(self):
        try:
            return await self._receive_queue.get()

        except aio.QueueClosedError:
            raise ConnectionError()

    def register(self, events):
        self._client.register(events)

    async def register_with_response(self, events):
        return await self._client.register_with_response(events)

    async def query(self, data):
        return await self._client.query(data)


def _is_enable_event(event):
    return (event.payload and
            event.payload.type == hat.event.common.EventPayloadType.JSON and
            event.payload.data is True)
