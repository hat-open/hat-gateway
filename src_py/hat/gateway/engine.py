"""Gateway engine"""

from collections.abc import Collection, Iterable
import asyncio
import collections
import contextlib
import logging

from hat import aio
from hat import json
import hat.event.common
import hat.event.eventer

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class Engine(aio.Resource):
    """Gateway engine"""

    def __init__(self,
                 conf: json.Data,
                 eventer_client: hat.event.eventer.Client,
                 events_queue_size: int = 1024):
        self._eventer_client = eventer_client
        self._async_group = aio.Group()
        self._events_queue = aio.Queue(events_queue_size)
        self._devices = {}

        for device_conf in conf['devices']:
            info = common.import_device_info(device_conf['module'])
            event_type_prefix = ('gateway', info.type, device_conf['name'])

            self._devices[event_type_prefix] = _DeviceProxy(
                conf=device_conf,
                eventer_client=eventer_client,
                event_type_prefix=event_type_prefix,
                async_group=self.async_group,
                create_device=info.create,
                events_queue_size=events_queue_size)

        self.async_group.spawn(self._run)

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group

    async def process_events(self, events: Iterable[hat.event.common.Event]):
        await self._events_queue.put(events)

    async def _run(self):
        try:
            event_types = [(*event_type_prefix, 'system', 'enable')
                           for event_type_prefix in self._devices.keys()]
            params = hat.event.common.QueryLatestParams(event_types)
            result = await self._eventer_client.query(params)

            for event in result.events:
                event_type_prefix = event.type[:3]
                device = self._devices.get(event_type_prefix)
                if not device or event.type[3:] != ('system', 'enable'):
                    continue

                device.set_enable(
                    isinstance(event.payload,
                               hat.event.common.EventPayloadJson) and
                    event.payload.data is True)

            while True:
                events = await self._events_queue.get()

                device_events = collections.defaultdict(collections.deque)

                for event in events:
                    event_type_prefix = event.type[:3]
                    device = self._devices.get(event_type_prefix)
                    if not device:
                        mlog.warning("received invalid event type prefix %s",
                                     event_type_prefix)
                        continue

                    if event.type[3:] == ('system', 'enable'):
                        device.set_enable(
                            isinstance(event.payload,
                                       hat.event.common.EventPayloadJson) and
                            event.payload.data is True)

                    else:
                        device_events[device].append(event)

                for device, dev_events in device_events.items():
                    await device.process_events(dev_events)

        except Exception as e:
            mlog.error("engine run error: %s", e, exc_info=e)

        finally:
            self.close()
            self._events_queue.close()


class _DeviceProxy(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix,
                 async_group: aio.Group,
                 create_device: common.CreateDevice,
                 events_queue_size: int = 1024):
        self._conf = conf
        self._eventer_client = eventer_client
        self._event_type_prefix = event_type_prefix
        self._async_group = async_group
        self._create_device = create_device
        self._events_queue_size = events_queue_size
        self._events_queue = None
        self._enable_event = asyncio.Event()

        self.async_group.spawn(self._run)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    def set_enable(self, enable: bool):
        if enable:
            if self._events_queue is not None:
                return

            self._events_queue = aio.Queue(self._events_queue_size)
            self._enable_event.set()

        else:
            if self._events_queue is None:
                return

            self._events_queue.close()
            self._events_queue = None
            self._enable_event.set()

    async def process_events(self, events: Collection[hat.event.common.Event]):
        if self._events_queue is None:
            mlog.warning("device not enabled - ignoring %s events",
                         len(events))
            return

        await self._events_queue.put(events)

    async def _run(self):
        try:
            while True:
                self._enable_event.clear()

                if self._events_queue is None:
                    await self._enable_event.wait()
                    continue

                events_queue = self._events_queue
                device = await aio.call(self._create_device, self._conf,
                                        self._eventer_client,
                                        self._event_type_prefix)

                try:
                    device.async_group.spawn(aio.call_on_cancel,
                                             events_queue.close)
                    await self._register_running(True)

                    while True:
                        events = await events_queue.get()

                        if not device.is_open:
                            raise Exception('device closed')

                        await aio.call(device.process_events, events)

                except aio.QueueClosedError:
                    if not events_queue.is_closed:
                        raise

                    if not device.is_open:
                        raise Exception('device closed')

                finally:
                    await aio.uncancellable(self._close_device(device))

        except Exception as e:
            mlog.error("device proxy run error: %s", e, exc_info=e)

        finally:
            self.close()

    async def _close_device(self, device):
        await device.async_close()

        with contextlib.suppress(ConnectionError):
            await self._register_running(False)

    async def _register_running(self, is_running):
        await self._eventer_client.register([
            hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway', 'running'),
                source_timestamp=hat.event.common.now(),
                payload=hat.event.common.EventPayloadJson(is_running))])
