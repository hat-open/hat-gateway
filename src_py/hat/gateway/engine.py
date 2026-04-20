"""Gateway engine"""

from collections.abc import Iterable
import asyncio
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
                 event_queue_size: int = 1024):
        self._async_group = aio.Group()

        self._devices = {
            device.event_type_prefix: device
            for device in (_DeviceProxy(conf=device_conf,
                                        async_group=self.async_group,
                                        eventer_client=eventer_client,
                                        event_queue_size=event_queue_size)
                           for device_conf in conf['devices'])}

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._async_group

    async def process_events(self, events: Iterable[hat.event.common.Event]):
        """Process received events"""
        for event in events:
            event_type_prefix = event.type[:3]

            device = self._devices.get(event_type_prefix)
            if not device:
                mlog.debug("no device - ignorring event with type %s",
                           event.type)
                continue

            await device.process_event(event)


class _DeviceProxy(aio.Resource):

    def __init__(self,
                 conf: common.DeviceConf,
                 async_group: aio.Group,
                 eventer_client: hat.event.eventer.Client,
                 event_queue_size: int = 1024):
        self._conf = conf
        self._async_group = async_group
        self._eventer_client = eventer_client
        self._event_queue_size = event_queue_size
        self._info = common.import_device_info(conf['module'])
        self._event_type_prefix = ('gateway', self._info.type, conf['name'])
        self._enabled = None
        self._enabled_event = asyncio.Event()
        self._event_queue = None
        self._log = _create_device_proxy_logger_adapter(conf['name'])

        self.async_group.spawn(self._device_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    @property
    def event_type_prefix(self) -> hat.event.common.EventType:
        return self._event_type_prefix

    async def process_event(self, event: hat.event.common.Event):
        if event.type[3:] == ('system', 'enable'):
            self._process_enable_event(event)
            return

        if self._event_queue is not None:
            with contextlib.suppress(aio.QueueClosedError):
                await self._event_queue.put(event)
                return

        self._log.debug("device not running - ignoring event with type %s",
                        event.type)

    def _process_enable_event(self, event):
        self._enabled = (
            isinstance(event.payload, hat.event.common.EventPayloadJson) and
            event.payload.data is True)
        self._enabled_event.set()

    async def _device_loop(self):
        try:
            result = await self._eventer_client.query(
                hat.event.common.QueryLatestParams(
                    [(*self._event_type_prefix, 'system', 'enable')]))

            if self._enabled is None:
                for event in result.events:
                    self._process_enable_event(event)

            while True:
                while not self._enabled:
                    self._enabled_event.clear()
                    await self._enabled_event.wait()

                if not self.is_open:
                    break

                async with self.async_group.create_subgroup() as subgroup:
                    subgroup.spawn(self._enabled_loop)

                    while self._enabled:
                        self._enabled_event.clear()
                        await self._enabled_event.wait()

        except Exception as e:
            self._log.error("device loop error: %s", e, exc_info=e)

        finally:
            self.close()

    async def _enabled_loop(self):
        try:
            device = await aio.call(self._info.create,
                                    self._conf,
                                    self._eventer_client,
                                    self._event_type_prefix)

        except Exception as e:
            self._log.error("error creating device: %s", e, exc_info=e)
            return

        async def cleanup():
            await device.async_close()

            with contextlib.suppress(ConnectionError):
                await self._register_running(False)

        try:
            self._event_queue = aio.Queue(self._event_queue_size)
            device.async_group.spawn(aio.call_on_cancel,
                                     self._event_queue.close)

            await self._register_running(True)

            while True:
                event = await self._event_queue.get()

                await aio.call(device.process_event, event)

        except aio.QueueClosedError:
            pass

        except Exception as e:
            self._log.error("enabled loop error: %s", e, exc_info=e)

        finally:
            self._event_queue = None
            await aio.uncancellable(cleanup())

    async def _register_running(self, is_running):
        await self._eventer_client.register([
            hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway', 'running'),
                source_timestamp=hat.event.common.now(),
                payload=hat.event.common.EventPayloadJson(is_running))])


def _create_device_proxy_logger_adapter(name):
    extra = {'meta': {'type': 'DeviceProxy',
                      'name': name}}

    return logging.LoggerAdapter(mlog, extra)
