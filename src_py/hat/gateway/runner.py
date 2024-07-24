import asyncio
import collections
import logging

from hat import aio
from hat import json
from hat.drivers import tcp
import hat.event.component
import hat.event.eventer

from hat.gateway import common
import hat.gateway.engine


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class MainRunner(aio.Resource):

    def __init__(self, conf: json.Data):
        self._conf = conf
        self._loop = asyncio.get_running_loop()
        self._async_group = aio.Group()
        self._eventer_component = None
        self._eventer_client = None
        self._engine = None

        self.async_group.spawn(self._run)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _run(self):
        try:
            await self._start()
            await self._loop.create_future()

        except Exception as e:
            mlog.error("main runner loop error: %s", e, exc_info=e)

        finally:
            self.close()
            await aio.uncancellable(self._stop())

    async def _start(self):
        event_server_conf = self._conf['event_server']

        subscriptions = collections.deque()
        for device_conf in self._conf['devices']:
            info = common.import_device_info(device_conf['module'])
            subscriptions.append(('gateway', info.type, device_conf['name'],
                                  'system', '*'))

        if 'monitor_component' in event_server_conf:
            monitor_component_conf = event_server_conf['monitor_component']

            self._eventer_component = await hat.event.component.connect(
                addr=tcp.Address(monitor_component_conf['host'],
                                 monitor_component_conf['port']),
                name=self._conf['name'],
                group=monitor_component_conf['gateway_group'],
                server_group=monitor_component_conf['event_server_group'],
                runner_cb=self._create_eventer_runner,
                events_cb=self._on_component_events,
                eventer_kwargs={'subscriptions': subscriptions})
            _bind_resource(self.async_group, self._eventer_component)

            await self._eventer_component.set_ready(True)

        elif 'eventer_server' in event_server_conf:
            eventer_server_conf = event_server_conf['eventer_server']

            self._eventer_client = await hat.event.eventer.connect(
                addr=tcp.Address(eventer_server_conf['host'],
                                 eventer_server_conf['port']),
                client_name=self._conf['name'],
                subscriptions=subscriptions,
                events_cb=self._on_client_events)
            _bind_resource(self.async_group, self._eventer_client)

            self._engine = hat.gateway.engine.Engine(
                conf=self._conf,
                eventer_client=self._eventer_client)
            _bind_resource(self.async_group, self._engine)

        else:
            raise Exception('invalid configuration')

    async def _stop(self):
        if self._engine and not self._eventer_component:
            await self._engine.async_close()

        if self._eventer_client:
            await self._eventer_client.async_close()

        if self._eventer_component:
            await self._eventer_component.async_close()

    async def _create_eventer_runner(self, monitor_component, server_data,
                                     eventer_client):
        self._engine = hat.gateway.engine.Engine(
            conf=self._conf,
            eventer_client=eventer_client)

        return self._engine

    async def _process_events(self, events):
        if not self._engine:
            return

        await self._engine.process_events(events)

    async def _on_component_events(self, monitor_component, eventer_client,
                                   events):
        await self._process_events(events)

    async def _on_client_events(self, eventer_client, events):
        await self._process_events(events)


def _bind_resource(async_group, resource):
    async_group.spawn(aio.call_on_done, resource.wait_closing(),
                      async_group.close)
