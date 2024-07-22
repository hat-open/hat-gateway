"""Ping device"""

from collections.abc import Collection
import asyncio
import contextlib
import logging

from hat import aio
from hat.drivers import icmp
import hat.event.common

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)


async def create(conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'PingDevice':
    device = PingDevice()
    device._eventer_client = eventer_client
    device._event_type_prefix = event_type_prefix
    device._devices_status = {}

    device._endpoint = await icmp.create_endpoint()

    for device_conf in conf['remote_devices']:
        device.async_group.spawn(device._remote_device_loop, device_conf)

    return device


class PingDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._endpoint._async_group

    def process_events(self, events: Collection[hat.event.common.Event]):
        pass

    async def _remote_device_loop(self, device_conf):
        name = device_conf['name']
        try:
            await self._register_status("NOT_AVAILABLE", name)
            while True:
                try:
                    await self._ping_retry(device_conf)
                    status = "AVAILABLE"
                    mlog.debug('ping to %s successfull', device_conf['host'])

                except Exception as e:
                    mlog.debug("device %s not available: %s",
                               device_conf['host'], e, exc_info=e)
                    status = "NOT_AVAILABLE"

                await self._register_status(status, name)
                await asyncio.sleep(device_conf['ping_delay'])

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("device %s loop error: %s", name, e, exc_info=e)

        finally:
            self.close()
            with contextlib.suppress(ConnectionError):
                await self._register_status("NOT_AVAILABLE", name)

    async def _ping_retry(self, device_conf):
        retry_count = device_conf['retry_count']
        for i in range(retry_count + 1):
            try:
                return await aio.wait_for(
                    self._endpoint.ping(device_conf['host']),
                    timeout=device_conf['ping_timeout'])

            except Exception as e:
                retry_msg = (f", retry {i}/{retry_count}" if i > 0 else "")
                mlog.debug('no ping response%s: %s', retry_msg, e, exc_info=e)

            await asyncio.sleep(device_conf['retry_delay'])

        raise Exception(f"no ping response after {retry_count} retries")

    async def _register_status(self, status, name):
        old_status = self._devices_status.get(name)
        if old_status == status:
            return
        mlog.debug('remote device %s status %s', name, status)
        self._devices_status[name] = status
        await self._eventer_client.register([hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'status', name),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson(status))])


info = common.DeviceInfo(
    type='ping',
    create=create,
    json_schema_id="hat-gateway://ping.yaml#/$defs/device",
    json_schema_repo=common.json_schema_repo)
