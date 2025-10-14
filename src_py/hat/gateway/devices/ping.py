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
    endpoint = await icmp.create_endpoint(name=conf['name'])

    try:
        device = PingDevice()
        device._eventer_client = eventer_client
        device._event_type_prefix = event_type_prefix
        device._endpoint = endpoint
        device._log = common.create_device_logger_adapter(mlog, conf['name'])

        for device_conf in conf['remote_devices']:
            remote_device = _RemoteDevice()
            remote_device._conf = device_conf
            remote_device._endpoint = endpoint
            remote_device._device = device
            remote_device._status = None
            remote_device._log = _create_remote_device_logger_adapter(
                conf['name'], device_conf['name'])

            remote_device.async_group.spawn(remote_device._ping_loop)

    except BaseException:
        await aio.uncancellable(endpoint.async_close())
        raise

    return device


class PingDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._endpoint._async_group

    def process_events(self, events: Collection[hat.event.common.Event]):
        pass

    async def register_status(self, remote_name, status):
        await self._eventer_client.register([
            hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway', 'status',
                      remote_name),
                source_timestamp=None,
                payload=hat.event.common.EventPayloadJson(status))])


info = common.DeviceInfo(
    type='ping',
    create=create,
    json_schema_id="hat-gateway://ping.yaml#/$defs/device",
    json_schema_repo=common.json_schema_repo)


def _create_remote_device_logger_adapter(name, remote_name):
    extra = {'info': {'type': 'PingRemoteDevice',
                      'name': name,
                      'remote_name': remote_name}}

    return logging.LoggerAdapter(mlog, extra)


class _RemoteDevice(aio.Resource):

    @property
    def async_group(self):
        return self._endpoint.async_group

    async def _ping_loop(self):
        try:
            await self._set_status("NOT_AVAILABLE")
            while True:
                try:
                    await self._ping_retry()
                    status = "AVAILABLE"
                    self._log.debug('ping successfull')

                except Exception as e:
                    self._log.debug("device not available: %s", e, exc_info=e)
                    status = "NOT_AVAILABLE"

                await self._set_status(status)
                await asyncio.sleep(self._conf['ping_delay'])

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("ping loop error: %s", e, exc_info=e)

        finally:
            self.close()
            with contextlib.suppress(ConnectionError):
                await self._set_status("NOT_AVAILABLE")

    async def _ping_retry(self):
        retry_count = self._conf['retry_count']
        for i in range(retry_count + 1):
            try:
                return await aio.wait_for(
                    self._endpoint.ping(self._conf['host']),
                    timeout=self._conf['ping_timeout'])

            except Exception as e:
                if self._log.isEnabledFor(logging.DEBUG):
                    retry_msg = f", retry {i}/{retry_count}" if i > 0 else ""
                    mlog.debug('no ping response%s: %s',
                               retry_msg, e, exc_info=e)

            await asyncio.sleep(self._conf['retry_delay'])

        raise Exception(f"no ping response after {retry_count} retries")

    async def _set_status(self, status):
        if self._status == status:
            return

        mlog.debug('remote device status %s', status)
        self._status = status

        await self._device.register_status(self._conf['name'], status)
