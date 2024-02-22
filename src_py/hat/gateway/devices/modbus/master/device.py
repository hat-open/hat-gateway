from collections.abc import Collection
import asyncio
import contextlib
import logging

from hat import aio
import hat.event.common
import hat.event.eventer

from hat.gateway import common
from hat.gateway.devices.modbus.master.connection import connect
from hat.gateway.devices.modbus.master.eventer_client import (RemoteDeviceEnableReq,  # NOQA
                                                              RemoteDeviceWriteReq,  # NOQA
                                                              StatusRes,
                                                              RemoteDeviceStatusRes,  # NOQA
                                                              RemoteDeviceWriteRes,  # NOQA
                                                              EventerClientProxy)  # NOQA
from hat.gateway.devices.modbus.master.remote_device import RemoteDevice


mlog = logging.getLogger(__name__)


class ModbusMasterDevice(aio.Resource):

    def __init__(self,
                 conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix):
        self._conf = conf
        self._log_prefix = f"gateway device {conf['name']}"
        self._eventer_client = EventerClientProxy(eventer_client,
                                                  event_type_prefix,
                                                  self._log_prefix)
        self._enabled_devices = set()
        self._status = None
        self._conn = None
        self._devices = {}
        self._readers = {}
        self._async_group = aio.Group()

        self.async_group.spawn(self._connection_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        try:
            for request in self._eventer_client.process_events(events):
                if isinstance(request, RemoteDeviceEnableReq):
                    self._log(logging.DEBUG,
                              'received remote device enable request')
                    if request.enable:
                        self._enable_remote_device(request.device_id)
                    else:
                        await self._disable_remote_device(request.device_id)

                elif isinstance(request, RemoteDeviceWriteReq):
                    self._log(logging.DEBUG,
                              'received remote device write request')
                    if self._conn and self._conn.is_open:
                        self._conn.async_group.spawn(
                            self._write, request.device_id, request.data_name,
                            request.request_id, request.value)

                else:
                    raise ValueError('invalid request')

        except Exception as e:
            self._log(logging.ERROR, 'process events error: %s', e, exc_info=e)
            self.close()

    async def _connection_loop(self):

        async def cleanup():
            if self._conn:
                await self._conn.async_close()

            with contextlib.suppress(Exception):
                await self._set_status('DISCONNECTED')

        try:
            self._log(logging.DEBUG, 'starting connection loop')

            enabled_devices = await self._eventer_client.query_enabled_devices()  # NOQA
            self._enabled_devices.update(enabled_devices)

            while True:
                await self._set_status('CONNECTING')

                try:
                    self._conn = await aio.wait_for(
                        connect(self._conf['connection'],
                                self._log_prefix),
                        self._conf['connection']['connect_timeout'])

                except aio.CancelledWithResultError as e:
                    self._conn = e.result
                    raise

                except Exception as e:
                    self._log(logging.INFO, 'connecting error: %s', e,
                              exc_info=e)
                    await self._set_status('DISCONNECTED')
                    await asyncio.sleep(
                        self._conf['connection']['connect_delay'])
                    continue

                await self._set_status('CONNECTED')
                self._devices = {}
                self._readers = {}

                self._log(logging.DEBUG, 'creating remote devices')
                for device_conf in self._conf['remote_devices']:
                    device = RemoteDevice(device_conf, self._conn,
                                          self._log_prefix)
                    self._devices[device.device_id] = device

                    if device.device_id in self._enabled_devices:
                        self._enable_remote_device(device.device_id)

                    else:
                        await self._notify_response(RemoteDeviceStatusRes(
                            device_id=device.device_id,
                            status='DISABLED'))

                await self._conn.wait_closing()
                await self._conn.async_close()

                await self._set_status('DISCONNECTED')

        except Exception as e:
            self._log(logging.ERROR, 'connection loop error: %s', e,
                      exc_info=e)

        finally:
            self._log(logging.DEBUG, 'closing connection loop')
            self.close()
            await aio.uncancellable(cleanup())

    async def _notify_response(self, response):
        await self._eventer_client.write([response])

    async def _set_status(self, status):
        if self._status == status:
            return

        self._log(logging.DEBUG, 'changing status: %s -> %s',
                  self._status, status)
        self._status = status
        await self._notify_response(StatusRes(status))

    def _enable_remote_device(self, device_id):
        self._log(logging.DEBUG, 'enabling device %s', device_id)
        self._enabled_devices.add(device_id)

        device = self._devices.get(device_id)
        if not device:
            self._log(logging.DEBUG, 'device %s is not available', device_id)
            return
        if not device.conn.is_open:
            self._log(logging.DEBUG, 'connection is not available')
            return

        reader = self._readers.get(device_id)
        if reader and reader.is_open:
            self._log(logging.DEBUG, 'reader %s is already running', device_id)
            return

        self._log(logging.DEBUG, 'creating reader for device %s', device_id)
        reader = device.create_reader(self._notify_response)
        self._readers[device.device_id] = reader

    async def _disable_remote_device(self, device_id):
        self._log(logging.DEBUG, 'disabling device %s', device_id)
        self._enabled_devices.discard(device_id)

        reader = self._readers.pop(device_id, None)
        if not reader:
            self._log(logging.DEBUG, 'device reader %s is not available',
                      device_id)
            return

        await reader.async_close()

    async def _write(self, device_id, data_name, request_id, value):
        self._log(logging.DEBUG,
                  'writing (device_id: %s; data_name: %s; value: %s)',
                  device_id, data_name, value)

        device = self._devices.get(device_id)
        if not device:
            self._log(logging.DEBUG, 'device %s is not available', device_id)
            return

        response = await device.write(data_name, request_id, value)
        if response:
            self._log(logging.DEBUG, 'writing result: %s', response.result)
            await self._notify_response(response)

    def _log(self, level, msg, *args, **kwargs):
        if not mlog.isEnabledFor(level):
            return

        mlog.log(level, f"{self._log_prefix}: {msg}", *args, **kwargs)
