import contextlib
import logging
import typing

from hat import aio
import hat.event.common

from hat.gateway import common


mlog = logging.getLogger(__name__)


class RemoteDeviceEnableReq(typing.NamedTuple):
    device_id: int
    enable: bool


class RemoteDeviceWriteReq(typing.NamedTuple):
    device_id: int
    data_name: str
    request_id: str
    value: int


Request: typing.TypeAlias = RemoteDeviceEnableReq | RemoteDeviceWriteReq


class StatusRes(typing.NamedTuple):
    status: str


class RemoteDeviceStatusRes(typing.NamedTuple):
    device_id: int
    status: str


class RemoteDeviceReadRes(typing.NamedTuple):
    device_id: int
    data_name: str
    result: str
    value: int | None
    cause: str | None


class RemoteDeviceWriteRes(typing.NamedTuple):
    device_id: int
    data_name: str
    request_id: str
    result: str


Response: typing.TypeAlias = (StatusRes |
                              RemoteDeviceStatusRes |
                              RemoteDeviceReadRes |
                              RemoteDeviceWriteRes)


class EventClientProxy(aio.Resource):

    def __init__(self,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix,
                 log_prefix: str):
        self._event_client = event_client
        self._event_type_prefix = event_type_prefix
        self._log_prefix = log_prefix
        self._async_group = event_client.async_group.create_subgroup()
        self._read_queue = aio.Queue()
        self.async_group.spawn(self._read_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    def write(self, responses: list[Response]):
        register_events = [
            _response_to_register_event(self._event_type_prefix, i)
            for i in responses]
        self._event_client.register(register_events)

    async def read(self) -> Request:
        try:
            return await self._read_queue.get()

        except aio.QueueClosedError:
            raise ConnectionError()

    async def query_enabled_devices(self) -> set[int]:
        self._log(logging.DEBUG, 'querying enabled devices')
        enabled_devices = set()

        events = await self._event_client.query(hat.event.common.QueryData(
            event_types=[(*self._event_type_prefix, 'system', 'remote_device',
                          '?', 'enable')],
            unique_type=True))
        self._log(logging.DEBUG, 'received %s events', len(events))

        for event in events:
            if not event.payload or not bool(event.payload.data):
                continue

            device_id_str = event.event_type[6]
            with contextlib.suppress(ValueError):
                enabled_devices.add(int(device_id_str))

        self._log(logging.DEBUG, 'detected %s enabled devices',
                  len(enabled_devices))
        return enabled_devices

    async def _read_loop(self):
        try:
            self._log(logging.DEBUG, 'starting read loop')
            while True:
                events = await self._event_client.receive()
                self._log(logging.DEBUG, 'received %s events', len(events))

                for event in events:
                    try:
                        req = _request_from_event(event)
                        self._read_queue.put_nowait(req)

                    except Exception as e:
                        self._log(logging.INFO, 'received invalid event: %s',
                                  e, exc_info=e)

        except ConnectionError:
            self._log(logging.DEBUG, 'connection closed')

        except Exception as e:
            self._log(logging.ERROR, 'read loop error: %s', e, exc_info=e)

        finally:
            self._log(logging.DEBUG, 'closing read loop')
            self.close()
            self._read_queue.close()

    def _log(self, level, msg, *args, **kwargs):
        if not mlog.isEnabledFor(level):
            return

        mlog.log(level, f"{self._log_prefix}: {msg}", *args, **kwargs)


def _request_from_event(event):
    event_type_suffix = event.event_type[5:]

    if event_type_suffix[0] != 'remote_device':
        raise Exception('unsupported event type')

    device_id = int(event_type_suffix[1])

    if event_type_suffix[2] == 'enable':
        enable = bool(event.payload.data)
        return RemoteDeviceEnableReq(device_id=device_id,
                                     enable=enable)

    if event_type_suffix[2] == 'write':
        data_name = event_type_suffix[3]
        request_id = event.payload.data['request_id']
        value = event.payload.data['value']
        return RemoteDeviceWriteReq(device_id=device_id,
                                    data_name=data_name,
                                    request_id=request_id,
                                    value=value)

    raise Exception('unsupported event type')


def _response_to_register_event(event_type_prefix, res):
    if isinstance(res, StatusRes):
        event_type = (*event_type_prefix, 'gateway', 'status')
        payload = res.status

    elif isinstance(res, RemoteDeviceStatusRes):
        event_type = (*event_type_prefix, 'gateway', 'remote_device',
                      str(res.device_id), 'status')
        payload = res.status

    elif isinstance(res, RemoteDeviceReadRes):
        event_type = (*event_type_prefix, 'gateway', 'remote_device',
                      str(res.device_id), 'read', res.data_name)
        payload = {'result': res.result}
        if res.value is not None:
            payload['value'] = res.value
        if res.cause is not None:
            payload['cause'] = res.cause

    elif isinstance(res, RemoteDeviceWriteRes):
        event_type = (*event_type_prefix, 'gateway', 'remote_device',
                      str(res.device_id), 'write', res.data_name)
        payload = {'request_id': res.request_id,
                   'result': res.result}

    else:
        raise ValueError('invalid response type')

    return hat.event.common.RegisterEvent(
        event_type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))
