from collections.abc import Collection, Iterable
import contextlib
import logging
import typing

from hat import aio
import hat.event.common
import hat.event.eventer

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


class EventerClientProxy(aio.Resource):

    def __init__(self,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix,
                 log_prefix: str):
        self._eventer_client = eventer_client
        self._event_type_prefix = event_type_prefix
        self._log_prefix = log_prefix

    @property
    def async_group(self) -> aio.Group:
        return self._eventer_client.async_group

    def process_events(self,
                       events: Collection[hat.event.common.Event]
                       ) -> Iterable[Request]:
        self._log(logging.DEBUG, 'received %s events', len(events))
        for event in events:
            try:
                yield _request_from_event(self._event_type_prefix, event)

            except Exception as e:
                self._log(logging.INFO, 'received invalid event: %s', e,
                          exc_info=e)

    async def write(self, responses: Iterable[Response]):
        events = [_response_to_register_event(self._event_type_prefix, i)
                  for i in responses]
        await self._eventer_client.register(events)

    async def query_enabled_devices(self) -> set[int]:
        self._log(logging.DEBUG, 'querying enabled devices')
        enabled_devices = set()

        event_type = (*self._event_type_prefix, 'system', 'remote_device',
                      '?', 'enable')
        params = hat.event.common.QueryLatestParams([event_type])
        result = await self._eventer_client.query(params)
        self._log(logging.DEBUG, 'received %s events', len(result.events))

        for event in result.events:
            if not event.payload or not bool(event.payload.data):
                continue

            device_id_str = event.type[len(self._event_type_prefix) + 2]
            with contextlib.suppress(ValueError):
                enabled_devices.add(int(device_id_str))

        self._log(logging.DEBUG, 'detected %s enabled devices',
                  len(enabled_devices))
        return enabled_devices

    def _log(self, level, msg, *args, **kwargs):
        if not mlog.isEnabledFor(level):
            return

        mlog.log(level, f"{self._log_prefix}: {msg}", *args, **kwargs)


def _request_from_event(event_type_prefix, event):
    event_type_suffix = event.type[len(event_type_prefix):]

    if event_type_suffix[:2] != ('system', 'remote_device'):
        raise Exception('unsupported event type')

    device_id = int(event_type_suffix[2])

    if event_type_suffix[3] == 'enable':
        enable = bool(event.payload.data)
        return RemoteDeviceEnableReq(device_id=device_id,
                                     enable=enable)

    if event_type_suffix[3] == 'write':
        data_name = event_type_suffix[4]
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
        type=event_type,
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload))
