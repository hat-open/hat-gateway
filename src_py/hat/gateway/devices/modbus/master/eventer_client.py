from collections.abc import Iterable
import contextlib
import logging

from hat import aio
import hat.event.common
import hat.event.eventer

from hat.gateway.devices.modbus.master import common


mlog = logging.getLogger(__name__)


class EventerClientProxy(aio.Resource):

    def __init__(self,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix,
                 name: str):
        self._eventer_client = eventer_client
        self._event_type_prefix = event_type_prefix
        self._log = common.create_device_logger_adapter(mlog, name)

    @property
    def async_group(self) -> aio.Group:
        return self._eventer_client.async_group

    def process_event(self, event: hat.event.common.Event) -> common.Request:
        return _request_from_event(self._event_type_prefix, event)

    async def write(self, responses: Iterable[common.Response]):
        events = [_response_to_register_event(self._event_type_prefix, i)
                  for i in responses]
        await self._eventer_client.register(events)

    async def query_enabled_devices(self) -> set[int]:
        self._log.debug('querying enabled devices')
        enabled_devices = set()

        event_type = (*self._event_type_prefix, 'system', 'remote_device',
                      '?', 'enable')
        params = hat.event.common.QueryLatestParams([event_type])
        result = await self._eventer_client.query(params)
        self._log.debug('received %s events', len(result.events))

        for event in result.events:
            if not event.payload or not bool(event.payload.data):
                continue

            device_id_str = event.type[len(self._event_type_prefix) + 2]
            with contextlib.suppress(ValueError):
                enabled_devices.add(int(device_id_str))

        self._log.debug('detected %s enabled devices', len(enabled_devices))
        return enabled_devices


def _request_from_event(event_type_prefix, event):
    event_type_suffix = event.type[len(event_type_prefix):]

    if event_type_suffix[:2] != ('system', 'remote_device'):
        raise Exception('unsupported event type')

    device_id = int(event_type_suffix[2])

    if event_type_suffix[3] == 'enable':
        enable = bool(event.payload.data)
        return common.RemoteDeviceEnableReq(device_id=device_id,
                                            enable=enable)

    if event_type_suffix[3] == 'write':
        data_name = event_type_suffix[4]
        request_id = event.payload.data['request_id']
        value = event.payload.data['value']
        return common.RemoteDeviceWriteReq(device_id=device_id,
                                           data_name=data_name,
                                           request_id=request_id,
                                           value=value)

    raise Exception('unsupported event type')


def _response_to_register_event(event_type_prefix, res):
    if isinstance(res, common.StatusRes):
        event_type = (*event_type_prefix, 'gateway', 'status')
        payload = res.status

    elif isinstance(res, common.RemoteDeviceStatusRes):
        event_type = (*event_type_prefix, 'gateway', 'remote_device',
                      str(res.device_id), 'status')
        payload = res.status

    elif isinstance(res, common.RemoteDeviceReadRes):
        event_type = (*event_type_prefix, 'gateway', 'remote_device',
                      str(res.device_id), 'read', res.data_name)
        payload = {'result': res.result}
        if res.value is not None:
            payload['value'] = res.value
        if res.cause is not None:
            payload['cause'] = res.cause

    elif isinstance(res, common.RemoteDeviceWriteRes):
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
