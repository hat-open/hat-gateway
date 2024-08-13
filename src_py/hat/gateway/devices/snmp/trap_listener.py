"""SNMP trap listener device"""

from collections.abc import Collection
import collections
import logging
import typing

from hat import aio
from hat import asn1
from hat.drivers import snmp
from hat.drivers import udp
import hat.event.common
import hat.event.eventer

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)


async def create(conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'SnmpTrapListenerDevice':
    device = SnmpTrapListenerDevice()
    device._eventer_client = eventer_client
    device._event_type_prefix = event_type_prefix
    device._remote_devices = collections.defaultdict(collections.deque)

    for remote_device_conf in conf['remote_devices']:
        version = snmp.Version[remote_device_conf['version']]
        remote_device = _RemoteDevice(
            name=remote_device_conf['name'],
            oids={_oid_from_str(oid)
                  for oid in remote_device_conf['oids']},
            string_hex_oids={_oid_from_str(oid)
                             for oid in remote_device_conf['string_hex_oids']})

        if remote_device_conf['version'] == 'V1':
            subkey = remote_device_conf['community']

        elif remote_device_conf['version'] == 'V2C':
            subkey = remote_device_conf['community']

        elif remote_device_conf['version'] == 'V3':
            subkey = (
                snmp.Context(
                    engine_id=bytes.fromhex(
                        remote_device_conf['context']['engine_id']),
                    name=remote_device_conf['context']['name'])
                if remote_device_conf['context'] else None)

        else:
            raise Exception('invalid version')

        device._remote_devices[(version, subkey)].append(remote_device)

    users = [
        snmp.User(
            name=user_conf['name'],
            auth_type=(snmp.AuthType[user_conf['authentication']['type']]
                       if user_conf['authentication'] else None),
            auth_password=(user_conf['authentication']['password']
                           if user_conf['authentication'] else None),
            priv_type=(snmp.PrivType[user_conf['privacy']['type']]
                       if user_conf['privacy'] else None),
            priv_password=(user_conf['privacy']['password']
                           if user_conf['privacy'] else None))
        for user_conf in conf['users']]

    device._listener = await snmp.create_trap_listener(
        local_addr=udp.Address(host=conf['local_host'],
                               port=conf['local_port']),
        v1_trap_cb=device._on_v1_trap,
        v2c_trap_cb=device._on_v2c_trap,
        v2c_inform_cb=device._on_v2c_inform,
        v3_trap_cb=device._on_v3_trap,
        v3_inform_cb=device._on_v3_inform,
        users=users)

    return device


info = common.DeviceInfo(
    type='snmp_trap_listener',
    create=create,
    json_schema_id="hat-gateway://snmp.yaml#/$defs/trap_listener",
    json_schema_repo=common.json_schema_repo)


class SnmpTrapListenerDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._listener.async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        pass

    async def _on_v1_trap(self, addr, community, trap):
        await self._process_data(version=snmp.Version.V1,
                                 subkey=community,
                                 data=trap.data)

    async def _on_v2c_trap(self, addr, community, trap):
        await self._process_data(version=snmp.Version.V2C,
                                 subkey=community,
                                 data=trap.data)

    async def _on_v2c_inform(self, addr, community, inform):
        await self._process_data(version=snmp.Version.V2C,
                                 subkey=community,
                                 data=inform.data)

    async def _on_v3_trap(self, addr, user, context, trap):
        await self._process_data(version=snmp.Version.V3,
                                 subkey=context,
                                 data=trap.data)

    async def _on_v3_inform(self, addr, user, context, inform):
        await self._process_data(version=snmp.Version.V3,
                                 subkey=context,
                                 data=inform.data)

    async def _process_data(self, version, subkey, data):
        try:
            events = collections.deque()

            for key in [(version, subkey),
                        (version, None)]:
                for remote_device in self._remote_devices.get(key, []):
                    for i in data:
                        if i.name not in remote_device.oids:
                            continue

                        event = hat.event.common.RegisterEvent(
                            type=(*self._event_type_prefix, 'gateway', 'data',
                                  remote_device.name, _oid_to_str(i.name)),
                            source_timestamp=None,
                            payload=hat.event.common.EventPayloadJson(
                                _event_payload_from_data(
                                    i, remote_device.string_hex_oids)))
                        events.append(event)

            if not events:
                return

            await self._eventer_client.register(events)

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("error processing data: %s", e, exc_info=e)


class _RemoteDevice(typing.NamedTuple):
    name: str
    oids: set[asn1.ObjectIdentifier]
    string_hex_oids: set[asn1.ObjectIdentifier]


def _oid_from_str(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))


def _oid_to_str(oid):
    return '.'.join(str(i) for i in oid)


def _event_payload_from_data(data, string_hex_oids):
    if isinstance(data, snmp.EmptyData):
        return {'type': 'ERROR',
                'value': 'EMPTY'}

    if isinstance(data, snmp.UnspecifiedData):
        return {'type': 'ERROR',
                'value': 'UNSPECIFIED'}

    if isinstance(data, snmp.NoSuchObjectData):
        return {'type': 'ERROR',
                'value': 'NO_SUCH_OBJECT'}

    if isinstance(data, snmp.NoSuchInstanceData):
        return {'type': 'ERROR',
                'value': 'NO_SUCH_INSTANCE'}

    if isinstance(data, snmp.EndOfMibViewData):
        return {'type': 'ERROR',
                'value': 'END_OF_MIB_VIEW'}

    if isinstance(data, snmp.IntegerData):
        return {'type': 'INTEGER',
                'value': data.value}

    if isinstance(data, snmp.UnsignedData):
        return {'type': 'UNSIGNED',
                'value': data.value}

    if isinstance(data, snmp.CounterData):
        return {'type': 'COUNTER',
                'value': data.value}

    if isinstance(data, snmp.BigCounterData):
        return {'type': 'BIG_COUNTER',
                'value': data.value}

    if isinstance(data, snmp.TimeTicksData):
        return {'type': 'TIME_TICKS',
                'value': data.value}

    if isinstance(data, snmp.StringData):
        if data.name in string_hex_oids:
            return {'type': 'STRING_HEX',
                    'value': data.value.hex()}

        return {'type': 'STRING',
                'value': str(data.value, encoding='utf-8', errors='replace')}

    if isinstance(data, snmp.ObjectIdData):
        return {'type': 'OBJECT_ID',
                'value': _oid_to_str(data.value)}

    if isinstance(data, snmp.IpAddressData):
        return {'type': 'IP_ADDRESS',
                'value': '.'.join(str(i) for i in data.value)}

    if isinstance(data, snmp.ArbitraryData):
        return {'type': 'ARBITRARY',
                'value': data.value.hex()}

    raise Exception('invalid response data')
