import collections

import pytest

from hat import aio
from hat import json
from hat import util
from hat.drivers import snmp
from hat.drivers import udp
import hat.event.common

from hat.gateway.devices.snmp.trap_listener import info


device_name = 'device_name'
event_type_prefix = 'gateway', info.type, device_name

community = 'community'
context = snmp.Context(engine_id=b'\x00\x01\x02',
                       name='context')
user = snmp.User(name='user',
                 auth_type=snmp.AuthType.SHA,
                 auth_password='auth pass',
                 priv_type=snmp.PrivType.DES,
                 priv_password='priv pass')

other_oid = 1, 2, 0
data_oid = 1, 2, 1
str_hex_oid = 1, 2, 2

remote_devs = {version: {i: f'{version.name} {i}'
                         for i in ['some', 'all']}
               for version in snmp.Version}

snmp_event_data = [
    (snmp.IntegerData(name=data_oid,
                      value=-123),
     {'type': 'INTEGER',
      'value': -123}),

    (snmp.UnsignedData(name=data_oid,
                       value=123),
     {'type': 'UNSIGNED',
      'value': 123}),

    (snmp.CounterData(name=data_oid,
                      value=321),
     {'type': 'COUNTER',
      'value': 321}),

    (snmp.BigCounterData(name=data_oid,
                         value=123456),
     {'type': 'BIG_COUNTER',
      'value': 123456}),

    (snmp.StringData(name=data_oid,
                     value=b'abc'),
     {'type': 'STRING',
      'value': 'abc'}),

    (snmp.StringData(name=str_hex_oid,
                     value=b'abc'),
     {'type': 'STRING_HEX',
      'value': '616263'}),

    (snmp.ObjectIdData(name=data_oid,
                       value=(1, 2, 3, 4, 5, 6)),
     {'type': 'OBJECT_ID',
      'value': '1.2.3.4.5.6'}),

    (snmp.IpAddressData(name=data_oid,
                        value=(192, 168, 0, 1)),
     {'type': 'IP_ADDRESS',
      'value': '192.168.0.1'}),

    (snmp.TimeTicksData(name=data_oid,
                        value=42),
     {'type': 'TIME_TICKS',
      'value': 42}),

    (snmp.ArbitraryData(name=data_oid,
                        value=b'\x01\x02\x03'),
     {'type': 'ARBITRARY',
      'value': '010203'})]


class EventerClient(aio.Resource):

    def __init__(self, event_cb=None):
        self._event_cb = event_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    @property
    def status(self):
        raise NotImplementedError()

    async def register(self, events, with_response=False):
        if self._event_cb:
            for event in events:
                await aio.call(self._event_cb, event)

    async def query(self, params):
        return hat.event.common.QueryResult([], False)


def encode_oid(oid):
    return '.'.join(str(i) for i in oid)


def decode_oid(oid_str):
    return tuple(int(i) for i in oid_str.split('.'))


def assert_data_event(event, remote_dev, oid, data):
    assert event.type == (*event_type_prefix, 'gateway', 'data',
                          remote_dev, encode_oid(oid))
    assert event.source_timestamp is None
    assert event.payload.data == data


@pytest.fixture
def addr():
    return udp.Address('127.0.0.1', util.get_unused_udp_port())


@pytest.fixture
def conf(addr):
    return {
        'local_host': addr.host,
        'local_port': addr.port,
        'users': [{'name': user.name,
                   'authentication': {'type': user.auth_type.name,
                                      'password': user.auth_password},
                   'privacy': {'type': user.priv_type.name,
                               'password': user.priv_password}}],
        'remote_devices': [{'version': 'V1',
                            'community': community,
                            'name': remote_devs[snmp.Version.V1]['some'],
                            'oids': [encode_oid(data_oid),
                                     encode_oid(str_hex_oid)],
                            'string_hex_oids': [encode_oid(str_hex_oid)]},
                           {'version': 'V1',
                            'community': None,
                            'name': remote_devs[snmp.Version.V1]['all'],
                            'oids': [encode_oid(data_oid),
                                     encode_oid(str_hex_oid)],
                            'string_hex_oids': [encode_oid(str_hex_oid)]},
                           {'version': 'V2C',
                            'community': community,
                            'name': remote_devs[snmp.Version.V2C]['some'],
                            'oids': [encode_oid(data_oid),
                                     encode_oid(str_hex_oid)],
                            'string_hex_oids': [encode_oid(str_hex_oid)]},
                           {'version': 'V2C',
                            'community': None,
                            'name': remote_devs[snmp.Version.V2C]['all'],
                            'oids': [encode_oid(data_oid),
                                     encode_oid(str_hex_oid)],
                            'string_hex_oids': [encode_oid(str_hex_oid)]},
                           {'version': 'V3',
                            'context': {'engine_id': context.engine_id.hex(),
                                        'name': context.name},
                            'name': remote_devs[snmp.Version.V3]['some'],
                            'oids': [encode_oid(data_oid),
                                     encode_oid(str_hex_oid)],
                            'string_hex_oids': [encode_oid(str_hex_oid)]},
                           {'version': 'V3',
                            'context': None,
                            'name': remote_devs[snmp.Version.V3]['all'],
                            'oids': [encode_oid(data_oid),
                                     encode_oid(str_hex_oid)],
                            'string_hex_oids': [encode_oid(str_hex_oid)]}]}


@pytest.fixture
def create_trap_sender(addr):

    async def create_trap_sender(version, community, context):
        if version == snmp.Version.V1:
            return await snmp.create_v1_trap_sender(remote_addr=addr,
                                                    community=community)

        if version == snmp.Version.V2C:
            return await snmp.create_v2c_trap_sender(remote_addr=addr,
                                                     community=community)

        if version == snmp.Version.V3:
            return await snmp.create_v3_trap_sender(
                remote_addr=addr,
                authoritative_engine_id=context.engine_id,
                context=context,
                user=user)

        raise ValueError('unsupported version')

    return create_trap_sender


def test_conf(conf):
    validator = json.DefaultSchemaValidator(info.json_schema_repo)
    validator.validate(info.json_schema_id, conf)


async def test_create(conf):
    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    assert device.is_open

    await device.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize('version', snmp.Version)
@pytest.mark.parametrize('snmp_data, event_data', snmp_event_data)
async def test_trap(addr, conf, create_trap_sender, version, snmp_data,
                    event_data):
    if (version == snmp.Version.V1 and
            isinstance(snmp_data, snmp.BigCounterData)):
        return

    trap = snmp.Trap(cause=(snmp.Cause(type=snmp.CauseType.COLD_START,
                                       value=0)
                            if version == snmp.Version.V1 else None),
                     oid=other_oid,
                     timestamp=0,
                     data=[snmp_data])

    event_queue = aio.Queue()
    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    assert event_queue.empty()

    sender = await create_trap_sender(version=version,
                                      community=community,
                                      context=context)
    sender.send_trap(trap)

    events = collections.deque()
    events.append(await event_queue.get())
    events.append(await event_queue.get())

    for remote_dev in remote_devs[version].values():
        event = util.first(
            events, lambda i: i.type[len(event_type_prefix) + 2] == remote_dev)

        assert_data_event(event, remote_dev, snmp_data.name, event_data)

    await sender.async_close()
    assert event_queue.empty()

    sender = await create_trap_sender(
        version=version,
        community=f'not {community}',
        context=context._replace(engine_id=context.engine_id + b'123',
                                 name=f'not {context.name}'))
    sender.send_trap(trap)

    event = await event_queue.get()
    assert_data_event(event, remote_devs[version]['all'], snmp_data.name,
                      event_data)

    await sender.async_close()
    assert event_queue.empty()

    await device.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize('version', [snmp.Version.V2C, snmp.Version.V3])
@pytest.mark.parametrize('snmp_data, event_data', snmp_event_data)
async def test_inform(addr, conf, create_trap_sender, version, snmp_data,
                      event_data):
    if (version == snmp.Version.V1 and
            isinstance(snmp_data, snmp.BigCounterData)):
        return

    inform = snmp.Inform(data=[snmp_data])

    event_queue = aio.Queue()
    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    assert event_queue.empty()

    sender = await create_trap_sender(version=version,
                                      community=community,
                                      context=context)
    await sender.send_inform(inform)

    events = collections.deque()
    events.append(await event_queue.get())
    events.append(await event_queue.get())

    for remote_dev in remote_devs[version].values():
        event = util.first(
            events, lambda i: i.type[len(event_type_prefix) + 2] == remote_dev)

        assert_data_event(event, remote_dev, snmp_data.name, event_data)

    await sender.async_close()
    assert event_queue.empty()

    sender = await create_trap_sender(
        version=version,
        community=f'not {community}',
        context=context._replace(engine_id=context.engine_id + b'123',
                                 name=f'not {context.name}'))
    await sender.send_inform(inform)

    event = await event_queue.get()
    assert_data_event(event, remote_devs[version]['all'], snmp_data.name,
                      event_data)

    await sender.async_close()
    assert event_queue.empty()

    await device.async_close()
    await eventer_client.async_close()
