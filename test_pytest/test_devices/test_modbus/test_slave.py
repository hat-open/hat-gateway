import itertools

import pytest

from hat.drivers import modbus
from hat.drivers import tcp
import hat.event.common

from hat import aio
from hat import json
from hat import util

from hat.gateway.devices.modbus.slave import info

device_name = 'device_name'
event_type_prefix = ('gateway', info.type, device_name)

next_event_ids = (hat.event.common.EventId(1, 1, instance)
                  for instance in itertools.count(1))


class EventerClient(aio.Resource):

    def __init__(self, event_cb=None, query_cb=None):
        self._event_cb = event_cb
        self._query_cb = query_cb
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

        if not with_response:
            return

        timestamp = hat.event.common.now()
        return [hat.event.common.Event(id=next(next_event_ids),
                                       type=event.type,
                                       timestamp=timestamp,
                                       source_timestamp=event.source_timestamp,
                                       payload=event.payload)
                for event in events]

    async def query(self, params):
        if not self._query_cb:
            return hat.event.common.QueryResult([], False)

        return await aio.call(self._query_cb, params)


def assert_connections_event(event, no_conns, slave_addr):
    assert event.type == (*event_type_prefix, 'gateway', 'connections')
    assert len(event.payload.data) == no_conns
    if no_conns == 0:
        return

    for conn in event.payload.data:
        assert conn['type'] == 'TCP'
        assert isinstance(conn['connection_id'], int)
        assert conn['local']['host'] == slave_addr.host
        assert conn['local']['port'] == slave_addr.port
        assert isinstance(conn['remote']['host'], str)
        assert isinstance(conn['remote']['port'], int)

    assert len(set(i['connection_id'] for i in event.payload.data)) == no_conns


@pytest.fixture
def slave_addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


@pytest.fixture
def transport_conf(slave_addr):
    return {
        'type': 'TCP',
        'local_host': slave_addr.host,
        'local_port': slave_addr.port,
        'remote_hosts': None,
        'max_connections': None,
        'keep_alive_timeout': 1}


@pytest.mark.parametrize("conf", [
    {'name': 'name',
     'modbus_type': 'TCP',
     'transport': {
        'type': 'TCP',
        'local_host': '127.0.0.1',
        'local_port': 54321,
        'remote_hosts': ['1.2.3', '192.168.0.13'],
        'max_connections': 3,
        'keep_alive_timeout': 1},
     'data': [
        {'name': 'data1',
         'device_id': 1,
         'data_type': 'COIL',
         'start_address': 123,
         'bit_offset': 0,
         'bit_count': 3}]},
    {'name': 'name',
     'modbus_type': 'ASCII',
     'transport': {
        'type': 'SERIAL',
        'port': '/dev/ttyS0',
        'baudrate': 9600,
        'bytesize': 'FIVEBITS',
        'parity': 'MARK',
        'stopbits': 'ONE_POINT_FIVE',
        'flow_control': {'xonxoff': False,
                         'rtscts': True,
                         'dsrdtr': False},
        'silent_interval': 0.005,
        'keep_alive_timeout': 1.5},
     'data': [
        {'name': 'data1',
         'device_id': 321,
         'data_type': 'DISCRETE_INPUT',
         'start_address': 0,
         'bit_offset': 13,
         'bit_count': 10}]},
])
def test_valid_conf(conf):
    validator = json.DefaultSchemaValidator(info.json_schema_repo)
    validator.validate(info.json_schema_id, conf)


async def test_create(transport_conf):
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': []}

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    assert device.is_open

    device.close()
    assert device.is_closing
    await device.wait_closed()
    assert device.is_closed


async def test_status(transport_conf, slave_addr):
    event_queue = aio.Queue()

    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': []}

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_connections_event(event, 0, slave_addr)

    masters = set()
    for _ in range(3):
        master = await modbus.create_tcp_master(
            modbus.ModbusType.TCP, slave_addr)
        masters.add(master)

        event = await event_queue.get()
        assert_connections_event(event, len(masters), slave_addr)

    while masters:
        master = masters.pop()
        await master.async_close()

        event = await event_queue.get()
        assert_connections_event(event, len(masters), slave_addr)

    assert event_queue.empty()

    await device.async_close()
