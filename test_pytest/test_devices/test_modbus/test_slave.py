import asyncio
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


def assert_connections_event(event, no_conns, slave_addr, master_addr=None):
    assert event.type == (*event_type_prefix, 'gateway', 'connections')
    assert len(event.payload.data) == no_conns
    if no_conns == 0:
        return

    for conn in event.payload.data:
        assert conn['type'] == 'TCP'
        assert isinstance(conn['connection_id'], int)
        assert isinstance(conn['remote']['host'], str)
        assert isinstance(conn['remote']['port'], int)
        if slave_addr:
            assert conn['local']['host'] == slave_addr.host
            assert conn['local']['port'] == slave_addr.port

    if master_addr:
        assert any(conn['local']['host'] == master_addr.host and
                   conn['local']['port'] == master_addr.port
                   for conn in event.payload.data)

    assert len(set(i['connection_id'] for i in event.payload.data)) == no_conns

    assert event.source_timestamp is None


def create_event(event_type, payload_data):
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload_data))


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
        assert master.info.remote_addr == slave_addr
        masters.add(master)

        event = await event_queue.get()
        assert_connections_event(
            event, len(masters), slave_addr, master.info.local_addr)

    while masters:
        master = masters.pop()
        await master.async_close()

        event = await event_queue.get()
        assert_connections_event(event, len(masters), slave_addr)

    assert event_queue.empty()

    await device.async_close()


async def test_query_data(transport_conf, slave_addr):
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': [
                {'name': f'd{i}',
                 'device_id': 1,
                 'data_type': 'COIL',
                 'start_address': i,
                 'bit_offset': 1,
                 'bit_count': 1} for i in range(10)]}

    queried_event_types = []

    def on_query(params):
        assert isinstance(params, hat.event.common.QueryLatestParams)
        queried_event_types.extend(params.event_types)
        return hat.event.common.QueryResult([], False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    for data_conf in conf['data']:
        event_type = (*event_type_prefix, 'system', data_conf['name'])
        assert any(hat.event.common.matches_query_type(event_type, query_type)
                   for query_type in queried_event_types)

    await device.async_close()


async def test_initiate_on_query(transport_conf, slave_addr):
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': [
                {'name': f'd{i}',
                 'device_id': 1,
                 'data_type': 'COIL',
                 'start_address': i * 3,
                 'bit_offset': 0,
                 'bit_count': 3} for i in range(7)]}

    queried_events = [
        create_event((*event_type_prefix, 'system', 'data', f'd{i}'),
                     {'value': i + 1})
        for i in range(7)]

    def on_query(params):
        return hat.event.common.QueryResult([queried_events], False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await modbus.create_tcp_master(modbus.ModbusType.TCP, slave_addr)

    master_read_value = await master.read(
        device_id=1,
        data_type=modbus.DataType.COIL,
        start_address=0,
        quantity=7)
    assert master_read_value == [evt.payload.data['value']
                                 for evt in queried_events]

    await master.async_close()
    await device.async_close()


async def test_initiate_on_query_overlap(transport_conf, slave_addr):
    # assert queried events are applied in event_id order when they overlap
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': [
                {'name': 'd1',
                 'device_id': 1,
                 'data_type': 'COIL',
                 'start_address': 0,
                 'bit_offset': 0,
                 'bit_count': 2},
                {'name': 'd2',
                 'device_id': 1,
                 'data_type': 'COIL',
                 'start_address': 0,
                 'bit_offset': 0,
                 'bit_count': 2},
                {'name': 'd3',
                 'device_id': 1,
                 'data_type': 'COIL',
                 'start_address': 0,
                 'bit_offset': 0,
                 'bit_count': 2}]}

    event1 = hat.event.common.Event(
        id=hat.event.common.EventId(1, 1, 1),
        type=(*event_type_prefix, 'system', 'data', 'd1'),
        timestamp=hat.event.common.now(),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson({'value': 1}))
    event2 = hat.event.common.Event(
        id=hat.event.common.EventId(1, 1, 2),
        type=(*event_type_prefix, 'system', 'data', 'd1'),
        timestamp=hat.event.common.now(),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson({'value': 2}))
    event3 = hat.event.common.Event(
        id=hat.event.common.EventId(1, 1, 3),
        type=(*event_type_prefix, 'system', 'data', 'd1'),
        timestamp=hat.event.common.now(),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson({'value': 3}))

    def on_query(params):
        return hat.event.common.QueryResult([event3, event2, event1], False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await modbus.create_tcp_master(modbus.ModbusType.TCP, slave_addr)

    master_read_value = await master.read(
        device_id=1,
        data_type=modbus.DataType.COIL,
        start_address=0,
        quantity=2)
    assert master_read_value == [1, 1]

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'data_type, start_address, bit_offset, bit_count, event_value, '
    'read_data_type, read_start_address, read_quantity, read_value', [
        (modbus.DataType.COIL, 0, 0, 1, 0,
         modbus.DataType.COIL, 0, 1, [0]),
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.COIL, 0, 1, [1]),
        (modbus.DataType.COIL, 0, 0, 2, 3,
         modbus.DataType.COIL, 0, 1, [1]),
        (modbus.DataType.COIL, 0, 0, 2, 3,
         modbus.DataType.COIL, 1, 1, [1]),
        (modbus.DataType.COIL, 0, 0, 2, 3,
         modbus.DataType.COIL, 0, 2, [1, 1]),
        (modbus.DataType.COIL, 0, 0, 3, 7,
         modbus.DataType.COIL, 0, 3, [1, 1, 1]),
        # read read more than configured
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.COIL, 0, 2, [1, 0]),
        (modbus.DataType.COIL, 1, 0, 1, 1,
         modbus.DataType.COIL, 0, 3, [0, 1, 0]),
        (modbus.DataType.COIL, 0, 0, 2, 3,
         modbus.DataType.COIL, 2, 1, [0]),
        (modbus.DataType.COIL, 0, 0, 2, 3,
         modbus.DataType.COIL, 1, 2, [1, 0]),
        # read from undefined address
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.COIL, 1, 1, [0]),

        (modbus.DataType.DISCRETE_INPUT, 123, 2, 2, 3,
         modbus.DataType.DISCRETE_INPUT, 125, 2, [1, 1]),
        (modbus.DataType.DISCRETE_INPUT, 123, 2, 2, 3,
         modbus.DataType.DISCRETE_INPUT, 124, 2, [1, 0]),

        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0x1234,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0x1234]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 0, 2, [0x1234, 0x5678]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 1, 1, [0x5678]),
        (modbus.DataType.HOLDING_REGISTER, 0, 8, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 1, 1, [0x3456]),
        (modbus.DataType.HOLDING_REGISTER, 0, 8, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 1, 2, [0x3456, 0x7800]),

        (modbus.DataType.INPUT_REGISTER, 0, 0, 32, 0x12345678,
         modbus.DataType.INPUT_REGISTER, 0, 2, [0x1234, 0x5678]),

        # invalid value for configured data are ignored
        (modbus.DataType.COIL, 0, 0, 1, 3,
         modbus.DataType.COIL, 0, 1, [0]),
        (modbus.DataType.COIL, 0, 0, 3, 15,
         modbus.DataType.COIL, 0, 3, [0, 0, 0]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0x12345,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 8, 0x1234,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0]),

        # wrong read data type
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.DISCRETE_INPUT, 0, 1, [0]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0xFFFF,
         modbus.DataType.COIL, 0, 1, [0]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0xFFFF,
         modbus.DataType.INPUT_REGISTER, 0, 1, [0]),
    ])
async def test_read(transport_conf, slave_addr, data_type, start_address,
                    bit_offset, bit_count, event_value, read_data_type,
                    read_start_address, read_quantity, read_value):
    start_address = 0
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': [
                {'name': 'd1',
                 'device_id': 1,
                 'data_type': data_type.name,
                 'start_address': start_address,
                 'bit_offset': bit_offset,
                 'bit_count': bit_count}]}

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await modbus.create_tcp_master(modbus.ModbusType.TCP, slave_addr)

    # assert slave data is initialized to 0
    if data_type in {modbus.DataType.COIL, modbus.DataType.DISCRETE_INPUT}:
        quantity = bit_count
    else:
        quantity = (bit_count // 16) or 1
    master_read_value = await master.read(
        device_id=1,
        data_type=data_type,
        start_address=start_address,
        quantity=bit_count)
    assert master_read_value == [0] * quantity

    event = create_event((*event_type_prefix, 'system', 'data', 'd1'),
                         {'value': event_value})
    await aio.call(device.process_events, [event])

    master_read_value = await master.read(
        device_id=1,
        data_type=read_data_type,
        start_address=read_start_address,
        quantity=read_quantity)
    assert master_read_value == read_value

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize('data_type', modbus.DataType)
async def test_read_invalid_device_id(transport_conf, slave_addr,
                                      data_type):
    device_id = 1
    invalid_device_id = 13
    start_address = 0
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': [
                {'name': 'd1',
                 'device_id': device_id,
                 'data_type': data_type.name,
                 'start_address': start_address,
                 'bit_offset': 0,
                 'bit_count': 1}]}

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await modbus.create_tcp_master(modbus.ModbusType.TCP, slave_addr)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(master.read(
            device_id=invalid_device_id,
            data_type=data_type,
            start_address=start_address,
            quantity=1), timeout=0.05)

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize('data_type, bit_count, write_values, event_value', [
    (modbus.DataType.COIL, 1, [1], 1),
    (modbus.DataType.COIL, 3, [1, 1, 1], 7),
    (modbus.DataType.HOLDING_REGISTER, 16, [0], 0),
    (modbus.DataType.HOLDING_REGISTER, 16, [0xFFFF], 65535),
    (modbus.DataType.HOLDING_REGISTER, 32, [0x1234, 0x5678], 305419896)])
@pytest.mark.parametrize('event_write_result, master_write_result', [
    ('SUCCESS', None),
    ('INVALID_FUNCTION_CODE', modbus.Error.INVALID_FUNCTION_CODE),
    ('INVALID_DATA_ADDRESS', modbus.Error.INVALID_DATA_ADDRESS),
    ('INVALID_DATA_VALUE', modbus.Error.INVALID_DATA_VALUE),
    ('FUNCTION_ERROR', modbus.Error.FUNCTION_ERROR),
    ('GATEWAY_PATH_UNAVAILABLE', modbus.Error.GATEWAY_PATH_UNAVAILABLE),
    ('GATEWAY_TARGET_DEVICE_FAILED_TO_RESPOND',
        modbus.Error.GATEWAY_TARGET_DEVICE_FAILED_TO_RESPOND)])
async def test_write(transport_conf, slave_addr, data_type, bit_count,
                     write_values, event_value,
                     event_write_result, master_write_result):
    event_queue = aio.Queue()
    device_id = 1
    data_name = 'd1'
    start_address = 0
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': transport_conf,
            'data': [
                {'name': data_name,
                 'device_id': device_id,
                 'data_type': data_type.name,
                 'start_address': start_address,
                 'bit_offset': 0,
                 'bit_count': bit_count}]}

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    await event_queue.get()

    master = await modbus.create_tcp_master(modbus.ModbusType.TCP, slave_addr)

    conns_event = await event_queue.get()
    assert_connections_event(conns_event, 1, slave_addr)
    connection_id = conns_event.payload.data[0]['connection_id']

    write_res_future = await master.async_group.spawn(
        master.write, device_id, data_type, start_address, write_values)

    write_req_event = await event_queue.get()
    assert write_req_event.type == (*event_type_prefix, 'gateway', 'write')
    assert isinstance(write_req_event.payload.data['request_id'], str)
    assert write_req_event.payload.data['connection_id'] == connection_id
    assert len(write_req_event.payload.data['data']) == 1
    assert write_req_event.payload.data['data'][0] == {'name': data_name,
                                                       'value': event_value}

    write_resp_event = create_event(
        (*event_type_prefix, 'system', 'write'),
        {'request_id': write_req_event.payload.data['request_id'],
         'result': event_write_result})
    await aio.call(device.process_events, [write_resp_event])

    write_res = await write_res_future
    assert write_res == master_write_result

    await master.async_close()
    await device.async_close()
