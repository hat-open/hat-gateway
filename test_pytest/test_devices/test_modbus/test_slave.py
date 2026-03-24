import asyncio
import atexit
import itertools
import subprocess
import time

import pytest

from hat.drivers import modbus
from hat.drivers import serial
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


def assert_connections_event(event, no_conns):
    assert event.type == (*event_type_prefix, 'gateway', 'connections')
    assert len(event.payload.data) == no_conns
    if no_conns == 0:
        return

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
def nullmodem(request, tmp_path):
    path1 = tmp_path / '1'
    path2 = tmp_path / '2'
    p = subprocess.Popen(['socat',
                          f'pty,link={path1},rawer',
                          f'pty,link={path2},rawer'],
                         stderr=subprocess.DEVNULL)
    while not path1.exists() or not path2.exists():
        time.sleep(0.001)

    def finalizer():
        p.terminate()

    atexit.register(finalizer)
    request.addfinalizer(finalizer)
    return path1, path2, p


@pytest.fixture
def slave_serial_port(nullmodem):
    return str(nullmodem[0])


@pytest.fixture
def master_serial_port(nullmodem):
    return str(nullmodem[1])


@pytest.fixture
def transport_conf_tcp(slave_addr):
    return {
        'type': 'TCP',
        'local_host': slave_addr.host,
        'local_port': slave_addr.port,
        'remote_hosts': None,
        'max_connections': None,
        'keep_alive_timeout': None}


@pytest.fixture
def transport_conf_serial(slave_serial_port):
    return {
        'type': 'SERIAL',
        'port': slave_serial_port,
        'baudrate': 9600,
        'bytesize': 'EIGHTBITS',
        'parity': 'NONE',
        'stopbits': 'ONE',
        'flow_control': {'xonxoff': False,
                         'rtscts': False,
                         'dsrdtr': False},
        'silent_interval': 0.005,
        'keep_alive_timeout': 0.1}


@pytest.fixture(params=['TCP', 'SERIAL'])
def transport_conf(transport_conf_tcp, transport_conf_serial, request):
    if request.param == 'TCP':
        return transport_conf_tcp

    if request.param == 'SERIAL':
        return transport_conf_serial


@pytest.fixture(params=modbus.ModbusType)
def conf(transport_conf, request):
    if transport_conf['type'] == 'TCP':
        data = []
    elif transport_conf['type'] == 'SERIAL':
        data = [{'name': 'data1',
                 'device_id': 1,
                 'data_type': 'COIL',
                 'start_address': 0,
                 'bit_offset': 0,
                 'bit_count': 1}]

    return {'name': 'name',
            'modbus_type': request.param.name,
            'transport': transport_conf,
            'data': data}


@pytest.fixture
def create_master_factory(conf, master_serial_port, slave_addr):
    transport_conf = conf['transport']

    async def create_master():
        if transport_conf['type'] == 'TCP':
            return await modbus.create_tcp_master(
                modbus_type=modbus.ModbusType[conf['modbus_type']],
                addr=slave_addr)

        if transport_conf['type'] == 'SERIAL':
            master = await modbus.create_serial_master(
                modbus_type=modbus.ModbusType[conf['modbus_type']],
                port=master_serial_port,
                baudrate=transport_conf['baudrate'],
                bytesize=serial.ByteSize[transport_conf['bytesize']],
                parity=serial.Parity[transport_conf['parity']],
                stopbits=serial.StopBits[transport_conf['stopbits']],
                xonxoff=transport_conf['flow_control']['xonxoff'],
                rtscts=transport_conf['flow_control']['rtscts'],
                dsrdtr=transport_conf['flow_control']['dsrdtr'],
                silent_interval=transport_conf['silent_interval'])
            # send first message in order to establish connection
            await master.send(modbus.ReadReq(device_id=1,
                                             data_type=modbus.DataType.COIL,
                                             start_address=0,
                                             quantity=1))
            return master

    return create_master


@pytest.mark.parametrize("conf", [
    {'name': 'name',
     'modbus_type': 'TCP',
     'transport': {
        'type': 'TCP',
        'local_host': '127.0.0.1',
        'local_port': 54321,
        'remote_hosts': ['1.2.3', '192.168.0.13'],
        'max_connections': 3,
        'keep_alive_timeout': None},
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


async def test_create(conf):
    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    assert device.is_open

    device.close()
    assert device.is_closing
    await device.wait_closed()
    assert device.is_closed


@pytest.mark.parametrize('modbus_type', modbus.ModbusType)
async def test_connections_tcp(transport_conf_tcp, slave_addr, modbus_type):
    conf = {'name': 'name',
            'modbus_type': modbus_type.name,
            'transport': transport_conf_tcp,
            'data': []}
    event_queue = aio.Queue()
    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_connections_event(event, 0)

    masters = set()
    for i in range(5):
        master = await modbus.create_tcp_master(modbus_type=modbus_type,
                                                addr=slave_addr)
        masters.add(master)

        assert master.info.remote_addr == slave_addr

        event = await event_queue.get()
        assert_connections_event(event, len(masters))

        for conn in event.payload.data:
            assert conn['type'] == 'TCP'
            assert conn['local']['host'] == slave_addr.host
            assert conn['local']['port'] == slave_addr.port

        assert any(conn['remote']['host'] == master.info.local_addr.host and
                   conn['remote']['port'] == master.info.local_addr.port
                   for conn in event.payload.data)

    while masters:
        master = masters.pop()
        await master.async_close()

        event = await event_queue.get()
        assert_connections_event(event, len(masters))

    assert event_queue.empty()

    await device.async_close()


@pytest.mark.parametrize('modbus_type', modbus.ModbusType)
async def test_connection_serial(transport_conf_serial, modbus_type,
                                 master_serial_port):
    conf = {'name': 'name',
            'modbus_type': modbus_type.name,
            'transport': transport_conf_serial,
            'data': [{'name': 'data1',
                      'device_id': 1,
                      'data_type': 'COIL',
                      'start_address': 0,
                      'bit_offset': 0,
                      'bit_count': 1}]}
    event_queue = aio.Queue()
    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_connections_event(event, 0)

    master = await modbus.create_serial_master(
        modbus_type=modbus.ModbusType[conf['modbus_type']],
        port=master_serial_port,
        baudrate=conf['transport']['baudrate'],
        bytesize=serial.ByteSize[conf['transport']['bytesize']],
        parity=serial.Parity[conf['transport']['parity']],
        stopbits=serial.StopBits[conf['transport']['stopbits']],
        xonxoff=conf['transport']['flow_control']['xonxoff'],
        rtscts=conf['transport']['flow_control']['rtscts'],
        dsrdtr=conf['transport']['flow_control']['dsrdtr'],
        silent_interval=conf['transport']['silent_interval'])
    # send first message in order to establish connection
    await master.send(modbus.ReadReq(device_id=1,
                                     data_type=modbus.DataType.COIL,
                                     start_address=0,
                                     quantity=1))

    event = await event_queue.get()
    assert_connections_event(event, 1)
    conn = event.payload.data[0]
    assert conn['type'] == 'SERIAL'

    await master.async_close()

    event = await event_queue.get()
    assert_connections_event(event, 0)

    assert event_queue.empty()

    await device.async_close()


async def test_keep_alive_timeout(conf, create_master_factory):
    keep_alive_timeout = 0.05
    conf = json.set_(conf, ['transport', 'keep_alive_timeout'],
                     keep_alive_timeout)
    event_queue = aio.Queue()
    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    event = await event_queue.get()
    assert_connections_event(event, 0)

    master = await create_master_factory()

    event = await event_queue.get()
    assert_connections_event(event, 1)

    # TODO check why keep_alive_timeout needs to be extended to 0.002
    await asyncio.sleep(keep_alive_timeout + 0.002)

    assert not event_queue.empty()
    event = event_queue.get_nowait()
    assert_connections_event(event, 0)

    assert event_queue.empty()

    await master.async_close()
    await device.async_close()


async def test_max_connections(slave_addr):
    conf = {'name': 'name',
            'modbus_type': 'TCP',
            'transport': {
                'type': 'TCP',
                'local_host': slave_addr.host,
                'local_port': slave_addr.port,
                'remote_hosts': None,
                'max_connections': 3,
                'keep_alive_timeout': None},
            'data': []}

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    masters = set()
    for _ in range(3):
        master = await modbus.create_tcp_master(
            modbus_type=modbus.ModbusType.TCP,
            addr=slave_addr)
        masters.add(master)

    # connection of 4th master should not succeed
    with pytest.raises(Exception):
        await modbus.create_tcp_master(
                modbus_type=modbus.ModbusType.TCP,
                addr=slave_addr)

    master = masters.pop()
    await master.async_close()

    # after one master disconnected, connection succeeds
    master = await modbus.create_tcp_master(
        modbus_type=modbus.modbus_type.TCP,
        addr=slave_addr)
    masters.add(master)

    while masters:
        master = masters.pop()
        await master.async_close()

    await device.async_close()


async def test_query(conf):
    conf = json.set_(conf,
                     'data',
                     [{'name': f'd{i}',
                       'device_id': 1,
                       'data_type': 'COIL',
                       'start_address': i,
                       'bit_offset': 1,
                       'bit_count': 1} for i in range(10)])

    queried_event_types = []

    def on_query(params):
        assert isinstance(params, hat.event.common.QueryLatestParams)
        queried_event_types.extend(params.event_types)
        return hat.event.common.QueryResult([], False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    for data_conf in conf['data']:
        event_type = (*event_type_prefix, 'system', 'data', data_conf['name'])
        assert any(hat.event.common.matches_query_type(event_type, query_type)
                   for query_type in queried_event_types)

    await device.async_close()


async def test_read_query_data(conf, create_master_factory):
    conf = json.set_(conf,
                     'data',
                     [{'name': f'd{i}',
                       'device_id': 1,
                       'data_type': 'COIL',
                       'start_address': i * 3,
                       'bit_offset': 0,
                       'bit_count': 3} for i in range(7)])

    queried_events = [
        create_event((*event_type_prefix, 'system', 'data', f'd{i}'),
                     {'value': i + 1})
        for i in range(7)]

    def on_query(params):
        return hat.event.common.QueryResult(queried_events, False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await create_master_factory()

    for i in range(7):
        read_value = await master.send(modbus.ReadReq(
            device_id=1,
            data_type=modbus.DataType.COIL,
            start_address=i * 3,
            quantity=3))
        read_value_int = int(''.join(str(v) for v in read_value), 2)
        assert read_value_int == queried_events[i].payload.data['value']

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'data_type, bit_count, read_quantity', [
        (modbus.DataType.COIL, 1, 1),
        (modbus.DataType.COIL, 3, 3),
        (modbus.DataType.DISCRETE_INPUT, 1, 1),
        (modbus.DataType.DISCRETE_INPUT, 3, 3),
        (modbus.DataType.HOLDING_REGISTER, 16, 1),
        (modbus.DataType.HOLDING_REGISTER, 8, 1),
        (modbus.DataType.HOLDING_REGISTER, 3 * 16, 3),
        (modbus.DataType.INPUT_REGISTER, 1, 1),
        (modbus.DataType.INPUT_REGISTER, 3 * 16, 3),
        ])
async def test_read_no_data(conf, create_master_factory,
                            data_type, bit_count, read_quantity):
    start_address = 0
    device_id = 1
    conf = json.set_(conf, 'data', [{'name': 'd1',
                                     'device_id': device_id,
                                     'data_type': data_type.name,
                                     'start_address': start_address,
                                     'bit_offset': 0,
                                     'bit_count': bit_count}])

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await create_master_factory()

    master_read_value = await master.send(modbus.ReadReq(
        device_id=device_id,
        data_type=data_type,
        start_address=start_address,
        quantity=read_quantity))
    assert master_read_value == read_quantity * [0]

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'data_type, start_address, bit_offset, bit_count, event_value, '
    'read_data_type, read_start_address, read_quantity, read_result', [
        (modbus.DataType.COIL, 0, 0, 1, 0,
         modbus.DataType.COIL, 0, 1, [0]),
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.COIL, 0, 1, [1]),
        (modbus.DataType.COIL, 0, 0, 2, 3,
         modbus.DataType.COIL, 0, 2, [1, 1]),
        (modbus.DataType.COIL, 0, 0, 2, 3,
         modbus.DataType.COIL, 1, 1, [1]),
        (modbus.DataType.COIL, 0, 3, 2, 3,
         modbus.DataType.COIL, 3, 2, [1, 1]),

        (modbus.DataType.DISCRETE_INPUT, 123, 2, 2, 3,
         modbus.DataType.DISCRETE_INPUT, 125, 2, [1, 1]),

        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0x1234,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0x1234]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 1,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [1]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 0, 2, [0x1234, 0x5678]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 1, 1, [0x5678]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 8, 0x12,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0x1200]),
        (modbus.DataType.HOLDING_REGISTER, 0, 8, 8, 0x12,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0x0012]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 1, 1,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0x8000]),
        (modbus.DataType.HOLDING_REGISTER, 0, 8, 16, 0x1234,
         modbus.DataType.HOLDING_REGISTER, 0, 2, [0x0012, 0x3400]),
        (modbus.DataType.HOLDING_REGISTER, 0, 8, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 1, 1, [0x3456]),
        (modbus.DataType.HOLDING_REGISTER, 0, 8, 32, 0x12345678,
         modbus.DataType.HOLDING_REGISTER, 1, 2, [0x3456, 0x7800]),

        (modbus.DataType.INPUT_REGISTER, 0, 0, 48, 0x123456789abc,
         modbus.DataType.INPUT_REGISTER, 0, 3, [0x1234, 0x5678, 0x9abc]),
        (modbus.DataType.INPUT_REGISTER, 0, 0, 64, 0x123456789abc,
         modbus.DataType.INPUT_REGISTER, 0, 3, [0, 0x1234, 0x5678]),

        # error due to read outside of configured memory
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.COIL, 1, 1, modbus.Error.INVALID_DATA_ADDRESS),
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.COIL, 0, 2, modbus.Error.INVALID_DATA_ADDRESS),
        (modbus.DataType.DISCRETE_INPUT, 123, 2, 2, 3,
         modbus.DataType.DISCRETE_INPUT, 124, 2,
         modbus.Error.INVALID_DATA_ADDRESS),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0x1234,
         modbus.DataType.HOLDING_REGISTER, 1, 1,
         modbus.Error.INVALID_DATA_ADDRESS),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0x1234,
         modbus.DataType.HOLDING_REGISTER, 0, 2,
         modbus.Error.INVALID_DATA_ADDRESS),

        # event with invalid value for configured data is ignored
        (modbus.DataType.COIL, 0, 0, 1, 3,
         modbus.DataType.COIL, 0, 1, [0]),
        (modbus.DataType.COIL, 0, 0, 3, 15,
         modbus.DataType.COIL, 0, 3, [0, 0, 0]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0x12345,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 32, 0x123456789,
         modbus.DataType.HOLDING_REGISTER, 0, 2, [0, 0]),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 8, 0x1234,
         modbus.DataType.HOLDING_REGISTER, 0, 1, [0]),

        # wrong read data type
        (modbus.DataType.COIL, 0, 0, 1, 1,
         modbus.DataType.DISCRETE_INPUT, 0, 1,
         modbus.Error.INVALID_DATA_ADDRESS),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0xFFFF,
         modbus.DataType.INPUT_REGISTER, 0, 1,
         modbus.Error.INVALID_DATA_ADDRESS),
        (modbus.DataType.HOLDING_REGISTER, 0, 0, 16, 0xFFFF,
         modbus.DataType.COIL, 0, 1, modbus.Error.INVALID_DATA_ADDRESS),
    ])
async def test_read(conf, create_master_factory, data_type,
                    start_address, bit_offset, bit_count, event_value,
                    read_data_type, read_start_address, read_quantity,
                    read_result):
    conf = json.set_(conf, 'data', [{'name': 'd1',
                                     'device_id': 1,
                                     'data_type': data_type.name,
                                     'start_address': start_address,
                                     'bit_offset': bit_offset,
                                     'bit_count': bit_count}])

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await create_master_factory()

    event = create_event((*event_type_prefix, 'system', 'data', 'd1'),
                         {'value': event_value})
    await aio.call(device.process_events, [event])

    master_read_result = await master.send(modbus.ReadReq(
        device_id=1,
        data_type=read_data_type,
        start_address=read_start_address,
        quantity=read_quantity))
    assert master_read_result == read_result

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'data_type, bit_count, write_values, gw_event_value', [
     (modbus.DataType.COIL, 1, [1], 1),
     (modbus.DataType.COIL, 3, [1, 1, 1], 7),
     (modbus.DataType.HOLDING_REGISTER, 16, [0], 0),
     (modbus.DataType.HOLDING_REGISTER, 16, [0xFFFF], 65535),
     (modbus.DataType.HOLDING_REGISTER, 32, [0x1234, 0x5678], 305419896),
     # data assigned to part of the registers and write to whole registers
     (modbus.DataType.HOLDING_REGISTER, 8, [0x1234], 0x12),
     (modbus.DataType.HOLDING_REGISTER, 24, [0x1234, 0x5678], 0x123456),
     ])
@pytest.mark.parametrize('system_event_success, write_response', [
    (True, modbus.Success()),
    (False, modbus.Error.FUNCTION_ERROR)])
async def test_write(conf, slave_addr, create_master_factory, data_type,
                     bit_count, write_values, gw_event_value,
                     system_event_success, write_response):
    event_queue = aio.Queue()
    device_id = 1
    data_name = 'd1'
    start_address = 0
    conf = json.set_(conf, 'data', [{'name': data_name,
                                     'device_id': device_id,
                                     'data_type': data_type.name,
                                     'start_address': start_address,
                                     'bit_offset': 0,
                                     'bit_count': bit_count}])

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    await event_queue.get()

    master = await create_master_factory()

    conns_event = await event_queue.get()
    assert_connections_event(conns_event, 1)
    connection_id = conns_event.payload.data[0]['connection_id']

    write_res_future = master.async_group.spawn(
        master.send, modbus.WriteReq(device_id=device_id,
                                     data_type=data_type,
                                     start_address=start_address,
                                     values=write_values))

    write_req_event = await event_queue.get()
    assert write_req_event.type == (*event_type_prefix, 'gateway', 'write')
    assert isinstance(write_req_event.payload.data['request_id'], str)
    assert write_req_event.payload.data['connection_id'] == connection_id
    assert len(write_req_event.payload.data['data']) == 1
    assert write_req_event.payload.data['data'][0] == {'name': data_name,
                                                       'value': gw_event_value}

    write_resp_event = create_event(
        (*event_type_prefix, 'system', 'write'),
        {'request_id': write_req_event.payload.data['request_id'],
         'success': system_event_success})
    await aio.call(device.process_events, [write_resp_event])

    write_res = await write_res_future
    assert write_res == write_response

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'data, write_type, write_address, write_values, event_data', [
        ([('d1', modbus.DataType.COIL, 0, 0, 1),
          ('d2', modbus.DataType.COIL, 1, 0, 1),
          ('d3', modbus.DataType.COIL, 2, 0, 1)],
         modbus.DataType.COIL, 0, [1, 0, 1],
         [{'name': 'd1', 'value': 1},
          {'name': 'd2', 'value': 0},
          {'name': 'd3', 'value': 1}]),

        ([('d1234', modbus.DataType.HOLDING_REGISTER, 0, 0, 16),
          ('d5678', modbus.DataType.HOLDING_REGISTER, 1, 0, 16)],
         modbus.DataType.HOLDING_REGISTER, 0, [0x1234, 0x5678],
         [{'name': 'd1234', 'value': 0x1234},
          {'name': 'd5678', 'value': 0x5678}]),

        ([('d1', modbus.DataType.HOLDING_REGISTER, 0, 0, 8),
          ('d2', modbus.DataType.HOLDING_REGISTER, 1, 8, 8)],
         modbus.DataType.HOLDING_REGISTER, 0, [0x1234, 0x5678],
         [{'name': 'd1', 'value': 0x12},
          {'name': 'd2', 'value': 0x78}]),
    ])
async def test_write_multiple(conf, slave_addr, create_master_factory, data,
                              write_type, write_address, write_values,
                              event_data):
    event_queue = aio.Queue()
    device_id = 1
    conf = json.set_(
        conf,
        'data',
        [{'name': name,
          'device_id': device_id,
          'data_type': data_type.name,
          'start_address': address,
          'bit_offset': bit_offset,
          'bit_count': bit_count}
         for name, data_type, address, bit_offset, bit_count in data])

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    await event_queue.get()

    master = await create_master_factory()

    conns_event = await event_queue.get()
    assert_connections_event(conns_event, 1)
    connection_id = conns_event.payload.data[0]['connection_id']

    master.async_group.spawn(
        master.send, modbus.WriteReq(device_id=device_id,
                                     data_type=write_type,
                                     start_address=write_address,
                                     values=write_values))

    write_req_event = await event_queue.get()
    assert write_req_event.type == (*event_type_prefix, 'gateway', 'write')
    assert isinstance(write_req_event.payload.data['request_id'], str)
    assert write_req_event.payload.data['connection_id'] == connection_id
    assert len(write_req_event.payload.data['data']) == len(event_data)
    assert write_req_event.payload.data['data'] == event_data

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'data, write_type, write_address, write_values', [
        ([('d1', modbus.DataType.COIL, 0, 0, 1)],
         modbus.DataType.COIL, 1, [1]),

        ([('d1', modbus.DataType.COIL, 0, 0, 1)],
         modbus.DataType.COIL, 1, [1, 0, 1]),

        ([('d1', modbus.DataType.COIL, 0, 0, 1),
          ('d3', modbus.DataType.COIL, 2, 0, 1)],
         modbus.DataType.COIL, 0, [1, 0, 1]),

        ([('d1', modbus.DataType.HOLDING_REGISTER, 0, 0, 16)],
         modbus.DataType.HOLDING_REGISTER, 1, [0x1234]),

        ([('d1', modbus.DataType.HOLDING_REGISTER, 2, 0, 16),
          ('d3', modbus.DataType.HOLDING_REGISTER, 2, 0, 16)],
         modbus.DataType.HOLDING_REGISTER, 3, [0, 1, 2]),

        ([('d1', modbus.DataType.HOLDING_REGISTER, 1, 0, 16)],
         modbus.DataType.HOLDING_REGISTER, 0, [0x1234, 0x5678]),

        ([('d1', modbus.DataType.HOLDING_REGISTER, 0, 0, 16)],
         modbus.DataType.COIL, 0, [1]),
    ])
async def test_write_error(conf, slave_addr, create_master_factory, data,
                           write_type, write_address, write_values):
    event_queue = aio.Queue()
    device_id = 1
    conf = json.set_(
        conf,
        'data',
        [{'name': name,
          'device_id': device_id,
          'data_type': data_type.name,
          'start_address': address,
          'bit_offset': bit_offset,
          'bit_count': bit_count}
         for name, data_type, address, bit_offset, bit_count in data])

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    await event_queue.get()

    master = await create_master_factory()

    conns_event = await event_queue.get()
    assert_connections_event(conns_event, 1)

    write_result = await master.send(modbus.WriteReq(
        device_id=device_id,
        data_type=write_type,
        start_address=write_address,
        values=write_values))
    assert write_result == modbus.Error.INVALID_DATA_ADDRESS

    assert event_queue.empty()

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'start_address, bit_offset, bit_count, write_address, and_mask, or_mask, '
    'queried_data_event_value, write_event_value', [
        # mask 1 bit at the register start
        # set 0
        (0, 0, 1, 0, 0, 0, 0, 0),
        (0, 0, 1, 0, 0, 0, 1, 0),
        (0, 0, 1, 0, 0x7fff, 0x7fff, 0, 0),
        (0, 0, 1, 0, 0x7fff, 0x7fff, 1, 0),
        # set 1
        (0, 0, 1, 0, 0, 0x8000, 0, 1),
        (0, 0, 1, 0, 0, 0x8000, 1, 1),
        (0, 0, 1, 0, 0x7fff, 0xffff, 0, 1),
        (0, 0, 1, 0, 0x7fff, 0xffff, 1, 1),
        # does not change -> no resulting write event
        (0, 0, 1, 0, 0x8000, 0, 0, None),
        (0, 0, 1, 0, 0x8000, 0, 1, None),
        (0, 0, 1, 0, 0xffff, 0x7fff, 0, None),
        (0, 0, 1, 0, 0xffff, 0x7fff, 1, None),
        # invert
        (0, 0, 1, 0, 0x8000, 0x8000, 0, 1),
        (0, 0, 1, 0, 0x8000, 0x8000, 1, 0),
        (0, 0, 1, 0, 0xffff, 0xffff, 0, 1),
        (0, 0, 1, 0, 0xffff, 0xffff, 1, 0),

        # mask 1 bit at the register end
        # set 0
        (0, 15, 1, 0, 0, 0, 0, 0),
        (0, 15, 1, 0, 0, 0, 1, 0),
        (0, 15, 1, 0, 0xfffe, 0xfffe, 0, 0),
        (0, 15, 1, 0, 0xfffe, 0xfffe, 1, 0),
        # set 1
        (0, 15, 1, 0, 0, 1, 0, 1),
        (0, 15, 1, 0, 0, 1, 1, 1),
        (0, 15, 1, 0, 0xfffe, 0xffff, 0, 1),
        (0, 15, 1, 0, 0xfffe, 0xffff, 1, 1),
        # does not change -> no resulting write event
        (0, 15, 1, 0, 1, 0, 0, None),
        (0, 15, 1, 0, 1, 0, 1, None),
        (0, 15, 1, 0, 0xffff, 0xfffe, 0, None),
        (0, 15, 1, 0, 0xffff, 0xfffe, 1, None),
        # invert
        (0, 15, 1, 0, 1, 1, 0, 1),
        (0, 15, 1, 0, 1, 1, 1, 0),
        (0, 15, 1, 0, 0xffff, 0xffff, 0, 1),
        (0, 15, 1, 0, 0xffff, 0xffff, 1, 0),

        # mask 2 bits in the middle of the register
        # set 0 on both bits
        (0, 7, 2, 0, 0, 0, 0, 0),
        (0, 7, 2, 0, 0, 0, 1, 0),
        (0, 7, 2, 0, 0, 0, 2, 0),
        (0, 7, 2, 0, 0, 0, 3, 0),
        # set 1 on both bits
        (0, 7, 2, 0, 0, 0x0180, 0, 3),
        (0, 7, 2, 0, 0, 0x0180, 1, 3),
        (0, 7, 2, 0, 0, 0x0180, 2, 3),
        (0, 7, 2, 0, 0, 0x0180, 3, 3),
        # does not change on both bits -> no resulting write event
        (0, 7, 2, 0, 0x0180, 0, 0, None),
        (0, 7, 2, 0, 0x0180, 0, 1, None),
        (0, 7, 2, 0, 0x0180, 0, 2, None),
        (0, 7, 2, 0, 0x0180, 0, 3, None),
        # invert on both bits
        (0, 7, 2, 0, 0x0180, 0x0180, 0, 3),
        (0, 7, 2, 0, 0x0180, 0x0180, 1, 2),
        (0, 7, 2, 0, 0x0180, 0x0180, 2, 1),
        (0, 7, 2, 0, 0x0180, 0x0180, 3, 0),

        # first bit set 1, second bit invert
        (0, 7, 2, 0, 0x0080, 0x0100, 1, 3),

        # end cases on masking entire register
        (0, 0, 16, 0, 0x1234, 0, 0, 0),
        (0, 0, 16, 0, 0x1234, 0, 0xffff, 0x1234),
        (0, 0, 16, 0, 0xffff, 0, 0x1234, 0x1234),
        (0, 0, 16, 0, 0, 0, 0x1234, 0),
        (0, 0, 16, 0, 0, 0x4321, 0x1234, 0x4321),
        (0, 0, 16, 0, 0, 0x4321, 0, 0x4321),
        ])
@pytest.mark.parametrize('system_event_success, master_write_result', [
    (True, modbus.Success()),
    (False, modbus.Error.FUNCTION_ERROR)])
async def test_write_mask(conf, slave_addr, create_master_factory,
                          start_address, bit_offset, bit_count, write_address,
                          and_mask, or_mask, queried_data_event_value,
                          write_event_value, system_event_success,
                          master_write_result):
    event_queue = aio.Queue()
    device_id = 1
    data_name = 'd1'
    data_type = modbus.DataType.HOLDING_REGISTER
    conf = json.set_(conf, 'data', [{'name': data_name,
                                     'device_id': device_id,
                                     'data_type': data_type.name,
                                     'start_address': start_address,
                                     'bit_offset': bit_offset,
                                     'bit_count': bit_count}])

    queried_event = create_event(
        (*event_type_prefix, 'system', 'data', 'd1'),
        {'value': queried_data_event_value})

    def on_query(params):
        return hat.event.common.QueryResult([queried_event], False)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait,
                                   query_cb=on_query)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    await event_queue.get()

    master = await create_master_factory()

    conns_event = await event_queue.get()
    assert_connections_event(conns_event, 1)
    connection_id = conns_event.payload.data[0]['connection_id']

    write_mask_res_future = master.async_group.spawn(
        master.send, modbus.WriteMaskReq(device_id=device_id,
                                         address=write_address,
                                         and_mask=and_mask,
                                         or_mask=or_mask))

    if write_event_value is None:
        write_res = await write_mask_res_future
        assert write_res == modbus.Success()
        assert event_queue.empty()

        await master.async_close()
        await device.async_close()
        return

    write_req_event = await event_queue.get()
    assert write_req_event.type == (*event_type_prefix, 'gateway', 'write')
    assert isinstance(write_req_event.payload.data['request_id'], str)
    assert write_req_event.payload.data['connection_id'] == connection_id
    assert len(write_req_event.payload.data['data']) == 1
    assert write_req_event.payload.data[
        'data'][0]['value'] == write_event_value

    write_mask_resp_event = create_event(
        (*event_type_prefix, 'system', 'write'),
        {'request_id': write_req_event.payload.data['request_id'],
         'success': system_event_success})
    await aio.call(device.process_events, [write_mask_resp_event])

    write_res = await write_mask_res_future
    assert write_res == master_write_result

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'data, write_address, and_mask, or_mask, event_data',
    [
     # 8 data of 1 bit, every second changed, remaining 8 bits irrelevant
     ([('d1', 0, 0, 1),
       ('d2', 0, 1, 1),
       ('d3', 0, 2, 1),
       ('d4', 0, 3, 1),
       ('d5', 0, 4, 1),
       ('d6', 0, 5, 1),
       ('d7', 0, 6, 1),
       ('d8', 0, 7, 1)], 0, 0x55ff, 0x88ff, [
       {'name': 'd1',
        'value': 1},
       {'name': 'd3',
        'value': 0},
       {'name': 'd5',
        'value': 1},
       {'name': 'd7',
        'value': 0}]),
     # 8 data of 1 bit, writing "no change" -> no event
     ([('d1', 0, 0, 1),
       ('d2', 0, 1, 1),
       ('d3', 0, 2, 1),
       ('d4', 0, 3, 1),
       ('d5', 0, 4, 1),
       ('d6', 0, 5, 1),
       ('d7', 0, 6, 1),
       ('d8', 0, 7, 1)], 0, 0xffff, 0, None),
     # 8 data of 2 bits
     ([('d1', 0, 0, 2),
       ('d2', 0, 2, 2),
       ('d3', 0, 4, 2),
       ('d4', 0, 6, 2),
       ('d5', 0, 8, 2),
       ('d6', 0, 10, 2),
       ('d7', 0, 12, 2),
       ('d8', 0, 14, 2)], 0, 0xcc55, 0xcc88, [
       {'name': 'd2',
        'value': 2},
       {'name': 'd4',
        'value': 2},
       {'name': 'd5',
        'value': 2},
       {'name': 'd6',
        'value': 0},
       {'name': 'd7',
        'value': 2},
       {'name': 'd8',
        'value': 0}]),
     # 3 data of 1 register
     ([('d1', 0, 0, 16),
       ('d2', 1, 0, 16),
       ('d3', 2, 0, 16)], 0, 0x0000ffffffff, 0xffff0000ffff, [
       {'name': 'd1',
        'value': 65535},
       {'name': 'd3',
        'value': 65535}]),
     # 2 data 1 register long, each offset by 8, written only to one
     ([('d1', 0, 8, 16),
       ('d2', 1, 8, 16)], 0, 0xff00ff, 0x00ff00, [
       {'name': 'd1',
        'value': 65280}]),
     # 2 data 1 register long, each offset by 8, written only to 3rd register
     ([('d1', 0, 8, 16),
       ('d2', 1, 8, 16)], 2, 0xffff, 0xffff, [
       {'name': 'd2',
        'value': 255}]),
     # 2 data, writting "no change" -> no event
     ([('d1', 0, 8, 16),
       ('d2', 1, 8, 16)], 2, 0xffffffffffff, 0, None),
     # 1 data 1 register long, writting to two registers
     ([('d1', 1, 0, 16)], 0, 0, 0x12345678, [
       {'name': 'd2',
        'value': 22136}]),
     # 1 data offset by 15, writting 1 to first register changes its 1st bit
     ([('d1', 0, 15, 16)], 0, 0, 0xffff, [
       {'name': 'd2',
        'value': 0x8000}]),
    ])
async def test_write_mask_multiple(conf, slave_addr, create_master_factory,
                                   data, write_address, and_mask, or_mask,
                                   event_data):
    event_queue = aio.Queue()
    device_id = 1
    data_type = modbus.DataType.HOLDING_REGISTER
    conf = json.set_(
        conf,
        'data',
        [{'name': name,
          'device_id': device_id,
          'data_type': data_type.name,
          'start_address': address,
          'bit_offset': bit_offset,
          'bit_count': bit_count}
         for name, address, bit_offset, bit_count in data])

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    await event_queue.get()

    master = await create_master_factory()

    conns_event = await event_queue.get()
    assert_connections_event(conns_event, 1)
    connection_id = conns_event.payload.data[0]['connection_id']

    write_res_future = master.async_group.spawn(
        master.send, modbus.WriteMaskReq(device_id=device_id,
                                         address=write_address,
                                         and_mask=and_mask,
                                         or_mask=or_mask))

    if event_data is None:
        write_res = await write_res_future
        assert write_res == modbus.Success()
        assert event_queue.empty()

        await master.async_close()
        await device.async_close()
        return

    write_req_event = await event_queue.get()
    assert write_req_event.type == (*event_type_prefix, 'gateway', 'write')
    assert isinstance(write_req_event.payload.data['request_id'], str)
    assert write_req_event.payload.data['connection_id'] == connection_id
    assert len(write_req_event.payload.data['data']) == len(event_data)
    assert write_req_event.payload.data['data'] == event_data

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize(
    'start_address, bit_offset, bit_count, write_address, and_mask, or_mask, ',
    [
        (0, 0, 16, 1, 0x1234, 0),
        (123, 0, 16, 0, 0x1234, 0),
        (0, 16, 16, 0, 0x1234, 0),
    ])
async def test_write_mask_error(conf, slave_addr, create_master_factory,
                                start_address, bit_offset, bit_count,
                                write_address, and_mask, or_mask):
    event_queue = aio.Queue()
    device_id = 1
    data_name = 'd1'
    data_type = modbus.DataType.HOLDING_REGISTER
    conf = json.set_(conf, 'data', [{'name': data_name,
                                     'device_id': device_id,
                                     'data_type': data_type.name,
                                     'start_address': start_address,
                                     'bit_offset': bit_offset,
                                     'bit_count': bit_count}])

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await create_master_factory()

    conns_event = await event_queue.get()
    assert_connections_event(conns_event, 1)

    write_mask_res = await master.send(
        modbus.WriteMaskReq(device_id=device_id,
                            address=write_address,
                            and_mask=and_mask,
                            or_mask=or_mask))
    assert write_mask_res == modbus.Error.INVALID_DATA_ADDRESS

    assert event_queue.empty()

    await master.async_close()
    await device.async_close()


@pytest.mark.parametrize('request', [
    modbus.ReadReq(device_id=0,
                   data_type=modbus.DataType.HOLDING_REGISTER,
                   start_address=0,
                   quantity=1),
    modbus.ReadReq(device_id=3,
                   data_type=modbus.DataType.HOLDING_REGISTER,
                   start_address=0,
                   quantity=1),
    modbus.WriteReq(device_id=13,
                    data_type=modbus.DataType.HOLDING_REGISTER,
                    start_address=0,
                    values=[1, 2, 3]),
    modbus.WriteMaskReq(device_id=345,
                        address=0,
                        and_mask=0,
                        or_mask=0)])
async def test_invalid_device_id(conf, create_master_factory, request):
    conf = json.set_(conf, 'data', [{'name': 'd1',
                                     'device_id': 1,
                                     'data_type': 'HOLDING_REGISTER',
                                     'start_address': 0,
                                     'bit_offset': 0,
                                     'bit_count': 1}])

    eventer_client = EventerClient()
    device = await aio.call(info.create, conf, eventer_client,
                            event_type_prefix)

    master = await create_master_factory()

    if conf['transport']['type'] == 'TCP':
        read_res = await master.send(request)
        assert read_res == modbus.Error.INVALID_DATA_ADDRESS

    elif conf['transport']['type'] == 'SERIAL':
        with pytest.raises(asyncio.TimeoutError):
            await aio.wait_for(master.send(request), timeout=0.05)

    await master.async_close()
    await device.async_close()
