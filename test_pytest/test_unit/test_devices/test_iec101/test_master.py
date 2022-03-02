import collections
import contextlib
import datetime
import itertools
import math

import pytest

from hat import aio
from hat import util
from hat.drivers import serial
from hat.drivers.iec60870 import iec101
from hat.drivers.iec60870 import link
from hat.gateway import common
from hat.gateway.devices.iec101 import master
import hat.event.common


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name, master.device_type, device_name)

default_time = iec101.time_from_datetime(
    datetime.datetime.now(datetime.timezone.utc))
default_indication_quality = iec101.IndicationQuality(False, True, False, True)
default_measurement_quality = iec101.MeasurementQuality(False, True, False,
                                                        True, False)
default_counter_quality = iec101.CounterQuality(False, True, False, True)
default_protection_quality = iec101.ProtectionQuality(False, True, False, True,
                                                      False)


class EventClient(common.DeviceEventClient):

    def __init__(self, query_result=[]):
        self._query_result = query_result
        self._receive_queue = aio.Queue()
        self._register_queue = aio.Queue()
        self._async_group = aio.Group()
        self._async_group.spawn(aio.call_on_cancel, self._receive_queue.close)
        self._async_group.spawn(aio.call_on_cancel, self._register_queue.close)

    @property
    def async_group(self):
        return self._async_group

    @property
    def receive_queue(self):
        return self._receive_queue

    @property
    def register_queue(self):
        return self._register_queue

    async def receive(self):
        try:
            return await self._receive_queue.get()
        except aio.QueueClosedError:
            raise ConnectionError()

    def register(self, events):
        try:
            for event in events:
                self._register_queue.put_nowait(event)
        except aio.QueueClosedError:
            raise ConnectionError()

    async def register_with_response(self, events):
        raise Exception('should not be used')

    async def query(self, data):
        return self._query_result


def get_conf(remote_addresses=[],
             asdu_address_size='TWO',
             reconnect_delay=0.01,
             response_timeout=0.1,
             send_retry_count=1,
             poll_class1_delay=1,
             poll_class2_delay=None,
             time_sync_delay=None):
    return {'port': '/dev/ttyS0',
            'baudrate': 9600,
            'bytesize': 'EIGHTBITS',
            'parity': 'NONE',
            'stopbits': 'ONE',
            'flow_control': {'xonxoff': False,
                             'rtscts': False,
                             'dsrdtr': False},
            'silent_interval': 0.001,
            'device_address_size': 'ONE',
            'cause_size': 'TWO',
            'asdu_address_size': asdu_address_size,
            'io_address_size': 'THREE',
            'reconnect_delay': reconnect_delay,
            'remote_devices': [{'address': address,
                                'response_timeout': response_timeout,
                                'send_retry_count': send_retry_count,
                                'poll_class1_delay': poll_class1_delay,
                                'poll_class2_delay': poll_class2_delay,
                                'reconnect_delay': reconnect_delay,
                                'time_sync_delay': time_sync_delay}
                               for address in remote_addresses]}


async def create_slave(conf, connection_cb):

    async def on_connection(conn):
        conn = iec101.Connection(
            conn=conn,
            cause_size=iec101.CauseSize[conf['cause_size']],
            asdu_address_size=iec101.AsduAddressSize[conf['asdu_address_size']],  # NOQA
            io_address_size=iec101.IoAddressSize[conf['io_address_size']])
        await aio.call(connection_cb, conn)

    return await link.unbalanced.create_slave(
        port=conf['port'],
        addrs=[i['address'] for i in conf['remote_devices']],
        connection_cb=on_connection,
        baudrate=conf['baudrate'],
        bytesize=serial.ByteSize[conf['bytesize']],
        parity=serial.Parity[conf['parity']],
        stopbits=serial.StopBits[conf['stopbits']],
        xonxoff=conf['flow_control']['xonxoff'],
        rtscts=conf['flow_control']['rtscts'],
        dsrdtr=conf['flow_control']['dsrdtr'],
        silent_interval=conf['silent_interval'],
        address_size=link.common.AddressSize[conf['device_address_size']],
        keep_alive_timeout=10)


def time_to_event_timestamp(time):
    if time is None:
        return None
    return hat.event.common.timestamp_from_datetime(
        iec101.time_to_datetime(time))


def time_from_event_timestamp(timestamp):
    if timestamp is None:
        return None
    return iec101.time_from_datetime(
        hat.event.common.timestamp_to_datetime(timestamp))


def assert_float_equal(value1, value2):
    assert math.isclose(value1, value2, rel_tol=1e-3)


def assert_time_equal(time1, time2):
    if time1 is None and time2 is None:
        return
    dt = abs(iec101.time_to_datetime(time1) - iec101.time_to_datetime(time2))
    assert dt < datetime.timedelta(seconds=1)


def assert_status_event(event, status, address=None):
    if address is None:
        assert event.event_type == (*event_type_prefix, 'gateway', 'status')
    else:
        assert event.event_type == (*event_type_prefix, 'gateway',
                                    'remote_device', str(address), 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def assert_data_event(event, address, data_type, asdu_address, io_address,
                      time, cause, payload):
    assert event.event_type == (*event_type_prefix, 'gateway', 'remote_device',
                                str(address), 'data', data_type,
                                str(asdu_address), str(io_address))

    assert_time_equal(time, time_from_event_timestamp(event.source_timestamp))

    payload = {'cause': ('INTERROGATED'
                         if cause.name.startswith('INTERROGATED_')
                         else cause.name),
               **payload}

    for key in {*payload.keys(), *event.payload.data.keys()}:
        if data_type in ('normalized', 'floating') and key == 'value':
            assert_float_equal(payload[key], event.payload.data[key])
        else:
            assert payload[key] == event.payload.data[key]


def assert_command_event(event, address, command_type, asdu_address,
                         io_address, cause, success, payload):
    assert event.event_type == (*event_type_prefix, 'gateway', 'remote_device',
                                str(address), 'command', command_type,
                                str(asdu_address), str(io_address))
    assert event.source_timestamp is None

    payload = {'cause': cause.name,
               'success': success,
               **payload}

    for key in {*payload.keys(), *event.payload.data.keys()}:
        if command_type in ('normalized', 'floating') and key == 'value':
            assert_float_equal(payload[key], event.payload.data[key])
        else:
            assert payload[key] == event.payload.data[key]


def assert_interrogation_event(event, address, asdu_address, cause,
                               interrogation_request):
    assert event.event_type == (*event_type_prefix, 'gateway', 'remote_device',
                                str(address), 'interrogation',
                                str(asdu_address))
    assert event.source_timestamp is None
    assert event.payload.data == {'request': interrogation_request}


def assert_counter_interrogation_event(event, address, asdu_address, cause,
                                       interrogation_request, freeze):
    assert event.event_type == (*event_type_prefix, 'gateway', 'remote_device',
                                str(address), 'counter_interrogation',
                                str(asdu_address))
    assert event.source_timestamp is None
    assert event.payload.data == {'request': interrogation_request,
                                  'freeze': freeze.name}


def assert_msg_equal(msg1, msg2):
    assert type(msg1) == type(msg2)
    assert msg1.is_test == msg2.is_test
    assert msg1.originator_address == msg2.originator_address
    assert msg1.asdu_address == msg2.asdu_address
    assert msg1.cause == msg2.cause

    if isinstance(msg1, iec101.CommandMsg):
        assert msg1.io_address == msg2.io_address
        assert msg1.is_negative_confirm == msg2.is_negative_confirm
        assert type(msg1.command) == type(msg2.command)  # NOQA

        if (isinstance(msg1.command, iec101.NormalizedCommand) or
                isinstance(msg1.command, iec101.FloatingCommand)):
            assert_float_equal(msg1.command.value.value,
                               msg2.command.value.value)
        else:
            assert msg1.command.value == msg2.command.value

        if hasattr(msg1.command, 'select'):
            assert msg1.command.select == msg2.command.select

        if hasattr(msg1.command, 'qualifier'):
            assert msg1.command.select == msg2.command.select

    elif isinstance(msg1, iec101.InterrogationMsg):
        assert msg1.request == msg2.request

    elif isinstance(msg1, iec101.CounterInterrogationMsg):
        assert msg1.request == msg2.request
        assert msg1.freeze == msg2.freeze

    else:
        raise ValueError('message type not supported')


async def wait_remote_device_connected_event(event_client, address):

    def check(event):
        return (event.event_type == (*event_type_prefix, 'gateway',
                                     'remote_device', str(address),
                                     'status') and
                event.payload.data == 'CONNECTED')

    await aio.first(event_client.register_queue, check)


@pytest.fixture
def create_event():
    instance_ids = itertools.count(0)

    def create_event(event_type, payload_data):
        event_id = hat.event.common.EventId(1, next(instance_ids))
        payload = hat.event.common.EventPayload(
            hat.event.common.EventPayloadType.JSON, payload_data)
        event = hat.event.common.Event(event_id=event_id,
                                       event_type=event_type,
                                       timestamp=hat.event.common.now(),
                                       source_timestamp=None,
                                       payload=payload)
        return event

    return create_event


@pytest.fixture
def create_enable_event(create_event):

    def create_enable_event(address, enable):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(address), 'enable'),
                            enable)

    return create_enable_event


@pytest.fixture
def create_interrogation_event(create_event):

    def create_interrogation_event(address, asdu_addr, request):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(address), 'interrogation', str(asdu_addr)),
                            {'request': request})

    return create_interrogation_event


@pytest.fixture
def create_counter_interrogation_event(create_event):

    def create_counter_interrogation_event(address, asdu_addr, request,
                                           freeze):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(address), 'counter_interrogation',
                             str(asdu_addr)),
                            {'request': request,
                             'freeze': freeze})

    return create_counter_interrogation_event


@pytest.fixture
def create_command_event(create_event):

    def create_command_event(address, command_type, asdu_addr, io_address,
                             payload):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(address), 'command', command_type,
                             str(asdu_addr), str(io_address)),
                            payload)

    return create_command_event


@pytest.fixture
async def serial_conns(monkeypatch):
    valid_args = None
    conns = collections.deque()

    async def create(port: str, *,
                     baudrate: int = 9600,
                     bytesize: serial.ByteSize = serial.ByteSize.EIGHTBITS,
                     parity: serial.Parity = serial.Parity.NONE,
                     stopbits: serial.StopBits = serial.StopBits.ONE,
                     xonxoff: bool = False,
                     rtscts: bool = False,
                     dsrdtr: bool = False,
                     silent_interval: float = 0):
        nonlocal valid_args
        args = {'port': port,
                'baudrate': baudrate,
                'bytesize': bytesize,
                'parity': parity,
                'stopbits': stopbits,
                'xonxoff': xonxoff,
                'rtscts': rtscts,
                'dsrdtr': dsrdtr}
        if valid_args is None:
            valid_args = args
        assert valid_args == args
        return Connection()

    class Connection(aio.Resource):

        def __init__(self):
            self._async_group = aio.Group()
            self._data = aio.Queue()
            self._async_group.spawn(aio.call_on_cancel, self._data.close)
            conns.append(self)

        @property
        def async_group(self):
            return self._async_group

        async def read(self, size):
            data = collections.deque()
            for _ in range(size):
                try:
                    data.append(await self._data.get())
                except aio.QueueClosedError:
                    raise ConnectionError()
            return bytes(data)

        async def write(self, data):
            if not self.is_open:
                raise ConnectionError()
            for conn in conns:
                if conn is self or not conn.is_open:
                    continue
                for i in data:
                    conn._data.put_nowait(i)

    monkeypatch.setattr(hat.drivers.serial, 'create', create)
    return conns


@pytest.fixture
async def create_event_client_connection_pair(serial_conns,
                                              create_enable_event):

    @contextlib.asynccontextmanager
    async def create_event_client_connection_pair(address):
        query_result = [create_enable_event(address, True)]
        event_client = EventClient(query_result)
        conf = get_conf([address])
        conn_queue = aio.Queue()
        slave = await create_slave(conf, conn_queue.put_nowait)
        device = await aio.call(master.create, conf, event_client,
                                event_type_prefix)
        conn = await conn_queue.get()

        yield event_client, conn

        await device.async_close()
        await slave.async_close()
        await event_client.async_close()

    return create_event_client_connection_pair


@pytest.mark.parametrize("conf", [
    get_conf(remote_addresses=[1, 2, 3])
])
def test_valid_conf(conf):
    master.json_schema_repo.validate(master.json_schema_id, conf)


async def test_create(serial_conns):
    event_client = EventClient()
    conf = get_conf()
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    assert device.is_open

    await device.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("conn_count", [0, 1, 5])
async def test_connect(serial_conns, create_enable_event, conn_count):
    query_result = [create_enable_event(i, True)
                    for i in range(conn_count)]
    event_client = EventClient(query_result)
    conf = get_conf(range(conn_count))
    conn_queue = aio.Queue()
    slave = await create_slave(conf, conn_queue.put_nowait)
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    for _ in range(conn_count):
        conn = await conn_queue.get()
        assert conn.is_open

    assert conn_queue.empty()

    await device.async_close()
    await slave.async_close()
    await event_client.async_close()


async def test_status(serial_conns):
    event_client = EventClient()
    conf = get_conf()
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    assert event_client.register_queue.empty()

    for conn in serial_conns:
        conn.close()

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    await device.async_close()

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    assert event_client.register_queue.empty()

    await event_client.async_close()


@pytest.mark.parametrize("address", [0])
async def test_enable_remote_device(serial_conns, create_enable_event,
                                    address):
    event_client = EventClient()
    conf = get_conf([address])
    conn_queue = aio.Queue()
    slave = await create_slave(conf, conn_queue.put_nowait)
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    assert event_client.register_queue.empty()

    event = create_enable_event(address, True)
    event_client.receive_queue.put_nowait([event])

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING', address)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED', address)

    assert event_client.register_queue.empty()

    event = create_enable_event(address, False)
    event_client.receive_queue.put_nowait([event])

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED', address)

    assert event_client.register_queue.empty()

    event = create_enable_event(address, True)
    event_client.receive_queue.put_nowait([event])

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING', address)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED', address)

    assert event_client.register_queue.empty()

    await device.async_close()

    events = [await event_client.register_queue.get(),
              await event_client.register_queue.get()]

    event = util.first(events, lambda i: 'remote_device' in i.event_type)
    assert_status_event(event, 'DISCONNECTED', address)

    event = util.first(events, lambda i: 'remote_device' not in i.event_type)
    assert_status_event(event, 'DISCONNECTED')

    assert event_client.register_queue.empty()

    await slave.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address_size, asdu_address", [
    ('ONE', 0xFF),
    ('TWO', 0xFFFF)
])
async def test_time_sync(serial_conns, create_enable_event, address,
                         asdu_address_size, asdu_address):
    last_datetime = datetime.datetime.now(datetime.timezone.utc)

    query_result = [create_enable_event(address, True)]
    event_client = EventClient(query_result)
    conf = get_conf([address],
                    asdu_address_size=asdu_address_size,
                    time_sync_delay=0.001)
    conn_queue = aio.Queue()
    slave = await create_slave(conf, conn_queue.put_nowait)
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)
    conn = await conn_queue.get()

    for _ in range(10):
        msgs = await conn.receive()
        assert len(msgs) == 1
        msg = msgs[0]

        assert isinstance(msg, iec101.ClockSyncMsg)
        assert msg.is_test is False
        assert msg.originator_address == 0
        assert msg.asdu_address == asdu_address
        assert msg.cause == iec101.ActivationReqCause.ACTIVATION

        new_datetime = iec101.time_to_datetime(msg.time)
        assert new_datetime >= last_datetime
        last_datetime = new_datetime

    await device.async_close()
    await slave.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("cause", list(iec101.CommandReqCause))
@pytest.mark.parametrize("command, command_type, payload", [
    (iec101.SingleCommand(value=iec101.SingleValue.ON,
                          select=False,
                          qualifier=0),
     'single',
     {'value': 'ON',
      'select': False,
      'qualifier': 0}),

    (iec101.DoubleCommand(value=iec101.DoubleValue.OFF,
                          select=True,
                          qualifier=1),
     'double',
     {'value': 'OFF',
      'select': True,
      'qualifier': 1}),

    (iec101.RegulatingCommand(value=iec101.RegulatingValue.HIGHER,
                              select=False,
                              qualifier=2),
     'regulating',
     {'value': 'HIGHER',
      'select': False,
      'qualifier': 2}),

    (iec101.NormalizedCommand(value=iec101.NormalizedValue(0.5),
                              select=True),
     'normalized',
     {'value': 0.5,
      'select': True}),

    (iec101.ScaledCommand(value=iec101.ScaledValue(42),
                          select=False),
     'scaled',
     {'value': 42,
      'select': False}),

    (iec101.FloatingCommand(value=iec101.FloatingValue(42.5),
                            select=True),
     'floating',
     {'value': 42.5,
      'select': True}),

    (iec101.BitstringCommand(value=iec101.BitstringValue(b'\x01\x02\x03\x04')),
     'bitstring',
     {'value': [1, 2, 3, 4]}),
])
async def test_command_request(create_event_client_connection_pair,
                               create_command_event, address, asdu_address,
                               io_address, cause, command, command_type,
                               payload):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        event = create_command_event(address, command_type, asdu_address,
                                     io_address,
                                     {'cause': cause.name, **payload})
        event_client.receive_queue.put_nowait([event])

        msgs = await conn.receive()
        assert len(msgs) == 1
        msg = msgs[0]
        assert_msg_equal(msg, iec101.CommandMsg(is_test=False,
                                                originator_address=0,
                                                asdu_address=asdu_address,
                                                io_address=io_address,
                                                command=command,
                                                is_negative_confirm=False,
                                                cause=cause))


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("interrogation_request", [42])
async def test_interrogation_request(create_event_client_connection_pair,
                                     create_interrogation_event,
                                     address, asdu_address,
                                     interrogation_request):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        event = create_interrogation_event(address, asdu_address,
                                           interrogation_request)
        event_client.receive_queue.put_nowait([event])

        msgs = await conn.receive()
        assert len(msgs) == 1
        msg = msgs[0]
        assert_msg_equal(msg, iec101.InterrogationMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu_address,
            request=interrogation_request,
            cause=iec101.CommandReqCause.ACTIVATION))


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("interrogation_request", [42])
@pytest.mark.parametrize("freeze", list(iec101.FreezeCode))
async def test_counter_interrogation_request(
        create_event_client_connection_pair,
        create_counter_interrogation_event,
        address, asdu_address, interrogation_request, freeze):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        event = create_counter_interrogation_event(
            address, asdu_address, interrogation_request, freeze.name)
        event_client.receive_queue.put_nowait([event])

        msgs = await conn.receive()
        assert len(msgs) == 1
        msg = msgs[0]
        assert_msg_equal(msg, iec101.CounterInterrogationMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu_address,
            request=interrogation_request,
            freeze=freeze,
            cause=iec101.CommandReqCause.ACTIVATION))


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("cause", [
    i for i in iec101.DataResCause
    if not (i.name.startswith('INTERROGATED_GROUP') or
            i.name.startswith('INTERROGATED_COUNTER0'))
])
@pytest.mark.parametrize("data, data_type, payload", [
    (iec101.SingleData(value=iec101.SingleValue.ON,
                       quality=default_indication_quality),
     'single',
     {'value': 'ON',
      'quality': default_indication_quality._asdict()}),

    (iec101.DoubleData(value=iec101.DoubleValue.OFF,
                       quality=default_indication_quality),
     'double',
     {'value': 'OFF',
      'quality': default_indication_quality._asdict()}),

    (iec101.StepPositionData(value=iec101.StepPositionValue(42, False),
                             quality=default_measurement_quality),
     'step_position',
     {'value': {'value': 42,
                'transient': False},
      'quality': default_measurement_quality._asdict()}),

    (iec101.BitstringData(value=iec101.BitstringValue(b'\x01\x02\x03\x04'),
                          quality=default_measurement_quality),
     'bitstring',
     {'value': [1, 2, 3, 4],
      'quality': default_measurement_quality._asdict()}),

    (iec101.NormalizedData(value=iec101.NormalizedValue(0.5),
                           quality=default_measurement_quality),
     'normalized',
     {'value': 0.5,
      'quality': default_measurement_quality._asdict()}),

    (iec101.ScaledData(value=iec101.ScaledValue(42),
                       quality=default_measurement_quality),
     'scaled',
     {'value': 42,
      'quality': default_measurement_quality._asdict()}),

    (iec101.FloatingData(value=iec101.FloatingValue(42.5),
                         quality=default_measurement_quality),
     'floating',
     {'value': 42.5,
      'quality': default_measurement_quality._asdict()}),

    (iec101.BinaryCounterData(value=iec101.BinaryCounterValue(123),
                              quality=default_counter_quality),
     'binary_counter',
     {'value': 123,
      'quality': default_counter_quality._asdict()}),

    (iec101.ProtectionData(value=iec101.ProtectionValue.ON,
                           quality=default_protection_quality,
                           elapsed_time=42),
     'protection',
     {'value': 'ON',
      'quality': default_protection_quality._asdict(),
      'elapsed_time': 42}),

    (iec101.ProtectionStartData(value=iec101.ProtectionStartValue(True, False,
                                                                  True, False,
                                                                  True, False),
                                quality=default_protection_quality,
                                duration_time=42),
     'protection_start',
     {'value': {'general': True,
                'l1': False,
                'l2': True,
                'l3': False,
                'ie': True,
                'reverse': False},
      'quality': default_protection_quality._asdict(),
      'duration_time': 42}),

    (iec101.ProtectionCommandData(value=iec101.ProtectionCommandValue(True,
                                                                      False,
                                                                      True,
                                                                      False),
                                  quality=default_protection_quality,
                                  operating_time=42),
     'protection_command',
     {'value': {'general': True,
                'l1': False,
                'l2': True,
                'l3': False},
      'quality': default_protection_quality._asdict(),
      'operating_time': 42}),

    (iec101.StatusData(value=iec101.StatusValue([True, False] * 8,
                                                [False, True] * 8),
                       quality=default_measurement_quality),
     'status',
     {'value': {'value': [True, False] * 8,
                'change': [False, True] * 8},
      'quality': default_measurement_quality._asdict()}),
])
async def test_data_response(create_event_client_connection_pair, address,
                             asdu_address, io_address, time, cause, data,
                             data_type, payload):
    if data_type in ('protection', 'protection_start', 'protection_command'):
        if time is None:
            return
    elif data_type == 'status':
        if time is not None:
            return

    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        msg = iec101.DataMsg(is_test=False,
                             originator_address=0,
                             asdu_address=asdu_address,
                             io_address=io_address,
                             data=data,
                             time=time,
                             cause=cause)
        await conn.send([msg])

        event = await event_client.register_queue.get()
        assert_data_event(event, address, data_type, asdu_address, io_address,
                          time, cause, payload)


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("cause", list(iec101.CommandResCause))
@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("command, command_type, payload", [
    (iec101.SingleCommand(value=iec101.SingleValue.ON,
                          select=False,
                          qualifier=0),
     'single',
     {'value': 'ON',
      'select': False,
      'qualifier': 0}),

    (iec101.DoubleCommand(value=iec101.DoubleValue.OFF,
                          select=True,
                          qualifier=0),
     'double',
     {'value': 'OFF',
      'select': True,
      'qualifier': 0}),

    (iec101.RegulatingCommand(value=iec101.RegulatingValue.LOWER,
                              select=False,
                              qualifier=0),
     'regulating',
     {'value': 'LOWER',
      'select': False,
      'qualifier': 0}),

    (iec101.NormalizedCommand(value=iec101.NormalizedValue(0.5),
                              select=True),
     'normalized',
     {'value': 0.5,
      'select': True}),

    (iec101.ScaledCommand(value=iec101.ScaledValue(42),
                          select=False),
     'scaled',
     {'value': 42,
      'select': False}),

    (iec101.FloatingCommand(value=iec101.FloatingValue(42.5),
                            select=True),
     'floating',
     {'value': 42.5,
      'select': True}),

    (iec101.BitstringCommand(value=iec101.BitstringValue(b'\x04\x03\x02\x01')),
     'bitstring',
     {'value': [4, 3, 2, 1]}),
])
async def test_command_response(create_event_client_connection_pair, address,
                                asdu_address, io_address, cause, success,
                                command, command_type, payload):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        msg = iec101.CommandMsg(is_test=False,
                                originator_address=0,
                                asdu_address=asdu_address,
                                io_address=io_address,
                                command=command,
                                is_negative_confirm=not success,
                                cause=cause)
        await conn.send([msg])

        event = await event_client.register_queue.get()
        assert_command_event(event, address, command_type, asdu_address,
                             io_address, cause, success, payload)


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("cause", list(iec101.CommandResCause))
@pytest.mark.parametrize("interrogation_request", [42])
async def test_interrogation_response(create_event_client_connection_pair,
                                      address, asdu_address, cause,
                                      interrogation_request):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        msg = iec101.InterrogationMsg(is_test=False,
                                      originator_address=0,
                                      asdu_address=asdu_address,
                                      request=interrogation_request,
                                      cause=cause)
        await conn.send([msg])

        event = await event_client.register_queue.get()
        assert_interrogation_event(event, address, asdu_address, cause,
                                   interrogation_request)


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("cause", list(iec101.CommandResCause))
@pytest.mark.parametrize("interrogation_request", [42])
@pytest.mark.parametrize("freeze", list(iec101.FreezeCode))
async def test_counter_interrogation_response(
        create_event_client_connection_pair, address, asdu_address, cause,
        interrogation_request, freeze):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        msg = iec101.CounterInterrogationMsg(is_test=False,
                                             originator_address=0,
                                             asdu_address=asdu_address,
                                             request=interrogation_request,
                                             freeze=freeze,
                                             cause=cause)
        await conn.send([msg])

        event = await event_client.register_queue.get()
        assert_counter_interrogation_event(event, address, asdu_address, cause,
                                           interrogation_request, freeze)
