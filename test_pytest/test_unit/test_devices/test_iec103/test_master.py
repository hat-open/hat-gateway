import collections
import contextlib
import datetime
import itertools
import math

import pytest

from hat import aio
from hat import util
from hat.drivers import iec103
from hat.drivers import serial
from hat.drivers.iec60870 import link
from hat.drivers.iec60870 import msgs as app
from hat.gateway import common
from hat.gateway.devices.iec103 import master
import hat.event.common


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name, master.device_type, device_name)

default_time = iec103.time_from_datetime(
    datetime.datetime.now(datetime.timezone.utc))


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


class Connection(aio.Resource):

    def __init__(self, conn):
        self._conn = conn
        self._encoder = app.iec103.encoder.Encoder()

    @property
    def async_group(self):
        return self._conn.async_group

    async def send(self, asdu):
        await self._conn.send(self._encoder.encode_asdu(asdu))

    async def receive(self):
        asdu_bytes = await self._conn.receive()
        asdu, _ = self._encoder.decode_asdu(asdu_bytes)
        return asdu


def get_conf(remote_addresses=[],
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
        conn = Connection(conn)
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
        address_size=link.common.AddressSize.ONE,
        keep_alive_timeout=10)


def time_to_event_timestamp(time):
    if time is None:
        return None
    if time.size == iec103.TimeSize.FOUR:
        # TODO use public implementation
        return master._time_iec103_to_source_ts(time)
    return hat.event.common.timestamp_from_datetime(
        iec103.time_to_datetime(time))


def time_from_event_timestamp(timestamp):
    if timestamp is None:
        return None
    return iec103.time_from_datetime(
        hat.event.common.timestamp_to_datetime(timestamp))


def assert_float_equal(value1, value2):
    assert math.isclose(value1, value2, rel_tol=1e-3)


def assert_time_equal(time1, time2):
    if time1 is None and time2 is None:
        return
    time1_dt = hat.event.common.timestamp_to_datetime(
        time_to_event_timestamp(time1))
    time2_dt = hat.event.common.timestamp_to_datetime(
        time_to_event_timestamp(time2))
    dt = abs(time1_dt - time2_dt)
    assert dt < datetime.timedelta(seconds=1)


def assert_status_event(event, status, address=None):
    if address is None:
        assert event.event_type == (*event_type_prefix, 'gateway', 'status')
    else:
        assert event.event_type == (*event_type_prefix, 'gateway',
                                    'remote_device', str(address), 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def assert_data_event(event, address, data_type, asdu_address, io_function,
                      io_information, time, cause, value):
    assert event.event_type == (*event_type_prefix, 'gateway', 'remote_device',
                                str(address), 'data', data_type,
                                str(asdu_address), str(io_function),
                                str(io_information))

    assert_time_equal(time, time_from_event_timestamp(event.source_timestamp))

    assert event.payload.data['cause'] == cause.name

    if data_type == 'double':
        assert event.payload.data['value'] == value

    else:
        assert event.payload.data['value']['overflow'] == value['overflow']
        assert event.payload.data['value']['invalid'] == value['invalid']
        assert_float_equal(event.payload.data['value']['value'],
                           value['value'])


def assert_command_event(event, address, asdu_address, io_function,
                         io_information, cause, session_id, success):
    assert event.event_type == (*event_type_prefix, 'gateway', 'remote_device',
                                str(address), 'command', str(asdu_address),
                                str(io_function), str(io_information))
    assert event.source_timestamp is None
    assert event.payload.data == {'session_id': session_id,
                                  'success': success}


def assert_interrogation_event(event, address, asdu_address):
    assert event.event_type == (*event_type_prefix, 'gateway', 'remote_device',
                                str(address), 'interrogation',
                                str(asdu_address))
    assert event.source_timestamp is None


def assert_asdu_equal(asdu1, asdu2):
    assert asdu1.type == asdu2.type
    assert asdu1.cause == asdu2.cause
    assert asdu1.address == asdu2.address

    assert len(asdu1.ios) == len(asdu2.ios)
    for io1, io2 in zip(asdu1.ios, asdu2.ios):
        assert io1.address == io2.address

        assert len(io1.elements) == len(io2.elements)
        for io_element1, io_element2 in zip(io1.elements, io2.elements):
            assert_io_element_equal(io_element1, io_element2)


def assert_io_element_equal(io_element1, io_element2):
    assert type(io_element1) == type(io_element2)

    if isinstance(io_element1,
                  app.iec103.common.IoElement_TIME_SYNCHRONIZATION):
        assert_time_equal(io_element1.time, io_element2.time)

    elif isinstance(io_element1,
                    app.iec103.common.IoElement_GENERAL_INTERROGATION):
        assert io_element1 == io_element2

    elif isinstance(io_element1,
                    app.iec103.common.IoElement_GENERAL_COMMAND):
        assert io_element1 == io_element2

    else:
        raise ValueError('io element not supported')


async def wait_remote_device_connected_event(event_client, address):

    def check(event):
        return (event.event_type == (*event_type_prefix, 'gateway',
                                     'remote_device', str(address),
                                     'status') and
                event.payload.data == 'CONNECTED')

    await aio.first(event_client.register_queue, check)


@pytest.fixture
def create_event():
    instance_ids = itertools.count(1)

    def create_event(event_type, payload_data):
        event_id = hat.event.common.EventId(1, 1, next(instance_ids))
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

    def create_interrogation_event(address, asdu_addr):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(address), 'interrogation', str(asdu_addr)),
                            None)

    return create_interrogation_event


@pytest.fixture
def create_command_event(create_event):

    def create_command_event(address, asdu_addr, io_function, io_information,
                             session_id, value):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(address), 'command', str(asdu_addr),
                             str(io_function), str(io_information)),
                            {'session_id': session_id,
                             'value': value})

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
        conf = get_conf([address],
                        poll_class1_delay=0.01)
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
async def test_time_sync(serial_conns, create_enable_event, address):
    last_datetime = datetime.datetime.now(datetime.timezone.utc)

    query_result = [create_enable_event(address, True)]
    event_client = EventClient(query_result)
    conf = get_conf([address],
                    time_sync_delay=0.001)
    conn_queue = aio.Queue()
    slave = await create_slave(conf, conn_queue.put_nowait)
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)
    conn = await conn_queue.get()

    for _ in range(10):
        asdu = await conn.receive()

        assert asdu.type == app.iec103.common.AsduType.TIME_SYNCHRONIZATION
        assert asdu.cause == app.iec103.common.Cause.TIME_SYNCHRONIZATION
        assert asdu.address == 0xFF

        assert len(asdu.ios) == 1
        io = asdu.ios[0]
        assert io.address == app.iec103.common.IoAddress(255, 0)

        assert len(io.elements) == 1
        element = io.elements[0]
        assert isinstance(element,
                          app.iec103.common.IoElement_TIME_SYNCHRONIZATION)

        new_datetime = iec103.time_to_datetime(element.time)
        assert new_datetime >= last_datetime
        last_datetime = new_datetime

    await device.async_close()
    await slave.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_function", [42])
@pytest.mark.parametrize("io_information", [24])
@pytest.mark.parametrize("session_id", [1])
@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("value", list(app.iec103.common.DoubleValue))
async def test_command(create_event_client_connection_pair,
                       create_command_event, address, asdu_address,
                       io_function, io_information, session_id, success,
                       value):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        event = create_command_event(address, asdu_address, io_function,
                                     io_information, session_id, value.name)

        event_client.receive_queue.put_nowait([event])

        asdu = await conn.receive()
        return_identifier = asdu.ios[0].elements[0].return_identifier

        assert_asdu_equal(asdu, app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.GENERAL_COMMAND,
            cause=app.iec103.common.Cause.GENERAL_COMMAND,
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(
                    io_function,
                    io_information),
                elements=[app.iec103.common.IoElement_GENERAL_COMMAND(
                    value=value,
                    return_identifier=return_identifier)])]))

        cause = (app.iec103.common.Cause.GENERAL_COMMAND if success
                 else app.iec103.common.Cause.GENERAL_COMMAND_NACK)
        asdu = app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.TIME_TAGGED_MESSAGE,
            cause=cause,
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(
                    io_function,
                    io_information),
                elements=[app.iec103.common.IoElement_TIME_TAGGED_MESSAGE(
                    app.iec103.common.DoubleWithTimeValue(
                        value=value,
                        time=default_time,
                        supplementary=return_identifier))])])
        await conn.send(asdu)

        event = await event_client.register_queue.get()
        assert_command_event(event, address, asdu_address, io_function,
                             io_information, cause, session_id, success)


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
async def test_interrogation(create_event_client_connection_pair,
                             create_interrogation_event,
                             address, asdu_address):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        event = create_interrogation_event(address, asdu_address)
        event_client.receive_queue.put_nowait([event])

        asdu = await conn.receive()
        scan_number = asdu.ios[0].elements[0].scan_number

        assert_asdu_equal(asdu, app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.GENERAL_INTERROGATION,
            cause=app.iec103.common.Cause.GENERAL_INTERROGATION,
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(255, 0),
                elements=[app.iec103.common.IoElement_GENERAL_INTERROGATION(
                    scan_number=scan_number)])]))

        asdu = app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.GENERAL_INTERROGATION_TERMINATION,
            cause=app.iec103.common.Cause.TERMINATION_OF_GENERAL_INTERROGATION,
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(255, 0),
                elements=[app.iec103.common.IoElement_GENERAL_INTERROGATION_TERMINATION(  # NOQA
                    scan_number=scan_number)])])
        await conn.send(asdu)

        event = await event_client.register_queue.get()
        assert_interrogation_event(event, address, asdu_address)


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_function", [42])
@pytest.mark.parametrize("io_information", [24])
@pytest.mark.parametrize("time", [default_time])
@pytest.mark.parametrize("cause", list(iec103.DataCause))
@pytest.mark.parametrize("value", list(app.iec103.common.DoubleValue))
async def test_double_data(create_event_client_connection_pair, address,
                           asdu_address, io_function, io_information, time,
                           cause, value):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        asdu = app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.TIME_TAGGED_MESSAGE,
            cause=app.iec103.common.Cause(cause.value),
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(io_function,
                                                    io_information),
                elements=[app.iec103.common.IoElement_TIME_TAGGED_MESSAGE(
                    app.iec103.common.DoubleWithTimeValue(
                        value=value,
                        time=time,
                        supplementary=0))])])
        await conn.send(asdu)

        event = await event_client.register_queue.get()
        assert_data_event(event, address, 'double', asdu_address, io_function,
                          io_information, time, cause, value.name)

        asdu = app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.TIME_TAGGED_MESSAGE_WITH_RELATIVE_TIME,  # NOQA
            cause=app.iec103.common.Cause(cause.value),
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(io_function,
                                                    io_information),
                elements=[app.iec103.common.IoElement_TIME_TAGGED_MESSAGE_WITH_RELATIVE_TIME(  # NOQA
                    app.iec103.common.DoubleWithRelativeTimeValue(
                        value=value,
                        relative_time=123,
                        fault_number=321,
                        time=time,
                        supplementary=0))])])
        await conn.send(asdu)

        event = await event_client.register_queue.get()
        assert_data_event(event, address, 'double', asdu_address, io_function,
                          io_information, time, cause, value.name)


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_function", [42])
@pytest.mark.parametrize("io_information", [24])
@pytest.mark.parametrize("cause", list(iec103.DataCause))
@pytest.mark.parametrize("data_types, values, overflows, invalids", [
    (['m1_i_l2'],
     [0],
     [False],
     [False]),

    (['m1_i_l2', 'm1_u_l12'],
     [0, 0.5],
     [False, True],
     [True, False]),

    (['m1_i_l2', 'm1_u_l12', 'm1_p', 'm1_q'],
     [0, 0.5, -1, -0.5],
     [False, True] * 2,
     [True, False] * 2),
])
async def test_m1_data(create_event_client_connection_pair, address,
                       asdu_address, io_function, io_information, cause,
                       data_types, values, overflows, invalids):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        elements = [
            app.iec103.common.IoElement_MEASURANDS_1(
                app.iec103.common.MeasurandValue(overflow=overflow,
                                                 invalid=invalid,
                                                 value=value))
            for overflow, invalid, value in zip(overflows, invalids, values)]
        asdu = app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.MEASURANDS_1,
            cause=app.iec103.common.Cause(cause.value),
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(io_function,
                                                    io_information),
                elements=elements)])
        await conn.send(asdu)

        events = collections.deque()
        for _ in data_types:
            event = await event_client.register_queue.get()
            events.append(event)

        for data_type, overflow, invalid, value in zip(data_types, overflows,
                                                       invalids, values):
            event = util.first(events, lambda i: i.event_type[-4] == data_type)
            assert_data_event(event, address, data_type, asdu_address,
                              io_function, io_information, None, cause,
                              {'overflow': overflow,
                               'invalid': invalid,
                               'value': value})


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_function", [42])
@pytest.mark.parametrize("io_information", [24])
@pytest.mark.parametrize("cause", list(iec103.DataCause))
@pytest.mark.parametrize("data_types, values, overflows, invalids", [
    (['m2_i_l1'],
     [0],
     [False],
     [False]),

    (['m2_i_l1', 'm2_i_l2'],
     [0, 0.5],
     [False, True],
     [True, False]),

    (['m2_i_l1', 'm2_i_l2', 'm2_i_l3'],
     [0, 0.5, -1],
     [False, True, False],
     [True, False, True]),

    (['m2_i_l1', 'm2_i_l2', 'm2_i_l3', 'm2_u_l1e', 'm2_u_l2e', 'm2_u_l3e'],
     [0, 0.5, -1, -0.5, 0.25, -0.25],
     [False, True, False] * 2,
     [True, False, True] * 2),

    (['m2_i_l1', 'm2_i_l2', 'm2_i_l3', 'm2_u_l1e', 'm2_u_l2e', 'm2_u_l3e',
      'm2_p', 'm2_q', 'm2_f'],
     [0, 0.5, -1, -0.5, 0.25, -0.25, 0.75, -0.75, 0.125],
     [False, True, False] * 3,
     [True, False, True] * 3),
])
async def test_m2_data(create_event_client_connection_pair, address,
                       asdu_address, io_function, io_information, cause,
                       data_types, values, overflows, invalids):
    async with create_event_client_connection_pair(address) as pair:
        event_client, conn = pair
        await wait_remote_device_connected_event(event_client, address)

        elements = [
            app.iec103.common.IoElement_MEASURANDS_2(
                app.iec103.common.MeasurandValue(overflow=overflow,
                                                 invalid=invalid,
                                                 value=value))
            for overflow, invalid, value in zip(overflows, invalids, values)]
        asdu = app.iec103.common.ASDU(
            type=app.iec103.common.AsduType.MEASURANDS_2,
            cause=app.iec103.common.Cause(cause.value),
            address=asdu_address,
            ios=[app.iec103.common.IO(
                address=app.iec103.common.IoAddress(io_function,
                                                    io_information),
                elements=elements)])
        await conn.send(asdu)

        events = collections.deque()
        for _ in data_types:
            event = await event_client.register_queue.get()
            events.append(event)

        for data_type, overflow, invalid, value in zip(data_types, overflows,
                                                       invalids, values):
            event = util.first(events, lambda i: i.event_type[-4] == data_type)
            assert_data_event(event, address, data_type, asdu_address,
                              io_function, io_information, None, cause,
                              {'overflow': overflow,
                               'invalid': invalid,
                               'value': value})
