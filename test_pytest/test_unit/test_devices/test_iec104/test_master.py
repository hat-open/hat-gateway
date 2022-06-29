import datetime
import itertools
import math

import pytest

from hat import aio
from hat import util
from hat.drivers import tcp
from hat.drivers.iec60870 import apci
from hat.drivers.iec60870 import iec104
from hat.gateway import common
from hat.gateway.devices.iec104 import master
import hat.event.common


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name, master.device_type, device_name)

default_time = iec104.time_from_datetime(
    datetime.datetime.now(datetime.timezone.utc))
default_indication_quality = iec104.IndicationQuality(False, True, False, True)
default_measurement_quality = iec104.MeasurementQuality(False, True, False,
                                                        True, False)
default_counter_quality = iec104.CounterQuality(False, True, False, True)
default_protection_quality = iec104.ProtectionQuality(False, True, False, True,
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


def time_to_event_timestamp(time):
    if time is None:
        return None
    return hat.event.common.timestamp_from_datetime(
        iec104.time_to_datetime(time))


def time_from_event_timestamp(timestamp):
    if timestamp is None:
        return None
    return iec104.time_from_datetime(
        hat.event.common.timestamp_to_datetime(timestamp))


def assert_float_equal(value1, value2):
    assert math.isclose(value1, value2, rel_tol=1e-3)


def assert_time_equal(time1, time2):
    if time1 is None and time2 is None:
        return
    dt = abs(iec104.time_to_datetime(time1) - iec104.time_to_datetime(time2))
    assert dt < datetime.timedelta(seconds=1)


def assert_status_event(event, status):
    assert event.event_type == (*event_type_prefix, 'gateway', 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


def assert_data_event(event, data_type, asdu_address, io_address, time, test,
                      cause, payload):
    assert event.event_type == (*event_type_prefix, 'gateway', 'data',
                                data_type, str(asdu_address), str(io_address))

    assert_time_equal(time, time_from_event_timestamp(event.source_timestamp))

    payload = {'cause': ('INTERROGATED'
                         if cause.name.startswith('INTERROGATED_')
                         else cause.name),
               'test': test,
               **payload}

    for key in {*payload.keys(), *event.payload.data.keys()}:
        if data_type in ('normalized', 'floating') and key == 'value':
            assert_float_equal(payload[key], event.payload.data[key])
        else:
            assert payload[key] == event.payload.data[key]


def assert_command_event(event, command_type, asdu_address, io_address, time,
                         cause, success, payload):
    assert event.event_type == (*event_type_prefix, 'gateway', 'command',
                                command_type, str(asdu_address),
                                str(io_address))

    assert_time_equal(time, time_from_event_timestamp(event.source_timestamp))

    payload = {'cause': cause.name,
               'success': success,
               **payload}

    for key in {*payload.keys(), *event.payload.data.keys()}:
        if command_type in ('normalized', 'floating') and key == 'value':
            assert_float_equal(payload[key], event.payload.data[key])
        else:
            assert payload[key] == event.payload.data[key]


def assert_interrogation_event(event, asdu_address, status,
                               interrogation_request):
    assert event.event_type == (*event_type_prefix, 'gateway', 'interrogation',
                                str(asdu_address))
    assert event.source_timestamp is None
    assert event.payload.data == {'status': status,
                                  'request': interrogation_request}


def assert_counter_interrogation_event(event, asdu_address, status,
                                       interrogation_request, freeze):
    assert event.event_type == (*event_type_prefix, 'gateway',
                                'counter_interrogation', str(asdu_address))
    assert event.source_timestamp is None
    assert event.payload.data == {'status': status,
                                  'request': interrogation_request,
                                  'freeze': freeze.name}


def assert_msg_equal(msg1, msg2):
    assert type(msg1) == type(msg2)
    assert msg1.is_test == msg2.is_test
    assert msg1.originator_address == msg2.originator_address
    assert msg1.asdu_address == msg2.asdu_address
    assert msg1.cause == msg2.cause

    if isinstance(msg1, iec104.CommandMsg):
        assert msg1.io_address == msg2.io_address
        assert msg1.is_negative_confirm == msg2.is_negative_confirm
        assert type(msg1.command) == type(msg2.command)  # NOQA
        assert_time_equal(msg1.time, msg2.time)

        if (isinstance(msg1.command, iec104.NormalizedCommand) or
                isinstance(msg1.command, iec104.FloatingCommand)):
            assert_float_equal(msg1.command.value.value,
                               msg2.command.value.value)
        else:
            assert msg1.command.value == msg2.command.value

        if hasattr(msg1.command, 'select'):
            assert msg1.command.select == msg2.command.select

        if hasattr(msg1.command, 'qualifier'):
            assert msg1.command.select == msg2.command.select

    elif isinstance(msg1, iec104.InterrogationMsg):
        assert msg1.cause == msg2.cause
        assert msg1.is_negative_confirm == msg2.is_negative_confirm
        assert msg1.request == msg2.request

    elif isinstance(msg1, iec104.CounterInterrogationMsg):
        assert msg1.cause == msg2.cause
        assert msg1.is_negative_confirm == msg2.is_negative_confirm
        assert msg1.request == msg2.request
        assert msg1.freeze == msg2.freeze

    else:
        raise ValueError('message type not supported')


async def wait_connected_event(event_client):

    def check(event):
        return (event.event_type == (*event_type_prefix, 'gateway',
                                     'status') and
                event.payload.data == 'CONNECTED')

    await aio.first(event_client.register_queue, check)


@pytest.fixture
def create_event():
    instance_ids = itertools.count(0)

    def create_event(event_type, payload_data, source_timestamp=None):
        event_id = hat.event.common.EventId(1, next(instance_ids))
        payload = hat.event.common.EventPayload(
            hat.event.common.EventPayloadType.JSON, payload_data)
        event = hat.event.common.Event(event_id=event_id,
                                       event_type=event_type,
                                       timestamp=hat.event.common.now(),
                                       source_timestamp=source_timestamp,
                                       payload=payload)
        return event

    return create_event


@pytest.fixture
def create_interrogation_event(create_event):

    def create_interrogation_event(asdu_addr, request):
        return create_event((*event_type_prefix, 'system', 'interrogation',
                             str(asdu_addr)),
                            {'request': request})

    return create_interrogation_event


@pytest.fixture
def create_counter_interrogation_event(create_event):

    def create_counter_interrogation_event(asdu_addr, request, freeze):
        return create_event((*event_type_prefix, 'system',
                             'counter_interrogation', str(asdu_addr)),
                            {'request': request,
                             'freeze': freeze})

    return create_counter_interrogation_event


@pytest.fixture
def create_command_event(create_event):

    def create_command_event(command_type, asdu_addr, io_address, time,
                             payload):
        return create_event((*event_type_prefix, 'system', 'command',
                             command_type, str(asdu_addr), str(io_address)),
                            payload,
                            time_to_event_timestamp(time))

    return create_command_event


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def create_conf(port):

    def create_conf(response_timeout=0.1,
                    supervisory_timeout=10,
                    test_timeout=20,
                    send_window_size=12,
                    receive_window_size=8,
                    reconnect_delay=0.01,
                    time_sync_delay=None):
        return {'remote_host': '127.0.0.1',
                'remote_port': port,
                'response_timeout': response_timeout,
                'supervisory_timeout': supervisory_timeout,
                'test_timeout': test_timeout,
                'send_window_size': send_window_size,
                'receive_window_size': receive_window_size,
                'reconnect_delay': reconnect_delay,
                'time_sync_delay': time_sync_delay}

    return create_conf


@pytest.fixture
def create_server(port):

    async def create_server(connection_cb):

        async def on_connection(conn):
            conn = iec104.Connection(conn)
            await aio.call(connection_cb, conn)

        return await apci.listen(connection_cb=on_connection,
                                 addr=tcp.Address('127.0.0.1', port))

    return create_server


@pytest.fixture
async def event_client_connection_pair(create_server, create_conf):
    event_client = EventClient()
    conn_queue = aio.Queue()
    server = await create_server(conn_queue.put_nowait)
    conf = create_conf()
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)
    conn = await conn_queue.get()

    yield event_client, conn

    await device.async_close()
    await server.async_close()
    await event_client.async_close()


def test_conf(create_conf):
    conf = create_conf()
    master.json_schema_repo.validate(master.json_schema_id, conf)


async def test_create(create_conf):
    event_client = EventClient()
    conf = create_conf()
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    assert device.is_open

    await device.async_close()
    await event_client.async_close()


async def test_status(create_conf, create_server):
    event_client = EventClient()
    conn_queue = aio.Queue()
    server = await create_server(conn_queue.put_nowait)
    conf = create_conf()
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    assert event_client.register_queue.empty()

    conn = await conn_queue.get()
    assert conn.is_open

    await conn.async_close()

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    conn = await conn_queue.get()
    assert conn.is_open

    await device.async_close()

    event = await event_client.register_queue.get()
    assert_status_event(event, 'DISCONNECTED')

    assert event_client.register_queue.empty()

    await server.async_close()
    await event_client.async_close()


async def test_time_sync(create_server, create_conf):
    last_datetime = datetime.datetime.now(datetime.timezone.utc)

    event_client = EventClient()
    conn_queue = aio.Queue()
    server = await create_server(conn_queue.put_nowait)
    conf = create_conf(time_sync_delay=0.01)
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)
    conn = await conn_queue.get()

    for _ in range(10):
        msgs = await conn.receive()
        assert len(msgs) == 1
        msg = msgs[0]

        assert isinstance(msg, iec104.ClockSyncMsg)
        assert msg.is_test is False
        assert msg.originator_address == 0
        assert msg.asdu_address == 0xFFFF
        assert msg.cause == iec104.ClockSyncReqCause.ACTIVATION

        new_datetime = iec104.time_to_datetime(msg.time)
        assert new_datetime >= last_datetime
        last_datetime = new_datetime

    await device.async_close()
    await server.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("cause", list(iec104.CommandReqCause))
@pytest.mark.parametrize("command, command_type, payload", [
    (iec104.SingleCommand(value=iec104.SingleValue.ON,
                          select=False,
                          qualifier=0),
     'single',
     {'value': 'ON',
      'select': False,
      'qualifier': 0}),

    (iec104.DoubleCommand(value=iec104.DoubleValue.OFF,
                          select=True,
                          qualifier=1),
     'double',
     {'value': 'OFF',
      'select': True,
      'qualifier': 1}),

    (iec104.RegulatingCommand(value=iec104.RegulatingValue.HIGHER,
                              select=False,
                              qualifier=2),
     'regulating',
     {'value': 'HIGHER',
      'select': False,
      'qualifier': 2}),

    (iec104.NormalizedCommand(value=iec104.NormalizedValue(0.5),
                              select=True),
     'normalized',
     {'value': 0.5,
      'select': True}),

    (iec104.ScaledCommand(value=iec104.ScaledValue(42),
                          select=False),
     'scaled',
     {'value': 42,
      'select': False}),

    (iec104.FloatingCommand(value=iec104.FloatingValue(42.5),
                            select=True),
     'floating',
     {'value': 42.5,
      'select': True}),

    (iec104.BitstringCommand(value=iec104.BitstringValue(b'\x01\x02\x03\x04')),
     'bitstring',
     {'value': [1, 2, 3, 4]}),
])
async def test_command_request(event_client_connection_pair,
                               create_command_event, asdu_address,
                               io_address, time, cause, command, command_type,
                               payload):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    event = create_command_event(command_type, asdu_address, io_address, time,
                                 {'cause': cause.name, **payload})
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.CommandMsg(is_test=False,
                                            originator_address=0,
                                            asdu_address=asdu_address,
                                            io_address=io_address,
                                            command=command,
                                            is_negative_confirm=False,
                                            time=time,
                                            cause=cause))


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("interrogation_request", [42])
async def test_interrogation_request(event_client_connection_pair,
                                     create_interrogation_event,
                                     asdu_address,
                                     interrogation_request):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    event = create_interrogation_event(asdu_address, interrogation_request)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.InterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=asdu_address,
        request=interrogation_request,
        is_negative_confirm=False,
        cause=iec104.CommandReqCause.ACTIVATION))


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("interrogation_request", [42])
@pytest.mark.parametrize("freeze", list(iec104.FreezeCode))
async def test_counter_interrogation_request(
        event_client_connection_pair,
        create_counter_interrogation_event,
        asdu_address, interrogation_request, freeze):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    event = create_counter_interrogation_event(
        asdu_address, interrogation_request, freeze.name)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.CounterInterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=asdu_address,
        request=interrogation_request,
        freeze=freeze,
        is_negative_confirm=False,
        cause=iec104.CommandReqCause.ACTIVATION))


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("test", [True, False])
@pytest.mark.parametrize("cause", [
    i for i in iec104.DataResCause
    if not (i.name.startswith('INTERROGATED_GROUP') or
            i.name.startswith('INTERROGATED_COUNTER0'))
])
@pytest.mark.parametrize("data, data_type, payload", [
    (iec104.SingleData(value=iec104.SingleValue.ON,
                       quality=default_indication_quality),
     'single',
     {'value': 'ON',
      'quality': default_indication_quality._asdict()}),

    (iec104.DoubleData(value=iec104.DoubleValue.OFF,
                       quality=default_indication_quality),
     'double',
     {'value': 'OFF',
      'quality': default_indication_quality._asdict()}),

    (iec104.StepPositionData(value=iec104.StepPositionValue(42, False),
                             quality=default_measurement_quality),
     'step_position',
     {'value': {'value': 42,
                'transient': False},
      'quality': default_measurement_quality._asdict()}),

    (iec104.BitstringData(value=iec104.BitstringValue(b'\x01\x02\x03\x04'),
                          quality=default_measurement_quality),
     'bitstring',
     {'value': [1, 2, 3, 4],
      'quality': default_measurement_quality._asdict()}),

    (iec104.NormalizedData(value=iec104.NormalizedValue(0.5),
                           quality=default_measurement_quality),
     'normalized',
     {'value': 0.5,
      'quality': default_measurement_quality._asdict()}),

    (iec104.ScaledData(value=iec104.ScaledValue(42),
                       quality=default_measurement_quality),
     'scaled',
     {'value': 42,
      'quality': default_measurement_quality._asdict()}),

    (iec104.FloatingData(value=iec104.FloatingValue(42.5),
                         quality=default_measurement_quality),
     'floating',
     {'value': 42.5,
      'quality': default_measurement_quality._asdict()}),

    (iec104.BinaryCounterData(value=iec104.BinaryCounterValue(123),
                              quality=default_counter_quality),
     'binary_counter',
     {'value': 123,
      'quality': default_counter_quality._asdict()}),

    (iec104.ProtectionData(value=iec104.ProtectionValue.ON,
                           quality=default_protection_quality,
                           elapsed_time=42),
     'protection',
     {'value': 'ON',
      'quality': default_protection_quality._asdict(),
      'elapsed_time': 42}),

    (iec104.ProtectionStartData(value=iec104.ProtectionStartValue(True, False,
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

    (iec104.ProtectionCommandData(value=iec104.ProtectionCommandValue(True,
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

    (iec104.StatusData(value=iec104.StatusValue([True, False] * 8,
                                                [False, True] * 8),
                       quality=default_measurement_quality),
     'status',
     {'value': {'value': [True, False] * 8,
                'change': [False, True] * 8},
      'quality': default_measurement_quality._asdict()}),
])
async def test_data_response(event_client_connection_pair, asdu_address,
                             io_address, time, test, cause, data, data_type,
                             payload):
    if data_type in ('protection', 'protection_start', 'protection_command'):
        if time is None:
            return
    elif data_type == 'status':
        if time is not None:
            return

    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    msg = iec104.DataMsg(is_test=test,
                         originator_address=0,
                         asdu_address=asdu_address,
                         io_address=io_address,
                         data=data,
                         time=time,
                         cause=cause)
    conn.send([msg])

    event = await event_client.register_queue.get()
    assert_data_event(event, data_type, asdu_address, io_address, time, test,
                      cause, payload)


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("cause", list(iec104.CommandResCause))
@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("command, command_type, payload", [
    (iec104.SingleCommand(value=iec104.SingleValue.ON,
                          select=False,
                          qualifier=0),
     'single',
     {'value': 'ON',
      'select': False,
      'qualifier': 0}),

    (iec104.DoubleCommand(value=iec104.DoubleValue.OFF,
                          select=True,
                          qualifier=0),
     'double',
     {'value': 'OFF',
      'select': True,
      'qualifier': 0}),

    (iec104.RegulatingCommand(value=iec104.RegulatingValue.LOWER,
                              select=False,
                              qualifier=0),
     'regulating',
     {'value': 'LOWER',
      'select': False,
      'qualifier': 0}),

    (iec104.NormalizedCommand(value=iec104.NormalizedValue(0.5),
                              select=True),
     'normalized',
     {'value': 0.5,
      'select': True}),

    (iec104.ScaledCommand(value=iec104.ScaledValue(42),
                          select=False),
     'scaled',
     {'value': 42,
      'select': False}),

    (iec104.FloatingCommand(value=iec104.FloatingValue(42.5),
                            select=True),
     'floating',
     {'value': 42.5,
      'select': True}),

    (iec104.BitstringCommand(value=iec104.BitstringValue(b'\x04\x03\x02\x01')),
     'bitstring',
     {'value': [4, 3, 2, 1]}),
])
async def test_command_response(event_client_connection_pair, asdu_address,
                                io_address, time, cause, success, command,
                                command_type, payload):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    msg = iec104.CommandMsg(is_test=False,
                            originator_address=0,
                            asdu_address=asdu_address,
                            io_address=io_address,
                            command=command,
                            is_negative_confirm=not success,
                            time=time,
                            cause=cause)
    conn.send([msg])

    event = await event_client.register_queue.get()
    assert_command_event(event, command_type, asdu_address, io_address, time,
                         cause, success, payload)


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("interrogation_request", [42])
@pytest.mark.parametrize("is_negative_confirm, cause, status", [
    (False, iec104.CommandResCause.ACTIVATION_CONFIRMATION, 'START'),
    (False, iec104.CommandResCause.ACTIVATION_TERMINATION, 'STOP'),
    (True, iec104.CommandResCause.ACTIVATION_CONFIRMATION, 'ERROR')
])
async def test_interrogation_response(event_client_connection_pair,
                                      asdu_address, interrogation_request,
                                      is_negative_confirm, cause, status):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    msg = iec104.InterrogationMsg(is_test=False,
                                  originator_address=0,
                                  asdu_address=asdu_address,
                                  request=interrogation_request,
                                  is_negative_confirm=is_negative_confirm,
                                  cause=cause)
    conn.send([msg])

    event = await event_client.register_queue.get()
    assert_interrogation_event(event, asdu_address, status,
                               interrogation_request)


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("interrogation_request", [42])
@pytest.mark.parametrize("freeze", list(iec104.FreezeCode))
@pytest.mark.parametrize("is_negative_confirm, cause, status", [
    (False, iec104.CommandResCause.ACTIVATION_CONFIRMATION, 'START'),
    (False, iec104.CommandResCause.ACTIVATION_TERMINATION, 'STOP'),
    (True, iec104.CommandResCause.ACTIVATION_CONFIRMATION, 'ERROR')
])
async def test_counter_interrogation_response(event_client_connection_pair,
                                              asdu_address,
                                              interrogation_request, freeze,
                                              is_negative_confirm, cause,
                                              status):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    msg = iec104.CounterInterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=asdu_address,
        request=interrogation_request,
        freeze=freeze,
        is_negative_confirm=is_negative_confirm,
        cause=cause)
    conn.send([msg])

    event = await event_client.register_queue.get()
    assert_counter_interrogation_event(event, asdu_address, status,
                                       interrogation_request, freeze)
