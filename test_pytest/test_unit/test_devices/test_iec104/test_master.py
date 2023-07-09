import datetime
import itertools
import math
import ssl
import subprocess

import pytest

from hat import aio
from hat import util
from hat.drivers import iec104
from hat.drivers import tcp
import hat.event.common

from hat.gateway.devices.iec104 import common
from hat.gateway.devices.iec104 import master


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


def assert_data_event(event, data_type, asdu_address, io_address, time,
                      is_test, cause, data_json):
    assert event.event_type == (*event_type_prefix, 'gateway', 'data',
                                data_type.value, str(asdu_address),
                                str(io_address))

    assert_time_equal(time, time_from_event_timestamp(event.source_timestamp))

    cause_str = ('INTERROGATED' if cause.name.startswith('INTERROGATED_')
                 else cause.name)

    assert is_test == event.payload.data['is_test']
    assert cause_str == event.payload.data['cause']

    for key in {*data_json.keys(), *event.payload.data['data'].keys()}:
        if data_type in (common.DataType.NORMALIZED,
                         common.DataType.FLOATING) and key == 'value':
            assert_float_equal(data_json[key], event.payload.data['data'][key])
        else:
            assert data_json[key] == event.payload.data['data'][key]


def assert_command_event(event, cmd_type, asdu_address, io_address, time,
                         is_test, is_negative_confirm, cause, cmd_json):
    assert event.event_type == (*event_type_prefix, 'gateway', 'command',
                                cmd_type.value, str(asdu_address),
                                str(io_address))

    assert_time_equal(time, time_from_event_timestamp(event.source_timestamp))

    assert is_test == event.payload.data['is_test']
    assert is_negative_confirm == event.payload.data['is_negative_confirm']
    assert cause.name == event.payload.data['cause']

    for key in {*cmd_json.keys(), *event.payload.data['command'].keys()}:
        if cmd_type in (common.CommandType.NORMALIZED,
                        common.CommandType.FLOATING) and key == 'value':
            assert_float_equal(cmd_json[key],
                               event.payload.data['command'][key])
        else:
            assert cmd_json[key] == event.payload.data['command'][key]


def assert_interrogation_event(event, asdu_address, is_test,
                               is_negative_confirm, request, cause):
    assert event.event_type == (*event_type_prefix, 'gateway', 'interrogation',
                                str(asdu_address))
    assert event.source_timestamp is None
    assert event.payload.data == {'is_test': is_test,
                                  'is_negative_confirm': is_negative_confirm,
                                  'request': request,
                                  'cause': cause.name}


def assert_counter_interrogation_event(event, asdu_address, is_test,
                                       is_negative_confirm, request, cause,
                                       freeze):
    assert event.event_type == (*event_type_prefix, 'gateway',
                                'counter_interrogation', str(asdu_address))
    assert event.source_timestamp is None
    assert event.payload.data == {'is_test': is_test,
                                  'is_negative_confirm': is_negative_confirm,
                                  'request': request,
                                  'cause': cause.name,
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
def pem_path(tmp_path):
    path = tmp_path / 'pem'
    subprocess.run(['openssl', 'req', '-batch', '-x509', '-noenc',
                    '-newkey', 'rsa:2048',
                    '-days', '1',
                    '-keyout', str(path),
                    '-out', str(path)],
                   stderr=subprocess.DEVNULL,
                   check=True)
    return path


@pytest.fixture
def create_event():
    instance_ids = itertools.count(1)

    def create_event(event_type, payload_data, source_timestamp=None):
        event_id = hat.event.common.EventId(1, 1, next(instance_ids))
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

    def create_interrogation_event(asdu_addr, is_test, request, cause):
        return create_event((*event_type_prefix, 'system', 'interrogation',
                             str(asdu_addr)),
                            {'is_test': is_test,
                             'request': request,
                             'cause': cause.name})

    return create_interrogation_event


@pytest.fixture
def create_counter_interrogation_event(create_event):

    def create_counter_interrogation_event(asdu_addr, is_test, request, cause,
                                           freeze):
        return create_event((*event_type_prefix, 'system',
                             'counter_interrogation', str(asdu_addr)),
                            {'is_test': is_test,
                             'request': request,
                             'cause': cause.name,
                             'freeze': freeze.name})

    return create_counter_interrogation_event


@pytest.fixture
def create_command_event(create_event):

    def create_command_event(cmd_type, asdu_addr, io_address, time,
                             is_test, cause, cmd_json):
        return create_event((*event_type_prefix, 'system', 'command',
                             cmd_type, str(asdu_addr), str(io_address)),
                            {'is_test': is_test,
                             'cause': cause.name,
                             'command': cmd_json},
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
                    time_sync_delay=None,
                    security=None):
        return {'remote_addresses': [{'host': '127.0.0.1',
                                      'port': port}],
                'response_timeout': response_timeout,
                'supervisory_timeout': supervisory_timeout,
                'test_timeout': test_timeout,
                'send_window_size': send_window_size,
                'receive_window_size': receive_window_size,
                'reconnect_delay': reconnect_delay,
                'time_sync_delay': time_sync_delay,
                'security': security}

    return create_conf


@pytest.fixture
async def create_server(port):

    async def create_server(connection_cb, **kwargs):
        return await iec104.listen(connection_cb=connection_cb,
                                   addr=tcp.Address('127.0.0.1', port),
                                   **kwargs)

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

    try:
        yield event_client, conn

    finally:
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


async def test_secure_connection(create_conf, create_server, pem_path):
    event_client = EventClient()
    conn_queue = aio.Queue()

    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.check_hostname = False
    ssl_ctx.load_cert_chain(pem_path)
    server = await create_server(conn_queue.put_nowait, ssl=ssl_ctx)

    conf = create_conf(security={'enabled': True,
                                 'cert_path': pem_path,
                                 'key_path': None,
                                 'verify_cert': False,
                                 'ca_path': None})
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTING')

    event = await event_client.register_queue.get()
    assert_status_event(event, 'CONNECTED')

    assert event_client.register_queue.empty()

    conn = await conn_queue.get()
    assert conn.is_open

    await device.async_close()
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


@pytest.mark.parametrize("is_test", [False, True])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("cause", list(iec104.CommandReqCause))
@pytest.mark.parametrize("command, cmd_type, cmd_json", [
    (iec104.SingleCommand(value=iec104.SingleValue.ON,
                          select=False,
                          qualifier=0),
     common.CommandType.SINGLE,
     {'value': 'ON',
      'select': False,
      'qualifier': 0}),

    (iec104.DoubleCommand(value=iec104.DoubleValue.OFF,
                          select=True,
                          qualifier=1),
     common.CommandType.DOUBLE,
     {'value': 'OFF',
      'select': True,
      'qualifier': 1}),

    (iec104.RegulatingCommand(value=iec104.RegulatingValue.HIGHER,
                              select=False,
                              qualifier=2),
     common.CommandType.REGULATING,
     {'value': 'HIGHER',
      'select': False,
      'qualifier': 2}),

    (iec104.NormalizedCommand(value=iec104.NormalizedValue(0.5),
                              select=True),
     common.CommandType.NORMALIZED,
     {'value': 0.5,
      'select': True}),

    (iec104.ScaledCommand(value=iec104.ScaledValue(42),
                          select=False),
     common.CommandType.SCALED,
     {'value': 42,
      'select': False}),

    (iec104.FloatingCommand(value=iec104.FloatingValue(42.5),
                            select=True),
     common.CommandType.FLOATING,
     {'value': 42.5,
      'select': True}),

    (iec104.BitstringCommand(value=iec104.BitstringValue(b'\x01\x02\x03\x04')),
     common.CommandType.BITSTRING,
     {'value': [1, 2, 3, 4]}),
])
async def test_command_request(event_client_connection_pair,
                               create_command_event, is_test, asdu_address,
                               io_address, time, cause, command, cmd_type,
                               cmd_json):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    event = create_command_event(cmd_type, asdu_address, io_address, time,
                                 is_test, cause, cmd_json)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.CommandMsg(is_test=is_test,
                                            originator_address=0,
                                            asdu_address=asdu_address,
                                            io_address=io_address,
                                            command=command,
                                            is_negative_confirm=False,
                                            time=time,
                                            cause=cause))


@pytest.mark.parametrize("is_test", [False, True])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("_request", [42])
@pytest.mark.parametrize("cause", list(iec104.CommandReqCause))
async def test_interrogation_request(event_client_connection_pair,
                                     create_interrogation_event,
                                     is_test, asdu_address, _request, cause):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    event = create_interrogation_event(asdu_address, is_test, _request, cause)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.InterrogationMsg(is_test=is_test,
                                                  originator_address=0,
                                                  asdu_address=asdu_address,
                                                  request=_request,
                                                  is_negative_confirm=False,
                                                  cause=cause))


@pytest.mark.parametrize("is_test", [False, True])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("_request", [42])
@pytest.mark.parametrize("cause", list(iec104.CommandReqCause))
@pytest.mark.parametrize("freeze", list(iec104.FreezeCode))
async def test_counter_interrogation_request(
        event_client_connection_pair, create_counter_interrogation_event,
        is_test, asdu_address, _request, cause, freeze):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    event = create_counter_interrogation_event(asdu_address, is_test,
                                               _request, cause, freeze)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.CounterInterrogationMsg(
        is_test=is_test,
        originator_address=0,
        asdu_address=asdu_address,
        request=_request,
        freeze=freeze,
        is_negative_confirm=False,
        cause=cause))


@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("is_test", [False, True])
@pytest.mark.parametrize("cause", [
    i for i in iec104.DataResCause
    if not (i.name.startswith('INTERROGATED_GROUP') or
            i.name.startswith('INTERROGATED_COUNTER0'))
])
@pytest.mark.parametrize("data, data_type, data_json", [
    (iec104.SingleData(value=iec104.SingleValue.ON,
                       quality=default_indication_quality),
     common.DataType.SINGLE,
     {'value': 'ON',
      'quality': default_indication_quality._asdict()}),

    (iec104.DoubleData(value=iec104.DoubleValue.OFF,
                       quality=default_indication_quality),
     common.DataType.DOUBLE,
     {'value': 'OFF',
      'quality': default_indication_quality._asdict()}),

    (iec104.StepPositionData(value=iec104.StepPositionValue(42, False),
                             quality=default_measurement_quality),
     common.DataType.STEP_POSITION,
     {'value': {'value': 42,
                'transient': False},
      'quality': default_measurement_quality._asdict()}),

    (iec104.BitstringData(value=iec104.BitstringValue(b'\x01\x02\x03\x04'),
                          quality=default_measurement_quality),
     common.DataType.BITSTRING,
     {'value': [1, 2, 3, 4],
      'quality': default_measurement_quality._asdict()}),

    (iec104.NormalizedData(value=iec104.NormalizedValue(0.5),
                           quality=default_measurement_quality),
     common.DataType.NORMALIZED,
     {'value': 0.5,
      'quality': default_measurement_quality._asdict()}),

    (iec104.ScaledData(value=iec104.ScaledValue(42),
                       quality=default_measurement_quality),
     common.DataType.SCALED,
     {'value': 42,
      'quality': default_measurement_quality._asdict()}),

    (iec104.FloatingData(value=iec104.FloatingValue(42.5),
                         quality=default_measurement_quality),
     common.DataType.FLOATING,
     {'value': 42.5,
      'quality': default_measurement_quality._asdict()}),

    (iec104.BinaryCounterData(value=iec104.BinaryCounterValue(123),
                              quality=default_counter_quality),
     common.DataType.BINARY_COUNTER,
     {'value': 123,
      'quality': default_counter_quality._asdict()}),

    (iec104.ProtectionData(value=iec104.ProtectionValue.ON,
                           quality=default_protection_quality,
                           elapsed_time=42),
     common.DataType.PROTECTION,
     {'value': 'ON',
      'quality': default_protection_quality._asdict(),
      'elapsed_time': 42}),

    (iec104.ProtectionStartData(value=iec104.ProtectionStartValue(True, False,
                                                                  True, False,
                                                                  True, False),
                                quality=default_protection_quality,
                                duration_time=42),
     common.DataType.PROTECTION_START,
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
     common.DataType.PROTECTION_COMMAND,
     {'value': {'general': True,
                'l1': False,
                'l2': True,
                'l3': False},
      'quality': default_protection_quality._asdict(),
      'operating_time': 42}),

    (iec104.StatusData(value=iec104.StatusValue([True, False] * 8,
                                                [False, True] * 8),
                       quality=default_measurement_quality),
     common.DataType.STATUS,
     {'value': {'value': [True, False] * 8,
                'change': [False, True] * 8},
      'quality': default_measurement_quality._asdict()}),
])
async def test_data_response(event_client_connection_pair, asdu_address,
                             io_address, time, is_test, cause, data, data_type,
                             data_json):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    if data_type in (common.DataType.PROTECTION,
                     common.DataType.PROTECTION_START,
                     common.DataType.PROTECTION_COMMAND):
        if time is None:
            return

    elif data_type == common.DataType.STATUS:
        if time is not None:
            return

    msg = iec104.DataMsg(is_test=is_test,
                         originator_address=0,
                         asdu_address=asdu_address,
                         io_address=io_address,
                         data=data,
                         time=time,
                         cause=cause)
    await conn.send([msg])

    event = await event_client.register_queue.get()
    assert_data_event(event, data_type, asdu_address, io_address, time,
                      is_test, cause, data_json)

    await conn.async_close()


@pytest.mark.parametrize("is_test", [False, True])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("cause", list(iec104.CommandResCause))
@pytest.mark.parametrize("is_negative_confirm", [True, False])
@pytest.mark.parametrize("command, cmd_type, cmd_json", [
    (iec104.SingleCommand(value=iec104.SingleValue.ON,
                          select=False,
                          qualifier=0),
     common.CommandType.SINGLE,
     {'value': 'ON',
      'select': False,
      'qualifier': 0}),

    (iec104.DoubleCommand(value=iec104.DoubleValue.OFF,
                          select=True,
                          qualifier=0),
     common.CommandType.DOUBLE,
     {'value': 'OFF',
      'select': True,
      'qualifier': 0}),

    (iec104.RegulatingCommand(value=iec104.RegulatingValue.LOWER,
                              select=False,
                              qualifier=0),
     common.CommandType.REGULATING,
     {'value': 'LOWER',
      'select': False,
      'qualifier': 0}),

    (iec104.NormalizedCommand(value=iec104.NormalizedValue(0.5),
                              select=True),
     common.CommandType.NORMALIZED,
     {'value': 0.5,
      'select': True}),

    (iec104.ScaledCommand(value=iec104.ScaledValue(42),
                          select=False),
     common.CommandType.SCALED,
     {'value': 42,
      'select': False}),

    (iec104.FloatingCommand(value=iec104.FloatingValue(42.5),
                            select=True),
     common.CommandType.FLOATING,
     {'value': 42.5,
      'select': True}),

    (iec104.BitstringCommand(value=iec104.BitstringValue(b'\x04\x03\x02\x01')),
     common.CommandType.BITSTRING,
     {'value': [4, 3, 2, 1]}),
])
async def test_command_response(event_client_connection_pair, is_test,
                                asdu_address, io_address, time, cause,
                                is_negative_confirm, command,
                                cmd_type, cmd_json):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    msg = iec104.CommandMsg(is_test=is_test,
                            originator_address=0,
                            asdu_address=asdu_address,
                            io_address=io_address,
                            command=command,
                            is_negative_confirm=is_negative_confirm,
                            time=time,
                            cause=cause)
    await conn.send([msg])

    event = await event_client.register_queue.get()
    assert_command_event(event, cmd_type, asdu_address, io_address, time,
                         is_test, is_negative_confirm, cause, cmd_json)


@pytest.mark.parametrize("is_test", [False, True])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("_request", [42])
@pytest.mark.parametrize("is_negative_confirm", [False, True])
@pytest.mark.parametrize("cause", list(iec104.CommandResCause))
async def test_interrogation_response(event_client_connection_pair,
                                      is_test, asdu_address, _request,
                                      is_negative_confirm, cause):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    msg = iec104.InterrogationMsg(is_test=is_test,
                                  originator_address=0,
                                  asdu_address=asdu_address,
                                  request=_request,
                                  is_negative_confirm=is_negative_confirm,
                                  cause=cause)
    await conn.send([msg])

    event = await event_client.register_queue.get()
    assert_interrogation_event(event, asdu_address, is_test,
                               is_negative_confirm, _request, cause)


@pytest.mark.parametrize("is_test", [False, True])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("freeze", list(iec104.FreezeCode))
@pytest.mark.parametrize("_request", [42])
@pytest.mark.parametrize("is_negative_confirm", [False, True])
@pytest.mark.parametrize("cause", list(iec104.CommandResCause))
async def test_counter_interrogation_response(event_client_connection_pair,
                                              is_test,  asdu_address, freeze,
                                              _request, is_negative_confirm,
                                              cause):
    event_client, conn = event_client_connection_pair
    await wait_connected_event(event_client)

    msg = iec104.CounterInterrogationMsg(
        is_test=is_test,
        originator_address=0,
        asdu_address=asdu_address,
        request=_request,
        freeze=freeze,
        is_negative_confirm=is_negative_confirm,
        cause=cause)
    await conn.send([msg])

    event = await event_client.register_queue.get()
    assert_counter_interrogation_event(event, asdu_address, is_test,
                                       is_negative_confirm, _request, cause,
                                       freeze)
