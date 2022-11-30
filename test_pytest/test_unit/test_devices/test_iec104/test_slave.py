import asyncio
import collections
import datetime
import itertools
import math
import ssl

import pytest

from hat import aio
from hat import util
from hat.drivers import iec104
from hat.drivers import tcp
from hat.gateway.devices.iec104 import common
from hat.gateway.devices.iec104 import slave
import hat.event.common
import pem


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name, slave.device_type, device_name)

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


def assert_float_equal(value1, value2):
    assert math.isclose(value1, value2, rel_tol=1e-3)


def assert_time_equal(time1, time2):
    if time1 is None and time2 is None:
        return
    dt = abs(iec104.time_to_datetime(time1) - iec104.time_to_datetime(time2))
    assert dt < datetime.timedelta(seconds=1)


def assert_connection_event(event, conn_count):
    assert event.event_type == (*event_type_prefix, 'gateway', 'connections')
    assert event.source_timestamp is None
    assert len(event.payload.data) == conn_count
    for connection in event.payload.data:
        assert 'connection_id' in connection
        for i in ('local', 'remote'):
            assert i in connection
            assert 'host' in connection[i]
            assert 'port' in connection[i]


def assert_command_event(event, cmd_type, asdu_address, io_address, time,
                         is_test, cause, cmd_json):
    assert event.event_type == (*event_type_prefix, 'gateway', 'command',
                                cmd_type.value, str(asdu_address),
                                str(io_address))

    assert_time_equal(
        time, common.time_from_source_timestamp(event.source_timestamp))

    assert is_test == event.payload.data['is_test']
    assert cause.name == event.payload.data['cause']
    assert 'connection_id' in event.payload.data

    for key in {*cmd_json.keys(), *event.payload.data['command'].keys()}:
        if cmd_type in (common.CommandType.NORMALIZED,
                        common.CommandType.FLOATING) and key == 'value':
            assert_float_equal(cmd_json[key],
                               event.payload.data['command'][key])
        else:
            assert cmd_json[key] == event.payload.data['command'][key]


def assert_msg_equal(msg1, msg2):
    assert type(msg1) == type(msg2)
    assert msg1.is_test == msg2.is_test
    assert msg1.originator_address == msg2.originator_address
    assert msg1.asdu_address == msg2.asdu_address
    assert msg1.cause == msg2.cause

    if isinstance(msg1, iec104.DataMsg):
        assert msg1.io_address == msg2.io_address
        assert type(msg1.data) == type(msg2.data)  # NOQA
        assert_time_equal(msg1.time, msg2.time)

        if (isinstance(msg1.data, iec104.NormalizedData) or
                isinstance(msg1.data, iec104.FloatingData)):
            assert_float_equal(msg1.data.value.value,
                               msg2.data.value.value)
        else:
            assert msg1.data.value == msg2.data.value

        assert msg1.data.quality == msg2.data.quality

        if hasattr(msg1.data, 'elapsed_time'):
            assert msg1.data.elapsed_time == msg2.data.elapsed_time

        if hasattr(msg1.data, 'duration_time'):
            assert msg1.data.duration_time == msg2.data.duration_time

        if hasattr(msg1.data, 'operating_time'):
            assert msg1.data.operating_time == msg2.data.operating_time

    elif isinstance(msg1, iec104.CommandMsg):
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


async def wait_connections_event(event_client, conn_count):

    def check(event):
        return (event.event_type == (*event_type_prefix, 'gateway',
                                     'connections') and
                len(event.payload.data) == conn_count)

    return await aio.first(event_client.register_queue, check)


@pytest.fixture
def pem_path(tmp_path):
    path = tmp_path / 'pem'
    pem.create_pem_file(path)
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
def create_command_event(create_event):

    def create_command_event(cmd_type, asdu_addr, io_address, time,
                             payload):
        return create_event((*event_type_prefix, 'system', 'command',
                             cmd_type.value, str(asdu_addr), str(io_address)),
                            payload,
                            common.time_to_source_timestamp(time))

    return create_command_event


@pytest.fixture
def create_data_event(create_event):

    def create_data_event(data_type, asdu_addr, io_address, time, payload):
        return create_event((*event_type_prefix, 'system', 'data',
                             data_type.value, str(asdu_addr), str(io_address)),
                            payload,
                            common.time_to_source_timestamp(time))

    return create_data_event


@pytest.fixture
def port():
    return util.get_unused_tcp_port()


@pytest.fixture
def create_conf(port):

    def create_conf(remote_hosts=None,
                    response_timeout=0.1,
                    supervisory_timeout=10,
                    test_timeout=20,
                    send_window_size=12,
                    receive_window_size=8,
                    security=None,
                    buffers=[],
                    data=[]):
        return {'local_host': '127.0.0.1',
                'local_port': port,
                'remote_hosts': remote_hosts,
                'response_timeout': response_timeout,
                'supervisory_timeout': supervisory_timeout,
                'test_timeout': test_timeout,
                'send_window_size': send_window_size,
                'receive_window_size': receive_window_size,
                'security': security,
                'buffers': buffers or [],
                'data': data or []}

    return create_conf


@pytest.fixture
def create_connection(port):

    async def create_connection(**kwargs):
        return await iec104.connect(tcp.Address('127.0.0.1', port), **kwargs)

    return create_connection


@pytest.fixture
async def create_event_client_connection_pair(create_conf, create_connection):

    resources = collections.deque()

    async def create_event_client_connection_pair(query_result=[], **kwargs):
        event_client = EventClient(query_result)
        conf = create_conf(**kwargs)
        device = await aio.call(slave.create, conf, event_client,
                                event_type_prefix)
        conn = await create_connection()
        resources.extend([conn, device, event_client])
        return event_client, conn

    try:
        yield create_event_client_connection_pair

    finally:
        for i in resources:
            await i.async_close()


def test_conf(create_conf):
    conf = create_conf()
    slave.json_schema_repo.validate(slave.json_schema_id, conf)


async def test_create(create_conf):
    event_client = EventClient()
    conf = create_conf()
    device = await aio.call(slave.create, conf, event_client,
                            event_type_prefix)

    assert device.is_open

    await device.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("conn_count", [0, 1, 5])
async def test_connections(create_conf, create_connection, conn_count):
    event_client = EventClient()
    conf = create_conf()
    device = await aio.call(slave.create, conf, event_client,
                            event_type_prefix)
    conns = collections.deque()

    event = await event_client.register_queue.get()
    assert_connection_event(event, len(conns))

    assert event_client.register_queue.empty()

    for _ in range(conn_count):
        conn = await create_connection()
        assert conn.is_open
        conns.append(conn)

        event = await event_client.register_queue.get()
        assert_connection_event(event, len(conns))

    while conns:
        conn = conns.pop()
        await conn.async_close()

        event = await event_client.register_queue.get()
        assert_connection_event(event, len(conns))

    assert event_client.register_queue.empty()

    await device.async_close()
    await event_client.async_close()


async def test_secure_connection(create_conf, create_connection, pem_path):
    event_client = EventClient()
    conf = create_conf(security={'enabled': True,
                                 'cert_path': pem_path,
                                 'key_path': None,
                                 'verify_cert': False,
                                 'ca_path': None})

    device = await aio.call(slave.create, conf, event_client,
                            event_type_prefix)

    event = await event_client.register_queue.get()
    assert_connection_event(event, 0)

    assert event_client.register_queue.empty()

    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.VerifyMode.CERT_NONE
    ssl_ctx.load_cert_chain(pem_path)
    conn = await create_connection(ssl_ctx=ssl_ctx)
    assert conn.is_open

    event = await event_client.register_queue.get()
    assert_connection_event(event, 1)

    await conn.async_close()
    await device.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("is_test", [True, False])
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
async def test_command_request(create_event_client_connection_pair, is_test,
                               asdu_address, io_address, time, cause,
                               command, cmd_type, cmd_json):
    event_client, conn = await create_event_client_connection_pair()
    await wait_connections_event(event_client, 1)

    msg = iec104.CommandMsg(is_test=is_test,
                            originator_address=0,
                            asdu_address=asdu_address,
                            io_address=io_address,
                            command=command,
                            is_negative_confirm=False,
                            time=time,
                            cause=cause)
    conn.send([msg])

    event = await event_client.register_queue.get()
    assert_command_event(event, cmd_type, asdu_address, io_address, time,
                         is_test, cause, cmd_json)


@pytest.mark.parametrize("is_test", [True, False])
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
async def test_command_response(create_event_client_connection_pair,
                                create_command_event, is_test, asdu_address,
                                io_address, time, cause, is_negative_confirm,
                                command, cmd_type, cmd_json):
    event_client, conn = await create_event_client_connection_pair()
    event = await wait_connections_event(event_client, 1)
    connection_id = event.payload.data[0]['connection_id']

    event = create_command_event(cmd_type, asdu_address, io_address, time,
                                 {'connection_id': connection_id,
                                  'is_test': is_test,
                                  'is_negative_confirm': is_negative_confirm,
                                  'cause': cause.name,
                                  'command': cmd_json})
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.CommandMsg(
        is_test=is_test,
        originator_address=0,
        asdu_address=asdu_address,
        io_address=io_address,
        command=command,
        is_negative_confirm=is_negative_confirm,
        time=time,
        cause=cause))


@pytest.mark.parametrize("is_test", [True, False])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("time", [None, default_time])
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
async def test_data_response(create_event_client_connection_pair,
                             create_data_event, is_test, asdu_address,
                             io_address, time, cause, data, data_type,
                             data_json):
    event_client, conn = await create_event_client_connection_pair(data=[
        {'data_type': data_type.name,
         'asdu_address': asdu_address,
         'io_address': io_address,
         'buffer': None}])
    await wait_connections_event(event_client, 1)

    if data_type in (common.DataType.PROTECTION,
                     common.DataType.PROTECTION_START,
                     common.DataType.PROTECTION_COMMAND):
        if time is None:
            return

    elif data_type == common.DataType.STATUS:
        if time is not None:
            return

    if data_type == common.DataType.BINARY_COUNTER:
        if cause == iec104.DataResCause.INTERROGATED_STATION:
            return

    else:
        if cause == iec104.DataResCause.INTERROGATED_COUNTER:
            return

    payload = {'is_test': is_test,
               'cause': ('INTERROGATED'
                         if cause.name.startswith('INTERROGATED_')
                         else cause.name),
               'data': data_json}
    event = create_data_event(data_type, asdu_address, io_address, time,
                              payload)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert_msg_equal(msg, iec104.DataMsg(is_test=is_test,
                                         originator_address=0,
                                         asdu_address=asdu_address,
                                         io_address=io_address,
                                         data=data,
                                         time=time,
                                         cause=cause))


@pytest.mark.parametrize("is_test", [True, False])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("asdu_address", [1, 2, 3, 0xFFFF])
async def test_interrogation(create_data_event,
                             create_event_client_connection_pair, is_test,
                             time, asdu_address):
    data = [
        iec104.DataMsg(
            is_test=False,
            originator_address=0,
            asdu_address=1,
            io_address=1,
            data=iec104.SingleData(value=iec104.SingleValue.ON,
                                   quality=default_indication_quality),
            time=time,
            cause=iec104.DataResCause.SPONTANEOUS),
        iec104.DataMsg(
            is_test=False,
            originator_address=0,
            asdu_address=2,
            io_address=1,
            data=iec104.DoubleData(value=iec104.DoubleValue.OFF,
                                   quality=default_indication_quality),
            time=time,
            cause=iec104.DataResCause.SPONTANEOUS),
        iec104.DataMsg(
            is_test=False,
            originator_address=0,
            asdu_address=2,
            io_address=2,
            data=iec104.BinaryCounterData(value=iec104.BinaryCounterValue(123),
                                          quality=default_counter_quality),
            time=time,
            cause=iec104.DataResCause.SPONTANEOUS)]

    events = [create_data_event(common.get_data_type(i.data),
                                i.asdu_address, i.io_address, i.time,
                                {'is_test': i.is_test,
                                 'cause': i.cause.name,
                                 'data': common.data_to_json(i.data)})
              for i in data]

    data_conf = [{'data_type': common.get_data_type(i.data).name,
                  'asdu_address': i.asdu_address,
                  'io_address': i.io_address,
                  'buffer': None}
                 for i in data]

    event_client, conn = await create_event_client_connection_pair(
        events, data=data_conf)
    await wait_connections_event(event_client, 1)

    req = iec104.InterrogationMsg(is_test=is_test,
                                  originator_address=0,
                                  asdu_address=asdu_address,
                                  request=42,
                                  is_negative_confirm=False,
                                  cause=iec104.CommandReqCause.ACTIVATION)
    conn.send([req])

    msgs = await conn.receive()
    assert len(msgs) == 1
    res = req._replace(cause=iec104.CommandResCause.ACTIVATION_CONFIRMATION)
    assert_msg_equal(msgs[0], res)

    for i in data:
        if isinstance(i.data, iec104.BinaryCounterData):
            continue
        if asdu_address != 0xFFFF and asdu_address != i.asdu_address:
            continue

        msgs = await conn.receive()
        assert len(msgs) == 1
        res = i._replace(is_test=is_test,
                         cause=iec104.DataResCause.INTERROGATED_STATION)
        assert_msg_equal(msgs[0], res)

    msgs = await conn.receive()
    assert len(msgs) == 1
    res = req._replace(cause=iec104.CommandResCause.ACTIVATION_TERMINATION)
    assert_msg_equal(msgs[0], res)


@pytest.mark.parametrize("is_test", [True, False])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("asdu_address", [1, 2, 3, 0xFFFF])
async def test_counter_interrogation(create_data_event,
                                     create_event_client_connection_pair,
                                     is_test, time, asdu_address):
    data = [
        iec104.DataMsg(
            is_test=False,
            originator_address=0,
            asdu_address=1,
            io_address=1,
            data=iec104.SingleData(value=iec104.SingleValue.ON,
                                   quality=default_indication_quality),
            time=time,
            cause=iec104.DataResCause.SPONTANEOUS),
        iec104.DataMsg(
            is_test=False,
            originator_address=0,
            asdu_address=2,
            io_address=1,
            data=iec104.DoubleData(value=iec104.DoubleValue.OFF,
                                   quality=default_indication_quality),
            time=time,
            cause=iec104.DataResCause.SPONTANEOUS),
        iec104.DataMsg(
            is_test=False,
            originator_address=0,
            asdu_address=2,
            io_address=2,
            data=iec104.BinaryCounterData(value=iec104.BinaryCounterValue(123),
                                          quality=default_counter_quality),
            time=time,
            cause=iec104.DataResCause.SPONTANEOUS)]

    events = [create_data_event(common.get_data_type(i.data),
                                i.asdu_address, i.io_address, i.time,
                                {'is_test': i.is_test,
                                 'cause': i.cause.name,
                                 'data': common.data_to_json(i.data)})
              for i in data]

    data_conf = [{'data_type': common.get_data_type(i.data).name,
                  'asdu_address': i.asdu_address,
                  'io_address': i.io_address,
                  'buffer': None}
                 for i in data]

    event_client, conn = await create_event_client_connection_pair(
        events, data=data_conf)
    await wait_connections_event(event_client, 1)

    req = iec104.CounterInterrogationMsg(
        is_test=is_test,
        originator_address=0,
        asdu_address=asdu_address,
        request=42,
        freeze=iec104.FreezeCode.READ,
        is_negative_confirm=False,
        cause=iec104.CommandReqCause.ACTIVATION)
    conn.send([req])

    msgs = await conn.receive()
    assert len(msgs) == 1
    res = req._replace(cause=iec104.CommandResCause.ACTIVATION_CONFIRMATION)
    assert_msg_equal(msgs[0], res)

    for i in data:
        if not isinstance(i.data, iec104.BinaryCounterData):
            continue
        if asdu_address != 0xFFFF and asdu_address != i.asdu_address:
            continue

        msgs = await conn.receive()
        assert len(msgs) == 1
        res = i._replace(is_test=is_test,
                         cause=iec104.DataResCause.INTERROGATED_COUNTER)
        assert_msg_equal(msgs[0], res)

    msgs = await conn.receive()
    assert len(msgs) == 1
    res = req._replace(cause=iec104.CommandResCause.ACTIVATION_TERMINATION)
    assert_msg_equal(msgs[0], res)


@pytest.mark.parametrize("is_test", [False])
@pytest.mark.parametrize("asdu_address", [1])
@pytest.mark.parametrize("io_address", [1])
@pytest.mark.parametrize("time", [None, default_time])
@pytest.mark.parametrize("cause", [iec104.DataResCause.SPONTANEOUS])
@pytest.mark.parametrize("change_count", [0, 1, 5, 100])
async def test_buffer(create_conf, create_data_event, create_connection,
                      is_test, asdu_address, io_address, time, cause,
                      change_count):
    event_client = EventClient()
    conf = create_conf(data=[{'data_type': 'SCALED',
                              'asdu_address': asdu_address,
                              'io_address': io_address,
                              'buffer': 'b1'}],
                       buffers=[{'name': 'b1',
                                 'size': change_count}])
    device = await aio.call(slave.create, conf, event_client,
                            event_type_prefix)

    for i in range(change_count):
        quality = default_measurement_quality._asdict()
        event = create_data_event(common.DataType.SCALED,
                                  asdu_address, io_address, time,
                                  {'is_test': is_test,
                                   'cause': cause.name,
                                   'data': {'value': i,
                                            'quality': quality}})
        event_client.receive_queue.put_nowait([event])

    await asyncio.sleep(0.01)

    conn = await create_connection(supervisory_timeout=0.001)

    for i in range(change_count):
        msgs = await conn.receive()
        assert len(msgs) == 1
        msg = iec104.DataMsg(
            is_test=is_test,
            originator_address=0,
            asdu_address=asdu_address,
            io_address=io_address,
            data=iec104.ScaledData(value=iec104.ScaledValue(i),
                                   quality=default_measurement_quality),
            time=time,
            cause=cause)
        assert_msg_equal(msgs[0], msg)

    await conn.async_close()
    await device.async_close()
    await event_client.async_close()
