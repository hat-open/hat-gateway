import asyncio
import collections
import datetime
import itertools
import math
import pytest

from hat import aio
from hat import json

from hat.drivers import iec101
from hat.drivers import serial
from hat.drivers.iec60870 import link
from hat.gateway import common
from hat.gateway.devices.iec101 import slave
import hat.event.common


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name, slave.device_type, device_name)
connections_event_type = (*event_type_prefix, 'gateway', 'connections')


class EventerClient(common.DeviceEventClient):

    def __init__(self, receive_queue=None, register_queue=None, query_cb=[]):
        self._receive_queue = (receive_queue if receive_queue is not None
                               else aio.Queue())
        self._register_queue = register_queue
        self._query_cb = query_cb
        self._async_group = aio.Group()

    @property
    def async_group(self):
        return self._async_group

    async def receive(self):
        return await self._receive_queue.get()

    def register(self, events):
        if self._register_queue is not None:
            self._register_queue.put_nowait(events)

    async def register_with_response(self, events):
        raise Exception('should not be used')

    async def query(self, data):
        if self._query_cb:
            return self._query_cb(data)
        return []


@pytest.fixture
def conf():
    return {
        "port": '/dev/ttyS0',
        "addresses": [123],
        "baudrate": 9600,
        "bytesize": "EIGHTBITS",
        "parity": "NONE",
        "stopbits": "ONE",
        "flow_control": {'xonxoff': False,
                         'rtscts': False,
                         'dsrdtr': False},
        "silent_interval": 0.001,
        "keep_alive_timeout": 3,
        "device_address_size": "ONE",
        "cause_size": "TWO",
        "asdu_address_size": "TWO",
        "io_address_size": "THREE",
        "buffers": [],
        "data": []
    }


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

    monkeypatch.setattr(serial, 'create', create)
    return conns


@pytest.fixture
async def master_conn_factory(conf):
    master = await link.unbalanced.create_master(
        port=conf['port'],
        baudrate=conf['baudrate'],
        bytesize=serial.ByteSize[conf['bytesize']],
        parity=serial.Parity[conf['parity']],
        stopbits=serial.StopBits[conf['stopbits']],
        xonxoff=conf['flow_control']['xonxoff'],
        rtscts=conf['flow_control']['rtscts'],
        dsrdtr=conf['flow_control']['dsrdtr'],
        silent_interval=conf['silent_interval'],
        address_size=link.common.AddressSize[conf['device_address_size']])
    connections = []

    async def create_connection(addr=None):
        addr = addr if addr is not None else conf['addresses'][0]
        conn = await master.connect(addr)

        conn101 = iec101.Connection(
            conn=conn,
            cause_size=iec101.CauseSize[conf['cause_size']],
            asdu_address_size=iec101.AsduAddressSize[
                conf['asdu_address_size']],
            io_address_size=iec101.IoAddressSize[conf['io_address_size']])
        connections.append(conn101)
        return conn101

    yield create_connection

    for conn in connections:
        await conn.async_close()
    await master.async_close()


next_instance_id = itertools.count(1)


def create_event(event_type, payload_data, source_ts=None):
    event_id = hat.event.common.EventId(1, 1, next(next_instance_id))
    payload = hat.event.common.EventPayload(
        hat.event.common.EventPayloadType.JSON, payload_data)
    event = hat.event.common.Event(event_id=event_id,
                                   event_type=event_type,
                                   timestamp=hat.event.common.now(),
                                   source_timestamp=source_ts,
                                   payload=payload)
    return event


def assert_time(event_ts, iec101_time):
    if event_ts is None:
        assert iec101_time is None
        return
    tz_local = datetime.datetime.now().astimezone().tzinfo
    time_dt = hat.event.common.timestamp_to_datetime(event_ts).astimezone(
        tz_local)
    assert time_dt.month == iec101_time.months
    assert time_dt.day == iec101_time.day_of_month
    assert time_dt.hour == iec101_time.hours
    assert time_dt.minute == iec101_time.minutes
    assert time_dt.second == iec101_time.milliseconds // 1000


def assert_quality(quality_json, quality_iec101):
    for quality_param in quality_json:
        assert getattr(quality_iec101, quality_param) == \
            quality_json[quality_param]


def assert_value(value_json, value_iec101):
    if isinstance(value_json, str):
        assert value_json == value_iec101.name
        return
    elif isinstance(value_json, float):
        math.isclose(value_json, value_iec101, rel_tol=1e-3)
        assert value_json == value_iec101
        return
    elif isinstance(value_json, dict):
        assert value_json == value_iec101._asdict()
        return
    assert value_json == value_iec101


def test_device_type():
    assert slave.device_type == 'iec101_slave'


def test_schema_validate(conf):
    slave.json_schema_repo.validate(slave.json_schema_id, conf)


async def test_create(conf, serial_conns):
    eventer_client = EventerClient()
    device = await slave.create(conf, eventer_client, event_type_prefix)

    assert isinstance(device, common.Device)
    assert device.is_open

    device.close()
    assert device.is_closing
    await device.wait_closed()

    await eventer_client.async_close()


async def test_connections(conf, serial_conns, master_conn_factory):
    register_queue = aio.Queue()

    eventer_client = EventerClient(register_queue=register_queue)
    addresses = [i for i in range(10)]
    conf = json.set_(conf, ['addresses'], addresses)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == connections_event_type
    assert len(conn_event.payload.data) == 0

    connection_ids = set()
    for conn_count, addr in enumerate(addresses):
        await master_conn_factory(addr)

        events = await register_queue.get()
        assert len(events) == 1
        conns_event = events[0]
        assert conn_event.event_type == connections_event_type
        assert len(conns_event.payload.data) == conn_count + 1
        assert conns_event.payload.data[conn_count]['address'] == addr
        for i in range(conn_count + 1):
            assert conns_event.payload.data[i]['address'] == addresses[i]
        connection_ids.add(
            conns_event.payload.data[conn_count]['connection_id'])

    assert len(connection_ids) == len(addresses)

    await device.async_close()
    await eventer_client.async_close()


async def test_keep_alive_timeout(conf, serial_conns, master_conn_factory):
    conf = {**conf, 'keep_alive_timeout': 0.05}
    register_queue = aio.Queue()

    eventer_client = EventerClient(register_queue=register_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == connections_event_type
    assert len(conn_event.payload.data) == 0

    await master_conn_factory()
    addr = conf['addresses'][0]

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == connections_event_type
    assert len(conn_event.payload.data) == 1
    assert conn_event.payload.data[0]['address'] == addr

    events = await register_queue.get()
    conn_event = events[0]
    assert conn_event.event_type == connections_event_type
    assert len(conn_event.payload.data) == 0

    await device.async_close()
    await eventer_client.async_close()


async def test_query(conf, serial_conns):
    query_queue = aio.Queue()

    def on_query(data):
        query_queue.put_nowait(data)
        return []

    eventer_client = EventerClient(query_cb=on_query)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    q_data = await query_queue.get()

    assert q_data.event_types == [(*event_type_prefix, 'system', 'data', '*')]
    assert q_data.unique_type

    await device.async_close()
    await eventer_client.async_close()


async def test_interrogate(conf, serial_conns, master_conn_factory):
    data_cnt = 10
    data_conf = [{
        'data_type': 'SINGLE',
        'asdu_address': 1,
        'io_address': i,
        'buffer': None,
    } for i in range(data_cnt)]
    conf = json.set_(conf, ['data'], data_conf)
    quality = {'invalid': False,
               'not_topical': False,
               'substituted': False,
               'blocked': False}
    query_events = [
        create_event((*event_type_prefix, 'system', 'data',
                      i['data_type'].lower(), str(i['asdu_address']),
                      str(i['io_address'])),
                     {'is_test': False,
                      'cause': 'SPONTANEOUS',
                      'data': {'value': 'ON',
                               'quality': quality}})
        for i in data_conf]

    def on_query(data):
        return query_events

    receive_queue = aio.Queue()
    eventer_client = EventerClient(query_cb=on_query,
                                   receive_queue=receive_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    new_events = [
        create_event(
            (*event_type_prefix, 'system', 'data', 'single', '1', str(i)),
            {'is_test': False,
             'cause': 'SPONTANEOUS',
             'data': {'value': 'OFF',
                      'quality': quality}})
        for i in range(5)]
    receive_queue.put_nowait(new_events)

    exp_gi_resp_events = new_events + query_events[5:]

    conn101 = await master_conn_factory()

    msg = iec101.InterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=1,
        request=0,
        is_negative_confirm=False,
        cause=iec101.CommandReqCause.ACTIVATION)
    await conn101.send([msg])

    msgs = await conn101.receive()
    gi_resp_msg = msgs[0]
    assert isinstance(gi_resp_msg, iec101.InterrogationMsg)
    assert gi_resp_msg.cause == iec101.CommandResCause.ACTIVATION_CONFIRMATION
    assert not gi_resp_msg.is_negative_confirm

    for data_event in exp_gi_resp_events:
        msgs = await conn101.receive()
        gi_data_msg = msgs[0]
        assert isinstance(gi_data_msg, iec101.DataMsg)
        assert gi_data_msg.is_test == data_event.payload.data['is_test']
        assert gi_data_msg.cause == iec101.DataResCause.INTERROGATED_STATION
        assert gi_data_msg.data.value.name == \
            data_event.payload.data['data']['value']
        assert gi_data_msg.asdu_address == int(data_event.event_type[7])
        assert gi_data_msg.io_address == int(data_event.event_type[8])
        assert gi_data_msg.time is None

    msgs = await conn101.receive()
    gi_resp_msg = msgs[0]
    assert isinstance(gi_resp_msg, iec101.InterrogationMsg)
    assert gi_resp_msg.cause == iec101.CommandResCause.ACTIVATION_TERMINATION
    assert not gi_resp_msg.is_negative_confirm

    await device.async_close()
    await eventer_client.async_close()


async def test_interrogate_deactivation(conf, serial_conns,
                                        master_conn_factory):
    data_conf = [{'data_type': 'SINGLE',
                  'asdu_address': 1,
                  'io_address': 1,
                  'buffer': None}]
    conf = json.set_(conf, ['data'], data_conf)
    data_events = [create_event((*event_type_prefix, 'system', 'data',
                                'single', '1', '1'),
                                {'is_test': False,
                                 'cause': 'SPONTANEOUS',
                                 'data': {'value': 'ON',
                                          'quality': {'invalid': False,
                                                      'not_topical': False,
                                                      'substituted': False,
                                                      'blocked': False}}})]

    def on_query(data):
        return data_events

    eventer_client = EventerClient(query_cb=on_query)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    conn101 = await master_conn_factory()

    msg = iec101.InterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=1,
        request=0,
        is_negative_confirm=False,
        cause=iec101.CommandReqCause.ACTIVATION)
    await conn101.send([msg])

    msg = iec101.InterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=1,
        request=0,
        is_negative_confirm=False,
        cause=iec101.CommandReqCause.DEACTIVATION)
    await conn101.send([msg])

    msgs = await conn101.receive()
    msg = msgs[0]
    assert isinstance(msg, iec101.InterrogationMsg)
    assert msg.cause == iec101.CommandResCause.ACTIVATION_CONFIRMATION
    assert not msg.is_negative_confirm

    msgs = await conn101.receive()
    msg = msgs[0]
    assert isinstance(msg, iec101.DataMsg)
    assert msg.cause == iec101.DataResCause.INTERROGATED_STATION

    msgs = await conn101.receive()
    msg = msgs[0]
    assert isinstance(msg, iec101.InterrogationMsg)
    assert msg.cause == iec101.CommandResCause.ACTIVATION_TERMINATION
    assert not msg.is_negative_confirm

    msgs = await conn101.receive()
    msg = msgs[0]
    assert isinstance(msg, iec101.InterrogationMsg)
    assert msg.cause == iec101.CommandResCause.UNKNOWN_CAUSE
    assert msg.is_negative_confirm

    await device.async_close()
    await eventer_client.async_close()


async def test_counter_interrogate(conf, serial_conns, master_conn_factory):
    data_conf = [
        {'data_type': 'SINGLE',
         'asdu_address': 1,
         'io_address': 1,
         'buffer': None},
        {'data_type': 'BINARY_COUNTER',
         'asdu_address': 1,
         'io_address': 2,
         'buffer': None},
        ]
    conf = json.set_(conf, ['data'], data_conf)
    non_counter_event = create_event(
        (*event_type_prefix, 'system', 'data', 'single', '1', '1'),
        {'is_test': False,
         'cause': 'SPONTANEOUS',
         'data': {'value': 'ON',
                  'quality': {'invalid': False,
                              'not_topical': False,
                              'substituted': False,
                              'blocked': False}}})
    counter_event = create_event(
        (*event_type_prefix, 'system', 'data', 'binary_counter', '1', '2'),
        {'is_test': False,
         'cause': 'SPONTANEOUS',
         'data': {'value': 123,
                  'quality': {'invalid': False,
                              'adjusted': False,
                              'overflow': False,
                              'sequence': False}}})
    data_events = [non_counter_event, counter_event]

    def on_query(data):
        return data_events

    eventer_client = EventerClient(query_cb=on_query)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    conn101 = await master_conn_factory()

    msg = iec101.CounterInterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=1,
        request=0,
        freeze=iec101.FreezeCode.READ,
        is_negative_confirm=False,
        cause=iec101.CommandReqCause.ACTIVATION)
    await conn101.send([msg])

    msgs = await conn101.receive()
    gi_resp_msg = msgs[0]
    assert isinstance(gi_resp_msg, iec101.CounterInterrogationMsg)
    assert gi_resp_msg.cause == iec101.CommandResCause.ACTIVATION_CONFIRMATION
    assert not gi_resp_msg.is_negative_confirm

    msgs = await conn101.receive()
    data_msg = msgs[0]

    assert isinstance(data_msg, iec101.DataMsg)
    assert data_msg.is_test == counter_event.payload.data['is_test']
    assert data_msg.cause == iec101.DataResCause.INTERROGATED_COUNTER
    assert isinstance(data_msg.data.value, iec101.BinaryCounterValue)
    assert data_msg.data.value.value == \
        counter_event.payload.data['data']['value']
    assert data_msg.asdu_address == int(counter_event.event_type[7])
    assert data_msg.io_address == int(counter_event.event_type[8])
    assert_quality(counter_event.payload.data['data']['quality'],
                   data_msg.data.quality)
    assert data_msg.time is None

    msgs = await conn101.receive()
    gi_resp_msg = msgs[0]
    assert isinstance(gi_resp_msg, iec101.CounterInterrogationMsg)
    assert gi_resp_msg.cause == iec101.CommandResCause.ACTIVATION_TERMINATION
    assert not gi_resp_msg.is_negative_confirm

    await device.async_close()
    await eventer_client.async_close()


def data_cases():
    quality = {'invalid': False,
               'not_topical': False,
               'substituted': False,
               'blocked': False}
    data = iec101.SingleData(value=iec101.SingleValue.ON,
                             quality=iec101.IndicationQuality(**quality))
    yield 'SINGLE', {'value': 'ON', 'quality': quality}, data

    quality = {'invalid': True,
               'not_topical': False,
               'substituted': True,
               'blocked': False}
    data = iec101.DoubleData(value=iec101.DoubleValue.INTERMEDIATE,
                             quality=iec101.IndicationQuality(**quality))
    yield 'DOUBLE', {'value': 'INTERMEDIATE', 'quality': quality}, data

    quality = {'invalid': False,
               'not_topical': False,
               'substituted': False,
               'blocked': False,
               'overflow': False}
    data = iec101.StepPositionData(
        value=iec101.StepPositionValue(value=13,
                                       transient=False),
        quality=iec101.MeasurementQuality(**quality))
    yield 'STEP_POSITION', {'value': {'value': 13, 'transient': False},
                            'quality': quality}, data

    quality = {'invalid': False,
               'not_topical': True,
               'substituted': False,
               'blocked': True,
               'overflow': False}
    data = iec101.BitstringData(
        value=iec101.BitstringValue(value=b'\x04\x03\x02\x01'),
        quality=iec101.MeasurementQuality(**quality))
    yield 'BITSTRING', {'value': [4, 3, 2, 1],
                        'quality': quality}, data

    quality = {'invalid': True,
               'not_topical': False,
               'substituted': True,
               'blocked': False,
               'overflow': True}
    data = iec101.NormalizedData(
        value=iec101.NormalizedValue(value=0.43),
        quality=iec101.MeasurementQuality(**quality))
    yield 'NORMALIZED', {'value': 0.43,
                         'quality': quality}, data

    quality = {'invalid': True,
               'not_topical': True,
               'substituted': True,
               'blocked': True,
               'overflow': True}
    data = iec101.ScaledData(
        value=iec101.ScaledValue(24),
        quality=iec101.MeasurementQuality(**quality))
    yield 'SCALED', {'value': 24,
                     'quality': quality}, data

    quality = {'invalid': False,
               'not_topical': False,
               'substituted': False,
               'blocked': False,
               'overflow': False}
    data = iec101.FloatingData(
        value=iec101.FloatingValue(123.45),
        quality=iec101.MeasurementQuality(**quality))
    yield 'FLOATING', {'value': 123.45,
                       'quality': quality}, data

    quality = {'invalid': False,
               'adjusted': False,
               'overflow': False,
               'sequence': False}
    data = iec101.BinaryCounterData(
        value=iec101.BinaryCounterValue(123),
        quality=iec101.CounterQuality(**quality))
    yield 'BINARY_COUNTER', {'value': 123,
                             'quality': quality}, data

    quality = {'invalid': False,
               'not_topical': False,
               'substituted': False,
               'blocked': False,
               'time_invalid': False}
    data = iec101.ProtectionData(
        value=iec101.ProtectionValue.ON,
        quality=iec101.ProtectionQuality(**quality),
        elapsed_time=13)
    yield 'PROTECTION', {'value': 'ON',
                         'quality': quality,
                         'elapsed_time': 13}, data

    quality = {'invalid': False,
               'not_topical': True,
               'substituted': False,
               'blocked': True,
               'time_invalid': False}
    data = iec101.ProtectionStartData(
        value=iec101.ProtectionStartValue(True, False, True, False,
                                          True, False),
        quality=iec101.ProtectionQuality(**quality),
        duration_time=13)
    yield 'PROTECTION_START', {'value': {'general': True,
                                         'l1': False,
                                         'l2': True,
                                         'l3': False,
                                         'ie': True,
                                         'reverse': False},
                               'quality': quality,
                               'duration_time': 13}, data

    quality = {'invalid': True,
               'not_topical': True,
               'substituted': True,
               'blocked': True,
               'time_invalid': True}
    data = iec101.ProtectionCommandData(
        value=iec101.ProtectionCommandValue(True,
                                            False,
                                            True,
                                            False),
        quality=iec101.ProtectionQuality(**quality),
        operating_time=13)
    yield 'PROTECTION_COMMAND', {'value': {'general': True,
                                           'l1': False,
                                           'l2': True,
                                           'l3': False},
                                 'quality': quality,
                                 'operating_time': 13}, data

    quality = {'invalid': False,
               'not_topical': False,
               'substituted': False,
               'blocked': False,
               'overflow': False}
    data = iec101.StatusData(
        value=iec101.StatusValue([True, False] * 8,
                                 [False, True] * 8),
        quality=iec101.MeasurementQuality(**quality))
    yield 'STATUS', {'value': {'value': [True, False] * 8,
                               'change': [False, True] * 8},
                     'quality': quality}, data


@pytest.mark.parametrize("data_type, event_data, exp_data", data_cases())
@pytest.mark.parametrize("source_ts", [hat.event.common.now(), None])
async def test_data(conf, serial_conns, master_conn_factory, data_type,
                    event_data, exp_data, source_ts):
    if data_type in ['PROTECTION',
                     'PROTECTION_START',
                     'PROTECTION_COMMAND'] and source_ts is None:
        return
    if data_type == 'STATUS' and source_ts:
        return

    data_conf = {
        'data_type': data_type,
        'asdu_address': 12,
        'io_address': 34,
        'buffer': None}
    conf = json.set_(conf, ['data'], [data_conf])

    receive_queue = aio.Queue()

    eventer_client = EventerClient(receive_queue=receive_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    conn101 = await master_conn_factory()

    data_event = create_event(
        (*event_type_prefix, 'system', 'data', data_type.lower(),
            str(data_conf['asdu_address']), str(data_conf['io_address'])),
        {'is_test': False,
         'cause': 'SPONTANEOUS',
         'data': event_data},
        source_ts)
    receive_queue.put_nowait([data_event])

    data_msgs = await conn101.receive()
    data_msg = data_msgs[0]

    assert isinstance(data_msg, iec101.DataMsg)
    assert data_msg.is_test == data_event.payload.data['is_test']
    assert data_msg.cause == iec101.DataResCause.SPONTANEOUS
    assert data_msg.asdu_address == int(data_event.event_type[7])
    assert data_msg.io_address == int(data_event.event_type[8])
    if data_type in ['NORMALIZED', 'FLOATING']:
        math.isclose(event_data['value'], data_msg.data.value.value,
                     rel_tol=1e-3)
        assert_quality(event_data['quality'], data_msg.data.quality)
    else:
        assert data_msg.data == exp_data
    assert_time(data_event.source_timestamp, data_msg.time)

    await device.async_close()
    await eventer_client.async_close()


async def test_data_bulk(conf, serial_conns, master_conn_factory):
    events_cnt = 10
    data_conf = [{
        'data_type': 'SINGLE',
        'asdu_address': 1,
        'io_address': i,
        'buffer': None,
    } for i in range(events_cnt)]
    conf = json.set_(conf, ['data'], data_conf)

    receive_queue = aio.Queue()

    eventer_client = EventerClient(receive_queue=receive_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    conn101 = await master_conn_factory()

    data_events = [create_event((*event_type_prefix, 'system', 'data',
                                i['data_type'].lower(), str(i['asdu_address']),
                                str(i['io_address'])),
                                {'is_test': False,
                                 'cause': 'SPONTANEOUS',
                                 'data': {'value': 'ON',
                                          'quality': {'invalid': False,
                                                      'not_topical': False,
                                                      'substituted': False,
                                                      'blocked': False}}})
                   for i in data_conf]
    receive_queue.put_nowait(data_events)

    data_msgs = []
    for _ in range(events_cnt):
        data_msgs.extend(await conn101.receive())

    assert len(data_msgs) == len(data_events)
    for data_msg, data_event in zip(data_msgs, data_events):
        assert isinstance(data_msg, iec101.DataMsg)
        assert data_msg.is_test == data_event.payload.data['is_test']
        assert data_msg.cause == iec101.DataResCause.SPONTANEOUS
        assert data_msg.data.value.name == \
            data_event.payload.data['data']['value']
        assert data_msg.asdu_address == int(data_event.event_type[7])
        assert data_msg.io_address == int(data_event.event_type[8])
        assert data_msg.time is None
        # TODO check quality
        # TODO check time

    await device.async_close()
    await eventer_client.async_close()


async def test_command(conf, serial_conns, master_conn_factory):
    register_queue = aio.Queue()
    receive_queue = aio.Queue()

    eventer_client = EventerClient(register_queue=register_queue,
                                   receive_queue=receive_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    conn101 = await master_conn_factory()

    await register_queue.get_until_empty()

    asdu = 1
    io = 2
    cmd_msg = iec101.CommandMsg(is_test=False,
                                originator_address=0,
                                asdu_address=asdu,
                                io_address=io,
                                command=iec101.SingleCommand(
                                    value=iec101.SingleValue.ON,
                                    select=False,
                                    qualifier=14),
                                is_negative_confirm=False,
                                cause=iec101.CommandReqCause.ACTIVATION)
    await conn101.send([cmd_msg])

    events = await register_queue.get()
    cmd_req_event = events[0]
    assert cmd_req_event.event_type == (
        *event_type_prefix, 'gateway', 'command', 'single', str(asdu), str(io))
    cmd_id = cmd_req_event.payload.data['connection_id']
    assert isinstance(cmd_req_event.payload.data['connection_id'], int)
    assert cmd_req_event.payload.data == {
        'is_test': cmd_msg.is_test,
        'cause': cmd_msg.cause.name,
        'command': {'value': cmd_msg.command.value.name,
                    'select': cmd_msg.command.select,
                    'qualifier': cmd_msg.command.qualifier},
        'connection_id': cmd_id}

    cmd_res_payload = {
        'connection_id': cmd_id,
        'is_test': False,
        'is_negative_confirm': False,
        'cause': 'ACTIVATION_CONFIRMATION',
        'command': {'value': cmd_msg.command.value.name,
                    'select': cmd_msg.command.select,
                    'qualifier': cmd_msg.command.qualifier}}
    cmd_res_event = create_event(
        (*event_type_prefix, 'system', 'command', 'single',
            str(asdu), str(io)),
        cmd_res_payload)
    receive_queue.put_nowait([cmd_res_event])

    msgs = await conn101.receive()
    assert len(msgs) == 1
    cmd_res_msg = msgs[0]
    assert isinstance(cmd_res_msg, iec101.CommandMsg)
    assert cmd_res_msg.is_test == cmd_res_payload['is_test']
    assert cmd_res_msg.asdu_address == asdu
    assert cmd_res_msg.io_address == io
    assert cmd_res_msg.is_negative_confirm == \
        cmd_res_payload['is_negative_confirm']
    assert cmd_res_msg.cause.name == cmd_res_payload['cause']
    assert cmd_res_msg.command.value.name == \
        cmd_res_payload['command']['value']
    assert cmd_res_msg.command.select == cmd_res_payload['command']['select']
    assert cmd_res_msg.command.qualifier == \
        cmd_res_payload['command']['qualifier']

    await device.async_close()
    await eventer_client.async_close()


async def test_buffer(conf, serial_conns, master_conn_factory):
    receive_queue = aio.Queue()
    eventer_client = EventerClient(receive_queue=receive_queue)
    data_conf = [
        {'data_type': 'DOUBLE',
         'asdu_address': 1,
         'io_address': 1,
         'buffer': 'b1'},
        {'data_type': 'DOUBLE',
         'asdu_address': 1,
         'io_address': 2,
         'buffer': 'b1'},
        {'data_type': 'DOUBLE',
         'asdu_address': 1,
         'io_address': 3,
         'buffer': None}]
    conf = json.set_(conf, ["buffers"], [{'name': 'b1', 'size': 100}])
    conf = json.set_(conf, ["data"], data_conf)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    data_events = []
    buffered_data_events = []
    for d_conf in data_conf:
        for value in ['INTERMEDIATE', 'OFF', 'ON', 'FAULT']:
            data_event = create_event(
                (*event_type_prefix, 'system', 'data',
                 d_conf['data_type'].lower(),
                    str(d_conf['asdu_address']), str(d_conf['io_address'])),
                {'is_test': False,
                 'cause': 'SPONTANEOUS',
                 'data': {'value': value,
                          'quality': {'invalid': False,
                                      'not_topical': False,
                                      'substituted': False,
                                      'blocked': False}}},
                hat.event.common.now())
            data_events.append(data_event)
            if d_conf['buffer']:
                buffered_data_events.append(data_event)
    receive_queue.put_nowait(data_events)

    conn = await master_conn_factory()

    for data_event in buffered_data_events:
        msgs = await conn.receive()
        data_msg = msgs[0]
        assert data_msg.is_test == data_event.payload.data['is_test']
        assert data_msg.cause == iec101.DataResCause.SPONTANEOUS
        assert data_msg.asdu_address == int(data_event.event_type[7])
        assert data_msg.io_address == int(data_event.event_type[8])
        assert data_msg.data.value.name == \
            data_event.payload.data['data']['value']
        assert_quality(data_event.payload.data['data']['quality'],
                       data_msg.data.quality)
        assert_time(data_event.source_timestamp, data_msg.time)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(conn.receive(), 0.01)

    await device.async_close()
    await eventer_client.async_close()


async def test_data_on_multi_masters(conf, serial_conns, master_conn_factory):
    masters_count = 5
    receive_queue = aio.Queue()
    eventer_client = EventerClient(receive_queue=receive_queue)
    addresses = [i for i in range(masters_count)]
    conf = json.set_(conf, ['addresses'], addresses)
    data_conf = [{'data_type': 'DOUBLE',
                  'asdu_address': 1,
                  'io_address': 2,
                  'buffer': None}]
    conf = json.set_(conf, ['data'], data_conf)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    master_conns = []
    for addr in addresses:
        conn = await master_conn_factory(addr)
        master_conns.append(conn)

    data_event = create_event(
        (*event_type_prefix, 'system', 'data', 'double', '1', '2'),
        {'is_test': False,
         'cause': 'SPONTANEOUS',
         'data': {'value': 'INTERMEDIATE',
                  'quality': {'invalid': False,
                              'not_topical': True,
                              'substituted': False,
                              'blocked': False}}})
    receive_queue.put_nowait([data_event])

    msgs = await master_conns[0].receive()
    data_msg = msgs[0]
    for master_conn in master_conns[1:]:
        msgs = await master_conn.receive()
        assert msgs[0] == data_msg

    await device.async_close()
    await eventer_client.async_close()


async def test_command_on_multi_masters(conf, serial_conns,
                                        master_conn_factory):
    masters_count = 3
    receive_queue = aio.Queue()
    register_queue = aio.Queue()
    eventer_client = EventerClient(receive_queue=receive_queue,
                                   register_queue=register_queue)
    addresses = [i for i in range(masters_count)]
    conf = json.set_(conf, ['addresses'], addresses)

    device = await slave.create(conf, eventer_client, event_type_prefix)

    master_conns = []
    for addr in addresses:
        conn = await master_conn_factory(addr)
        master_conns.append(conn)

    events = await register_queue.get_until_empty()
    conns_event = events[0]
    assert len(conns_event.payload.data) == masters_count

    master_conn1 = master_conns[0]
    master_conn1_conn_id = conns_event.payload.data[0]['connection_id']

    asdu = 1
    io = 2
    cmd_msg = iec101.CommandMsg(is_test=False,
                                originator_address=0,
                                asdu_address=asdu,
                                io_address=io,
                                command=iec101.SingleCommand(
                                    value=iec101.SingleValue.OFF,
                                    select=False,
                                    qualifier=14),
                                is_negative_confirm=False,
                                cause=iec101.CommandReqCause.ACTIVATION)
    await master_conn1.send([cmd_msg])

    events = await register_queue.get()
    assert len(events) == 1
    cmd_req_event = events[0]
    assert cmd_req_event.event_type == (
        *event_type_prefix, 'gateway', 'command', 'single', str(asdu), str(io))
    assert cmd_req_event.payload.data['connection_id'] == master_conn1_conn_id

    await asyncio.sleep(0.01)
    assert register_queue.empty()

    cmd_res_payload = {
        'connection_id': master_conn1_conn_id,
        'is_test': False,
        'is_negative_confirm': False,
        'cause': 'ACTIVATION_CONFIRMATION',
        'command': {'value': cmd_msg.command.value.name,
                    'select': cmd_msg.command.select,
                    'qualifier': cmd_msg.command.qualifier}}
    cmd_res_event = create_event(
        (*event_type_prefix, 'system', 'command', 'single',
            str(asdu), str(io)),
        cmd_res_payload)
    receive_queue.put_nowait([cmd_res_event])

    msgs = await master_conn1.receive()
    assert len(msgs) == 1
    cmd_res_msg = msgs[0]
    assert isinstance(cmd_res_msg, iec101.CommandMsg)

    for master_conn in master_conns[1:]:
        with pytest.raises(asyncio.TimeoutError):
            await aio.wait_for(master_conn.receive(), 0.01)

    await device.async_close()
    await eventer_client.async_close()
