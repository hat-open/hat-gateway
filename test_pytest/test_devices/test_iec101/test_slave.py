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
import hat.event.common

from hat.gateway import common
from hat.gateway.devices.iec101.slave import info


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


def get_conf(addresses=[],
             keep_alive_timeout=3,
             buffers=[],
             data=[]):
    return {"port": '/dev/ttyS0',
            "addresses": addresses,
            "baudrate": 9600,
            "bytesize": "EIGHTBITS",
            "parity": "NONE",
            "stopbits": "ONE",
            "flow_control": {'xonxoff': False,
                             'rtscts': False,
                             'dsrdtr': False},
            "silent_interval": 0.001,
            "keep_alive_timeout": keep_alive_timeout,
            "device_address_size": "ONE",
            "cause_size": "TWO",
            "asdu_address_size": "TWO",
            "io_address_size": "THREE",
            "buffers": buffers,
            "data": data}


def create_event(event_type, payload_data, source_timestamp=None):
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayloadJson(payload_data))


def create_data_event(data_type, asdu_address, io_address, data,
                      is_test=False, cause='SPONTANEOUS',
                      source_timestamp=None):
    return create_event((*event_type_prefix, 'system', 'data',
                         data_type.lower(), str(asdu_address),
                         str(io_address)),
                        {'is_test': is_test,
                         'cause': cause,
                         'data': data},
                        source_timestamp=source_timestamp)


def assert_time(timestamp, iec101_time):
    if timestamp is None:
        assert iec101_time is None
        return

    tz_local = datetime.datetime.now().astimezone().tzinfo
    time_dt = hat.event.common.timestamp_to_datetime(timestamp).astimezone(
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


def assert_connections_event(event, addresses):
    assert event.type == (*event_type_prefix, 'gateway', 'connections')
    assert [i['address'] for i in event.payload.data] == addresses


async def create_device(conf, eventer_client):
    return await aio.call(info.create, conf, eventer_client, event_type_prefix)


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

        async def drain(self):
            pass

        async def reset_input_buffer(self):
            return 0

    monkeypatch.setattr(serial, 'create', create)
    return conns


@pytest.fixture
async def create_master_conn(serial_conns):
    conf = get_conf()

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

    async def create_master_conn(address):
        conn = await master.connect(address)

        return iec101.MasterConnection(
            conn=conn,
            cause_size=iec101.CauseSize[conf['cause_size']],
            asdu_address_size=iec101.AsduAddressSize[
                conf['asdu_address_size']],
            io_address_size=iec101.IoAddressSize[conf['io_address_size']])

    try:
        yield create_master_conn

    finally:
        await master.async_close()


def test_device_type():
    assert info.type == 'iec101_slave'


def test_schema_validate():
    conf = get_conf()
    validator = json.DefaultSchemaValidator(info.json_schema_repo)
    validator.validate(info.json_schema_id, conf)


async def test_create(serial_conns):
    conf = get_conf()

    eventer_client = EventerClient()
    device = await create_device(conf, eventer_client)

    assert isinstance(device, common.Device)
    assert device.is_open

    await device.async_close()
    await eventer_client.async_close()


async def test_connections(serial_conns, create_master_conn):
    addresses = [i for i in range(10)]
    event_queue = aio.Queue()

    conf = get_conf(addresses)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await create_device(conf, eventer_client)

    event = await event_queue.get()
    assert_connections_event(event, [])

    for i, addr in enumerate(addresses):
        await create_master_conn(addr)

        event = await event_queue.get()
        assert_connections_event(event, addresses[:i+1])

    await device.async_close()
    await eventer_client.async_close()


async def test_keep_alive_timeout(serial_conns, create_master_conn):
    address = 123
    event_queue = aio.Queue()

    conf = get_conf([address], keep_alive_timeout=0.05)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await create_device(conf, eventer_client)

    event = await event_queue.get()
    assert_connections_event(event, [])

    await create_master_conn(address)

    event = await event_queue.get()
    assert_connections_event(event, [address])

    event = await event_queue.get()
    assert_connections_event(event, [])

    await device.async_close()
    await eventer_client.async_close()


async def test_query(serial_conns):
    query_queue = aio.Queue()

    conf = get_conf()

    def on_query(params):
        query_queue.put_nowait(params)
        return hat.event.common.QueryResult([], False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await create_device(conf, eventer_client)

    params = await query_queue.get()
    assert isinstance(params, hat.event.common.QueryLatestParams)
    assert params.event_types == [(*event_type_prefix, 'system', 'data', '*')]

    await device.async_close()
    await eventer_client.async_close()


async def test_interrogate(serial_conns, create_master_conn):
    address = 123
    asdu_address = 1
    data_count = 10

    conf = get_conf([address],
                    data=[{'data_type': 'SINGLE',
                           'asdu_address': asdu_address,
                           'io_address': i,
                           'buffer': None}
                          for i in range(data_count)])

    quality = {'invalid': False,
               'not_topical': False,
               'substituted': False,
               'blocked': False}
    query_events = [create_data_event('SINGLE', asdu_address, i,
                                      {'value': 'ON',
                                       'quality': quality})
                    for i in range(data_count)]

    def on_query(params):
        return hat.event.common.QueryResult(query_events, False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await create_device(conf, eventer_client)

    new_events = [create_data_event('SINGLE', asdu_address, i,
                                    {'value': 'OFF',
                                     'quality': quality})
                  for i in range(data_count // 2)]
    await aio.call(device.process_events, new_events)

    exp_gi_resp_events = new_events + query_events[data_count // 2:]

    conn = await create_master_conn(address)

    msg = iec101.InterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=1,
        request=0,
        is_negative_confirm=False,
        cause=iec101.CommandReqCause.ACTIVATION)
    await conn.send([msg])

    msgs = await conn.receive()
    gi_resp_msg = msgs[0]
    assert isinstance(gi_resp_msg, iec101.InterrogationMsg)
    assert gi_resp_msg.cause == iec101.CommandResCause.ACTIVATION_CONFIRMATION
    assert not gi_resp_msg.is_negative_confirm

    for data_event in exp_gi_resp_events:
        msgs = await conn.receive()
        gi_data_msg = msgs[0]
        assert isinstance(gi_data_msg, iec101.DataMsg)
        assert gi_data_msg.is_test == data_event.payload.data['is_test']
        assert gi_data_msg.cause == iec101.DataResCause.INTERROGATED_STATION
        assert gi_data_msg.data.value.name == \
            data_event.payload.data['data']['value']
        assert gi_data_msg.asdu_address == int(
            data_event.type[len(event_type_prefix) + 3])
        assert gi_data_msg.io_address == int(
            data_event.type[len(event_type_prefix) + 4])
        assert gi_data_msg.time is None

    msgs = await conn.receive()
    gi_resp_msg = msgs[0]
    assert isinstance(gi_resp_msg, iec101.InterrogationMsg)
    assert gi_resp_msg.cause == iec101.CommandResCause.ACTIVATION_TERMINATION
    assert not gi_resp_msg.is_negative_confirm

    await device.async_close()
    await eventer_client.async_close()


async def test_interrogate_deactivation(serial_conns, create_master_conn):
    address = 123

    conf = get_conf([address],
                    data=[{'data_type': 'SINGLE',
                           'asdu_address': 1,
                           'io_address': 1,
                           'buffer': None}])

    def on_query(params):
        events = [create_data_event('SINGLE', 1, 1,
                                    {'value': 'ON',
                                     'quality': {'invalid': False,
                                                 'not_topical': False,
                                                 'substituted': False,
                                                 'blocked': False}})]
        return hat.event.common.QueryResult(events, False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await create_device(conf, eventer_client)

    conn101 = await create_master_conn(address)

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
    assert msg.cause == iec101.CommandResCause.DEACTIVATION_CONFIRMATION
    assert msg.is_negative_confirm

    await device.async_close()
    await eventer_client.async_close()


async def test_counter_interrogate(serial_conns, create_master_conn):
    address = 123

    conf = get_conf([address],
                    data=[{'data_type': 'SINGLE',
                           'asdu_address': 1,
                           'io_address': 1,
                           'buffer': None},
                          {'data_type': 'BINARY_COUNTER',
                           'asdu_address': 1,
                           'io_address': 2,
                           'buffer': None}])

    non_counter_event = create_data_event('SINGLE', 1, 1,
                                          {'value': 'ON',
                                           'quality': {'invalid': False,
                                                       'not_topical': False,
                                                       'substituted': False,
                                                       'blocked': False}})
    counter_event = create_data_event('BINARY_COUNTER', 1, 2,
                                      {'value': 123,
                                       'quality': {'invalid': False,
                                                   'adjusted': False,
                                                   'overflow': False,
                                                   'sequence': False}})

    def on_query(params):
        events = [non_counter_event, counter_event]
        return hat.event.common.QueryResult(events, False)

    eventer_client = EventerClient(query_cb=on_query)
    device = await create_device(conf, eventer_client)

    conn101 = await create_master_conn(address)

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
    assert data_msg.asdu_address == int(
        counter_event.type[len(event_type_prefix) + 3])
    assert data_msg.io_address == int(
        counter_event.type[len(event_type_prefix) + 4])
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
@pytest.mark.parametrize("source_timestamp", [hat.event.common.now(), None])
async def test_data(serial_conns, create_master_conn, data_type, event_data,
                    exp_data, source_timestamp):
    if data_type in ['PROTECTION',
                     'PROTECTION_START',
                     'PROTECTION_COMMAND'] and source_timestamp is None:
        return

    if data_type == 'STATUS' and source_timestamp:
        return

    address = 123

    conf = get_conf([address],
                    data=[{'data_type': data_type,
                           'asdu_address': 12,
                           'io_address': 34,
                           'buffer': None}])

    eventer_client = EventerClient()
    device = await create_device(conf, eventer_client)

    conn101 = await create_master_conn(address)

    data_event = create_data_event(data_type, 12, 34, event_data,
                                   source_timestamp=source_timestamp)
    await aio.call(device.process_events, [data_event])

    data_msgs = await conn101.receive()
    data_msg = data_msgs[0]

    assert isinstance(data_msg, iec101.DataMsg)
    assert data_msg.is_test == data_event.payload.data['is_test']
    assert data_msg.cause == iec101.DataResCause.SPONTANEOUS
    assert data_msg.asdu_address == int(
        data_event.type[len(event_type_prefix) + 3])
    assert data_msg.io_address == int(
        data_event.type[len(event_type_prefix) + 4])
    if data_type in ['NORMALIZED', 'FLOATING']:
        assert math.isclose(event_data['value'], data_msg.data.value.value,
                            rel_tol=1e-3)
        assert_quality(event_data['quality'], data_msg.data.quality)
    else:
        assert data_msg.data == exp_data
    assert_time(data_event.source_timestamp, data_msg.time)

    await device.async_close()
    await eventer_client.async_close()


async def test_data_bulk(serial_conns, create_master_conn):
    events_count = 10
    address = 123

    conf = get_conf([address],
                    data=[{'data_type': 'SINGLE',
                           'asdu_address': 1,
                           'io_address': i,
                           'buffer': None}
                          for i in range(events_count)])

    eventer_client = EventerClient()
    device = await create_device(conf, eventer_client)

    conn101 = await create_master_conn(address)

    data_events = [create_data_event('SINGLE', 1, i,
                                     {'value': 'ON',
                                      'quality': {'invalid': False,
                                                  'not_topical': False,
                                                  'substituted': False,
                                                  'blocked': False}})
                   for i in range(events_count)]
    await aio.call(device.process_events, data_events)

    data_msgs = []
    for _ in range(events_count):
        data_msgs.extend(await conn101.receive())

    assert len(data_msgs) == len(data_events)
    for data_msg, data_event in zip(data_msgs, data_events):
        assert isinstance(data_msg, iec101.DataMsg)
        assert data_msg.is_test == data_event.payload.data['is_test']
        assert data_msg.cause == iec101.DataResCause.SPONTANEOUS
        assert data_msg.data.value.name == \
            data_event.payload.data['data']['value']
        assert data_msg.asdu_address == int(
            data_event.type[len(event_type_prefix) + 3])
        assert data_msg.io_address == int(
            data_event.type[len(event_type_prefix) + 4])
        assert data_msg.time is None
        # TODO check quality
        # TODO check time

    await device.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize("command_type, command, command_json", [
    ('SINGLE',
     iec101.SingleCommand(value=iec101.SingleValue.ON,
                          select=False,
                          qualifier=14),
     {'value': 'ON',
      'select': False,
      'qualifier': 14}),

    ('DOUBLE',
     iec101.DoubleCommand(value=iec101.DoubleValue.FAULT,
                          select=True,
                          qualifier=11),
     {'value': 'FAULT',
      'select': True,
      'qualifier': 11}),

    ('REGULATING',
     iec101.RegulatingCommand(value=iec101.RegulatingValue.HIGHER,
                              select=False,
                              qualifier=2),
     {'value': 'HIGHER',
      'select': False,
      'qualifier': 2}),

    ('NORMALIZED',
     iec101.NormalizedCommand(value=iec101.NormalizedValue(0.5),
                              select=True),
     {'value': 0.5,
      'select': True}),

    ('SCALED',
     iec101.ScaledCommand(value=iec101.ScaledValue(42),
                          select=False),
     {'value': 42,
      'select': False}),

    ('FLOATING',
     iec101.FloatingCommand(value=iec101.FloatingValue(42.5),
                            select=True),
     {'value': 42.5,
      'select': True}),

    ('BITSTRING',
     iec101.BitstringCommand(value=iec101.BitstringValue(b'\x01\x02\x03\x04')),
     {'value': [1, 2, 3, 4]})
    ])
@pytest.mark.parametrize("negative_resp", [True, False])
async def test_command(serial_conns, create_master_conn,
                       command_type, command, command_json, negative_resp):
    event_queue = aio.Queue()
    address = 123

    conf = get_conf([address])

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await create_device(conf, eventer_client)

    conn101 = await create_master_conn(address)

    await event_queue.get_until_empty()

    asdu = 1
    io = 2
    cmd_msg = iec101.CommandMsg(is_test=False,
                                originator_address=0,
                                asdu_address=asdu,
                                io_address=io,
                                command=command,
                                is_negative_confirm=False,
                                cause=iec101.CommandReqCause.ACTIVATION)
    await conn101.send([cmd_msg])

    cmd_req_event = await event_queue.get()
    assert cmd_req_event.type == (
        *event_type_prefix, 'gateway', 'command', command_type.lower(),
        str(asdu), str(io))
    conn_id = cmd_req_event.payload.data['connection_id']
    assert isinstance(cmd_req_event.payload.data['connection_id'], int)
    assert len(cmd_req_event.payload.data) == 4
    assert cmd_req_event.payload.data['is_test'] == cmd_msg.is_test
    assert cmd_req_event.payload.data['cause'] == cmd_msg.cause.name
    assert cmd_req_event.payload.data['connection_id'] == conn_id
    if command_type == 'NORMALIZED':
        command_event = cmd_req_event.payload.data['command']
        assert math.isclose(command_event['value'],
                            command_json['value'],
                            rel_tol=1e-3)
        assert command_event['select'] == command_json['select']
    else:
        assert cmd_req_event.payload.data['command'] == command_json

    cmd_res_payload = {
        'connection_id': conn_id,
        'is_test': cmd_msg.is_test,
        'is_negative_confirm': negative_resp,
        'cause': 'ACTIVATION_CONFIRMATION',
        'command': command_json}
    cmd_res_event = create_event(
        (*event_type_prefix, 'system', 'command', command_type.lower(),
            str(asdu), str(io)),
        cmd_res_payload)
    await aio.call(device.process_events, [cmd_res_event])

    msgs = await conn101.receive()
    assert len(msgs) == 1
    cmd_res_msg = msgs[0]
    assert isinstance(cmd_res_msg, iec101.CommandMsg)
    assert cmd_res_msg.is_test == cmd_res_payload['is_test']
    assert cmd_res_msg.asdu_address == asdu
    assert cmd_res_msg.io_address == io
    assert cmd_res_msg.is_negative_confirm == negative_resp
    assert cmd_res_msg.cause.name == cmd_res_payload['cause']
    if command_type == 'NORMALIZED':
        assert math.isclose(cmd_res_msg.command.value.value,
                            command.value.value, rel_tol=1e-3)
    else:
        assert cmd_res_msg.command == command

    await device.async_close()
    await eventer_client.async_close()


async def test_command_wrong_conn_id(serial_conns, create_master_conn):
    event_queue = aio.Queue()
    address = 123

    conf = get_conf([address])

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await create_device(conf, eventer_client)

    conn101 = await create_master_conn(address)

    await event_queue.get_until_empty()

    asdu = 1
    io = 2
    command = iec101.SingleCommand(value=iec101.SingleValue.ON,
                                   select=False,
                                   qualifier=14)
    cmd_msg = iec101.CommandMsg(is_test=False,
                                originator_address=0,
                                asdu_address=asdu,
                                io_address=io,
                                command=command,
                                is_negative_confirm=False,
                                cause=iec101.CommandReqCause.ACTIVATION)
    await conn101.send([cmd_msg])

    cmd_req_event = await event_queue.get()
    conn_id = cmd_req_event.payload.data['connection_id']

    cmd_res_payload = {
        'connection_id': conn_id + 1,
        'is_test': cmd_msg.is_test,
        'is_negative_confirm': False,
        'cause': 'ACTIVATION_CONFIRMATION',
        'command': {'value': 'ON',
                    'select': False,
                    'qualifier': 14}}
    cmd_res_event = create_event(
        (*event_type_prefix, 'system', 'command', 'single',
            str(asdu), str(io)),
        cmd_res_payload)
    await aio.call(device.process_events, [cmd_res_event])

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(conn101.receive(), 0.1)

    await device.async_close()
    await eventer_client.async_close()


async def test_buffer(serial_conns, create_master_conn):
    address = 123

    conf = get_conf([address],
                    keep_alive_timeout=0.05,
                    buffers=[{'name': 'b1', 'size': 100}],
                    data=[{'data_type': 'DOUBLE',
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
                           'buffer': None}])

    eventer_client = EventerClient()
    device = await create_device(conf, eventer_client)

    data_events = []
    buffered_data_events = []
    for d_conf in conf['data']:
        for value in ['INTERMEDIATE', 'OFF', 'ON', 'FAULT']:
            data_event = create_data_event(
                d_conf['data_type'], d_conf['asdu_address'],
                d_conf['io_address'], {'value': value,
                                       'quality': {'invalid': False,
                                                   'not_topical': False,
                                                   'substituted': False,
                                                   'blocked': False}},
                source_timestamp=hat.event.common.now())
            data_events.append(data_event)
            if d_conf['buffer']:
                buffered_data_events.append(data_event)
    await aio.call(device.process_events, data_events)

    conn = await create_master_conn(address)

    for data_event in buffered_data_events:
        msgs = await conn.receive()
        data_msg = msgs[0]
        assert data_msg.is_test == data_event.payload.data['is_test']
        assert data_msg.cause.name == data_event.payload.data['cause']
        assert data_msg.asdu_address == int(
            data_event.type[len(event_type_prefix) + 3])
        assert data_msg.io_address == int(
            data_event.type[len(event_type_prefix) + 4])
        assert data_msg.data.value.name == \
            data_event.payload.data['data']['value']
        assert_quality(data_event.payload.data['data']['quality'],
                       data_msg.data.quality)
        assert_time(data_event.source_timestamp, data_msg.time)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(conn.receive(), 0.01)

    # check buffer is empty for another master
    conn2 = await create_master_conn(address)
    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(conn2.receive(), 0.01)

    await device.async_close()
    await eventer_client.async_close()


async def test_buffer_size(serial_conns, create_master_conn):
    address = 123

    conf = get_conf([address],
                    buffers=[{'name': 'b1', 'size': 100},
                             {'name': 'b2', 'size': 25}],
                    data=[{'data_type': 'DOUBLE',
                           'asdu_address': 1,
                           'io_address': 2,
                           'buffer': 'b1'},
                          {'data_type': 'DOUBLE',
                           'asdu_address': 1,
                           'io_address': 3,
                           'buffer': 'b2'}])

    eventer_client = EventerClient()
    device = await create_device(conf, eventer_client)

    data_events = []
    buffered_events = {'b1': [], 'b2': []}
    for data_conf in conf['data']:
        for i in range(100):
            for value in ['INTERMEDIATE', 'OFF', 'ON', 'FAULT']:
                data_event = create_data_event(
                    data_conf['data_type'], data_conf['asdu_address'],
                    data_conf['io_address'], {
                        'value': value,
                        'quality': {'invalid': bool(i % 2),
                                    'not_topical': bool(i % 2),
                                    'substituted': bool(i % 2),
                                    'blocked': bool(i % 2)}},
                    source_timestamp=hat.event.common.now())
                data_events.append(data_event)
                buffered_events[data_conf['buffer']].append(data_event)
    await aio.call(device.process_events, data_events)

    conn = await create_master_conn(address)

    # ordered notification of data from buffers is assumed
    exp_buffered_events = itertools.chain(buffered_events['b1'][-100:],
                                          buffered_events['b2'][-25:])
    for data_event in exp_buffered_events:
        msgs = await conn.receive()
        data_msg = msgs[0]
        assert data_msg.is_test == data_event.payload.data['is_test']
        assert data_msg.cause.name == data_event.payload.data['cause']
        assert data_msg.asdu_address == int(
            data_event.type[len(event_type_prefix) + 3])
        assert data_msg.io_address == int(
            data_event.type[len(event_type_prefix) + 4])
        assert data_msg.data.value.name == \
            data_event.payload.data['data']['value']
        assert_quality(data_event.payload.data['data']['quality'],
                       data_msg.data.quality)
        assert_time(data_event.source_timestamp, data_msg.time)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(conn.receive(), 0.01)

    await device.async_close()
    await eventer_client.async_close()


async def test_data_on_multi_masters(serial_conns, create_master_conn):
    masters_count = 5
    addresses = list(range(masters_count))

    conf = get_conf(addresses,
                    data=[{'data_type': 'DOUBLE',
                           'asdu_address': 1,
                           'io_address': 2,
                           'buffer': None}])

    eventer_client = EventerClient()
    device = await create_device(conf, eventer_client)

    master_conns = []
    for addr in addresses:
        conn = await create_master_conn(addr)
        master_conns.append(conn)

    data_event = create_data_event('DOUBLE', 1, 2,
                                   {'value': 'INTERMEDIATE',
                                    'quality': {'invalid': False,
                                                'not_topical': True,
                                                'substituted': False,
                                                'blocked': False}})
    await aio.call(device.process_events, [data_event])

    msgs = await master_conns[0].receive()
    data_msg = msgs[0]
    for master_conn in master_conns[1:]:
        msgs = await master_conn.receive()
        assert msgs[0] == data_msg

    await device.async_close()
    await eventer_client.async_close()


async def test_command_on_multi_masters(serial_conns, create_master_conn):
    masters_count = 3
    event_queue = aio.Queue()
    addresses = list(range(masters_count))

    conf = get_conf(addresses)

    eventer_client = EventerClient(event_cb=event_queue.put_nowait)
    device = await create_device(conf, eventer_client)

    master_conns = []
    for addr in addresses:
        conn = await create_master_conn(addr)
        master_conns.append(conn)

    conns_event = await event_queue.get_until_empty()
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

    cmd_req_event = await event_queue.get()
    assert cmd_req_event.type == (
        *event_type_prefix, 'gateway', 'command', 'single', str(asdu), str(io))
    assert cmd_req_event.payload.data['connection_id'] == master_conn1_conn_id

    await asyncio.sleep(0.01)
    assert event_queue.empty()

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
    await aio.call(device.process_events, [cmd_res_event])

    msgs = await master_conn1.receive()
    assert len(msgs) == 1
    cmd_res_msg = msgs[0]
    assert isinstance(cmd_res_msg, iec101.CommandMsg)

    for master_conn in master_conns[1:]:
        with pytest.raises(asyncio.TimeoutError):
            await aio.wait_for(master_conn.receive(), 0.01)

    await device.async_close()
    await eventer_client.async_close()
