import collections
import datetime
import itertools
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
        "addresses": [123, 235],
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


async def create_master(conf):
    return await link.unbalanced.create_master(
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


def assert_quality(event_quality, msg_quality):
    for quality_param in event_quality:
        assert getattr(msg_quality, quality_param) == \
            event_quality[quality_param]


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


async def test_connections(conf, serial_conns):
    conf = {**conf, 'keep_alive_timeout': 0.05}
    register_queue = aio.Queue()

    eventer_client = EventerClient(register_queue=register_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == (*event_type_prefix,
                                     'gateway', 'connections')
    assert len(conn_event.payload.data) == 0

    master = await create_master(conf)
    addr = conf['addresses'][0]
    conn = await master.connect(addr)

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == (*event_type_prefix,
                                     'gateway', 'connections')
    assert len(conn_event.payload.data) == 1
    assert conn_event.payload.data[0]['address'] == addr
    conn1_id = conn_event.payload.data[0]['connection_id']

    conn.close()

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == (*event_type_prefix,
                                     'gateway', 'connections')
    assert len(conn_event.payload.data) == 0

    conn = await master.connect(addr)

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == (*event_type_prefix,
                                     'gateway', 'connections')
    assert len(conn_event.payload.data) == 1
    assert conn_event.payload.data[0]['address'] == addr
    assert conn_event.payload.data[0]['connection_id'] != conn1_id

    master.close()

    events = await register_queue.get()
    assert len(events) == 1
    conn_event = events[0]
    assert conn_event.event_type == (*event_type_prefix,
                                     'gateway', 'connections')
    assert len(conn_event.payload.data) == 0

    await master.async_close()
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


async def test_interrogate(conf, serial_conns):
    data_conf = [{
        'data_type': 'SINGLE',
        'asdu_address': 1,
        'io_address': i,
        'buffer': None,
    } for i in range(10)]
    conf = json.set_(conf, ['data'], data_conf)
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

    def on_query(data):
        return data_events

    eventer_client = EventerClient(query_cb=on_query)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    master = await create_master(conf)
    addr = conf['addresses'][0]
    conn = await master.connect(addr)

    conn101 = iec101.Connection(
        conn=conn,
        cause_size=iec101.CauseSize[conf['cause_size']],
        asdu_address_size=iec101.AsduAddressSize[conf['asdu_address_size']],  # NOQA
        io_address_size=iec101.IoAddressSize[conf['io_address_size']])

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

    # TODO receive all msgs as bulk
    data_msgs = []
    for _ in range(len(data_events)):
        msgs = await conn101.receive()
        data_msgs.extend(msgs)

    assert len(data_msgs) == len(data_events)
    for gi_data_msg, data_event in zip(data_msgs, data_events):
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

    await master.async_close()
    await device.async_close()
    await eventer_client.async_close()


@pytest.mark.parametrize("data_type, value, exp_value, quality", [
    ('SINGLE', 'ON', iec101.SingleValue.ON,
        {'invalid': False,
         'not_topical': False,
         'substituted': False,
         'blocked': False}),
    ('DOUBLE', 'INTERMEDIATE', iec101.DoubleValue.INTERMEDIATE,
        {'invalid': True,
         'not_topical': False,
         'substituted': True,
         'blocked': False}),
    ('STEP_POSITION', {'value': 13, 'transient': False},
        iec101.StepPositionValue(value=13, transient=False),
        {'invalid': False,
         'not_topical': False,
         'substituted': False,
         'blocked': False,
         'overflow': False})

    ])
@pytest.mark.parametrize("source_ts", [hat.event.common.now(), None])
async def test_data(conf, serial_conns, data_type, value, exp_value,
                    quality, source_ts):
    data_conf = {
        'data_type': data_type,
        'asdu_address': 12,
        'io_address': 34,
        'buffer': None}
    conf = json.set_(conf, ['data'], [data_conf])

    receive_queue = aio.Queue()

    eventer_client = EventerClient(receive_queue=receive_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    master = await create_master(conf)
    addr = conf['addresses'][0]
    conn = await master.connect(addr)

    conn101 = iec101.Connection(
        conn=conn,
        cause_size=iec101.CauseSize[conf['cause_size']],
        asdu_address_size=iec101.AsduAddressSize[conf['asdu_address_size']],
        io_address_size=iec101.IoAddressSize[conf['io_address_size']])

    data_event = create_event(
        (*event_type_prefix, 'system', 'data', data_type.lower(),
            str(data_conf['asdu_address']), str(data_conf['io_address'])),
        {'is_test': False,
         'cause': 'SPONTANEOUS',
         'data': {'value': value,
                  'quality': quality}},
        source_ts)
    receive_queue.put_nowait([data_event])

    data_msgs = await conn101.receive()
    data_msg = data_msgs[0]

    assert isinstance(data_msg, iec101.DataMsg)
    assert data_msg.is_test == data_event.payload.data['is_test']
    assert data_msg.cause == iec101.DataResCause.SPONTANEOUS
    assert data_msg.asdu_address == int(data_event.event_type[7])
    assert data_msg.io_address == int(data_event.event_type[8])
    assert data_msg.data.value == exp_value
    assert_quality(quality, data_msg.data.quality)
    assert_time(data_event.source_timestamp, data_msg.time)

    await master.async_close()
    await device.async_close()
    await eventer_client.async_close()


async def test_data_bulk(conf, serial_conns):
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

    master = await create_master(conf)
    addr = conf['addresses'][0]
    conn = await master.connect(addr)

    conn101 = iec101.Connection(
        conn=conn,
        cause_size=iec101.CauseSize[conf['cause_size']],
        asdu_address_size=iec101.AsduAddressSize[conf['asdu_address_size']],  # NOQA
        io_address_size=iec101.IoAddressSize[conf['io_address_size']])

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

    await master.async_close()
    await device.async_close()
    await eventer_client.async_close()


async def test_command(conf, serial_conns):
    register_queue = aio.Queue()
    receive_queue = aio.Queue()

    eventer_client = EventerClient(register_queue=register_queue,
                                   receive_queue=receive_queue)
    device = await slave.create(conf, eventer_client, event_type_prefix)

    master = await create_master(conf)
    addr = conf['addresses'][0]
    conn = await master.connect(addr)

    conn101 = iec101.Connection(
        conn=conn,
        cause_size=iec101.CauseSize[conf['cause_size']],
        asdu_address_size=iec101.AsduAddressSize[conf['asdu_address_size']],  # NOQA
        io_address_size=iec101.IoAddressSize[conf['io_address_size']])

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

    await master.async_close()
    await device.async_close()
    await eventer_client.async_close()
