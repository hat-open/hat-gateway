import collections
import pytest

from hat import aio

from hat.drivers import serial
from hat.drivers.iec60870 import link
from hat.gateway import common
from hat.gateway.devices.iec101 import slave


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name, slave.device_type, device_name)


class EventerClient(common.DeviceEventClient):

    def __init__(self, receive_queue=None, register_queue=None,
                 query_queue=None, query_cb=[]):
        self._receive_queue = (receive_queue if receive_queue is not None
                               else aio.Queue())
        self._register_queue = register_queue
        self._query_cb = query_cb
        self._query_queue = query_queue
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
        if self._query_queue:
            self._query_queue.put_nowait(data)
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
