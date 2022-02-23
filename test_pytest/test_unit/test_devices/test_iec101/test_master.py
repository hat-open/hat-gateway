import collections
import itertools

import pytest

from hat import aio
from hat.drivers import serial
from hat.drivers.iec60870 import iec101
from hat.drivers.iec60870 import link
from hat.gateway import common
from hat.gateway.devices.iec101 import master
import hat.event.common


gateway_name = 'gateway_name'
device_name = 'device_name'
event_type_prefix = ('gateway', gateway_name, master.device_type, device_name)


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


def get_conf(remote_addresses=[], reconnect_delay=0.01):
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
            'asdu_address_size': 'TWO',
            'io_address_size': 'THREE',
            'reconnect_delay': reconnect_delay,
            'remote_devices': [{'address': address,
                                'reconnect_delay': reconnect_delay}
                               for address in remote_addresses]}


async def create_slave(conf, connection_cb):

    async def on_connection(conn):
        conn = iec101.Connection(
            conn=conn,
            cause_size=iec101.CauseSize[conf['cause_size']],
            asdu_address_size=iec101.AsduAddressSize[conf['asdu_address_size']],
            io_address_size=iec101.IoAddressSize[conf['io_address_size']])
        await aio.call(connection_cb(conn))

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
def create_remote_device_enable_event(create_event):

    def create_remote_device_enable_event(device_addr, enable):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(device_addr), 'enable'),
                            enable)

    return create_remote_device_enable_event


@pytest.fixture
async def patch_serial(monkeypatch):
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
            self._async_group.spawn(aio.call_on_cancel, self._data.close())
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
                if conn is self and conn.is_open:
                    continue
                for i in data:
                    conn._data.put_nowait(i)

    monkeypatch.setattr(hat.drivers.serial, 'create', create)


@pytest.mark.parametrize("conf", [
    get_conf(remote_addresses=[1, 2, 3])
])
def test_valid_conf(conf):
    master.json_schema_repo.validate(master.json_schema_id, conf)


async def test_create(patch_serial):
    event_client = EventClient()
    conf = get_conf()
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)

    assert device.is_open

    await device.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("conn_count", [0, 1, 5])
async def test_connect(patch_serial, create_remote_device_enable_event,
                       conn_count):
    query_result = [create_remote_device_enable_event(i, True)
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