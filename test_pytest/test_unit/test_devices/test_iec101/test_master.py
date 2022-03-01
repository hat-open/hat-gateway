import collections
import itertools

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
            'asdu_address_size': 'TWO',
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


def assert_status_event(event, status, address=None):
    if address is None:
        assert event.event_type == (*event_type_prefix, 'gateway', 'status')
    else:
        assert event.event_type == (*event_type_prefix, 'gateway',
                                    'remote_device', str(address), 'status')
    assert event.source_timestamp is None
    assert event.payload.data == status


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

    def create_command_event(address, asdu_addr, io_address, payload):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(address), 'counter_interrogation',
                             str(asdu_addr)),
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
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("interrogation_request", [42])
@pytest.mark.parametrize("freeze", list(iec101.FreezeCode))
async def test_interrogation_request(serial_conns, create_enable_event,
                                     create_interrogation_event,
                                     create_counter_interrogation_event,
                                     address, asdu_address,
                                     interrogation_request,
                                     freeze):
    query_result = [create_enable_event(address, True)]
    event_client = EventClient(query_result)
    conf = get_conf([address])
    conn_queue = aio.Queue()
    slave = await create_slave(conf, conn_queue.put_nowait)
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)
    conn = await conn_queue.get()

    event = create_interrogation_event(address, asdu_address,
                                       interrogation_request)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg == iec101.InterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=asdu_address,
        request=interrogation_request,
        cause=iec101.CommandReqCause.ACTIVATION)

    event = create_counter_interrogation_event(
        address, asdu_address, interrogation_request, freeze.name)
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg == iec101.CounterInterrogationMsg(
        is_test=False,
        originator_address=0,
        asdu_address=asdu_address,
        request=interrogation_request,
        freeze=freeze,
        cause=iec101.CommandReqCause.ACTIVATION)

    await device.async_close()
    await slave.async_close()
    await event_client.async_close()


@pytest.mark.parametrize("address", [0])
@pytest.mark.parametrize("asdu_address", [123])
@pytest.mark.parametrize("io_address", [321])
@pytest.mark.parametrize("cause", list(iec101.CommandReqCause))
@pytest.mark.parametrize("command, payload", [
])
async def test_command_request(serial_conns, create_enable_event,
                               create_command_event, address, asdu_address,
                               io_address, cause, command, payload):
    query_result = [create_enable_event(address, True)]
    event_client = EventClient(query_result)
    conf = get_conf([address])
    conn_queue = aio.Queue()
    slave = await create_slave(conf, conn_queue.put_nowait)
    device = await aio.call(master.create, conf, event_client,
                            event_type_prefix)
    conn = await conn_queue.get()

    event = create_command_event(address, asdu_address, io_address,
                                 {'cause': cause.name, **payload})
    event_client.receive_queue.put_nowait([event])

    msgs = await conn.receive()
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg == iec101.CommandMsg(
        is_test=False,
        originator_address=0,
        asdu_address=asdu_address,
        io_address=io_address,
        command=command,
        is_negative_confirm=False,
        cause=cause)

    await device.async_close()
    await slave.async_close()
    await event_client.async_close()
