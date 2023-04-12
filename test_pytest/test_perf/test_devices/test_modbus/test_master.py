import asyncio
import itertools
import sys
import time
import subprocess
import atexit

import pytest

from hat import aio
from hat.drivers import modbus
from hat.gateway import common
from hat.gateway.devices.modbus import master
import hat.event.common


pytestmark = [pytest.mark.skipif(sys.platform == 'win32',
                                 reason="can't simulate serial"),
              pytest.mark.perf]


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

    @property
    def async_group(self):
        return self._async_group

    @property
    def receive_queue(self):
        return self._receive_queue

    async def receive(self):
        try:
            return await self._receive_queue.get()
        except aio.QueueClosedError:
            raise ConnectionError()

    def register(self, events):
        pass

    async def register_with_response(self, events):
        raise Exception('should not be used')

    async def query(self, data):
        return self._query_result


@pytest.fixture
def nullmodem(request, tmp_path):
    path1 = tmp_path / '1'
    path2 = tmp_path / '2'
    p = subprocess.Popen(
        ['socat',
         f'pty,link={path1},raw,echo=0',
         f'pty,link={path2},raw,echo=0'])
    while not path1.exists() or not path2.exists():
        time.sleep(0.001)

    def finalizer():
        p.terminate()

    atexit.register(finalizer)
    request.addfinalizer(finalizer)
    return str(path1), str(path2), p


@pytest.fixture
def conn_conf(nullmodem):
    return {'modbus_type': 'RTU',
            'transport': {'type': 'SERIAL',
                          'port': nullmodem[1],
                          'baudrate': 9600,
                          'bytesize': 'EIGHTBITS',
                          'parity': 'NONE',
                          'stopbits': 'ONE',
                          'flow_control': {'xonxoff': False,
                                           'rtscts': False,
                                           'dsrdtr': False},
                          'silent_interval': 0},
            'connect_timeout': 1,
            'connect_delay': 0,
            'request_timeout': 1,
            'request_delay': 0,
            'request_retry_immediate_count': 1,
            'request_retry_delayed_count': 1,
            'request_retry_delay': 0}


@pytest.fixture
def create_event():
    counter = itertools.count(1)

    def create_event(event_type, payload_data):
        event_id = hat.event.common.EventId(1, 1, next(counter))
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

    def create_remote_device_enable_event(device_id, enable):
        return create_event((*event_type_prefix, 'system', 'remote_device',
                             str(device_id), 'enable'),
                            enable)

    return create_remote_device_enable_event


@pytest.mark.parametrize('remote_device_count', [50])
@pytest.mark.parametrize('data_count', [1000])
@pytest.mark.parametrize('interval', [0.1])
@pytest.mark.parametrize('duration', [10])
async def test_read(profile, nullmodem, conn_conf,
                    create_remote_device_enable_event, remote_device_count,
                    data_count, interval, duration):

    async def on_read(slave, device_id, _data_type, start_address, quantity):
        return list(range(quantity))

    slave = await modbus.create_serial_slave(modbus_type=modbus.ModbusType.RTU,
                                             port=nullmodem[0],
                                             read_cb=on_read,
                                             silent_interval=0)

    conf = {
        'name': 'name',
        'connection': conn_conf,
        'remote_devices': [
            {'device_id': device_id,
             'data': [
                {'name': 'data',
                 'interval': interval,
                 'data_type': 'HOLDING_REGISTER',
                 'start_address': start_address,
                 'bit_offset': 0,
                 'bit_count': 32}
                for start_address in range(1, data_count + 1)]}
            for device_id in range(1, remote_device_count + 1)]}

    event_client = EventClient([
        create_remote_device_enable_event(device_id, True)
        for device_id in range(1, remote_device_count + 1)])

    with profile(f"remote_device_count_{remote_device_count}_"
                 f"data_count_{data_count}_"
                 f"interval_{interval}_"
                 f"duration_{duration}"):
        device = await aio.call(master.create, conf, event_client,
                                event_type_prefix)
        await asyncio.sleep(duration)

    await device.async_close()
    await slave.async_close()
    await event_client.async_close()
