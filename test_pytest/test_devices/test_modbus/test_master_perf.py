import asyncio
import atexit
import itertools
import subprocess
import sys
import time

import pytest

from hat import aio
from hat.drivers import modbus
import hat.event.common

from hat.gateway.devices.modbus.master import info


pytestmark = [pytest.mark.skipif(sys.platform == 'win32',
                                 reason="can't simulate serial"),
              pytest.mark.perf]


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


@pytest.fixture
def nullmodem(request, tmp_path):
    path1 = tmp_path / '1'
    path2 = tmp_path / '2'
    p = subprocess.Popen(
        ['socat', '-d0',
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


def create_event(event_type, payload_data):
    return hat.event.common.Event(
        id=next(next_event_ids),
        type=event_type,
        timestamp=hat.event.common.now(),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload_data))


def create_remote_device_enable_event(device_id, enable):
    return create_event((*event_type_prefix, 'system', 'remote_device',
                         str(device_id), 'enable'),
                        enable)


@pytest.mark.parametrize('remote_device_count', [50])
@pytest.mark.parametrize('data_count', [1000])
@pytest.mark.parametrize('interval', [0.1])
@pytest.mark.parametrize('duration', [10])
async def test_read(profile, nullmodem, conn_conf, remote_device_count,
                    data_count, interval, duration):
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

    def on_query(params):
        events = [create_remote_device_enable_event(device_id, True)
                  for device_id in range(1, remote_device_count + 1)]

        return hat.event.common.QueryResult(events, False)

    async def on_read(slave, device_id, _data_type, start_address, quantity):
        return list(range(quantity))

    eventer_client = EventerClient(query_cb=on_query)
    slave = await modbus.create_serial_slave(modbus_type=modbus.ModbusType.RTU,
                                             port=nullmodem[0],
                                             read_cb=on_read,
                                             silent_interval=0)

    with profile(f"remote_device_count_{remote_device_count}_"
                 f"data_count_{data_count}_"
                 f"interval_{interval}_"
                 f"duration_{duration}"):
        device = await aio.call(info.create, conf, eventer_client,
                                event_type_prefix)
        await asyncio.sleep(duration)

    await device.async_close()
    await slave.async_close()
    await eventer_client.async_close()
