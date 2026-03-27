from collections.abc import Collection
import asyncio
import itertools
import logging
import uuid

from hat import aio
from hat.drivers import modbus
from hat.drivers import serial
from hat.drivers import tcp
import hat.event.common
import hat.event.eventer

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)


connection_timeout = 2
write_response_timeout = 10


async def create(conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'ModbusSlaveDevice':
    device = ModbusSlaveDevice()
    device._conf = conf
    device._device_ids = set(data['device_id'] for data in conf['data'])

    device._keep_alive_events = {}
    device._keep_alive_timeout = conf['transport']['keep_alive_timeout']

    device._eventer_client = eventer_client
    device._event_type_prefix = event_type_prefix

    device._next_conn_ids = itertools.count(1)
    device._conns = {}

    device._next_write_req_id = _unique_ids()
    device._write_reqs = {}

    device._async_group = aio.Group()
    device._loop = asyncio.get_running_loop()

    device._data = {data['name']: data for data in conf['data']}
    device._cache = {
        device_id: {dt: {} for dt in modbus.DataType}
        for device_id in device._device_ids}

    device._log = _create_logger_adapter(conf['name'])

    await device._init_cache()

    try:
        await device._register_connections()

        await device._connection_loop()

    except BaseException:
        await aio.uncancellable(device.async_close())
        raise

    return device


info: common.DeviceInfo = common.DeviceInfo(
    type="modbus_slave",
    create=create,
    json_schema_id="hat-gateway://modbus.yaml#/$defs/slave",
    json_schema_repo=common.json_schema_repo)


class ModbusSlaveDevice(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        sorted_events = _sort_events(events)
        for event in sorted_events:
            try:
                self._log.debug('received event: %s', event)
                await self._process_event(event)

            except Exception as e:
                self._log.warning('error processing event: %s', e, exc_info=e)

    async def _connection_loop(self):
        try:
            transport_conf = self._conf['transport']
            modbus_type = modbus.ModbusType[self._conf['modbus_type']]

            if transport_conf['type'] == 'TCP':
                slave_queue = aio.Queue()
                tcp_server = await modbus.create_tcp_server(
                    modbus_type=modbus_type,
                    addr=tcp.Address(transport_conf['local_host'],
                                     transport_conf['local_port']),
                    slave_cb=slave_queue.put_nowait,
                    request_cb=self._on_request)

                self._async_group.spawn(self._tcp_connection_loop, tcp_server,
                                        slave_queue)

            elif transport_conf['type'] == 'SERIAL':
                port = transport_conf['port']
                baudrate = transport_conf['baudrate']
                bytesize = serial.ByteSize[transport_conf['bytesize']]
                parity = serial.Parity[transport_conf['parity']]
                stopbits = serial.StopBits[transport_conf['stopbits']]
                xonxoff = transport_conf['flow_control']['xonxoff']
                rtscts = transport_conf['flow_control']['rtscts']
                dsrdtr = transport_conf['flow_control']['dsrdtr']
                silent_interval = transport_conf['silent_interval']

                conn = await modbus.create_serial_slave(
                    modbus_type=modbus_type,
                    port=port,
                    baudrate=baudrate,
                    bytesize=bytesize,
                    parity=parity,
                    stopbits=stopbits,
                    xonxoff=xonxoff,
                    rtscts=rtscts,
                    dsrdtr=dsrdtr,
                    request_cb=self._on_request,
                    silent_interval=silent_interval)

                self._async_group.spawn(self._serial_connection_loop, conn)

            else:
                raise ValueError('unsupported link type')

        except Exception as e:
            self._log.warning('connection loop error: %s', e, exc_info=e)

    async def _tcp_connection_loop(self, tcp_server, conn_queue):
        transport_conf = self._conf['transport']

        remote_hosts = transport_conf['remote_hosts']
        max_connections = transport_conf['max_connections']

        try:
            while True:
                try:
                    conn = await conn_queue.get()

                    remote_host = conn.info.remote_addr.host

                    if (remote_hosts is not None and
                            remote_host not in remote_hosts):
                        conn.close()
                        self._log.warning('connection closed: remote host %s '
                                          'not allowed', remote_host)
                        continue

                    if (max_connections is not None and
                            len(self._conns) >= max_connections):
                        conn.close()
                        self._log.debug('connection closed: max connections '
                                        'exceeded (remote host %s)',
                                        remote_host)
                        continue

                    self._async_group.spawn(self._on_tcp_connection, conn)

                except asyncio.CancelledError:
                    break

                except Exception as e:
                    self._log.error('tcp connection error: %s', e, exc_info=e)
                    await asyncio.sleep(connection_timeout)

        finally:
            if tcp_server:
                tcp_server.close()

    async def _on_tcp_connection(self, conn):
        conn_id = next(self._next_conn_ids)

        try:
            self._conns[conn] = conn_id
            await self._register_connections()

            if self._keep_alive_timeout is not None:
                self._keep_alive_events[conn] = asyncio.Event()
                self._async_group.spawn(self._keep_alive_loop, conn)

            await conn.wait_closed()

        except Exception as e:
            self._log.error('tcp connection error: %s', e, exc_info=e)

        finally:
            await self._cleanup_connection(conn)

    async def _serial_connection_loop(self, conn):
        while True:
            try:
                self._keep_alive_events[conn] = asyncio.Event()
                await aio.wait_for(self._keep_alive_events[conn].wait(),
                                   self._keep_alive_timeout)
                self._keep_alive_events[conn].clear()

                conn_id = next(self._next_conn_ids)
                self._conns[conn] = conn_id

                self._async_group.spawn(self._keep_alive_loop, conn)

                await self._register_connections()

                await conn.wait_closed()

            except TimeoutError:
                self._log.error('serial connection error: no communication')
                await asyncio.sleep(connection_timeout)
                continue

            except Exception as e:
                self._log.error('serial connection error: %s', e, exc_info=e)
                await asyncio.sleep(connection_timeout)
                continue

            finally:
                await self._cleanup_connection(conn)

    async def _keep_alive_loop(self, conn):
        try:
            while True:
                event = self._keep_alive_events.get(conn)
                if not event:
                    return

                await aio.wait_for(event.wait(), self._keep_alive_timeout)
                event.clear()

        except TimeoutError:
            self._log.warning('keep alive timeout')

        finally:
            await self._cleanup_connection(conn)

    async def _register_connections(self):
        if self._conf['transport']['type'] == 'TCP':
            payload = [{'type': 'TCP',
                        'connection_id': conn_id,
                        'local': {'host': conn.info.local_addr.host,
                                  'port': conn.info.local_addr.port},
                        'remote': {'host': conn.info.remote_addr.host,
                                   'port': conn.info.remote_addr.port}}
                       for conn, conn_id in self._conns.items()]

        elif self._conf['transport']['type'] == 'SERIAL':
            payload = [{'type': 'SERIAL',
                        'connection_id': conn_id}
                       for conn, conn_id in self._conns.items()]

        else:
            raise ValueError('unsupported transport type')

        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'connections'),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson(payload))

        await self._eventer_client.register([event])

    async def _cleanup_connection(self, conn):
        conn.close()

        if conn in self._conns:
            self._conns.pop(conn)
            await self._register_connections()

        event = self._keep_alive_events.pop(conn, None)
        if event:
            event.set()

    async def _process_event(self, event):
        suffix = event.type[len(self._event_type_prefix):]

        if suffix[:2] == ('system', 'data'):
            data_name = suffix[2]
            await self._process_data_event(event, data_name)

        elif suffix[:2] == ('system', 'write'):
            await self._process_write_event(event)

        else:
            raise Exception('unsupported event type')

    async def _process_data_event(self, event, data_name):
        if data_name not in self._data:
            raise Exception(f'data {data_name} not configured')

        self._write_event_value(data_name, event.payload.data['value'])

    async def _process_write_event(self, event):
        request_id = event.payload.data['request_id']
        req_future = self._write_reqs.pop(request_id, None)

        if not req_future:
            raise Exception('unrecognized request_id')

        req_future.set_result(event.payload.data['success'])

    async def _on_request(self, conn, request):
        device_id = request.device_id

        if device_id not in self._device_ids:
            self._log.debug('device %s not configured', device_id)

            if self._conf['transport']['type'] == 'TCP':
                return modbus.Error.INVALID_DATA_ADDRESS

            else:
                return

        event = self._keep_alive_events.get(conn)
        if event:
            event.set()

        if isinstance(request, modbus.ReadReq):
            return self._process_read_request(request)

        elif isinstance(request, modbus.WriteReq):
            return await self._process_write_request(request, conn)

        elif isinstance(request, modbus.WriteMaskReq):
            return await self._process_write_mask_request(request, conn)

        else:
            raise TypeError('unsupported request')

    def _process_read_request(self, request):
        cache = self._cache[request.device_id][request.data_type]

        quantity = (request.quantity
                    if request.data_type != modbus.DataType.QUEUE else 1)

        values = []
        for i in range(quantity):
            value = cache.get(request.start_address + i)
            if value is None:
                return modbus.Error.INVALID_DATA_ADDRESS

            values.append(value)

        return values

    async def _process_write_request(self, request, conn):
        if request.device_id == 0:
            self._log.warning('broadcast device_id 0 not supported')
            return modbus.Error.FUNCTION_ERROR

        data = []

        try:
            if request.data_type == modbus.DataType.COIL:
                data = self._write_coil_to_data(request)

            elif request.data_type == modbus.DataType.HOLDING_REGISTER:
                data = self._write_holding_register_to_data(request)

            else:
                raise Exception('invalid data type')

        except Exception as e:
            self._log.warning('write error: %s', e, exc_info=e)
            return modbus.Error.INVALID_DATA_ADDRESS

        if not data:
            return modbus.Success

        try:
            return await self._write_response(conn, data)

        except Exception as e:
            self._log.error('write error: %s', e, exc_info=e)
            return modbus.Error.FUNCTION_ERROR

    async def _process_write_mask_request(self, request, conn):
        if request.device_id == 0:
            self._log.warning('broadcast device_id 0 not supported')
            return modbus.Error.FUNCTION_ERROR

        data = []

        try:
            data = self._write_mask_to_data(request)

        except Exception as e:
            self._log.warning('write mask error: %s', e, exc_info=e)
            return modbus.Error.INVALID_DATA_ADDRESS

        if not data:
            return modbus.Success

        try:
            return await self._write_response(conn, data)

        except Exception as e:
            self._log.error('write mask error: %s', e, exc_info=e)
            return modbus.Error.FUNCTION_ERROR

    async def _write_response(self, conn, data):
        request_id = next(self._next_write_req_id)
        req_future = self._loop.create_future()
        self._write_reqs[request_id] = req_future

        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'write'),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson({
                'request_id': request_id,
                'connection_id': self._conns[conn],
                'data': data}))

        await self._eventer_client.register([event])

        try:
            res = await aio.wait_for(req_future, write_response_timeout)
            if not res:
                return modbus.Error.FUNCTION_ERROR

        except TimeoutError:
            self._write_reqs.pop(request_id)
            raise

        return modbus.Success

    def _write_coil_to_data(self, request):
        if any(v not in {0, 1} for v in request.values):
            raise ValueError('invalid write coil value')

        cache = self._cache[request.device_id][request.data_type]

        for address in range(request.start_address,
                             request.start_address + len(request.values)):
            if cache.get(address) is None:
                raise Exception(f'write on unconfigured address: {address}')

        return self._write_to_data(request.device_id, request.data_type,
                                   request.start_address, request.values)

    def _write_holding_register_to_data(self, request):
        if not all(isinstance(
                v, int) and 0 <= v <= 0xFFFF for v in request.values):
            raise ValueError('invalid write holding register value')

        cache = self._cache[request.device_id][request.data_type]

        for address in range(request.start_address,
                             request.start_address + len(request.values)):
            if cache.get(address) is None:
                raise Exception(f'write on unconfigured address: {address}')

        bit_values = [int(b) for v in request.values for b in f"{v:016b}"]

        return self._write_to_data(request.device_id, request.data_type,
                                   request.start_address, bit_values)

    def _write_mask_to_data(self, request):
        if not (0 <= request.and_mask <= 0xFFFF
                and 0 <= request.or_mask <= 0xFFFF):
            raise ValueError('invalid write mask values')

        data_type = modbus.DataType.HOLDING_REGISTER
        cache = self._cache[request.device_id][data_type]

        if cache.get(request.address) is None:
            raise Exception(
                f'write mask on unconfigured address: {request.address}')

        prev = cache.get(request.address, 0)
        value = modbus.apply_mask(prev, request.and_mask, request.or_mask)
        bit_values = [int(b) for b in f"{value:016b}"]

        data = self._write_to_data(request.device_id, data_type,
                                   request.address, bit_values)

        for d in list(data):
            conf = self._data[d['name']]
            data_start = conf['start_address'] * 16 + conf['bit_offset']
            bit_count = conf['bit_count']
            data_end = data_start + bit_count

            overlap_start = max(data_start, request.address * 16)
            overlap_end = min(data_end, request.address * 16 + 16)

            for bit_address in range(overlap_start, overlap_end):
                if not ((request.and_mask >> 15 - bit_address) & 1):
                    break

            else:
                data.remove(d)

        return data

    def _write_to_data(self, device_id, data_type, start_address, values):
        register_size = 1 if data_type == modbus.DataType.COIL else 16

        req_bit_start = start_address * register_size
        req_bit_end = req_bit_start + len(values)

        data = []

        for name, conf in self._data.items():
            conf_data_type = modbus.DataType[conf['data_type']]

            if conf_data_type != data_type or conf['device_id'] != device_id:
                continue

            data_bit_start = (conf['start_address'] * register_size +
                              conf['bit_offset'])
            data_bit_end = data_bit_start + conf['bit_count']
            bit_count = conf['bit_count']

            overlap_start = max(data_bit_start, req_bit_start)
            overlap_end = min(data_bit_end, req_bit_end)

            if overlap_start >= overlap_end:
                continue

            value = 0
            for i in range(bit_count):
                bit_address = data_bit_start + i
                bit = self._get_bit(device_id, conf_data_type, bit_address)
                value |= bit << (bit_count - 1 - i)

            for bit_address in range(overlap_start, overlap_end):
                bit_index_req = bit_address - req_bit_start
                bit = values[bit_index_req]

                data_offset = bit_address - data_bit_start
                shift = bit_count - 1 - data_offset

                mask = 1 << shift
                value &= ~mask
                value |= bit << shift

            data.append({'name': name,
                         'value': value})

        return data

    async def _init_cache(self):
        event_types = [(*self._event_type_prefix, 'system', 'data', '*')]
        params = hat.event.common.QueryLatestParams(event_types)
        result = await self._eventer_client.query(params)

        data = {data['name']: data for data in self._conf['data']}
        for data_name in data:
            self._write_event_value(data_name, 0)

        events = _sort_events(result.events)
        for event in events:
            try:
                data_name = event.type[len(self._event_type_prefix) + 2]

                if data_name not in data:
                    raise Exception(f'data {data_name} not configured')

                self._write_event_value(data_name, event.payload.data['value'])

            except Exception as e:
                self._log.debug('skipping initial data: %s', e, exc_info=e)

    def _write_event_value(self, data_name, value):
        conf = self._data[data_name]
        data_type = modbus.DataType[conf['data_type']]
        device_id = conf['device_id']

        bit_count = conf['bit_count']

        if (not isinstance(value, int) or value < 0
                or value >> bit_count):
            self._log.warning('invalid data value for %s: %s',
                              data_name, value)
            return

        register_size = (1 if data_type in {modbus.DataType.COIL,
                                            modbus.DataType.DISCRETE_INPUT}
                         else 16)

        bit_start = conf['start_address'] * register_size + conf['bit_offset']

        for i in range(bit_count):
            bit = (value >> (bit_count - 1 - i)) & 1
            bit_address = bit_start + i

            self._set_bit(device_id, data_type, bit_address, bit)

    def _get_bit(self, device_id, data_type, bit_address):
        if data_type == modbus.DataType.COIL:
            return self._cache[device_id][data_type][bit_address]

        elif data_type == modbus.DataType.HOLDING_REGISTER:
            reg_index = bit_address // 16
            reg_bit = 15 - (bit_address % 16)

            reg_val = self._cache[device_id][data_type][reg_index]
            return (reg_val >> reg_bit) & 1

        else:
            return 0

    def _set_bit(self, device_id, data_type, bit_address, bit):
        if data_type in {modbus.DataType.COIL,
                         modbus.DataType.DISCRETE_INPUT}:
            self._cache[device_id][data_type][bit_address] = bit

        elif data_type in {modbus.DataType.HOLDING_REGISTER,
                           modbus.DataType.INPUT_REGISTER}:

            reg_index = bit_address // 16
            reg_bit = 15 - (bit_address % 16)

            reg_val = self._cache[device_id][data_type].get(reg_index, 0)

            if bit:
                reg_val |= (1 << reg_bit)
            else:
                reg_val &= ~(1 << reg_bit)

            self._cache[device_id][data_type][reg_index] = reg_val


def _sort_events(events):
    return sorted(events)


def _unique_ids():
    prefix = uuid.uuid1()
    for i in itertools.count(1):
        yield f'{prefix}_{i}'


def _create_logger_adapter(name):
    extra = {'meta': {'type': 'ModbusSlaveDevice',
                      'name': name}}

    return logging.LoggerAdapter(mlog, extra)
