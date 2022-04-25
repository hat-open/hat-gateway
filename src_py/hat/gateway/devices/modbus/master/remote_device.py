import asyncio
import collections
import itertools
import logging
import math
import time
import typing

from hat import aio
from hat import json
from hat.gateway.devices.modbus.master.connection import (DataType,
                                                          Error,
                                                          Connection)
from hat.gateway.devices.modbus.master.event_client import (RemoteDeviceStatusRes,  # NOQA
                                                            RemoteDeviceReadRes,  # NOQA
                                                            RemoteDeviceWriteRes,  # NOQA
                                                            Response)


mlog = logging.getLogger(__name__)


ResponseCb = typing.Callable[[Response], None]


class _DataInfo(typing.NamedTuple):
    data_type: DataType
    register_size: int
    start_address: int
    bit_count: int
    bit_offset: int
    quantity: int
    interval: typing.Optional[float]
    name: str


class RemoteDevice:

    def __init__(self,
                 conf: json.Data,
                 conn: Connection):
        self._conn = conn
        self._device_id = conf['device_id']
        self._data_infos = {}

        for i in conf['data']:
            data_type = DataType[i['data_type']]
            register_size = _get_register_size(data_type)
            start_address = i['start_address']
            bit_count = i['bit_count']
            bit_offset = i['bit_offset']
            quantity = math.ceil((bit_count + bit_offset) / register_size)
            interval = i['interval']
            name = i['name']
            self._data_infos[name] = _DataInfo(data_type=data_type,
                                               register_size=register_size,
                                               start_address=start_address,
                                               bit_count=bit_count,
                                               bit_offset=bit_offset,
                                               quantity=quantity,
                                               interval=interval,
                                               name=name)

    @property
    def conn(self) -> Connection:
        return self._conn

    @property
    def device_id(self) -> int:
        return self._device_id

    def create_reader(self, response_cb: ResponseCb) -> aio.Resource:
        return _Reader(conn=self._conn,
                       device_id=self._device_id,
                       data_infos=self._data_infos.values(),
                       response_cb=response_cb)

    async def write(self,
                    data_name: str,
                    request_id: str,
                    value: int
                    ) -> typing.Optional[RemoteDeviceWriteRes]:
        data_info = self._data_infos.get(data_name)
        if not data_info:
            mlog.debug('data %s is not available', data_name)
            return

        if data_info.data_type == DataType.COIL:
            result = await self._write_coil(data_info, value)

        elif data_info.data_type == DataType.HOLDING_REGISTER:
            result = await self._write_holding_register(data_info, value)

        else:
            mlog.debug('write unsupported for %s', data_info.data_type)
            return

        return RemoteDeviceWriteRes(
            device_id=self._device_id,
            data_name=data_name,
            request_id=request_id,
            result=result.name if result else 'SUCCESS')

    async def _write_coil(self, data_info, value):
        address = data_info.start_address + data_info.bit_offset
        registers = [(value >> (data_info.bit_count - i - 1)) & 1
                     for i in range(data_info.bit_count)]
        return await self._conn.write(device_id=self._device_id,
                                      data_type=data_info.data_type,
                                      start_address=address,
                                      values=registers)

    async def _write_holding_register(self, data_info, value):
        address = data_info.start_address + (data_info.bit_offset // 16)
        bit_count = data_info.bit_count
        bit_offset = data_info.bit_offset % 16

        if bit_offset:
            mask_prefix_size = bit_offset
            mask_suffix_size = max(16 - bit_offset - bit_count, 0)
            mask_size = 16 - mask_prefix_size - mask_suffix_size
            and_mask = (((0xFFFF << (16 - mask_prefix_size)) & 0xFFFF) |
                        ((0xFFFF << mask_suffix_size) >> 16))
            or_mask = (((value >> (bit_count - mask_size)) &
                        ((1 << mask_size) - 1)) <<
                       mask_suffix_size)
            result = await self._conn.write_mask(device_id=self._device_id,
                                                 address=address,
                                                 and_mask=and_mask,
                                                 or_mask=or_mask)
            if result:
                return result
            address += 1
            bit_count -= mask_size

        register_count = bit_count // 16
        if register_count:
            registers = [(value >> (bit_count - 16 * (i + 1))) & 0xFFFF
                         for i in range(register_count)]
            result = await self._conn.write(device_id=self._device_id,
                                            data_type=data_info.data_type,
                                            start_address=address,
                                            values=registers)
            if result:
                return result
            address += register_count
            bit_count -= 16 * register_count

        if not bit_count:
            return

        and_mask = (0xFFFF << (16 - bit_count)) >> 16
        or_mask = (value & ((1 << bit_count) - 1)) << (16 - bit_count)
        return await self._conn.write_mask(device_id=self._device_id,
                                           address=address,
                                           and_mask=and_mask,
                                           or_mask=or_mask)


class _Reader(aio.Resource):

    def __init__(self, conn, device_id, data_infos, response_cb):
        self._conn = conn
        self._device_id = device_id
        self._response_cb = response_cb

        self._async_group = conn.async_group.create_subgroup()
        self._status = None
        self._is_connected = {}
        self._last_responses = {}

        self._start_read_loops(data_infos)

        self._async_group.spawn(aio.call_on_cancel, self._eval_status)
        self._eval_status()

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    def _start_read_loops(self, data_infos):
        type_interval_infos_dict = {}

        for data_info in data_infos:
            data_type = data_info.data_type
            interval = data_info.interval

            if interval is None:
                continue

            interval_infos_dict = type_interval_infos_dict.get(data_type)
            if interval_infos_dict is None:
                interval_infos_dict = {}
                type_interval_infos_dict[data_type] = interval_infos_dict

            data_infos_queue = interval_infos_dict.get(interval)
            if data_infos_queue is None:
                data_infos_queue = collections.deque()
                interval_infos_dict[interval] = data_infos_queue

            data_infos_queue.append(data_info)

        for data_type, interval_infos_dict in type_interval_infos_dict.items():
            for interval, data_infos_queue in interval_infos_dict.items():
                data_infos_queue = sorted(
                    data_infos_queue,
                    key=lambda i: (i.start_address, i.quantity))
                data_infos_queue = collections.deque(data_infos_queue)

                while data_infos_queue:
                    self._start_read_loop(data_infos_queue, interval,
                                          data_type)

    def _start_read_loop(self, data_infos_queue, interval, data_type):
        max_quantity = _get_max_quantity(data_type)
        start_address = None
        quantity = None
        data_infos = collections.deque()

        while data_infos_queue:
            data_info = data_infos_queue.popleft()

            if start_address is None:
                start_address = data_info.start_address

            if quantity is None:
                quantity = data_info.quantity

            elif data_info.start_address > start_address + quantity:
                data_infos_queue.appendleft(data_info)
                break

            else:
                new_quantity = data_info.quantity + (data_info.start_address -
                                                     start_address)

                if new_quantity > max_quantity:
                    data_infos_queue.appendleft(data_info)
                    break

                if new_quantity > quantity:
                    quantity = new_quantity

            data_infos.append(data_info)

        if start_address is None or quantity is None:
            return

        is_connected_key = len(self._is_connected)
        self._is_connected[is_connected_key] = False

        self.async_group.spawn(self._read_loop, data_infos, interval,
                               data_type, start_address, quantity,
                               is_connected_key)

    async def _read_loop(self, data_infos, interval, data_type, start_address,
                         quantity, is_connected_key):
        try:
            mlog.debug('starting read loop')
            last_read_time = time.monotonic() - interval

            while True:
                dt = time.monotonic() - last_read_time
                if dt < interval:
                    await asyncio.sleep(interval - dt)

                mlog.debug('reading data')
                last_read_time = time.monotonic()
                result = await self._conn.read(device_id=self._device_id,
                                               data_type=data_type,
                                               start_address=start_address,
                                               quantity=quantity)

                for data_info in data_infos:
                    self._process_read_result(data_info, start_address, result)

                is_connected = not (isinstance(result, Error) and
                                    result.name == 'TIMEOUT')
                self._is_connected[is_connected_key] = is_connected
                self._eval_status()

        except ConnectionError:
            mlog.debug('connection closed')

        except Exception as e:
            mlog.error('read loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing read loop')
            self.close()

    def _process_read_result(self, data_info, start_address, result):
        last_response = self._last_responses.get(data_info.name)

        if isinstance(result, Error):
            value = None

        else:
            offset = data_info.start_address - start_address
            value = _get_registers_value(
                data_info.register_size, data_info.bit_offset,
                data_info.bit_count,
                result[offset:offset+data_info.quantity])

        if isinstance(result, Error):
            mlog.debug('received error response (device_id: %s; '
                       'data_name: %s): %s',
                       self._device_id, data_info.name, result)
            response = RemoteDeviceReadRes(device_id=self._device_id,
                                           data_name=data_info.name,
                                           result=result.name,
                                           value=None,
                                           cause=None)

        elif (last_response is None or
                last_response.result != 'SUCCESS'):
            mlog.debug('received initial value (device_id: %s; '
                       'data_name: %s): %s',
                       self._device_id, data_info.name, value)
            response = RemoteDeviceReadRes(device_id=self._device_id,
                                           data_name=data_info.name,
                                           result='SUCCESS',
                                           value=value,
                                           cause='INTERROGATE')

        elif last_response.value != value:
            mlog.debug('data value change value (device_id: %s; '
                       'data_name: %s): %s -> %s',
                       self._device_id, data_info.name,
                       last_response.value, value)
            response = RemoteDeviceReadRes(device_id=self._device_id,
                                           data_name=data_info.name,
                                           result='SUCCESS',
                                           value=value,
                                           cause='CHANGE')

        else:
            mlog.debug('no data change')
            response = None

        if response:
            self._last_responses[data_info.name] = response
            self._response_cb(response)

    def _eval_status(self):
        if not self.is_open:
            status = 'DISABLED'
        elif all(self._is_connected.values()):
            status = 'CONNECTED'
        else:
            status = 'CONNECTING'

        if self._status == status:
            return

        mlog.debug('changing remote device status: %s -> %s', self._status,
                   status)
        self._status = status
        self._response_cb(RemoteDeviceStatusRes(device_id=self._device_id,
                                                status=status))


def _get_register_size(data_type):
    if data_type in (DataType.COIL,
                     DataType.DISCRETE_INPUT):
        return 1

    if data_type in (DataType.HOLDING_REGISTER,
                     DataType.INPUT_REGISTER,
                     DataType.QUEUE):
        return 16

    raise ValueError('invalid data type')


def _get_max_quantity(data_type):
    if data_type in (DataType.COIL,
                     DataType.DISCRETE_INPUT):
        return 2000

    if data_type in (DataType.HOLDING_REGISTER,
                     DataType.INPUT_REGISTER):
        return 125

    if data_type == DataType.QUEUE:
        return 1

    raise ValueError('invalid data type')


def _get_registers_value(register_size, bit_offset, bit_count, values):
    result = 0
    bits = itertools.chain(_get_registers_bits(register_size, values),
                           itertools.repeat(0))
    for i in itertools.islice(bits, bit_offset, bit_offset + bit_count):
        result = (result << 1) | i
    return result


def _get_registers_bits(register_size, values):
    for value in values:
        for i in range(register_size):
            yield (value >> (register_size - i - 1)) & 1
