import asyncio
import collections
import itertools
import logging
import math
import typing

from hat import aio
from hat import json
from hat.gateway.devices.modbus.master.connection import (DataType,
                                                          Error,
                                                          Connection)
from hat.gateway.devices.modbus.master.event_client import (RemoteDeviceStatusRes,  # NOQA
                                                            RemoteDeviceReadRes,  # NOQA
                                                            Response)


mlog = logging.getLogger(__name__)


ResponseCb = typing.Callable[[Response], None]


class RemoteDevice:

    def __init__(self,
                 conf: json.Data,
                 conn: Connection):
        self._conn = conn
        self._device_id = conf['device_id']
        self._data = {i['name']: Data(i, self._device_id, conn)
                      for i in conf['data']}

    @property
    def conn(self) -> Connection:
        return self._conn

    @property
    def device_id(self) -> int:
        return self._device_id

    @property
    def data(self) -> typing.Dict[int, 'Data']:
        return self._data


class RemoteDeviceReader(aio.Resource):

    def __init__(self,
                 remote_device: RemoteDevice,
                 response_cb: ResponseCb):
        self._response_cb = response_cb
        self._device_id = remote_device.device_id
        self._async_group = remote_device.conn.async_group.create_subgroup()
        self._status = None
        self._data_readers = collections.deque()

        reader_data = {}
        for data in remote_device.data.values():
            if data.interval is None:
                continue
            key = (data.data_type, data.start_address, data.quantity,
                   data.interval)
            reader_data.setdefault(key, []).append(data)

        for key, data in reader_data.items():
            data_type, start_address, quantity, interval = key
            data_reader = _DataReader(self._async_group, remote_device.conn,
                                      data, remote_device.device_id, data_type,
                                      start_address, quantity, interval,
                                      self._on_response)
            self._data_readers.append(data_reader)

        self._async_group.spawn(aio.call_on_cancel, self._eval_status)
        self._eval_status()

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    def _on_response(self, res):
        self._response_cb(res)
        self._eval_status()

    def _eval_status(self):
        if not self.is_open:
            status = 'DISABLED'
        elif all(i.is_connected for i in self._data_readers):
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


class Data:

    def __init__(self,
                 conf: json.Data,
                 device_id: int,
                 conn: Connection):
        self._device_id = device_id
        self._conn = conn
        self._data_type = DataType[conf['data_type']]
        self._register_size = _get_register_size(self._data_type)
        self._start_address = conf['start_address']
        self._bit_count = conf['bit_count']
        self._bit_offset = conf['bit_offset']
        self._quantity = math.ceil((self._bit_count + self._bit_offset) /
                                   self._register_size)
        self._interval = conf['interval']
        self._name = conf['name']

    @property
    def device_id(self) -> int:
        return self._device_id

    @property
    def conn(self) -> Connection:
        return self._conn

    @property
    def data_type(self) -> DataType:
        return self._data_type

    @property
    def register_size(self) -> int:
        return self._register_size

    @property
    def start_address(self) -> int:
        return self._start_address

    @property
    def bit_count(self) -> int:
        return self._bit_count

    @property
    def bit_offset(self) -> int:
        return self._bit_offset

    @property
    def quantity(self) -> int:
        return self._quantity

    @property
    def interval(self) -> typing.Optional[float]:
        return self._interval

    @property
    def name(self) -> str:
        return self._name

    async def write(self, value: int) -> typing.Optional[Error]:
        if self._data_type == DataType.COIL:
            address = self._start_address + self._bit_offset
            registers = [(value >> (self._bit_count - i - 1)) & 1
                         for i in range(self._bit_count)]
            result = await self._conn.write(self._device_id, self._data_type,
                                            address, registers)
            return result

        elif self._data_type == DataType.HOLDING_REGISTER:
            address = self._start_address + (self._bit_offset // 16)
            bit_count = self._bit_count
            bit_offset = self._bit_offset % 16

            if bit_offset:
                mask_prefix_size = bit_offset
                mask_suffix_size = max(16 - bit_offset - bit_count, 0)
                mask_size = 16 - mask_prefix_size - mask_suffix_size
                and_mask = (((0xFFFF << (16 - mask_prefix_size)) & 0xFFFF) |
                            ((0xFFFF << mask_suffix_size) >> 16))
                or_mask = (((value >> (bit_count - mask_size)) &
                            ((1 << mask_size) - 1)) <<
                           mask_suffix_size)
                result = await self._conn.write_mask(self._device_id, address,
                                                     and_mask, or_mask)
                if result:
                    return result
                address += 1
                bit_count -= mask_size

            register_count = bit_count // 16
            if register_count:
                registers = [(value >> (bit_count - 16 * (i + 1))) & 0xFFFF
                             for i in range(register_count)]
                result = await self._conn.write(self._device_id,
                                                self._data_type,
                                                address, registers)
                if result:
                    return result
                address += register_count
                bit_count -= 16 * register_count

            if bit_count:
                and_mask = (0xFFFF << (16 - bit_count)) >> 16
                or_mask = (value & ((1 << bit_count) - 1)) << (16 - bit_count)
                result = await self._conn.write_mask(self._device_id, address,
                                                     and_mask, or_mask)
                return result

            return

        raise Exception(f'write unsupported for {self._data_type}')

    async def read(self) -> typing.Union[int, Error]:
        result = await self._conn.read(self._device_id, self._data_type,
                                       self._start_address, self._quantity)
        if isinstance(result, Error):
            return result
        return _get_registers_value(self._register_size, self._bit_offset,
                                    self._bit_count, result)


class _DataReader(aio.Resource):

    def __init__(self,
                 async_group: aio.Group,
                 conn: Connection,
                 data: typing.List[Data],
                 device_id: int,
                 data_type: DataType,
                 start_address: int,
                 quantity: int,
                 interval: float,
                 response_cb: ResponseCb):
        self._async_group = async_group
        self._conn = conn
        self._data = data
        self._device_id = device_id
        self._data_type = data_type
        self._start_address = start_address
        self._quantity = quantity
        self._interval = interval
        self._response_cb = response_cb
        self._is_connected = False

        async_group.spawn(self._read_loop)

    @property
    def async_group(self):
        return self._async_group

    @property
    def is_connected(self):
        return self._is_connected

    async def _read_loop(self):
        try:
            mlog.debug('starting read loop')
            last_responses = {data: None for data in self._data}

            while True:
                mlog.debug('reading data')
                result = await self._conn.read(
                    self._device_id, self._data_type, self._start_address,
                    self._quantity)
                mlog.debug('received response (device_id: %s; data_type: %s; '
                           'start_address: %s; quantity: %s): %s',
                           self._device_id, self._data_type,
                           self._start_address, self._quantity, result)

                self._is_connected = not (isinstance(result, Error) and
                                          result.name == 'TIMEOUT')

                for data in self._data:
                    last_response = last_responses[data]

                    if isinstance(result, Error):
                        value = None

                    else:
                        offset = data.start_address - self._start_address
                        value = _get_registers_value(
                            data.register_size, data.bit_offset,
                            data.bit_count,
                            result[offset:offset+data.quantity])

                    if isinstance(result, Error):
                        mlog.debug('received error response (device_id: %s; '
                                   'data_name: %s): %s',
                                   data.device_id, data.name, result)
                        response = _create_read_response(
                            data, result.name, None, None)

                    elif (last_response is None or
                            last_response.result != 'SUCCESS'):
                        mlog.debug('received initial value (device_id: %s; '
                                   'data_name: %s): %s',
                                   data.device_id, data.name, value)
                        response = _create_read_response(
                            data, 'SUCCESS', value, 'INTERROGATE')

                    elif last_response.value != value:
                        mlog.debug('data value change value (device_id: %s; '
                                   'data_name: %s): %s -> %s',
                                   data.device_id, data.name,
                                   last_response.value, value)
                        response = _create_read_response(
                            data, 'SUCCESS', value, 'CHANGE')

                    else:
                        mlog.debug('no data change')
                        response = None

                    if response:
                        last_responses[data] = response
                        self._response_cb(response)

                mlog.debug('waiting poll interval: %s', self._interval)
                await asyncio.sleep(self._interval)

        except ConnectionError:
            mlog.debug('connection closed')

        except Exception as e:
            mlog.error('read loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing read loop')
            self.close()


def _create_read_response(data, result, value, cause):
    return RemoteDeviceReadRes(
        device_id=data.device_id,
        data_name=data.name,
        result=result,
        value=value,
        cause=cause)


def _get_register_size(data_type):
    if data_type in (DataType.COIL,
                     DataType.DISCRETE_INPUT):
        return 1

    if data_type in (DataType.HOLDING_REGISTER,
                     DataType.INPUT_REGISTER,
                     DataType.QUEUE):
        return 16

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
