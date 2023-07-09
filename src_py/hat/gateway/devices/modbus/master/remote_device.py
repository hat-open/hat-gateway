import asyncio
import collections
import enum
import functools
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


ResponseCb: typing.TypeAlias = typing.Callable[[Response], None]


class _Status(enum.Enum):
    CONNECTING = 'CONNECTING'
    CONNECTED = 'CONNECTED'
    DISCONNECTED = 'DISCONNECTED'
    DISABLED = 'DISABLED'


class _DataInfo(typing.NamedTuple):
    data_type: DataType
    register_size: int
    start_address: int
    bit_count: int
    bit_offset: int
    quantity: int
    interval: float | None
    name: str


class _DataGroup(typing.NamedTuple):
    data_infos: list[_DataInfo]
    interval: float
    data_type: DataType
    start_address: int
    quantity: int


class RemoteDevice:

    def __init__(self,
                 conf: json.Data,
                 conn: Connection,
                 log_prefix: str):
        self._conn = conn
        self._device_id = conf['device_id']
        self._timeout_poll_delay = conf['timeout_poll_delay']
        self._log_prefix = f"{log_prefix}: remote device id {self._device_id}"
        self._data_infos = {data_info.name: data_info
                            for data_info in _get_data_infos(conf)}
        self._data_groups = list(_group_data_infos(self._data_infos.values()))

    @property
    def conn(self) -> Connection:
        return self._conn

    @property
    def device_id(self) -> int:
        return self._device_id

    def create_reader(self, response_cb: ResponseCb) -> aio.Resource:
        return _Reader(conn=self._conn,
                       device_id=self._device_id,
                       timeout_poll_delay=self._timeout_poll_delay,
                       data_groups=self._data_groups,
                       response_cb=response_cb,
                       log_prefix=self._log_prefix)

    async def write(self,
                    data_name: str,
                    request_id: str,
                    value: int
                    ) -> RemoteDeviceWriteRes | None:
        data_info = self._data_infos.get(data_name)
        if not data_info:
            self._log(logging.DEBUG, 'data %s is not available', data_name)
            return

        if data_info.data_type == DataType.COIL:
            result = await self._write_coil(data_info, value)

        elif data_info.data_type == DataType.HOLDING_REGISTER:
            result = await self._write_holding_register(data_info, value)

        else:
            self._log(logging.DEBUG, 'write unsupported for %s',
                      data_info.data_type)
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

    def _log(self, level, msg, *args, **kwargs):
        if not mlog.isEnabledFor(level):
            return

        mlog.log(level, f"{self._log_prefix}: {msg}", *args, **kwargs)


class _Reader(aio.Resource):

    def __init__(self, conn, device_id, timeout_poll_delay, data_groups,
                 response_cb, log_prefix):
        self._conn = conn
        self._device_id = device_id
        self._timeout_poll_delay = timeout_poll_delay
        self._response_cb = response_cb
        self._log_prefix = log_prefix
        self._status = None
        self._async_group = conn.async_group.create_subgroup()

        self.async_group.spawn(self._read_loop, data_groups)

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def _read_loop(self, data_groups):
        try:
            self._log(logging.DEBUG, 'starting read loop')
            loop = asyncio.get_running_loop()

            if not data_groups:
                self._set_status(_Status.CONNECTED)
                await loop.create_future()

            last_read_times = [None for _ in data_groups]
            last_responses = {}
            self._set_status(_Status.CONNECTING)

            while True:
                now = time.monotonic()
                sleep_dt = None
                read_data_groups = collections.deque()

                for i, data_group in enumerate(data_groups):
                    last_read_time = last_read_times[i]
                    if last_read_time is not None:
                        dt = data_group.interval - now + last_read_time
                        if dt > 0:
                            if sleep_dt is None or dt < sleep_dt:
                                sleep_dt = dt
                            continue

                    read_data_groups.append(data_group)
                    last_read_times[i] = now

                if not read_data_groups:
                    await asyncio.sleep(sleep_dt)
                    continue

                timeout = False

                self._log(logging.DEBUG, 'reading data')
                for data_group in read_data_groups:
                    result = await self._conn.read(
                        device_id=self._device_id,
                        data_type=data_group.data_type,
                        start_address=data_group.start_address,
                        quantity=data_group.quantity)

                    if isinstance(result, Error) and result.name == 'TIMEOUT':
                        timeout = True
                        break

                    for data_info in data_group.data_infos:
                        last_response = last_responses.get(data_info.name)

                        response = self._process_read_result(
                            data_info, data_group.start_address,
                            result, last_response)

                        if response:
                            last_responses[data_info.name] = response
                            self._response_cb(response)

                if timeout:
                    self._set_status(_Status.DISCONNECTED)
                    await asyncio.sleep(self._timeout_poll_delay)

                    last_read_times = [None for _ in data_groups]
                    last_responses = {}
                    self._set_status(_Status.CONNECTING)

                elif all(t is not None for t in last_read_times):
                    self._set_status(_Status.CONNECTED)

        except ConnectionError:
            self._log(logging.DEBUG, 'connection closed')

        except Exception as e:
            self._log(logging.ERROR, 'read loop error: %s', e, exc_info=e)

        finally:
            self._log(logging.DEBUG, 'closing read loop')
            self.close()
            self._set_status(_Status.DISABLED)

    def _process_read_result(self, data_info, start_address, result,
                             last_response):
        if isinstance(result, Error):
            self._log(logging.DEBUG, 'data name %s: error response %s',
                      data_info.name, result)
            return RemoteDeviceReadRes(device_id=self._device_id,
                                       data_name=data_info.name,
                                       result=result.name,
                                       value=None,
                                       cause=None)

        offset = data_info.start_address - start_address
        value = _get_registers_value(
            data_info.register_size, data_info.bit_offset,
            data_info.bit_count,
            result[offset:offset+data_info.quantity])

        if last_response is None or last_response.result != 'SUCCESS':
            self._log(logging.DEBUG, 'data name %s: initial value %s',
                      data_info.name, value)
            return RemoteDeviceReadRes(device_id=self._device_id,
                                       data_name=data_info.name,
                                       result='SUCCESS',
                                       value=value,
                                       cause='INTERROGATE')

        if last_response.value != value:
            self._log(logging.DEBUG, 'data name %s: value change %s -> %s',
                      data_info.name, last_response.value, value)
            return RemoteDeviceReadRes(device_id=self._device_id,
                                       data_name=data_info.name,
                                       result='SUCCESS',
                                       value=value,
                                       cause='CHANGE')

        self._log(logging.DEBUG, 'data name %s: no value change',
                  data_info.name)

    def _set_status(self, status):
        if self._status == status:
            return

        self._log(logging.DEBUG, 'changing remote device status %s -> %s',
                  self._status, status)
        self._status = status
        self._response_cb(RemoteDeviceStatusRes(device_id=self._device_id,
                                                status=status.name))

    def _log(self, level, msg, *args, **kwargs):
        if not mlog.isEnabledFor(level):
            return

        mlog.log(level, f"{self._log_prefix}: {msg}", *args, **kwargs)


def _get_data_infos(conf):
    for i in conf['data']:
        data_type = DataType[i['data_type']]
        register_size = _get_register_size(data_type)
        bit_count = i['bit_count']
        bit_offset = i['bit_offset']

        yield _DataInfo(
            data_type=data_type,
            register_size=register_size,
            start_address=i['start_address'],
            bit_count=bit_count,
            bit_offset=bit_offset,
            quantity=math.ceil((bit_count + bit_offset) / register_size),
            interval=i['interval'],
            name=i['name'])


def _group_data_infos(data_infos):
    type_interval_infos_dict = collections.defaultdict(
        functools.partial(collections.defaultdict, collections.deque))

    for data_info in data_infos:
        data_type = data_info.data_type
        interval = data_info.interval

        if interval is None:
            continue

        type_interval_infos_dict[data_type][interval].append(data_info)

    for data_type, interval_infos_dict in type_interval_infos_dict.items():
        for interval, data_infos_queue in interval_infos_dict.items():
            yield from _group_data_infos_with_type_interval(
                data_infos_queue, data_type, interval)


def _group_data_infos_with_type_interval(data_infos, data_type, interval):
    data_infos_queue = sorted(data_infos,
                              key=lambda i: (i.start_address, i.quantity))
    data_infos_queue = collections.deque(data_infos_queue)

    while data_infos_queue:
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
                new_quantity = (data_info.quantity +
                                data_info.start_address -
                                start_address)

                if new_quantity > max_quantity:
                    data_infos_queue.appendleft(data_info)
                    break

                if new_quantity > quantity:
                    quantity = new_quantity

            data_infos.append(data_info)

        if start_address is None or quantity is None:
            continue

        yield _DataGroup(data_infos=data_infos,
                         interval=interval,
                         data_type=data_type,
                         start_address=start_address,
                         quantity=quantity)


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
