import asyncio
import enum
import logging
import time
import typing

from hat import json
from hat import aio
from hat.drivers import modbus
from hat.drivers import serial
from hat.drivers import tcp


mlog = logging.getLogger(__name__)


DataType: typing.TypeAlias = modbus.DataType


Error = enum.Enum('Error', [
    'INVALID_FUNCTION_CODE',
    'INVALID_DATA_ADDRESS',
    'INVALID_DATA_VALUE',
    'FUNCTION_ERROR',
    'TIMEOUT'])


async def connect(conf: json.Data,
                  log_prefix: str
                  ) -> 'Connection':
    transport_conf = conf['transport']
    modbus_type = modbus.ModbusType[conf['modbus_type']]

    if transport_conf['type'] == 'TCP':
        addr = tcp.Address(transport_conf['host'], transport_conf['port'])
        master = await modbus.create_tcp_master(
            modbus_type=modbus_type,
            addr=addr,
            response_timeout=conf['request_timeout'])

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
        master = await modbus.create_serial_master(
            modbus_type=modbus_type,
            port=port,
            baudrate=baudrate,
            bytesize=bytesize,
            parity=parity,
            stopbits=stopbits,
            xonxoff=xonxoff,
            rtscts=rtscts,
            dsrdtr=dsrdtr,
            silent_interval=silent_interval,
            response_timeout=conf['request_timeout'])

    else:
        raise ValueError('unsupported link type')

    conn = Connection()
    conn._conf = conf
    conn._log_prefix = log_prefix
    conn._master = master
    conn._request_queue = aio.Queue()

    conn.async_group.spawn(conn._request_loop)

    return conn


class Connection(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        return self._master.async_group

    async def read(self,
                   device_id: int,
                   data_type: modbus.DataType,
                   start_address: int,
                   quantity: int
                   ) -> list[int] | Error:
        self._log(logging.DEBUG, 'enqueuing read request')
        return await self._request(self._master.read, device_id, data_type,
                                   start_address, quantity)

    async def write(self,
                    device_id: int,
                    data_type: modbus.DataType,
                    start_address: int,
                    values: list[int]
                    ) -> Error | None:
        self._log(logging.DEBUG, 'enqueuing write request')
        return await self._request(self._master.write, device_id, data_type,
                                   start_address, values)

    async def write_mask(self,
                         device_id: int,
                         address: int,
                         and_mask: int,
                         or_mask: int
                         ) -> Error | None:
        self._log(logging.DEBUG, 'enqueuing write mask request')
        return await self._request(self._master.write_mask, device_id,
                                   address, and_mask, or_mask)

    async def _request_loop(self):
        future = None
        result_t = None

        try:
            self._log(logging.DEBUG, 'starting request loop')
            while True:
                fn, args, delayed_count, future = await self._request_queue.get()  # NOQA
                self._log(logging.DEBUG, 'dequed request')

                if result_t is not None and self._conf['request_delay'] > 0:
                    dt = time.monotonic() - result_t
                    if dt < self._conf['request_delay']:
                        await asyncio.sleep(self._conf['request_delay'] - dt)

                if future.done():
                    continue

                delayed_count -= 1
                immediate_count = (
                    self._conf['request_retry_immediate_count'] + 1)

                while True:
                    try:
                        immediate_count -= 1
                        result = await fn(*args)
                        result_t = time.monotonic()

                        self._log(logging.DEBUG, 'received result %s', result)
                        if isinstance(result, modbus.Error):
                            result = Error[result.name]

                        if not future.done():
                            self._log(logging.DEBUG, 'setting request result')
                            future.set_result(result)

                        break

                    except TimeoutError:
                        self._log(logging.DEBUG, 'single request timeout')

                        if immediate_count > 0:
                            self._log(logging.DEBUG, 'immediate request retry')
                            continue

                        if delayed_count > 0:
                            self._log(logging.DEBUG, 'delayed request retry')
                            self.async_group.spawn(self._delay_request, fn,
                                                   args, delayed_count, future)
                            future = None

                        elif not future.done():
                            self._log(logging.DEBUG,
                                      'request resulting in timeout')
                            future.set_result(Error.TIMEOUT)

                        break

                    except Exception as e:
                        self._log(logging.DEBUG, 'setting request exception')
                        if not future.done():
                            future.set_exception(e)
                        raise

        except ConnectionError:
            self._log(logging.DEBUG, 'connection closed')

        except Exception as e:
            self._log(logging.ERROR, 'request loop error: %s', e, exc_info=e)

        finally:
            self._log(logging.DEBUG, 'closing request loop')
            self.close()
            self._request_queue.close()

            while True:
                if future and not future.done():
                    future.set_exception(ConnectionError())
                if self._request_queue.empty():
                    break
                _, __, ___, future = self._request_queue.get_nowait()

    async def _request(self, fn, *args):
        try:
            future = asyncio.Future()
            delayed_count = self._conf['request_retry_delayed_count'] + 1
            self._request_queue.put_nowait((fn, args, delayed_count, future))
            return await future

        except aio.QueueClosedError:
            raise ConnectionError()

    async def _delay_request(self, fn, args, delayed_count, future):
        try:
            await asyncio.sleep(self._conf['request_retry_delay'])
            self._request_queue.put_nowait((fn, args, delayed_count, future))
            future = None

        except aio.QueueClosedError:
            pass

        finally:
            if future and not future.done():
                future.set_exception(ConnectionError())

    def _log(self, level, msg, *args, **kwargs):
        if not mlog.isEnabledFor(level):
            return

        mlog.log(level, f"{self._log_prefix}: {msg}", *args, **kwargs)
