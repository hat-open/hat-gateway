import asyncio
import logging
import time

from hat import json
from hat import aio
from hat.drivers import modbus
from hat.drivers import serial
from hat.drivers import tcp

from hat.gateway.devices.modbus.master import common


mlog = logging.getLogger(__name__)


async def connect(conf: json.Data,
                  name: str,
                  request_queue_size: int = 1024
                  ) -> 'Connection':
    transport_conf = conf['transport']
    modbus_type = modbus.ModbusType[conf['modbus_type']]

    if transport_conf['type'] == 'TCP':
        addr = tcp.Address(transport_conf['host'], transport_conf['port'])
        master = await modbus.create_tcp_master(
            modbus_type=modbus_type,
            addr=addr,
            response_timeout=conf['request_timeout'],
            name=name)

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
            response_timeout=conf['request_timeout'],
            name=name)

    else:
        raise ValueError('unsupported link type')

    return Connection(conf=conf,
                      master=master,
                      name=name,
                      request_queue_size=request_queue_size)


class Connection(aio.Resource):

    def __init__(self,
                 conf: json.Data,
                 master: modbus.Master,
                 name: str,
                 request_queue_size: int):
        self._conf = conf
        self._master = master
        self._name = name
        self._request_queue = aio.Queue(request_queue_size)
        self._loop = asyncio.get_running_loop()
        self._log = common.create_device_logger_adapter(mlog, name)

        self.async_group.spawn(self._request_loop)

    @property
    def async_group(self) -> aio.Group:
        return self._master.async_group

    async def send(self,
                   req: modbus.Request
                   ) -> modbus.Response | common.Timeout:
        self._log.debug('enqueuing request')

        try:
            future = self._loop.create_future()
            delayed_count = self._conf['request_retry_delayed_count'] + 1
            await self._request_queue.put((req, delayed_count, future))

            return await future

        except aio.QueueClosedError:
            raise ConnectionError()

    async def _request_loop(self):
        future = None
        res_t = None

        try:
            self._log.debug('starting request loop')
            while True:
                req, delayed_count, future = await self._request_queue.get()  # NOQA
                self._log.debug('dequed request')

                if res_t is not None and self._conf['request_delay'] > 0:
                    dt = time.monotonic() - res_t
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
                        res = await self._master.send(req)
                        res_t = time.monotonic()

                        self._log.debug('received response %s', res)
                        if not future.done():
                            future.set_result(res)

                        break

                    except TimeoutError:
                        self._log.debug('single request timeout')

                        if immediate_count > 0:
                            self._log.debug('immediate request retry')
                            continue

                        if delayed_count > 0:
                            self._log.debug('delayed request retry')
                            self.async_group.spawn(self._delay_request, req,
                                                   delayed_count, future)
                            future = None

                        elif not future.done():
                            self._log.debug('request resulting in timeout')
                            future.set_result(common.Timeout())

                        break

                    except Exception as e:
                        self._log.debug('setting request exception')
                        if not future.done():
                            future.set_exception(e)
                        raise

        except ConnectionError:
            self._log.debug('connection closed')

        except Exception as e:
            self._log.error('request loop error: %s', e, exc_info=e)

        finally:
            self._log.debug('closing request loop')
            self.close()
            self._request_queue.close()

            while True:
                if future and not future.done():
                    future.set_exception(ConnectionError())
                if self._request_queue.empty():
                    break
                _, __, future = self._request_queue.get_nowait()

    async def _delay_request(self, req, delayed_count, future):
        try:
            await asyncio.sleep(self._conf['request_retry_delay'])
            await self._request_queue.put((req, delayed_count, future))
            future = None

        except aio.QueueClosedError:
            pass

        finally:
            if future and not future.done():
                future.set_exception(ConnectionError())
