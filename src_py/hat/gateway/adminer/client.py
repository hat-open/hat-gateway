import asyncio
import logging

from hat import aio
from hat import json
from hat.drivers import chatter
from hat.drivers import tcp

from hat.gateway.adminer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class AdminerError(Exception):
    """Errors reported by Gateway Adminer Server"""


async def connect(addr: tcp.Address,
                  **kwargs
                  ) -> 'Client':
    """Connect to Gateway Adminer Server

    Additional arguments are passed to `hat.chatter.connect` coroutine.

    """
    client = Client()
    client._loop = asyncio.get_running_loop()
    client._conv_msg_type_futures = {}

    client._conn = await chatter.connect(addr, **kwargs)

    try:
        client.async_group.spawn(client._receive_loop)

    except BaseException:
        await aio.uncancellable(client.async_close())
        raise

    return client


class Client(aio.Resource):
    """Gateway adminer client

    For creating new client see `connect` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._conn.async_group

    async def get_log_conf(self) -> json.Data:
        """Get logging configuration"""
        data = await self._send(
            req_msg_type='HatGatewayAdminer.MsgGetLogConfReq',
            req_msg_data=None,
            res_msg_type='HatGatewayAdminer.MsgGetLogConfRes')

        return json.decode(data)

    async def set_log_conf(self, conf: json.Data):
        """Set logging configuration"""
        await self._send(req_msg_type='HatGatewayAdminer.MsgSetLogConfReq',
                         req_msg_data=json.encode(conf),
                         res_msg_type='HatGatewayAdminer.MsgSetLogConfRes')

    async def _send(self, req_msg_type, req_msg_data, res_msg_type):
        conv = await common.send_msg(
            conn=self._conn,
            msg_type=req_msg_type,
            msg_data=req_msg_data,
            last=False)

        if not self.is_open:
            raise ConnectionError()

        future = self._loop.create_future()
        self._conv_msg_type_futures[conv] = res_msg_type, future

        try:
            return await future

        finally:
            self._conv_msg_type_futures.pop(conv, None)

    async def _receive_loop(self):
        mlog.debug("starting receive loop")
        try:
            while True:
                mlog.debug("waiting for incoming message")
                msg, msg_type, msg_data = await common.receive_msg(self._conn)

                mlog.debug(f"received message {msg_type}")

                res_msg_type, future = self._conv_msg_type_futures.get(
                    msg.conv, (None, None))
                if not future or future.done():
                    return

                if res_msg_type != msg_type:
                    raise Exception('invalid response message type')

                if msg_data[0] == 'error':
                    future.set_exception(AdminerError(msg_data[1]))

                future.set_result(msg_data[1])

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("read loop error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping receive loop")
            self.close()

            for _, future in self._conv_msg_type_futures.values():
                if not future.done():
                    future.set_exception(ConnectionError())
