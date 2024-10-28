import logging
import typing

from hat import aio
from hat import json
from hat.drivers import chatter
from hat.drivers import tcp

from hat.gateway.adminer import common


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""

GetLogConfCb: typing.TypeAlias = aio.AsyncCallable[[None], json.Data]
"""Get logging configuratio callback"""

SetLogConfCb: typing.TypeAlias = aio.AsyncCallable[[json.Data], None]
"""Set logging configuratio callback"""


async def listen(addr: tcp.Address,
                 *,
                 get_log_conf_cb: GetLogConfCb | None = None,
                 set_log_conf_cb: SetLogConfCb | None = None,
                 **kwargs
                 ) -> 'Server':
    """Create listening Gateway Adminer Server instance"""
    server = Server()
    server._get_log_conf_cb = get_log_conf_cb
    server._set_log_conf_cb = set_log_conf_cb

    server._srv = await chatter.listen(server._connection_loop, addr, **kwargs)
    mlog.debug("listening on %s", addr)

    return server


class Server(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    async def _connection_loop(self, conn):
        mlog.debug("starting connection loop")
        try:
            while True:
                mlog.debug("waiting for incomming messages")
                msg, msg_type, msg_data = await common.receive_msg(conn)

                mlog.debug(f"received message {msg_type}")

                if msg_type == 'HatGatewayAdminer.MsgGetLogConfReq':
                    await self._process_msg_get_log_conf(
                        conn=conn,
                        conv=msg.conv,
                        req_msg_data=msg_data)

                elif msg_type == 'HatGatewayAdminer.MsgSetLogConfReq':
                    await self._process_msg_set_log_conf(
                        conn=conn,
                        conv=msg.conv,
                        req_msg_data=msg_data)

                else:
                    raise Exception('unsupported message type')

        except ConnectionError:
            pass

        except Exception as e:
            mlog.error("on connection error: %s", e, exc_info=e)

        finally:
            mlog.debug("stopping connection loop")
            conn.close()

    async def _process_msg_get_log_conf(self, conn, conv, req_msg_data):
        try:
            if not self._get_log_conf_cb:
                raise Exception('not implemented')

            result = await aio.call(self._get_log_conf_cb)

            res_msg_data = 'success', json.encode(result)

        except Exception as e:
            res_msg_data = 'error', str(e)

        await common.send_msg(
            conn, 'HatGatewayAdminer.MsgGetLogConfRes', res_msg_data,
            conv=conv)

    async def _process_msg_set_log_conf(self, conn, conv, req_msg_data):
        try:
            if not self._set_log_conf_cb:
                raise Exception('not implemented')

            conf = json.decode(req_msg_data)
            await aio.call(self._set_log_conf_cb, conf)

            res_msg_data = 'success', None

        except Exception as e:
            res_msg_data = 'error', str(e)

        await common.send_msg(
            conn, 'HatGatewayAdminer.MsgSetLogConfRes', res_msg_data,
            conv=conv)
