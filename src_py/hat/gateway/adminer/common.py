from hat.gateway.common import *  # NOQA

import typing

from hat import sbs
from hat.drivers import chatter

from hat.gateway.common import sbs_repo


MsgType: typing.TypeAlias = str


async def send_msg(conn: chatter.Connection,
                   msg_type: MsgType,
                   msg_data: sbs.Data,
                   **kwargs
                   ) -> chatter.Conversation:
    msg = sbs_repo.encode(msg_type, msg_data)
    return await conn.send(chatter.Data(msg_type, msg), **kwargs)


async def receive_msg(conn: chatter.Connection
                      ) -> tuple[chatter.Msg, MsgType, sbs.Data]:
    msg = await conn.receive()
    msg_data = sbs_repo.decode(msg.data.type, msg.data.data)
    return msg, msg.data.type, msg_data
