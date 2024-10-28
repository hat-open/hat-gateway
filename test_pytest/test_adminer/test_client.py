import asyncio

import pytest

from hat import aio
from hat import json
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.gateway.adminer import common
import hat.gateway.adminer


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_connect(addr):
    with pytest.raises(ConnectionError):
        await hat.gateway.adminer.connect(addr)

    conn_queue = aio.Queue()

    srv = await chatter.listen(conn_queue.put_nowait, addr)
    client = await hat.gateway.adminer.connect(addr)
    conn = await conn_queue.get()

    assert conn.is_open
    assert client.is_open
    assert srv.is_open

    await client.async_close()
    await srv.async_close()


async def test_get_log_conf(addr):
    conn_queue = aio.Queue()
    conf = {'version': 1}

    srv = await chatter.listen(conn_queue.put_nowait, addr)
    client = await hat.gateway.adminer.connect(addr)
    conn = await conn_queue.get()

    task = asyncio.create_task(client.get_log_conf())

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert not msg.last
    assert msg_type == 'HatGatewayAdminer.MsgGetLogConfReq'
    assert msg_data is None

    await common.send_msg(conn, 'HatGatewayAdminer.MsgGetLogConfRes',
                          ('success', json.encode(conf)),
                          conv=msg.conv)

    result = await task
    assert result == conf

    task = asyncio.create_task(client.get_log_conf())

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert not msg.last
    assert msg_type == 'HatGatewayAdminer.MsgGetLogConfReq'
    assert msg_data is None

    await common.send_msg(conn, 'HatGatewayAdminer.MsgGetLogConfRes',
                          ('error', 'abc'),
                          conv=msg.conv)

    with pytest.raises(hat.gateway.adminer.AdminerError, match='abc'):
        await task

    await client.async_close()
    await srv.async_close()


async def test_set_log_conf(addr):
    conn_queue = aio.Queue()
    conf = {'version': 1}

    srv = await chatter.listen(conn_queue.put_nowait, addr)
    client = await hat.gateway.adminer.connect(addr)
    conn = await conn_queue.get()

    task = asyncio.create_task(client.set_log_conf(conf))

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert not msg.last
    assert msg_type == 'HatGatewayAdminer.MsgSetLogConfReq'
    assert json.decode(msg_data) == conf

    await common.send_msg(conn, 'HatGatewayAdminer.MsgSetLogConfRes',
                          ('success', None),
                          conv=msg.conv)

    await task

    task = asyncio.create_task(client.set_log_conf(conf))

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert not msg.last
    assert msg_type == 'HatGatewayAdminer.MsgSetLogConfReq'
    assert json.decode(msg_data) == conf

    await common.send_msg(conn, 'HatGatewayAdminer.MsgSetLogConfRes',
                          ('error', 'abc'),
                          conv=msg.conv)

    with pytest.raises(hat.gateway.adminer.AdminerError, match='abc'):
        await task

    await client.async_close()
    await srv.async_close()
