import pytest

from hat import json
from hat import util
from hat.drivers import chatter
from hat.drivers import tcp

from hat.gateway.adminer import common
import hat.gateway.adminer


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_listen(addr):
    srv = await hat.gateway.adminer.listen(addr)
    conn = await chatter.connect(addr)

    assert conn.is_open
    assert srv.is_open

    await conn.async_close()
    await srv.async_close()


async def test_get_log_conf(addr):
    conf = {'version': 1}

    def on_get_log_conf():
        return conf

    srv = await hat.gateway.adminer.listen(addr,
                                           get_log_conf_cb=on_get_log_conf)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatGatewayAdminer.MsgGetLogConfReq',
                          None,
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg.last
    assert msg_type == 'HatGatewayAdminer.MsgGetLogConfRes'
    assert msg_data[0] == 'success'
    assert json.decode(msg_data[1]) == conf

    await conn.async_close()
    await srv.async_close()


async def test_get_log_conf_error(addr):
    def on_get_log_conf():
        raise Exception('abc')

    srv = await hat.gateway.adminer.listen(addr,
                                           get_log_conf_cb=on_get_log_conf)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatGatewayAdminer.MsgGetLogConfReq',
                          None,
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg.last
    assert msg_type == 'HatGatewayAdminer.MsgGetLogConfRes'
    assert msg_data[0] == 'error'
    assert msg_data[1] == 'abc'

    await conn.async_close()
    await srv.async_close()


async def test_get_log_conf_not_implemented(addr):
    srv = await hat.gateway.adminer.listen(addr)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatGatewayAdminer.MsgGetLogConfReq',
                          None,
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg.last
    assert msg_type == 'HatGatewayAdminer.MsgGetLogConfRes'
    assert msg_data[0] == 'error'
    assert msg_data[1] == 'not implemented'

    await conn.async_close()
    await srv.async_close()


async def test_set_log_conf(addr):
    conf = {'version': 1}

    def on_set_log_conf(c):
        assert c == conf

    srv = await hat.gateway.adminer.listen(addr,
                                           set_log_conf_cb=on_set_log_conf)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatGatewayAdminer.MsgSetLogConfReq',
                          json.encode(conf),
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg.last
    assert msg_type == 'HatGatewayAdminer.MsgSetLogConfRes'
    assert msg_data[0] == 'success'
    assert msg_data[1] is None

    await conn.async_close()
    await srv.async_close()


async def test_set_log_conf_error(addr):
    conf = {'version': 1}

    def on_set_log_conf(c):
        assert c == conf
        raise Exception('abc')

    srv = await hat.gateway.adminer.listen(addr,
                                           set_log_conf_cb=on_set_log_conf)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatGatewayAdminer.MsgSetLogConfReq',
                          json.encode(conf),
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg.last
    assert msg_type == 'HatGatewayAdminer.MsgSetLogConfRes'
    assert msg_data[0] == 'error'
    assert msg_data[1] == 'abc'

    await conn.async_close()
    await srv.async_close()


async def test_set_log_conf_not_implemented(addr):
    conf = {'version': 1}

    srv = await hat.gateway.adminer.listen(addr)
    conn = await chatter.connect(addr)

    await common.send_msg(conn, 'HatGatewayAdminer.MsgSetLogConfReq',
                          json.encode(conf),
                          last=False)

    msg, msg_type, msg_data = await common.receive_msg(conn)
    assert msg.last
    assert msg_type == 'HatGatewayAdminer.MsgSetLogConfRes'
    assert msg_data[0] == 'error'
    assert msg_data[1] == 'not implemented'

    await conn.async_close()
    await srv.async_close()
