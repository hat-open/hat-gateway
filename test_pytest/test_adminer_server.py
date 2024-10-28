import collections
import logging.config

import pytest

from hat import util
from hat.drivers import tcp

import hat.gateway.adminer
import hat.gateway.adminer_server


@pytest.fixture
def addr():
    return tcp.Address('127.0.0.1', util.get_unused_tcp_port())


async def test_adminer_server(monkeypatch, addr):
    log_conf_queue = collections.deque()

    monkeypatch.setattr(logging.config, 'dictConfig', log_conf_queue.append)

    server = await hat.gateway.adminer_server.create_adminer_server(
        addr=addr,
        log_conf=None)
    client = await hat.gateway.adminer.connect(addr)

    assert client.is_open
    assert server.is_open

    log_conf = await client.get_log_conf()
    assert log_conf is None

    assert len(log_conf_queue) == 0

    await client.set_log_conf({'version': 1})

    log_conf = log_conf_queue.popleft()
    assert log_conf == {'version': 1}
    assert len(log_conf_queue) == 0

    log_conf = await client.get_log_conf()
    assert log_conf == {'version': 1}

    await client.async_close()
    await server.async_close()
