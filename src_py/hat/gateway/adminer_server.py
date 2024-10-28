import logging

from hat import aio
from hat import json
from hat.drivers import tcp

from hat.gateway import adminer


async def create_adminer_server(addr: tcp.Address,
                                log_conf: json.Data,
                                **kwargs
                                ) -> 'AdminerServer':
    """Create adminer server"""
    server = AdminerServer()
    server._log_conf = log_conf

    server._srv = await adminer.listen(addr,
                                       get_log_conf_cb=server._on_get_log_conf,
                                       set_log_conf_cb=server._on_set_log_conf,
                                       **kwargs)

    return server


class AdminerServer(aio.Resource):
    """Adminer server

    For creating new server see `create_adminer_server` coroutine.

    """

    @property
    def async_group(self) -> aio.Group:
        """Async group"""
        return self._srv.async_group

    def _on_get_log_conf(self):
        return self._log_conf

    def _on_set_log_conf(self, log_conf):
        logging.config.dictConfig(log_conf)
        self._log_conf = log_conf
