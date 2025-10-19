from hat.gateway.common import *  # NOQA

import logging


def create_device_logger_adapter(logger: logging.Logger,
                                 name: str
                                 ) -> logging.LoggerAdapter:
    extra = {'meta': {'type': 'ModbusMasterDevice',
                      'name': name}}

    return logging.LoggerAdapter(logger, extra)


def create_remote_device_logger_adapter(logger: logging.Logger,
                                        name: str,
                                        device_id: int
                                        ) -> logging.LoggerAdapter:
    extra = {'meta': {'type': 'ModbusMasterRemoteDevice',
                      'name': name,
                      'device_id': device_id}}

    return logging.LoggerAdapter(logger, extra)
