from hat.gateway.common import *  # NOQA

import logging
import typing


class RemoteDeviceEnableReq(typing.NamedTuple):
    device_id: int
    enable: bool


class RemoteDeviceWriteReq(typing.NamedTuple):
    device_id: int
    data_name: str
    request_id: str
    value: int


Request: typing.TypeAlias = RemoteDeviceEnableReq | RemoteDeviceWriteReq


class StatusRes(typing.NamedTuple):
    status: str


class RemoteDeviceStatusRes(typing.NamedTuple):
    device_id: int
    status: str


class RemoteDeviceReadRes(typing.NamedTuple):
    device_id: int
    data_name: str
    result: str
    value: int | None
    cause: str | None


class RemoteDeviceWriteRes(typing.NamedTuple):
    device_id: int
    data_name: str
    request_id: str
    result: str


Response: typing.TypeAlias = (StatusRes |
                              RemoteDeviceStatusRes |
                              RemoteDeviceReadRes |
                              RemoteDeviceWriteRes)


class Timeout(typing.NamedTuple):
    pass


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
