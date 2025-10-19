"""Common gateway interfaces"""

from collections.abc import Collection
import abc
import importlib.resources
import typing

from hat import aio
from hat import json
from hat import sbs
import hat.event.common
import hat.event.eventer
import hat.monitor.common


with importlib.resources.as_file(importlib.resources.files(__package__) /
                                 'json_schema_repo.json') as _path:
    json_schema_repo: json.SchemaRepository = json.merge_schema_repositories(
        json.json_schema_repo,
        json.decode_file(_path))
    """JSON schema repository"""

with importlib.resources.as_file(importlib.resources.files(__package__) /
                                 'sbs_repo.json') as _path:
    sbs_repo: sbs.Repository = sbs.Repository.from_json(_path)
    """SBS schema repository"""


class Device(aio.Resource):
    """Device interface"""

    @abc.abstractmethod
    async def process_events(self,
                             events: Collection[hat.event.common.Event]):
        """Process received events

        This method can be coroutine or regular function.

        """


DeviceConf: typing.TypeAlias = json.Data
"""Device configuration"""

EventTypePrefix: typing.TypeAlias = tuple[hat.event.common.EventTypeSegment,
                                          hat.event.common.EventTypeSegment,
                                          hat.event.common.EventTypeSegment]
"""Event type prefix (``gateway/<device_type>/<device_name>``)"""

CreateDevice: typing.TypeAlias = aio.AsyncCallable[[DeviceConf,
                                                    hat.event.eventer.Client,
                                                    EventTypePrefix],
                                                   Device]
"""Create device callable"""


class DeviceInfo(typing.NamedTuple):
    """Device info

    Device is implemented as python module which is dynamically imported.
    It is expected that this module contains `info` which is instance of
    `DeviceInfo`.

    If device defines JSON schema repository and JSON schema id, JSON schema
    repository will be used for additional validation of device configuration
    with JSON schema id.

    """
    type: str
    create: CreateDevice
    json_schema_id: str | None = None
    json_schema_repo: json.SchemaRepository | None = None


def import_device_info(py_module_str: str) -> DeviceInfo:
    """Import device info"""
    py_module = importlib.import_module(py_module_str)
    info = py_module.info

    if not isinstance(info, DeviceInfo):
        raise Exception('invalid device implementation')

    return info
