"""Common gateway interfaces"""

import abc
import importlib.resources
import typing

from hat import aio
from hat import json
import hat.event.common
import hat.monitor.common


with importlib.resources.as_file(importlib.resources.files(__package__) /
                                 'json_schema_repo.json') as _path:
    json_schema_repo: json.SchemaRepository = json.SchemaRepository(
        json.json_schema_repo,
        hat.monitor.common.json_schema_repo,
        json.SchemaRepository.from_json(_path))
    """JSON schema repository"""

DeviceConf: typing.TypeAlias = json.Data
"""Device configuration"""

EventTypePrefix: typing.TypeAlias = hat.event.common.EventType
"""Event type prefix"""

CreateDevice: typing.TypeAlias = aio.AsyncCallable[[DeviceConf,
                                                    'DeviceEventClient',
                                                    EventTypePrefix],
                                                   'Device']
"""Create device callable"""


class Device(aio.Resource):
    """Device interface

    Device is implemented as python module which is dynamically imported.
    It is expected that this module implements:

        * device_type (str): device type identification
        * json_schema_id (Optional[str]): JSON schema id
        * json_schema_repo (Optional[json.SchemaRepository]): JSON schema repo
        * create (CreateDevice): creating new device instance

    If module defines JSON schema repositoy and JSON schema id, JSON schema
    repository will be used for additional validation of device configuration
    with JSON schema id.

    `create` is called with device configuration, appropriate instance of
    `DeviceEventClient` and event type prefix. Event type prefix is defined
    as [``gateway``, `<gateway_name>`, `<device_type>`, `<device_name>`].

    """


class DeviceEventClient(aio.Resource):
    """Device's event client interface"""

    @abc.abstractmethod
    async def receive(self) -> list[hat.event.common.Event]:
        """Receive device events"""

    @abc.abstractmethod
    def register(self, events: list[hat.event.common.RegisterEvent]):
        """Register device events"""

    @abc.abstractmethod
    async def register_with_response(self,
                                     events: list[hat.event.common.RegisterEvent]  # NOQA
                                     ) -> list[hat.event.common.Event | None]:
        """Register device events

        Each `RegisterEvent` from `events` is paired with results `Event` if
        new event was successfully created or `None` is new event could not be
        created.

        """

    @abc.abstractmethod
    async def query(self,
                    data: hat.event.common.QueryData
                    ) -> list[hat.event.common.Event]:
        """Query device events from server"""
