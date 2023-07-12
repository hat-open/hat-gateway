"""IEC 60870-5-101 slave device"""

import logging

from hat import json

from hat.gateway.devices.iec101 import common


mlog: logging.Logger = logging.getLogger(__name__)

device_type: str = 'iec101_slave'

json_schema_id: str = "hat-gateway://iec101.yaml#/definitions/slave"

json_schema_repo: json.SchemaRepository = common.json_schema_repo


async def create(conf: common.DeviceConf,
                 event_client: common.DeviceEventClient,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec101SlaveDevice':
    pass


class Iec101SlaveDevice(common.Device):
    pass
