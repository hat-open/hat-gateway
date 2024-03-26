"""Modbus master device"""

from hat.gateway import common
from hat.gateway.devices.modbus.master.device import ModbusMasterDevice


info: common.DeviceInfo = common.DeviceInfo(
    type="modbus_master",
    create=ModbusMasterDevice,
    json_schema_id="hat-gateway://modbus.yaml#/$defs/master",
    json_schema_repo=common.json_schema_repo)
