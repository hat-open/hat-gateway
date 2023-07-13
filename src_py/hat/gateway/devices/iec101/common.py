from hat.gateway.common import *  # NOQA

import enum
import typing

from hat import json
from hat.drivers import iec101
import hat.event.common


class DataType(enum.Enum):
    SINGLE = 'single'
    DOUBLE = 'double'
    STEP_POSITION = 'step_position'
    BITSTRING = 'bitstring'
    NORMALIZED = 'normalized'
    SCALED = 'scaled'
    FLOATING = 'floating'
    BINARY_COUNTER = 'binary_counter'
    PROTECTION = 'protection'
    PROTECTION_START = 'protection_start'
    PROTECTION_COMMAND = 'protection_command'
    STATUS = 'status'


class CommandType(enum.Enum):
    SINGLE = 'single'
    DOUBLE = 'double'
    REGULATING = 'regulating'
    NORMALIZED = 'normalized'
    SCALED = 'scaled'
    FLOATING = 'floating'
    BITSTRING = 'bitstring'


class DataKey(typing.NamedTuple):
    data_type: DataType
    asdu_address: iec101.AsduAddress
    io_address: iec101.IoAddress


class CommandKey(typing.NamedTuple):
    cmd_type: CommandType
    asdu_address: iec101.AsduAddress
    io_address: iec101.IoAddress


def data_to_json(data: iec101.Data) -> json.Data:
    if isinstance(data, (iec101.SingleData,
                         iec101.DoubleData,
                         iec101.StepPositionData,
                         iec101.BitstringData,
                         iec101.ScaledData,
                         iec101.FloatingData,
                         iec101.BinaryCounterData,
                         iec101.StatusData)):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality)}

    if isinstance(data, iec101.NormalizedData):
        return {'value': _value_to_json(data.value),
                'quality': (_quality_to_json(data.quality)
                            if data.quality else None)}

    if isinstance(data, iec101.ProtectionData):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality),
                'elapsed_time': data.elapsed_time}

    if isinstance(data, iec101.ProtectionStartData):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality),
                'duration_time': data.duration_time}

    if isinstance(data, iec101.ProtectionCommandData):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality),
                'operating_time': data.operating_time}

    raise ValueError('unsupported data')


def data_from_json(data_type: DataType,
                   data: json.Data) -> iec101.Data:
    if data_type == DataType.SINGLE:
        return iec101.SingleData(
            value=_value_from_json(data_type, data['value']),
            quality=_indication_quality_from_json(data['quality']))

    if data_type == DataType.DOUBLE:
        return iec101.DoubleData(
            value=_value_from_json(data_type, data['value']),
            quality=_indication_quality_from_json(data['quality']))

    if data_type == DataType.STEP_POSITION:
        return iec101.StepPositionData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.BITSTRING:
        return iec101.BitstringData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.NORMALIZED:
        return iec101.NormalizedData(
            value=_value_from_json(data_type, data['value']),
            quality=(_measurement_quality_from_json(data['quality'])
                     if data['quality'] else None))

    if data_type == DataType.SCALED:
        return iec101.ScaledData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.FLOATING:
        return iec101.FloatingData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.BINARY_COUNTER:
        return iec101.BinaryCounterData(
            value=_value_from_json(data_type, data['value']),
            quality=_counter_quality_from_json(data['quality']))

    if data_type == DataType.PROTECTION:
        return iec101.ProtectionData(
            value=_value_from_json(data_type, data['value']),
            quality=_protection_quality_from_json(data['quality']),
            elapsed_time=data['elapsed_time'])

    if data_type == DataType.PROTECTION_START:
        return iec101.ProtectionStartData(
            value=_value_from_json(data_type, data['value']),
            quality=_protection_quality_from_json(data['quality']),
            duration_time=data['duration_time'])

    if data_type == DataType.PROTECTION_COMMAND:
        return iec101.ProtectionCommandData(
            value=_value_from_json(data_type, data['value']),
            quality=_protection_quality_from_json(data['quality']),
            operating_time=data['operating_time'])

    if data_type == DataType.STATUS:
        return iec101.StatusData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    raise ValueError('unsupported data type')


def command_to_json(cmd: iec101.Command) -> json.Data:
    if isinstance(cmd, (iec101.SingleCommand,
                        iec101.DoubleCommand,
                        iec101.RegulatingCommand)):
        return {'value': _value_to_json(cmd.value),
                'select': cmd.select,
                'qualifier': cmd.qualifier}

    if isinstance(cmd, (iec101.NormalizedCommand,
                        iec101.ScaledCommand,
                        iec101.FloatingCommand)):
        return {'value': _value_to_json(cmd.value),
                'select': cmd.select}

    if isinstance(cmd, iec101.BitstringCommand):
        return {'value': _value_to_json(cmd.value)}

    raise ValueError('unsupported command')


def command_from_json(cmd_type: CommandType,
                      cmd: json.Data
                      ) -> iec101.Command:
    if cmd_type == CommandType.SINGLE:
        return iec101.SingleCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'],
            qualifier=cmd['qualifier'])

    if cmd_type == CommandType.DOUBLE:
        return iec101.DoubleCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'],
            qualifier=cmd['qualifier'])

    if cmd_type == CommandType.REGULATING:
        return iec101.RegulatingCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'],
            qualifier=cmd['qualifier'])

    if cmd_type == CommandType.NORMALIZED:
        return iec101.NormalizedCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'])

    if cmd_type == CommandType.SCALED:
        return iec101.ScaledCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'])

    if cmd_type == CommandType.FLOATING:
        return iec101.FloatingCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'])

    if cmd_type == CommandType.BITSTRING:
        return iec101.BitstringCommand(
            value=_value_from_json(cmd_type, cmd['value']))

    raise ValueError('unsupported command type')


def time_to_source_timestamp(t: iec101.Time | None
                             ) -> hat.event.common.Timestamp | None:
    return (
        hat.event.common.timestamp_from_datetime(iec101.time_to_datetime(t))
        if t else None)


def time_from_source_timestamp(t: hat.event.common.Timestamp | None,
                               ) -> iec101.Time | None:
    return (
        iec101.time_from_datetime(hat.event.common.timestamp_to_datetime(t))
        if t else None)


def get_data_type(data: iec101.Data) -> DataType:
    if isinstance(data, iec101.SingleData):
        return DataType.SINGLE

    if isinstance(data, iec101.DoubleData):
        return DataType.DOUBLE

    if isinstance(data, iec101.StepPositionData):
        return DataType.STEP_POSITION

    if isinstance(data, iec101.BitstringData):
        return DataType.BITSTRING

    if isinstance(data, iec101.NormalizedData):
        return DataType.NORMALIZED

    if isinstance(data, iec101.ScaledData):
        return DataType.SCALED

    if isinstance(data, iec101.FloatingData):
        return DataType.FLOATING

    if isinstance(data, iec101.BinaryCounterData):
        return DataType.BINARY_COUNTER

    if isinstance(data, iec101.ProtectionData):
        return DataType.PROTECTION

    if isinstance(data, iec101.ProtectionStartData):
        return DataType.PROTECTION_START

    if isinstance(data, iec101.ProtectionCommandData):
        return DataType.PROTECTION_COMMAND

    if isinstance(data, iec101.StatusData):
        return DataType.STATUS

    raise ValueError('unsupported data')


def get_command_type(cmd: iec101.Command) -> CommandType:
    if isinstance(cmd, iec101.SingleCommand):
        return CommandType.SINGLE

    if isinstance(cmd, iec101.DoubleCommand):
        return CommandType.DOUBLE

    if isinstance(cmd, iec101.RegulatingCommand):
        return CommandType.REGULATING

    if isinstance(cmd, iec101.NormalizedCommand):
        return CommandType.NORMALIZED

    if isinstance(cmd, iec101.ScaledCommand):
        return CommandType.SCALED

    if isinstance(cmd, iec101.FloatingCommand):
        return CommandType.FLOATING

    if isinstance(cmd, iec101.BitstringCommand):
        return CommandType.BITSTRING

    raise ValueError('unsupported command')


def cause_to_json(cls: typing.Type[enum.Enum],
                  cause: enum.Enum | int
                  ) -> json.Data:
    return (cause.name if isinstance(cause, cls) else
            cause.value if isinstance(cause, enum.Enum) else
            cause)


def cause_from_json(cls: typing.Type[enum.Enum],
                    cause: json.Data
                    ) -> enum.Enum | int:
    return cls[cause] if isinstance(cause, str) else cause


def _value_to_json(value):
    if isinstance(value, (iec101.SingleValue,
                          iec101.DoubleValue,
                          iec101.RegulatingValue,
                          iec101.ProtectionValue)):
        return value.name

    if isinstance(value, iec101.StepPositionValue):
        return {'value': value.value,
                'transient': value.transient}

    if isinstance(value, iec101.BitstringValue):
        return list(value.value)

    if isinstance(value, (iec101.NormalizedValue,
                          iec101.ScaledValue,
                          iec101.FloatingValue,
                          iec101.BinaryCounterValue)):
        return value.value

    if isinstance(value, iec101.ProtectionStartValue):
        return {'general': value.general,
                'l1': value.l1,
                'l2': value.l2,
                'l3': value.l3,
                'ie': value.ie,
                'reverse': value.reverse}

    if isinstance(value, iec101.ProtectionCommandValue):
        return {'general': value.general,
                'l1': value.l1,
                'l2': value.l2,
                'l3': value.l3}

    if isinstance(value, iec101.StatusValue):
        return {'value': value.value,
                'change': value.change}

    raise ValueError('unsupported value')


def _value_from_json(data_cmd_type, value):
    if data_cmd_type in (DataType.SINGLE, CommandType.SINGLE):
        return iec101.SingleValue[value]

    if data_cmd_type in (DataType.DOUBLE, CommandType.DOUBLE):
        return iec101.DoubleValue[value]

    if data_cmd_type == CommandType.REGULATING:
        return iec101.RegulatingValue[value]

    if data_cmd_type == DataType.STEP_POSITION:
        return iec101.StepPositionValue(value=value['value'],
                                        transient=value['transient'])

    if data_cmd_type in (DataType.BITSTRING, CommandType.BITSTRING):
        return iec101.BitstringValue(value=bytes(value))

    if data_cmd_type in (DataType.NORMALIZED, CommandType.NORMALIZED):
        return iec101.NormalizedValue(value=value)

    if data_cmd_type in (DataType.SCALED, CommandType.SCALED):
        return iec101.ScaledValue(value=value)

    if data_cmd_type in (DataType.FLOATING, CommandType.FLOATING):
        return iec101.FloatingValue(value=value)

    if data_cmd_type == DataType.BINARY_COUNTER:
        return iec101.BinaryCounterValue(value=value)

    if data_cmd_type == DataType.PROTECTION:
        return iec101.ProtectionValue[value]

    if data_cmd_type == DataType.PROTECTION_START:
        return iec101.ProtectionStartValue(general=value['general'],
                                           l1=value['l1'],
                                           l2=value['l2'],
                                           l3=value['l3'],
                                           ie=value['ie'],
                                           reverse=value['reverse'])

    if data_cmd_type == DataType.PROTECTION_COMMAND:
        return iec101.ProtectionCommandValue(general=value['general'],
                                             l1=value['l1'],
                                             l2=value['l2'],
                                             l3=value['l3'])

    if data_cmd_type == DataType.STATUS:
        return iec101.StatusValue(value=value['value'],
                                  change=value['change'])

    raise ValueError('unsupported data or command type')


def _quality_to_json(quality):
    if isinstance(quality, iec101.IndicationQuality):
        return {'invalid': quality.invalid,
                'not_topical': quality.not_topical,
                'substituted': quality.substituted,
                'blocked': quality.blocked}

    if isinstance(quality, iec101.MeasurementQuality):
        return {'invalid': quality.invalid,
                'not_topical': quality.not_topical,
                'substituted': quality.substituted,
                'blocked': quality.blocked,
                'overflow': quality.overflow}

    if isinstance(quality, iec101.CounterQuality):
        return {'invalid': quality.invalid,
                'adjusted': quality.adjusted,
                'overflow': quality.overflow,
                'sequence': quality.sequence}

    if isinstance(quality, iec101.ProtectionQuality):
        return {'invalid': quality.invalid,
                'not_topical': quality.not_topical,
                'substituted': quality.substituted,
                'blocked': quality.blocked,
                'time_invalid': quality.time_invalid}

    raise ValueError('unsupported quality')


def _indication_quality_from_json(quality):
    return iec101.IndicationQuality(invalid=quality['invalid'],
                                    not_topical=quality['not_topical'],
                                    substituted=quality['substituted'],
                                    blocked=quality['blocked'])


def _measurement_quality_from_json(quality):
    return iec101.MeasurementQuality(invalid=quality['invalid'],
                                     not_topical=quality['not_topical'],
                                     substituted=quality['substituted'],
                                     blocked=quality['blocked'],
                                     overflow=quality['overflow'])


def _counter_quality_from_json(quality):
    return iec101.CounterQuality(invalid=quality['invalid'],
                                 adjusted=quality['adjusted'],
                                 overflow=quality['overflow'],
                                 sequence=quality['sequence'])


def _protection_quality_from_json(quality):
    return iec101.ProtectionQuality(invalid=quality['invalid'],
                                    not_topical=quality['not_topical'],
                                    substituted=quality['substituted'],
                                    blocked=quality['blocked'],
                                    time_invalid=quality['time_invalid'])
