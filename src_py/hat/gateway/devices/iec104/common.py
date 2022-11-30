import enum
import ssl
import typing

from hat import json
from hat.drivers import iec104
import hat.event.common

from hat.gateway.common import *  # NOQA


class SslProtocol(enum.Enum):
    TLS_CLIENT = ssl.PROTOCOL_TLS_CLIENT
    TLS_SERVER = ssl.PROTOCOL_TLS_SERVER


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
    asdu_address: iec104.AsduAddress
    io_address: iec104.IoAddress


class CommandKey(typing.NamedTuple):
    cmd_type: CommandType
    asdu_address: iec104.AsduAddress
    io_address: iec104.IoAddress


def create_ssl_ctx(conf: json.Data,
                   protocol: SslProtocol
                   ) -> ssl.SSLContext:
    ctx = ssl.SSLContext(protocol.value)
    ctx.check_hostname = False

    if conf['verify_cert']:
        ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        ctx.load_default_certs()
        if conf['ca_path']:
            ctx.load_verify_locations(cafile=conf['ca_path'])

    else:
        ctx.verify_mode = ssl.VerifyMode.CERT_NONE

    ctx.load_cert_chain(certfile=conf['cert_path'],
                        keyfile=conf['key_path'])

    return ctx


def data_to_json(data: iec104.Data) -> json.Data:
    if isinstance(data, (iec104.SingleData,
                         iec104.DoubleData,
                         iec104.StepPositionData,
                         iec104.BitstringData,
                         iec104.ScaledData,
                         iec104.FloatingData,
                         iec104.BinaryCounterData,
                         iec104.StatusData)):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality)}

    if isinstance(data, iec104.NormalizedData):
        return {'value': _value_to_json(data.value),
                'quality': (_quality_to_json(data.quality)
                            if data.quality else None)}

    if isinstance(data, iec104.ProtectionData):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality),
                'elapsed_time': data.elapsed_time}

    if isinstance(data, iec104.ProtectionStartData):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality),
                'duration_time': data.duration_time}

    if isinstance(data, iec104.ProtectionCommandData):
        return {'value': _value_to_json(data.value),
                'quality': _quality_to_json(data.quality),
                'operating_time': data.operating_time}

    raise ValueError('unsupported data')


def data_from_json(data_type: DataType,
                   data: json.Data) -> iec104.Data:
    if data_type == DataType.SINGLE:
        return iec104.SingleData(
            value=_value_from_json(data_type, data['value']),
            quality=_indication_quality_from_json(data['quality']))

    if data_type == DataType.DOUBLE:
        return iec104.DoubleData(
            value=_value_from_json(data_type, data['value']),
            quality=_indication_quality_from_json(data['quality']))

    if data_type == DataType.STEP_POSITION:
        return iec104.StepPositionData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.BITSTRING:
        return iec104.BitstringData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.NORMALIZED:
        return iec104.NormalizedData(
            value=_value_from_json(data_type, data['value']),
            quality=(_measurement_quality_from_json(data['quality'])
                     if data['quality'] else None))

    if data_type == DataType.SCALED:
        return iec104.ScaledData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.FLOATING:
        return iec104.FloatingData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    if data_type == DataType.BINARY_COUNTER:
        return iec104.BinaryCounterData(
            value=_value_from_json(data_type, data['value']),
            quality=_counter_quality_from_json(data['quality']))

    if data_type == DataType.PROTECTION:
        return iec104.ProtectionData(
            value=_value_from_json(data_type, data['value']),
            quality=_protection_quality_from_json(data['quality']),
            elapsed_time=data['elapsed_time'])

    if data_type == DataType.PROTECTION_START:
        return iec104.ProtectionStartData(
            value=_value_from_json(data_type, data['value']),
            quality=_protection_quality_from_json(data['quality']),
            duration_time=data['duration_time'])

    if data_type == DataType.PROTECTION_COMMAND:
        return iec104.ProtectionCommandData(
            value=_value_from_json(data_type, data['value']),
            quality=_protection_quality_from_json(data['quality']),
            operating_time=data['operating_time'])

    if data_type == DataType.STATUS:
        return iec104.StatusData(
            value=_value_from_json(data_type, data['value']),
            quality=_measurement_quality_from_json(data['quality']))

    raise ValueError('unsupported data type')


def command_to_json(cmd: iec104.Command) -> json.Data:
    if isinstance(cmd, (iec104.SingleCommand,
                        iec104.DoubleCommand,
                        iec104.RegulatingCommand)):
        return {'value': _value_to_json(cmd.value),
                'select': cmd.select,
                'qualifier': cmd.qualifier}

    if isinstance(cmd, (iec104.NormalizedCommand,
                        iec104.ScaledCommand,
                        iec104.FloatingCommand)):
        return {'value': _value_to_json(cmd.value),
                'select': cmd.select}

    if isinstance(cmd, iec104.BitstringCommand):
        return {'value': _value_to_json(cmd.value)}

    raise ValueError('unsupported command')


def command_from_json(cmd_type: CommandType,
                      cmd: json.Data
                      ) -> iec104.Command:
    if cmd_type == CommandType.SINGLE:
        return iec104.SingleCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'],
            qualifier=cmd['qualifier'])

    if cmd_type == CommandType.DOUBLE:
        return iec104.DoubleCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'],
            qualifier=cmd['qualifier'])

    if cmd_type == CommandType.REGULATING:
        return iec104.RegulatingCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'],
            qualifier=cmd['qualifier'])

    if cmd_type == CommandType.NORMALIZED:
        return iec104.NormalizedCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'])

    if cmd_type == CommandType.SCALED:
        return iec104.ScaledCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'])

    if cmd_type == CommandType.FLOATING:
        return iec104.FloatingCommand(
            value=_value_from_json(cmd_type, cmd['value']),
            select=cmd['select'])

    if cmd_type == CommandType.BITSTRING:
        return iec104.BitstringCommand(
            value=_value_from_json(cmd_type, cmd['value']))

    raise ValueError('unsupported command type')


def time_to_source_timestamp(t: typing.Optional[iec104.Time]
                             ) -> typing.Optional[hat.event.common.Timestamp]:
    return (
        hat.event.common.timestamp_from_datetime(iec104.time_to_datetime(t))
        if t else None)


def time_from_source_timestamp(t: typing.Optional[hat.event.common.Timestamp],
                               ) -> typing.Optional[iec104.Time]:
    return (
        iec104.time_from_datetime(hat.event.common.timestamp_to_datetime(t))
        if t else None)


def get_data_type(data: iec104.Data) -> DataType:
    if isinstance(data, iec104.SingleData):
        return DataType.SINGLE

    if isinstance(data, iec104.DoubleData):
        return DataType.DOUBLE

    if isinstance(data, iec104.StepPositionData):
        return DataType.STEP_POSITION

    if isinstance(data, iec104.BitstringData):
        return DataType.BITSTRING

    if isinstance(data, iec104.NormalizedData):
        return DataType.NORMALIZED

    if isinstance(data, iec104.ScaledData):
        return DataType.SCALED

    if isinstance(data, iec104.FloatingData):
        return DataType.FLOATING

    if isinstance(data, iec104.BinaryCounterData):
        return DataType.BINARY_COUNTER

    if isinstance(data, iec104.ProtectionData):
        return DataType.PROTECTION

    if isinstance(data, iec104.ProtectionStartData):
        return DataType.PROTECTION_START

    if isinstance(data, iec104.ProtectionCommandData):
        return DataType.PROTECTION_COMMAND

    if isinstance(data, iec104.StatusData):
        return DataType.STATUS

    raise ValueError('unsupported data')


def get_command_type(cmd: iec104.Command) -> CommandType:
    if isinstance(cmd, iec104.SingleCommand):
        return CommandType.SINGLE

    if isinstance(cmd, iec104.DoubleCommand):
        return CommandType.DOUBLE

    if isinstance(cmd, iec104.RegulatingCommand):
        return CommandType.REGULATING

    if isinstance(cmd, iec104.NormalizedCommand):
        return CommandType.NORMALIZED

    if isinstance(cmd, iec104.ScaledCommand):
        return CommandType.SCALED

    if isinstance(cmd, iec104.FloatingCommand):
        return CommandType.FLOATING

    if isinstance(cmd, iec104.BitstringCommand):
        return CommandType.BITSTRING

    raise ValueError('unsupported command')


def _value_to_json(value):
    if isinstance(value, (iec104.SingleValue,
                          iec104.DoubleValue,
                          iec104.RegulatingValue,
                          iec104.ProtectionValue)):
        return value.name

    if isinstance(value, iec104.StepPositionValue):
        return {'value': value.value,
                'transient': value.transient}

    if isinstance(value, iec104.BitstringValue):
        return list(value.value)

    if isinstance(value, (iec104.NormalizedValue,
                          iec104.ScaledValue,
                          iec104.FloatingValue,
                          iec104.BinaryCounterValue)):
        return value.value

    if isinstance(value, iec104.ProtectionStartValue):
        return {'general': value.general,
                'l1': value.l1,
                'l2': value.l2,
                'l3': value.l3,
                'ie': value.ie,
                'reverse': value.reverse}

    if isinstance(value, iec104.ProtectionCommandValue):
        return {'general': value.general,
                'l1': value.l1,
                'l2': value.l2,
                'l3': value.l3}

    if isinstance(value, iec104.StatusValue):
        return {'value': value.value,
                'change': value.change}

    raise ValueError('unsupported value')


def _value_from_json(data_cmd_type, value):
    if data_cmd_type in (DataType.SINGLE, CommandType.SINGLE):
        return iec104.SingleValue[value]

    if data_cmd_type in (DataType.DOUBLE, CommandType.DOUBLE):
        return iec104.DoubleValue[value]

    if data_cmd_type == CommandType.REGULATING:
        return iec104.RegulatingValue[value]

    if data_cmd_type == DataType.STEP_POSITION:
        return iec104.StepPositionValue(value=value['value'],
                                        transient=value['transient'])

    if data_cmd_type in (DataType.BITSTRING, CommandType.BITSTRING):
        return iec104.BitstringValue(value=bytes(value))

    if data_cmd_type in (DataType.NORMALIZED, CommandType.NORMALIZED):
        return iec104.NormalizedValue(value=value)

    if data_cmd_type in (DataType.SCALED, CommandType.SCALED):
        return iec104.ScaledValue(value=value)

    if data_cmd_type in (DataType.FLOATING, CommandType.FLOATING):
        return iec104.FloatingValue(value=value)

    if data_cmd_type == DataType.BINARY_COUNTER:
        return iec104.BinaryCounterValue(value=value)

    if data_cmd_type == DataType.PROTECTION:
        return iec104.ProtectionValue[value]

    if data_cmd_type == DataType.PROTECTION_START:
        return iec104.ProtectionStartValue(general=value['general'],
                                           l1=value['l1'],
                                           l2=value['l2'],
                                           l3=value['l3'],
                                           ie=value['ie'],
                                           reverse=value['reverse'])

    if data_cmd_type == DataType.PROTECTION_COMMAND:
        return iec104.ProtectionCommandValue(general=value['general'],
                                             l1=value['l1'],
                                             l2=value['l2'],
                                             l3=value['l3'])

    if data_cmd_type == DataType.STATUS:
        return iec104.StatusValue(value=value['value'],
                                  change=value['change'])

    raise ValueError('unsupported data or command type')


def _quality_to_json(quality):
    if isinstance(quality, iec104.IndicationQuality):
        return {'invalid': quality.invalid,
                'not_topical': quality.not_topical,
                'substituted': quality.substituted,
                'blocked': quality.blocked}

    if isinstance(quality, iec104.MeasurementQuality):
        return {'invalid': quality.invalid,
                'not_topical': quality.not_topical,
                'substituted': quality.substituted,
                'blocked': quality.blocked,
                'overflow': quality.overflow}

    if isinstance(quality, iec104.CounterQuality):
        return {'invalid': quality.invalid,
                'adjusted': quality.adjusted,
                'overflow': quality.overflow,
                'sequence': quality.sequence}

    if isinstance(quality, iec104.ProtectionQuality):
        return {'invalid': quality.invalid,
                'not_topical': quality.not_topical,
                'substituted': quality.substituted,
                'blocked': quality.blocked,
                'time_invalid': quality.time_invalid}

    raise ValueError('unsupported quality')


def _indication_quality_from_json(quality):
    return iec104.IndicationQuality(invalid=quality['invalid'],
                                    not_topical=quality['not_topical'],
                                    substituted=quality['substituted'],
                                    blocked=quality['blocked'])


def _measurement_quality_from_json(quality):
    return iec104.MeasurementQuality(invalid=quality['invalid'],
                                     not_topical=quality['not_topical'],
                                     substituted=quality['substituted'],
                                     blocked=quality['blocked'],
                                     overflow=quality['overflow'])


def _counter_quality_from_json(quality):
    return iec104.CounterQuality(invalid=quality['invalid'],
                                 adjusted=quality['adjusted'],
                                 overflow=quality['overflow'],
                                 sequence=quality['sequence'])


def _protection_quality_from_json(quality):
    return iec104.ProtectionQuality(invalid=quality['invalid'],
                                    not_topical=quality['not_topical'],
                                    substituted=quality['substituted'],
                                    blocked=quality['blocked'],
                                    time_invalid=quality['time_invalid'])
