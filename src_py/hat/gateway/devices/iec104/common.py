import enum

import hat.event.common
from hat.drivers.iec60870 import iec104


def msg_to_event(msg, event_type_prefix, device):
    if isinstance(msg, iec104.DataMsg):
        return _data_msg_to_event(msg, event_type_prefix)
    if isinstance(msg, iec104.CommandMsg):
        return _command_msg_to_event(msg, event_type_prefix, device)
    elif isinstance(msg, iec104.InterrogationMsg):
        return _interrogation_msg_to_event(
            msg, event_type_prefix, device, is_counter=False)
    elif isinstance(msg, iec104.CounterInterrogationMsg):
        return _interrogation_msg_to_event(
            msg, event_type_prefix, device, is_counter=True)
    raise Exception('message not supported')


def event_to_msg(event, event_type_prefix, device):
    etype_suffix = event.event_type[len(event_type_prefix):]
    if etype_suffix[:2] == ('system', 'data'):
        data_type = etype_suffix[2]
        asdu = int(etype_suffix[3])
        io = int(etype_suffix[4])
        return _event_to_data_msg(event, data_type, asdu, io)
    if etype_suffix[:2] == ('system', 'command'):
        command_type = etype_suffix[2]
        asdu = int(etype_suffix[3])
        io = int(etype_suffix[4])
        return _event_to_command_msg(
            event, command_type, asdu, io, device)
    elif etype_suffix[:2] == ('system', 'interrogation'):
        asdu = int(etype_suffix[2])
        return _event_to_interrogation_msg(
            event, asdu, device, is_counter=False)
    elif etype_suffix[:2] == ('system', 'counter_interrogation'):
        asdu = int(etype_suffix[2])
        return _event_to_interrogation_msg(
            event, asdu, device, is_counter=True)
    else:
        raise Exception('event type not supported')


def _data_msg_to_event(msg, event_type_prefix):
    source_timestamp = hat.event.common.timestamp_from_datetime(
        iec104.time_to_datetime(msg.time)) if msg.time else None
    data_type = _msg_to_data_type(msg)
    payload = _msg_to_data_payload(msg)
    return hat.event.common.RegisterEvent(
            event_type=(*event_type_prefix, 'gateway', 'data',
                        data_type, str(msg.asdu_address), str(msg.io_address)),
            source_timestamp=source_timestamp,
            payload=hat.event.common.EventPayload(
                type=hat.event.common.EventPayloadType.JSON,
                data=payload))


def _command_msg_to_event(msg, event_type_prefix, device):
    command_type = _msg_to_command_type(msg)
    payload = _msg_to_command_payload(msg, device)
    source_timestamp = hat.event.common.timestamp_from_datetime(
        iec104.time_to_datetime(msg.time)) if msg.time else None
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway', 'command',
                    command_type, str(msg.asdu_address), str(msg.io_address)),
        source_timestamp=source_timestamp,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))


def _interrogation_msg_to_event(msg, event_type_prefix, device, is_counter):
    if device == 'master':
        if msg.is_negative_confirm:
            status = 'ERROR'
        elif msg.cause == iec104.CommandResCause.ACTIVATION_CONFIRMATION:
            status = 'START'
        elif msg.cause == iec104.CommandResCause.ACTIVATION_TERMINATION:
            status = 'STOP'
        else:
            status = 'ERROR'
        payload = {'status': status,
                   'request': msg.request}
    else:
        if msg.cause != iec104.CommandReqCause.ACTIVATION:
            return
        payload = {'request': msg.request}
    if is_counter:
        payload['freeze'] = msg.freeze.name
    return hat.event.common.RegisterEvent(
        event_type=(*event_type_prefix, 'gateway',
                    'counter_interrogation' if is_counter else 'interrogation',
                    str(msg.asdu_address)),
        source_timestamp=None,
        payload=hat.event.common.EventPayload(
            type=hat.event.common.EventPayloadType.JSON,
            data=payload))


def _msg_to_data_type(msg):
    if isinstance(msg.data, iec104.SingleData):
        return 'single'
    elif isinstance(msg.data, iec104.DoubleData):
        return 'double'
    elif isinstance(msg.data, iec104.StepPositionData):
        return 'step_position'
    elif isinstance(msg.data, iec104.BitstringData):
        return 'bitstring'
    elif isinstance(msg.data, iec104.NormalizedData):
        return 'normalized'
    elif isinstance(msg.data, iec104.ScaledData):
        return 'scaled'
    elif isinstance(msg.data, iec104.FloatingData):
        return 'floating'
    elif isinstance(msg.data, iec104.BinaryCounterData):
        return 'binary_counter'
    elif isinstance(msg.data, iec104.ProtectionData):
        return 'protection'
    elif isinstance(msg.data, iec104.ProtectionStartData):
        return 'protection_start'
    elif isinstance(msg.data, iec104.ProtectionCommandData):
        return 'protection_command'
    elif isinstance(msg.data, iec104.StatusData):
        return 'status'
    raise Exception('data message not supported')


def _msg_to_data_payload(msg):
    if isinstance(msg.cause, enum.Enum):
        cause = ('INTERROGATED' if msg.cause.name.startswith('INTERROGATED')
                 else msg.cause.name)
    else:
        cause = msg.cause
    payload = {
        'value': _msg_to_data_value(msg),
        'quality': msg.data.quality._asdict() if msg.data.quality else None,
        'cause': cause,
        'test': msg.is_test
    }
    if isinstance(msg.data, iec104.ProtectionData):
        payload['elapsed_time'] = msg.data.elapsed_time
    if isinstance(msg.data, iec104.ProtectionStartData):
        payload['duration_time'] = msg.data.duration_time
    if isinstance(msg.data, iec104.ProtectionCommandData):
        payload['operating_time'] = msg.data.operating_time
    return payload


def _msg_to_data_value(msg):
    if isinstance(msg.data, (iec104.SingleData,
                             iec104.DoubleData,
                             iec104.ProtectionData)):
        return msg.data.value.name
    elif isinstance(msg.data, (iec104.StepPositionData,
                               iec104.ProtectionStartData,
                               iec104.ProtectionCommandData,
                               iec104.StatusData)):
        return msg.data.value._asdict()
    elif isinstance(msg.data, iec104.BitstringData):
        return list(msg.data.value.value)
    if isinstance(msg.data, (iec104.NormalizedData,
                             iec104.ScaledData,
                             iec104.FloatingData,
                             iec104.BinaryCounterData)):
        return msg.data.value.value
    raise Exception('data message not supported')


def _msg_to_command_type(msg):
    if isinstance(msg.command, iec104.SingleCommand):
        return 'single'
    elif isinstance(msg.command, iec104.DoubleCommand):
        return 'double'
    elif isinstance(msg.command, iec104.RegulatingCommand):
        return 'regulating'
    elif isinstance(msg.command, iec104.NormalizedCommand):
        return 'normalized'
    elif isinstance(msg.command, iec104.ScaledCommand):
        return 'scaled'
    elif isinstance(msg.command, iec104.FloatingCommand):
        return 'floating'
    elif isinstance(msg.command, iec104.BitstringCommand):
        return 'bitstring'
    raise Exception('command message not supported')


def _msg_to_command_payload(msg, device):
    cause = msg.cause.name if isinstance(msg.cause, enum.Enum) else msg.cause
    payload = {'value': _msg_to_command_value(msg),
               'cause': cause}
    if device == 'master':
        payload['success'] = not msg.is_negative_confirm
    if hasattr(msg.command, 'select'):
        payload['select'] = msg.command.select
    if hasattr(msg.command, 'qualifier'):
        payload['qualifier'] = msg.command.qualifier
    return payload


def _msg_to_command_value(msg):
    if isinstance(msg.command, (iec104.SingleCommand,
                                iec104.DoubleCommand,
                                iec104.RegulatingCommand)):
        return msg.command.value.name
    elif isinstance(msg.command, iec104.BitstringCommand):
        return list(msg.command.value.value)
    return msg.command.value.value


def _event_to_data_msg(event, data_type, asdu, io):
    if not isinstance(event.payload.data['cause'], str):
        cause = event.payload.data['cause']
    elif event.payload.data['cause'] != 'INTERROGATED':
        cause = iec104.DataResCause[event.payload.data['cause']]
    elif data_type == 'binary_counter':
        cause = iec104.DataResCause.INTERROGATED_COUNTER
    else:
        cause = iec104.DataResCause.INTERROGATED_STATION
    return iec104.DataMsg(
        is_test=event.payload.data['test'],
        originator_address=0,
        asdu_address=asdu,
        io_address=io,
        data=_event_to_data(event, data_type),
        time=_source_timestamp_to_time_iec104(event.source_timestamp),
        cause=cause)


def _event_to_command_msg(event, command_type, asdu, io, device):
    if not isinstance(event.payload.data['cause'], str):
        cause = event.payload.data['cause']
    elif device == 'master':
        cause = iec104.CommandReqCause[event.payload.data['cause']]
    elif device == 'slave':
        cause = iec104.CommandResCause[event.payload.data['cause']]
    else:
        raise ValueError('unsupported device/cause')
    if device == 'slave':
        is_negative_confirm = not event.payload.data['success']
    else:
        is_negative_confirm = False
    return iec104.CommandMsg(
        is_test=False,
        originator_address=0,
        asdu_address=asdu,
        io_address=io,
        command=_event_to_command(event, command_type),
        is_negative_confirm=is_negative_confirm,
        time=_source_timestamp_to_time_iec104(event.source_timestamp),
        cause=cause)


def _event_to_interrogation_msg(event, asdu, device, is_counter):
    if device == 'master':
        cause = iec104.CommandReqCause.ACTIVATION
        is_negative_confirm = False
    else:
        if event.payload.data['status'] == 'START':
            cause = iec104.CommandResCause.ACTIVATION_CONFIRMATION
            is_negative_confirm = False
        elif event.payload.data['status'] == 'STOP':
            cause = iec104.CommandResCause.ACTIVATION_TERMINATION
            is_negative_confirm = False
        else:
            cause = iec104.CommandResCause.ACTIVATION_CONFIRMATION
            is_negative_confirm = True
    if is_counter:
        return iec104.CounterInterrogationMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu,
            request=event.payload.data['request'],
            freeze=iec104.FreezeCode[event.payload.data['freeze']],
            is_negative_confirm=is_negative_confirm,
            cause=cause)
    else:
        return iec104.InterrogationMsg(
            is_test=False,
            originator_address=0,
            asdu_address=asdu,
            request=event.payload.data['request'],
            is_negative_confirm=is_negative_confirm,
            cause=cause)


def _source_timestamp_to_time_iec104(source_timestamp):
    return iec104.time_from_datetime(
            hat.event.common.timestamp_to_datetime(
                source_timestamp)) if source_timestamp else None


def _event_to_command(event, command_type):
    if command_type == 'single':
        return iec104.SingleCommand(
            value=iec104.SingleValue[event.payload.data['value']],
            select=event.payload.data['select'],
            qualifier=event.payload.data['qualifier'])
    elif command_type == 'double':
        return iec104.DoubleCommand(
            value=iec104.DoubleValue[event.payload.data['value']],
            select=event.payload.data['select'],
            qualifier=event.payload.data['qualifier'])
    elif command_type == 'regulating':
        return iec104.RegulatingCommand(
            value=iec104.RegulatingValue[event.payload.data['value']],
            select=event.payload.data['select'],
            qualifier=event.payload.data['qualifier'])
    elif command_type == 'normalized':
        return iec104.NormalizedCommand(
            value=iec104.NormalizedValue(
                value=event.payload.data['value']),
            select=event.payload.data['select'])
    elif command_type == 'scaled':
        return iec104.ScaledCommand(
            value=iec104.ScaledValue(
                value=event.payload.data['value']),
            select=event.payload.data['select'])
    elif command_type == 'floating':
        return iec104.FloatingCommand(
            value=iec104.FloatingValue(
                value=event.payload.data['value']),
            select=event.payload.data['select'])
    elif command_type == 'bitstring':
        return iec104.BitstringCommand(
            value=iec104.BitstringValue(
                value=bytes(event.payload.data['value'])))
    raise Exception('command type not supported')


def _event_to_data(event, data_type):
    if data_type == 'single':
        return iec104.SingleData(
            value=iec104.SingleValue[event.payload.data['value']],
            quality=_event_to_quality(event, data_type))
    if data_type == 'double':
        return iec104.DoubleData(
            value=iec104.DoubleValue[event.payload.data['value']],
            quality=_event_to_quality(event, data_type))
    if data_type == 'step_position':
        return iec104.StepPositionData(
            value=iec104.StepPositionValue(**event.payload.data['value']),
            quality=_event_to_quality(event, data_type))
    if data_type == 'bitstring':
        return iec104.BitstringData(
            value=iec104.BitstringValue(
                value=bytes(event.payload.data['value'])),
            quality=_event_to_quality(event, data_type))
    if data_type == 'normalized':
        return iec104.NormalizedData(
            value=iec104.NormalizedValue(value=event.payload.data['value']),
            quality=_event_to_quality(event, data_type))
    if data_type == 'scaled':
        return iec104.ScaledData(
            value=iec104.ScaledValue(value=event.payload.data['value']),
            quality=_event_to_quality(event, data_type))
    if data_type == 'floating':
        return iec104.FloatingData(
            value=iec104.FloatingValue(value=event.payload.data['value']),
            quality=_event_to_quality(event, data_type))
    if data_type == 'binary_counter':
        return iec104.BinaryCounterData(
            value=iec104.BinaryCounterValue(value=event.payload.data['value']),
            quality=_event_to_quality(event, data_type))
    if data_type == 'protection':
        return iec104.ProtectionData(
            value=iec104.ProtectionValue[event.payload.data['value']],
            quality=_event_to_quality(event, data_type),
            elapsed_time=event.payload.data['elapsed_time'])
    if data_type == 'protection_start':
        return iec104.ProtectionStartData(
            value=iec104.ProtectionStartValue(**event.payload.data['value']),
            quality=_event_to_quality(event, data_type),
            duration_time=event.payload.data['duration_time'])
    if data_type == 'protection_command':
        return iec104.ProtectionCommandData(
            value=iec104.ProtectionCommandValue(**event.payload.data['value']),
            quality=_event_to_quality(event, data_type),
            operating_time=event.payload.data['operating_time'])
    if data_type == 'status':
        return iec104.StatusData(
            value=iec104.StatusValue(**event.payload.data['value']),
            quality=_event_to_quality(event, data_type))
    raise Exception('data type not supported')


def _event_to_quality(event, data_type):
    quality = {
        'single': iec104.IndicationQuality,
        'double': iec104.IndicationQuality,
        'step_position': iec104.MeasurementQuality,
        'bitstring': iec104.MeasurementQuality,
        'normalized': iec104.MeasurementQuality,
        'scaled': iec104.MeasurementQuality,
        'floating': iec104.MeasurementQuality,
        'binary_counter': iec104.CounterQuality,
        'protection': iec104.ProtectionQuality,
        'protection_start': iec104.ProtectionQuality,
        'protection_command': iec104.ProtectionQuality,
        'status': iec104.MeasurementQuality,
    }[data_type]
    return quality(**event.payload.data['quality'])
