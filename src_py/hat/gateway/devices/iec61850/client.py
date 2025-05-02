"""IEC 61850 client device"""

from collections.abc import Collection
import asyncio
import collections
import datetime
import logging

from hat import aio
from hat import json
from hat.drivers import iec61850
from hat.drivers import tcp
import hat.event.common

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)


termination_timeout: int = 100

report_segments_timeout: int = 100


async def create(conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec61850ClientDevice':

    entry_id_event_types = [
        (*event_type_prefix, 'gateway', 'entry_id', i['report_id'])
        for i in conf['rcbs'] if i['ref']['type'] == 'BUFFERED']
    if entry_id_event_types:
        result = await eventer_client.query(
            hat.event.common.QueryLatestParams(entry_id_event_types))
        rcbs_entry_ids = {
            event.type[5]: (bytes.fromhex(event.payload.data)
                            if event.payload.data is not None else None)
            for event in result.events}
    else:
        rcbs_entry_ids = {}

    device = Iec61850ClientDevice()

    device._rcbs_entry_ids = rcbs_entry_ids
    device._conf = conf
    device._eventer_client = eventer_client
    device._event_type_prefix = event_type_prefix
    device._conn = None
    device._conn_status = None
    device._terminations = {}
    device._reports_segments = {}

    device._data_ref_names = {(i['ref']['logical_device'],
                               i['ref']['logical_node'],
                               *i['ref']['names']): i['name']
                              for i in conf['data']}
    device._command_name_confs = {i['name']: i for i in conf['commands']}
    device._command_name_ctl_nums = {i['name']: 0 for i in conf['commands']}
    device._change_name_value_refs = {
        i['name']: _value_ref_from_conf(i['ref']) for i in conf['changes']}
    device._rcb_type = {rcb_conf['report_id']: rcb_conf['ref']['type']
                        for rcb_conf in conf['rcbs']}

    device._persist_dyn_datasets = set(_get_persist_dyn_datasets(conf))
    device._dyn_datasets_values = {
        ds_ref: values
        for ds_ref, values in _get_dyn_datasets_values(conf)}

    dataset_confs = {_dataset_ref_from_conf(ds_conf['ref']): ds_conf
                     for ds_conf in device._conf['datasets']}
    device._report_value_refs = collections.defaultdict(collections.deque)
    device._values_data = collections.defaultdict(set)
    for rcb_conf in device._conf['rcbs']:
        ds_ref = _dataset_ref_from_conf(rcb_conf['dataset'])
        dataset_conf = dataset_confs[ds_ref]
        for value_conf in dataset_conf['values']:
            value_ref = _value_ref_from_conf(value_conf)
            device._report_value_refs[rcb_conf['report_id']].append(value_ref)
            for data_ref in device._data_ref_names.keys():
                if _data_matches_value(data_ref, value_ref):
                    device._values_data[value_ref].add(data_ref)

    value_types = _value_types_from_conf(conf)
    device._value_types = value_types
    device._cmd_value_types = {
        cmd_ref: value_type
        for cmd_ref, value_type in _get_command_value_types(conf, value_types)}
    device._data_value_types = {
        value_ref: value_type
        for value_ref, value_type in _get_data_value_types(conf, value_types)}

    device._async_group = aio.Group()
    device._async_group.spawn(device._connection_loop)
    device._loop = asyncio.get_running_loop()

    return device


info: common.DeviceInfo = common.DeviceInfo(
    type="iec61850_client",
    create=create,
    json_schema_id="hat-gateway://iec61850.yaml#/$defs/client",
    json_schema_repo=common.json_schema_repo)


class Iec61850ClientDevice(common.Device):

    @property
    def async_group(self) -> aio.Group:
        return self._async_group

    async def process_events(self, events: Collection[hat.event.common.Event]):
        try:
            for event in events:
                suffix = event.type[len(self._event_type_prefix):]

                if suffix[:2] == ('system', 'command'):
                    cmd_name, = suffix[2:]
                    await self._process_cmd_req(event, cmd_name)

                elif suffix[:2] == ('system', 'change'):
                    val_name, = suffix[2:]
                    await self._process_change_req(event, val_name)

                else:
                    raise Exception('unsupported event type')

        except Exception as e:
            mlog.warning('error processing event: %s', e, exc_info=e)

    async def _connection_loop(self):

        async def cleanup():
            await self._register_status('DISCONNECTED')

            if self._conn:
                await self._conn.async_close()

        conn_conf = self._conf['connection']
        try:
            while True:
                await self._register_status('CONNECTING')
                try:
                    mlog.debug('connecting to %s:%s',
                               conn_conf['host'], conn_conf['port'])
                    self._conn = await aio.wait_for(
                        iec61850.connect(
                            addr=tcp.Address(conn_conf['host'],
                                             conn_conf['port']),
                            data_value_types=self._data_value_types,
                            cmd_value_types=self._cmd_value_types,
                            report_data_refs=self._report_value_refs,
                            report_cb=self._on_report,
                            termination_cb=self._on_termination,
                            status_delay=conn_conf['status_delay'],
                            status_timeout=conn_conf['status_timeout'],
                            local_tsel=conn_conf.get('local_tsel'),
                            remote_tsel=conn_conf.get('remote_tsel'),
                            local_ssel=conn_conf.get('local_ssel'),
                            remote_ssel=conn_conf.get('remote_ssel'),
                            local_psel=conn_conf.get('local_psel'),
                            remote_psel=conn_conf.get('remote_psel'),
                            local_ap_title=conn_conf.get('local_ap_title'),
                            remote_ap_title=conn_conf.get('remote_ap_title'),
                            local_ae_qualifier=conn_conf.get(
                                'local_ae_qualifier'),
                            remote_ae_qualifier=conn_conf.get(
                                'remote_ae_qualifier'),
                            local_detail_calling=conn_conf.get(
                                'local_detail_calling')),
                        conn_conf['connect_timeout'])

                except Exception as e:
                    mlog.warning('connnection failed: %s', e, exc_info=e)
                    await self._register_status('DISCONNECTED')
                    await asyncio.sleep(conn_conf['reconnect_delay'])
                    continue

                mlog.debug('connected')
                await self._register_status('CONNECTED')

                initialized = False
                try:
                    await self._create_dynamic_datasets()
                    for rcb_conf in self._conf['rcbs']:
                        await self._init_rcb(rcb_conf)
                    initialized = True

                except Exception as e:
                    mlog.warning(
                        'initialization failed: %s, closing connection',
                        e, exc_info=e)
                    self._conn.close()

                await self._conn.wait_closing()
                await self._register_status('DISCONNECTED')
                await self._conn.wait_closed()
                self._conn = None
                self._terminations = {}
                self._reports_segments = {}
                if not initialized:
                    await asyncio.sleep(conn_conf['reconnect_delay'])

        except Exception as e:
            mlog.error('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection loop')
            self.close()
            await aio.uncancellable(cleanup())

    async def _process_cmd_req(self, event, cmd_name):
        if not self._conn or not self._conn.is_open:
            mlog.warning('command %s ignored: no connection', cmd_name)
            return

        if cmd_name not in self._command_name_confs:
            raise Exception('unexpected command name')

        cmd_conf = self._command_name_confs[cmd_name]
        cmd_ref = iec61850.CommandRef(**cmd_conf['ref'])
        action = event.payload.data['action']
        evt_session_id = event.payload.data['session_id']
        if (action == 'SELECT' and
                cmd_conf['model'] == 'SBO_WITH_NORMAL_SECURITY'):
            cmd = None
        else:
            ctl_num = self._command_name_ctl_nums[cmd_name]
            ctl_num = _update_ctl_num(ctl_num, action, cmd_conf['model'])
            self._command_name_ctl_nums[cmd_name] = ctl_num
            value_type = _value_type_from_ref(self._value_types, cmd_ref)
            cmd = _command_from_event(event, cmd_conf, ctl_num, value_type)

        term_future = None
        if (action == 'OPERATE' and
                cmd_conf['model'] in ['DIRECT_WITH_ENHANCED_SECURITY',
                                      'SBO_WITH_ENHANCED_SECURITY']):
            term_future = self._loop.create_future()
            self._conn.async_group.spawn(
                self._wait_cmd_term, cmd_name, cmd_ref, cmd, evt_session_id,
                term_future)

        try:
            resp = await aio.wait_for(
                self._send_command(action, cmd_ref, cmd),
                self._conf['connection']['response_timeout'])

        except (asyncio.TimeoutError, ConnectionError) as e:
            mlog.warning('send command failed: %s', e, exc_info=e)
            if term_future and not term_future.done():
                term_future.cancel()
            return

        if resp is not None:
            if term_future and not term_future.done():
                term_future.cancel()

        event = _cmd_resp_to_event(
            self._event_type_prefix, cmd_name, evt_session_id, action, resp)
        await self._register_events([event])

    async def _send_command(self, action, cmd_ref, cmd):
        if action == 'SELECT':
            return await self._conn.select(cmd_ref, cmd)

        if action == 'CANCEL':
            return await self._conn.cancel(cmd_ref, cmd)

        if action == 'OPERATE':
            return await self._conn.operate(cmd_ref, cmd)

        raise Exception('unsupported action')

    async def _wait_cmd_term(self, cmd_name, cmd_ref, cmd, session_id, future):
        cmd_session_id = _get_command_session_id(cmd_ref, cmd)
        self._terminations[cmd_session_id] = future
        try:
            term = await aio.wait_for(future, termination_timeout)
            event = _cmd_resp_to_event(
                self._event_type_prefix, cmd_name, session_id, 'TERMINATION',
                term.error)
            await self._register_events([event])

        except asyncio.TimeoutError:
            mlog.warning('command termination timeout')

        finally:
            del self._terminations[cmd_session_id]

    async def _process_change_req(self, event, value_name):
        if not self._conn or not self._conn.is_open:
            mlog.warning('change event %s ignored: no connection', value_name)
            return

        if value_name not in self._change_name_value_refs:
            raise Exception('unexpected command name')

        ref = self._change_name_value_refs[value_name]
        value_type = _value_type_from_ref(self._value_types, ref)
        if value_type is None:
            raise Exception('value type undefined')

        value = _value_from_json(event.payload.data['value'], value_type)
        try:
            resp = await aio.wait_for(
                self._conn.write_data(ref, value),
                self._conf['connection']['response_timeout'])

        except asyncio.TimeoutError:
            mlog.warning('write data response timeout')
            return

        except ConnectionError as e:
            mlog.warning('connection error on write data: %s', e, exc_info=e)
            return

        session_id = event.payload.data['session_id']
        event = _write_data_resp_to_event(
            self._event_type_prefix, value_name, session_id, resp)
        await self._register_events([event])

    async def _on_report(self, report):
        if report.report_id not in self._report_value_refs:
            mlog.warning('unexpected report dropped')
            return

        seq_num = report.sequence_number
        if report.more_segments_follow:
            if seq_num in self._reports_segments:
                segment_data, timeout_timer = self._reports_segments[seq_num]
                timeout_timer.cancel()
            else:
                segment_data = collections.deque()

            segment_data.extend(report.data)
            timeout_timer = self._loop.call_later(
                report_segments_timeout, self._reports_segments.pop, seq_num)
            self._reports_segments[seq_num] = (segment_data, timeout_timer)
            return

        if seq_num in self._reports_segments:
            report_data, timeout_timer = self._reports_segments[seq_num]
            timeout_timer.cancel()
            report_data.extend(report.data)

        else:
            report_data = report.data

        events = collections.deque()

        events.extend(self._report_data_to_events(report_data))

        if self._rcb_type[report.report_id] == 'BUFFERED':
            events.append(hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway',
                      'entry_id', report.report_id),
                source_timestamp=None,
                payload=hat.event.common.EventPayloadJson(
                    report.entry_id.hex()
                    if report.entry_id is not None else None)))

        await self._register_events(events)
        if self._rcb_type[report.report_id] == 'BUFFERED':
            self._rcbs_entry_ids[report.report_id] = report.entry_id

    def _report_data_to_events(self, report_data):
        report_data_values_types = collections.defaultdict(collections.deque)
        for rv in report_data:
            data_refs = self._values_data.get(rv.ref)
            value_type = _value_type_from_ref(self._value_types, rv.ref)
            for data_ref in data_refs:
                report_data_values_types[data_ref].append((rv, value_type))

        for data_ref, report_values_types in report_data_values_types.items():
            data_name = self._data_ref_names[data_ref]
            try:
                yield _report_values_to_event(
                    self._event_type_prefix, report_values_types, data_name,
                    data_ref)

            except Exception as e:
                mlog.warning('report values dropped: %s', e, exc_info=e)
                continue

    def _on_termination(self, termination):
        cmd_session_id = _get_command_session_id(
            termination.ref, termination.cmd)
        if cmd_session_id not in self._terminations:
            mlog.warning('unexpected termination dropped')
            return

        term_future = self._terminations[cmd_session_id]
        if not term_future.done():
            self._terminations[cmd_session_id].set_result(termination)

    async def _init_rcb(self, rcb_conf):
        ref = iec61850.RcbRef(
            logical_device=rcb_conf['ref']['logical_device'],
            logical_node=rcb_conf['ref']['logical_node'],
            type=iec61850.RcbType[rcb_conf['ref']['type']],
            name=rcb_conf['ref']['name'])
        mlog.debug('initiating rcb %s', ref)

        get_attrs = collections.deque((iec61850.RcbAttrType.REPORT_ID,
                                       iec61850.RcbAttrType.DATASET))
        if 'conf_revision' in rcb_conf:
            get_attrs.append(iec61850.RcbAttrType.CONF_REVISION)

        get_rcb_resp = await self._conn.get_rcb_attrs(ref, get_attrs)
        _validate_get_rcb_response(get_rcb_resp, rcb_conf)

        await self._set_rcb(ref, [(iec61850.RcbAttrType.REPORT_ENABLE, False)])

        if ref.type == iec61850.RcbType.BUFFERED:
            if 'reservation_time' in rcb_conf:
                await self._set_rcb(
                    ref, [(iec61850.RcbAttrType.RESERVATION_TIME,
                           rcb_conf['reservation_time'])])

            entry_id = self._rcbs_entry_ids.get(rcb_conf['report_id'])
            if rcb_conf.get('purge_buffer') or entry_id is None:
                await self._set_rcb(
                    ref, [(iec61850.RcbAttrType.PURGE_BUFFER, True)])

            else:
                try:
                    await self._set_rcb(
                        ref, [(iec61850.RcbAttrType.ENTRY_ID, entry_id)],
                        critical=True)

                except Exception as e:
                    mlog.warning('%s', e, exc_info=e)
                    # try setting entry id to 0 in order to resynchronize
                    await self._set_rcb(
                        ref, [(iec61850.RcbAttrType.ENTRY_ID, b'\x00')])

        elif ref.type == iec61850.RcbType.UNBUFFERED:
            await self._set_rcb(ref, [(iec61850.RcbAttrType.RESERVE, True)])

        attrs = collections.deque()
        if 'trigger_options' in rcb_conf:
            attrs.append((iec61850.RcbAttrType.TRIGGER_OPTIONS,
                          set(iec61850.TriggerCondition[i]
                              for i in rcb_conf['trigger_options'])))
        if 'optional_fields' in rcb_conf:
            attrs.append((iec61850.RcbAttrType.OPTIONAL_FIELDS,
                          set(iec61850.OptionalField[i]
                              for i in rcb_conf['optional_fields'])))
        if 'buffer_time' in rcb_conf:
            attrs.append((iec61850.RcbAttrType.BUFFER_TIME,
                          rcb_conf['buffer_time']))
        if 'integrity_period' in rcb_conf:
            attrs.append((iec61850.RcbAttrType.INTEGRITY_PERIOD,
                          rcb_conf['integrity_period']))
        if attrs:
            await self._set_rcb(ref, attrs)

        await self._set_rcb(
            ref, [(iec61850.RcbAttrType.REPORT_ENABLE, True)], critical=True)
        await self._set_rcb(
            ref, [(iec61850.RcbAttrType.GI, True)], critical=True)
        mlog.debug('rcb %s initiated', ref)

    async def _set_rcb(self, ref, attrs, critical=False):
        try:
            resp = await self._conn.set_rcb_attrs(ref, attrs)
            attrs_failed = set((attr, attr_res)
                               for attr, attr_res in resp.items()
                               if isinstance(attr_res, iec61850.ServiceError))
            if attrs_failed:
                raise Exception(f"set attribute errors: {attrs_failed}")

        except Exception as e:
            if critical:
                raise Exception(f'set rcb {ref} failed') from e

            else:
                mlog.warning('set rcb %s failed: %s', ref, e, exc_info=e)

    async def _create_dynamic_datasets(self):
        logical_devices = set(i.logical_device
                              for i in self._persist_dyn_datasets)
        existing_ds_refs = set()
        for ld in logical_devices:
            res = await self._conn.get_persisted_dataset_refs(ld)
            if isinstance(res, iec61850.ServiceError):
                raise Exception(f'get datasets for ld {ld} failed: {res}')

            existing_ds_refs.update(res)

        existing_persisted_ds_refs = existing_ds_refs.intersection(
            self._persist_dyn_datasets)
        for ds_ref, ds_value_refs in self._dyn_datasets_values.items():
            if ds_ref in existing_persisted_ds_refs:
                res = await self._conn.get_dataset_data_refs(ds_ref)
                if isinstance(res, iec61850.ServiceError):
                    raise Exception(f'get ds {ds_ref} data refs failed: {res}')
                else:
                    exist_ds_value_refs = res

                if ds_value_refs == list(exist_ds_value_refs):
                    mlog.debug('dataset %s already exists', ds_ref)
                    continue

                mlog.debug("dataset %s exists, but different", ds_ref)
                res = await self._conn.delete_dataset(ds_ref)
                if res is not None:
                    raise Exception(f'delete dataset {ds_ref} failed: {res}')
                mlog.debug("dataset %s deleted", ds_ref)

            res = await self._conn.create_dataset(ds_ref, ds_value_refs)
            if res is not None:
                raise Exception(f'create dataset {ds_ref} failed: {res}')

            mlog.debug("dataset %s crated", ds_ref)

    async def _register_events(self, events):
        try:
            await self._eventer_client.register(events)

        except ConnectionError:
            self.close()

    async def _register_status(self, status):
        if status == self._conn_status:
            return

        event = hat.event.common.RegisterEvent(
            type=(*self._event_type_prefix, 'gateway', 'status'),
            source_timestamp=None,
            payload=hat.event.common.EventPayloadJson(status))
        await self._register_events([event])
        self._conn_status = status
        mlog.debug('registered status %s', status)


def _value_ref_from_conf(value_ref_conf):
    return iec61850.DataRef(
        logical_device=value_ref_conf['logical_device'],
        logical_node=value_ref_conf['logical_node'],
        fc=value_ref_conf['fc'],
        names=tuple(value_ref_conf['names']))


def _dataset_ref_from_conf(ds_conf):
    if isinstance(ds_conf, str):
        return iec61850.NonPersistedDatasetRef(ds_conf)

    elif isinstance(ds_conf, dict):
        return iec61850.PersistedDatasetRef(**ds_conf)


def _data_matches_value(data_path, value_ref):
    value_path = (value_ref.logical_device,
                  value_ref.logical_node,
                  *value_ref.names)
    if len(data_path) == len(value_path):
        return data_path == value_path

    if len(data_path) > len(value_path):
        return data_path[:len(value_path)] == value_path

    return value_path[:len(data_path)] == data_path


def _get_persist_dyn_datasets(conf):
    for ds_conf in conf['datasets']:
        if not ds_conf['dynamic']:
            continue

        ds_ref = _dataset_ref_from_conf(ds_conf['ref'])
        if isinstance(ds_ref, iec61850.PersistedDatasetRef):
            yield ds_ref


def _get_dyn_datasets_values(conf):
    for ds_conf in conf['datasets']:
        if not ds_conf['dynamic']:
            continue

        ds_ref = _dataset_ref_from_conf(ds_conf['ref'])
        yield ds_ref, [_value_ref_from_conf(val_conf)
                       for val_conf in ds_conf['values']]


def _get_data_value_types(conf, value_types):
    for ds_conf in conf['datasets']:
        for val_ref_conf in ds_conf['values']:
            value_ref = _value_ref_from_conf(val_ref_conf)
            value_type = _value_type_to_iec61850(
                _value_type_from_ref(value_types, value_ref))
            yield value_ref, value_type

    for change_conf in conf['changes']:
        value_ref = _value_ref_from_conf(change_conf['ref'])
        value_type = _value_type_to_iec61850(
            _value_type_from_ref(value_types, value_ref))
        yield value_ref, value_type


def _get_command_value_types(conf, value_types):
    for cmd_conf in conf['commands']:
        cmd_ref = iec61850.CommandRef(**cmd_conf['ref'])
        value_type = _value_type_to_iec61850(
            _value_type_from_ref(value_types, cmd_ref))
        yield cmd_ref, value_type


def _value_types_from_conf(conf):
    value_types = {}
    for vt in conf['value_types']:
        ref = (vt['logical_device'],
               vt['logical_node'],
               vt['name'])
        value_type = _value_type_from_vt_conf(vt['type'])
        if ref in value_types:
            value_types[ref] = _merge_types(
                value_type, value_types[ref], set(ref))
        else:
            value_types[ref] = value_type

    return value_types


def _value_type_from_vt_conf(vt_conf):
    if isinstance(vt_conf, str):
        if vt_conf in ['BOOLEAN',
                       'INTEGER',
                       'UNSIGNED',
                       'FLOAT',
                       'BIT_STRING',
                       'OCTET_STRING',
                       'VISIBLE_STRING',
                       'MMS_STRING']:
            return iec61850.BasicValueType(vt_conf)

        if vt_conf in ['QUALITY',
                       'TIMESTAMP',
                       'DOUBLE_POINT',
                       'DIRECTION',
                       'SEVERITY',
                       'ANALOGUE',
                       'VECTOR',
                       'STEP_POSITION',
                       'BINARY_CONTROL']:
            return iec61850.AcsiValueType(vt_conf)

        raise Exception('unsupported value type')

    if vt_conf['type'] == 'ARRAY':
        element_type = _value_type_from_vt_conf(vt_conf['element_type'])
        return iec61850.ArrayValueType(element_type)

    if vt_conf['type'] == 'STRUCT':
        return {el_conf['name']: _value_type_from_vt_conf(el_conf['type'])
                for el_conf in vt_conf['elements']}

    raise Exception('unsupported value type')


def _merge_types(type1, type2, ref):
    if isinstance(type1, dict) and isinstance(type2, dict):
        return {**type1,
                **{k: (subtype if k not in type1
                       else _merge_types(type1[k], subtype, ref.union(k)))
                   for k, subtype in type2.items()}}

    mlog.warning('value types conflict on reference %s: %s and %s',
                 ref, type1, type2)
    return type2


def _value_type_from_ref(value_types, ref):
    left_names = []
    if isinstance(ref, iec61850.DataRef):
        left_names = ref.names[1:]
        ref = (ref.logical_device, ref.logical_node, ref.names[0])

    if ref not in value_types:
        return

    value_type = value_types[ref]
    while left_names:
        name = left_names[0]
        left_names = left_names[1:]
        if isinstance(name, str):
            value_type = value_type[name]

        if isinstance(name, int):
            value_type = value_type.type

    return value_type


def _value_type_to_iec61850(val_type):
    if (isinstance(val_type, iec61850.BasicValueType) or
            isinstance(val_type, iec61850.AcsiValueType)):
        return val_type

    if isinstance(val_type, iec61850.ArrayValueType):
        return iec61850.ArrayValueType(_value_type_to_iec61850(val_type.type))

    if isinstance(val_type, dict):
        return iec61850.StructValueType(
            [(k, _value_type_to_iec61850(v)) for k, v in val_type.items()])


_epoch_start = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)


def _command_from_event(event, cmd_conf, control_number, value_type):
    if cmd_conf['with_operate_time']:
        operate_time = iec61850.Timestamp(
            value=_epoch_start,
            leap_second=False,
            clock_failure=False,
            not_synchronized=False,
            accuracy=0)
    else:
        operate_time = None
    return iec61850.Command(
        value=_value_from_json(event.payload.data['value'], value_type),
        operate_time=operate_time,
        origin=iec61850.Origin(
            category=iec61850.OriginCategory[
                event.payload.data['origin']['category']],
            identification=event.payload.data[
                'origin']['identification'].encode('utf-8')),
        control_number=control_number,
        t=_timestamp_from_event_timestamp(event.timestamp),
        test=event.payload.data['test'],
        checks=set(iec61850.Check[i] for i in event.payload.data['checks']))


def _timestamp_from_event_timestamp(timestamp):
    return iec61850.Timestamp(
        value=hat.event.common.timestamp_to_datetime(timestamp),
        leap_second=False,
        clock_failure=False,
        not_synchronized=False,
        accuracy=None)


def _cmd_resp_to_event(event_type_prefix, cmd_name, event_session_id, action,
                       resp):
    success = resp is None
    payload = {'session_id': event_session_id,
               'action': action,
               'success': success}
    if not success:
        if resp.service_error is not None:
            payload['service_error'] = resp.service_error.name
        if resp.additional_cause is not None:
            payload['additional_cause'] = resp.additional_cause.name
        if resp.test_error is not None:
            payload['test_error'] = resp.test_error.name

    return hat.event.common.RegisterEvent(
        type=(*event_type_prefix, 'gateway', 'command', cmd_name),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload))


def _write_data_resp_to_event(event_type_prefix, value_name, session_id, resp):
    success = resp is None
    payload = {'session_id': session_id,
               'success': success}
    if not success:
        payload['error'] = resp.name
    return hat.event.common.RegisterEvent(
        type=(*event_type_prefix, 'gateway', 'change', value_name),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload))


def _get_command_session_id(cmd_ref, cmd):
    return (cmd_ref, cmd.control_number)


def _report_values_to_event(event_type_prefix, report_values_types, data_name,
                            data_ref):
    reasons = list(set(reason.name for rv, _ in report_values_types
                       for reason in rv.reasons or []))
    data = _event_data_from_report(report_values_types, data_ref)
    payload = {'data': data,
               'reasons': reasons}
    return hat.event.common.RegisterEvent(
        type=(*event_type_prefix, 'gateway', 'data', data_name),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload))


def _event_data_from_report(report_values_types, data_ref):
    values_json = {}
    for rv, value_type in report_values_types:
        val_json = _value_to_json(rv.value, value_type)
        val_path = [rv.ref.logical_device, rv.ref.logical_node, *rv.ref.names]
        values_json = json.set_(values_json, val_path, val_json)
    return json.get(values_json, list(data_ref))


def _value_from_json(event_value, value_type):
    if isinstance(value_type, iec61850.BasicValueType):
        if value_type == iec61850.BasicValueType.OCTET_STRING:
            return bytes.fromhex(event_value)
        else:
            return event_value

    if value_type == iec61850.AcsiValueType.QUALITY:
        return iec61850.Quality(
            validity=iec61850.QualityValidity[event_value['validity']],
            details={iec61850.QualityDetail[i]
                     for i in event_value['details']},
            source=iec61850.QualitySource[event_value['source']],
            test=event_value['test'],
            operator_blocked=event_value['operator_blocked'])

    if value_type == iec61850.AcsiValueType.TIMESTAMP:
        return iec61850.Timestamp(
            value=datetime.datetime.fromtimestamp(event_value['value'],
                                                  datetime.timezone.utc),
            leap_second=event_value['leap_second'],
            clock_failure=event_value['clock_failure'],
            not_synchronized=event_value['not_synchronized'],
            accuracy=event_value.get('accuracy'))

    if value_type == iec61850.AcsiValueType.DOUBLE_POINT:
        return iec61850.DoublePoint[event_value]

    if value_type == iec61850.AcsiValueType.DIRECTION:
        return iec61850.Direction[event_value]

    if value_type == iec61850.AcsiValueType.SEVERITY:
        return iec61850.Severity[event_value]

    if value_type == iec61850.AcsiValueType.ANALOGUE:
        return iec61850.Analogue(i=event_value.get('i'),
                                 f=event_value.get('f'))

    if value_type == iec61850.AcsiValueType.VECTOR:
        return iec61850.Vector(
            magnitude=_value_from_json(event_value['magnitude'],
                                       iec61850.AcsiValueType.ANALOGUE),
            angle=(_value_from_json(event_value['angle'],
                                    iec61850.AcsiValueType.ANALOGUE)
                   if 'angle' in event_value else None))

    if value_type == iec61850.AcsiValueType.STEP_POSITION:
        return iec61850.StepPosition(value=event_value['value'],
                                     transient=event_value.get('transient'))

    if value_type == iec61850.AcsiValueType.BINARY_CONTROL:
        return iec61850.BinaryControl[event_value]

    if isinstance(value_type, iec61850.ArrayValueType):
        return [_value_from_json(val, value_type.type) for val in event_value]

    if isinstance(value_type, dict):
        return {k: _value_from_json(v, value_type[k])
                for k, v in event_value.items()}

    raise Exception('unsupported value type')


def _value_to_json(data_value, value_type):
    if isinstance(value_type, iec61850.BasicValueType):
        if value_type == iec61850.BasicValueType.OCTET_STRING:
            return data_value.hex()

        elif value_type == iec61850.BasicValueType.BIT_STRING:
            return list(data_value)

        else:
            return data_value

    if isinstance(value_type, iec61850.AcsiValueType):
        if value_type == iec61850.AcsiValueType.QUALITY:
            return {'validity': data_value.validity.name,
                    'details': [i.name for i in data_value.details],
                    'source': data_value.source.name,
                    'test': data_value.test,
                    'operator_blocked': data_value.operator_blocked}

        if value_type == iec61850.AcsiValueType.TIMESTAMP:
            val = {'value': data_value.value.timestamp(),
                   'leap_second': data_value.leap_second,
                   'clock_failure': data_value.clock_failure,
                   'not_synchronized': data_value.not_synchronized}
            if data_value.accuracy is not None:
                val['accuracy'] = data_value.accuracy
            return val

        if value_type in [iec61850.AcsiValueType.DOUBLE_POINT,
                          iec61850.AcsiValueType.DIRECTION,
                          iec61850.AcsiValueType.SEVERITY,
                          iec61850.AcsiValueType.BINARY_CONTROL]:
            return data_value.name

        if value_type == iec61850.AcsiValueType.ANALOGUE:
            val = {}
            if data_value.i is not None:
                val['i'] = data_value.i
            if data_value.f is not None:
                val['f'] = data_value.f
            return val

        if value_type == iec61850.AcsiValueType.VECTOR:
            val = {'magnitude': _value_to_json(
                        data_value.magnitude, iec61850.AcsiValueType.ANALOGUE)}
            if data_value.angle is not None:
                val['angle'] = _value_to_json(
                    data_value.angle, iec61850.AcsiValueType.ANALOGUE)
            return val

        if value_type == iec61850.AcsiValueType.STEP_POSITION:
            val = {'value': data_value.value}
            if data_value.transient is not None:
                val['transient'] = data_value.transient
            return val

    if isinstance(value_type, iec61850.ArrayValueType):
        return [_value_to_json(i, value_type.type) for i in data_value]

    if isinstance(value_type, dict):
        return {
            child_name: _value_to_json(child_value, value_type[child_name])
            for child_name, child_value in data_value.items()}

    raise Exception('unsupported value type')


def _update_ctl_num(ctl_num, action, cmd_model):
    if action == 'SELECT' or (
        action == 'OPERATE' and
        cmd_model in ['DIRECT_WITH_NORMAL_SECURITY',
                      'DIRECT_WITH_ENHANCED_SECURITY']):
        return (ctl_num + 1) % 256

    return ctl_num


def _validate_get_rcb_response(get_rcb_resp, rcb_conf):
    for k, v in get_rcb_resp.items():
        if isinstance(v, iec61850.ServiceError):
            raise Exception(f"get {k.name} failed: {v}")

        if (k == iec61850.RcbAttrType.REPORT_ID and
                v != rcb_conf['report_id']):
            raise Exception(f"rcb report id {v} different from "
                            f"configured {rcb_conf['report_id']}")

        if (k == iec61850.RcbAttrType.DATASET and
                v != _dataset_ref_from_conf(rcb_conf['dataset'])):
            raise Exception(f"rcb dataset {v} different from "
                            f"configured {rcb_conf['report_id']}")

        if (k == iec61850.RcbAttrType.CONF_REVISION and
                v != rcb_conf['conf_revision']):
            raise Exception(
                f"Conf revision {v} different from "
                f"the configuration defined {rcb_conf['conf_revision']}")
