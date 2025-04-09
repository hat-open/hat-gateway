"""IEC 61850 client device"""

from collections.abc import Collection
import asyncio
import collections
import datetime
import logging
import typing

from hat import aio
from hat import json
from hat.drivers import iec61850
from hat.drivers import tcp
import hat.event.common

from hat.gateway import common


mlog: logging.Logger = logging.getLogger(__name__)


termination_timeout: int = 100


async def create(conf: common.DeviceConf,
                 eventer_client: hat.event.eventer.Client,
                 event_type_prefix: common.EventTypePrefix
                 ) -> 'Iec61850ClientDevice':

    result = await eventer_client.query(
        hat.event.common.QueryLatestParams(
            event_types=[
                (*event_type_prefix, 'gateway', 'entry_id', i['report_id'])
                for i in conf['rcbs']]))
    rcbs_entry_ids = {}
    for event in result.events:
        rpt_id = event.type[5]
        rcbs_entry_ids[rpt_id] = bytes.fromhex(event.payload.data)

    device = Iec61850ClientDevice()

    device._rcbs_entry_ids = rcbs_entry_ids
    device._conf = conf
    device._eventer_client = eventer_client
    device._event_type_prefix = event_type_prefix
    device._conn = None
    device._terminations = {}
    device._conn_status = None

    device._data_ref_names = {(i['ref']['logical_device'],
                               i['ref']['logical_node'],
                               *i['ref']['names']): i['name']
                              for i in conf['data']}
    device._command_name_confs = {i['name']: i for i in conf['commands']}
    device._command_name_ctl_nums = {i['name']: 0 for i in conf['commands']}
    device._change_name_value_refs = {
        i['name']: _value_ref_from_conf(i['ref']) for i in conf['changes']}
    device._persist_dyn_datasets = set()
    device._dyn_datasets = {}
    for ds_conf in device._conf['datasets']:
        if not ds_conf['dynamic']:
            continue

        ds_ref = _dataset_ref_from_conf(ds_conf['ref'])
        if isinstance(ds_ref, iec61850.PersistedDatasetRef):
            device._persist_dyn_datasets.add(ds_ref)
        values = [iec61850.DataRef(val_conf['logical_device'],
                                   val_conf['logical_node'],
                                   val_conf['fc'],
                                   tuple(val_conf['names']))
                  for val_conf in ds_conf['values']]
        device._dyn_datasets[ds_ref] = values

    dataset_confs = {_dataset_ref_from_conf(ds_conf['ref']): ds_conf
                     for ds_conf in device._conf['datasets']}
    device._report_value_refs = collections.defaultdict(collections.deque)
    device._data_values = collections.defaultdict(collections.deque)
    device._values_data = collections.defaultdict(collections.deque)
    for rcb_conf in device._conf['rcbs']:
        ds_ref = _dataset_ref_from_conf(rcb_conf['dataset'])
        dataset_conf = dataset_confs[ds_ref]
        for value_conf in dataset_conf['values']:
            value_ref = _value_ref_from_conf(value_conf)
            device._report_value_refs[rcb_conf['report_id']].append(value_ref)

            for data_ref in device._data_ref_names.keys():
                if _data_matches_value(data_ref, value_ref):
                    device._data_values[data_ref].append(value_ref)
                    device._values_data[value_ref].append(data_ref)

    device._value_types_nodes = {
        (vt['logical_device'],
         vt['logical_node'],
         vt['name']): _node_from_value_type(vt)
        for vt in conf['value_types']}

    device._cmd_value_types = {}
    for cmd_conf in conf['commands']:
        cmd_ref = iec61850.CommandRef(**cmd_conf['ref'])
        value_type = _value_type_from_ref(device._value_types_nodes, cmd_ref)
        device._cmd_value_types[cmd_ref] = value_type

    device._data_value_types = {}
    for ds_conf in conf['datasets']:
        for val_ref_conf in ds_conf['values']:
            value_ref = _value_ref_from_conf(val_ref_conf)
            device._data_value_types[value_ref] = _value_type_from_ref(
                device._value_types_nodes, value_ref)
    for change_conf in conf['changes']:
        value_ref = _value_ref_from_conf(change_conf['ref'])
        device._data_value_types[value_ref] = _value_type_from_ref(
                device._value_types_nodes, value_ref)

    device._async_group = aio.Group()
    device._async_group.spawn(device._connection_loop)

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

                try:
                    await self._create_dynamic_datasets()
                    for rcb_conf in self._conf['rcbs']:
                        await self._init_rcb(rcb_conf)

                except Exception as e:
                    mlog.warning(
                        'initialization failed: %s, closing connection',
                        e, exc_info=e)
                    self._conn.close()
                    await self._register_status('DISCONNECTED')
                    await self._conn.wait_closed()
                    self._conn = None
                    await asyncio.sleep(conn_conf['reconnect_delay'])
                    continue

                await self._conn.wait_closing()
                await self._register_status('DISCONNECTED')
                await self._conn.wait_closed()
                self._conn = None

        except Exception as e:
            mlog.error('connection loop error: %s', e, exc_info=e)

        finally:
            mlog.debug('closing connection loop')
            self.close()
            await aio.uncancellable(cleanup())

    async def _process_cmd_req(self, event, cmd_name):
        if not self._conn:
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
            ctrl_num = self._get_cmd_control_number(
                cmd_name, event, cmd_conf['model'])
            node = _node_from_ref(self._value_types_nodes, cmd_ref)
            cmd = _command_from_event(event, cmd_conf, ctrl_num, node)

        term_future = None
        if (action == 'OPERATE' and
                cmd_conf['model'] in ['DIRECT_WITH_ENHANCED_SECURITY',
                                      'SBO_WITH_ENHANCED_SECURITY']):
            term_future = asyncio.Future()
            self._conn.async_group.spawn(
                self._wait_cmd_term, cmd_name, cmd_ref, cmd, evt_session_id,
                term_future)

        try:
            resp = await aio.wait_for(
                self._send_command(action, cmd_ref, cmd),
                self._conf['connection']['response_timeout'])

        except (asyncio.TimeoutError, ConnectionError) as e:
            mlog.warning('send command failed: %s', e, exc_info=e)
            if term_future:
                term_future.cancel()
            return

        if resp is not None:
            if term_future:
                term_future.cancel()

        event = _cmd_resp_to_event(
            self._event_type_prefix, cmd_name, evt_session_id, action, resp)
        await self._register_events([event])

    def _get_cmd_control_number(self, cmd_name, cmd_event, cmd_model):
        ctl_num = self._command_name_ctl_nums[cmd_name]
        action = cmd_event.payload.data['action']
        if action == 'SELECT' or (
            action == 'OPERATE' and
            cmd_model in ['DIRECT_WITH_NORMAL_SECURITY',
                          'DIRECT_WITH_ENHANCED_SECURITY']):
            ctl_num = (ctl_num + 1) % 256
            self._command_name_ctl_nums[cmd_name] = ctl_num
        return ctl_num

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
            self._terminations.pop(cmd_session_id)

    async def _process_change_req(self, event, value_name):
        if not self._conn:
            mlog.warning('change event %s ignored: no connection', value_name)
            return

        if value_name not in self._change_name_value_refs:
            raise Exception('unexpected command name')

        ref = self._change_name_value_refs[value_name]
        node = _node_from_ref(self._value_types_nodes, ref)
        if node is None:
            raise Exception('value type undefined')

        value = _value_from_json(event.payload.data['value'], node)
        try:
            resp = await aio.wait_for(
                iec61850.write_data(ref, value),
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

        events = collections.deque()

        if report.entry_id is not None:
            event = hat.event.common.RegisterEvent(
                type=(*self._event_type_prefix, 'gateway',
                      'entry_id', report.report_id),
                source_timestamp=None,
                payload=hat.event.common.EventPayloadJson(
                    report.entry_id.hex()))
            events.append(event)

        report_data_values = collections.defaultdict(collections.deque)
        for rv in report.data:
            data_refs = self._values_data.get(rv.ref)
            for data_ref in data_refs:
                report_data_values[data_ref].append(rv)

        for data_ref, report_values in report_data_values.items():
            if len(report_values) != self._data_values[data_ref]:
                mlog.warning(
                    'report values dropped: report does not include all '
                    'values of data %s, only %s', data_ref, report_values)
                continue

            data_name = self._data_ref_names[data_ref]
            try:
                event = _report_values_to_event(
                    self._event_type_prefix, report_values, data_name,
                    data_ref, self._value_types_nodes)
                events.append(event)

            except Exception as e:
                mlog.warning('report values dropped: %s', e, exc_info=e)
                continue

        await self._register_events(events)
        if report.entry_id is not None:
            self._rcbs_entry_ids[report.report_id] = report.entry_id

    def _on_termination(self, termination):
        cmd_session_id = _get_command_session_id(
            termination.ref, termination.cmd)
        if cmd_session_id not in self._terminations:
            mlog.warning('unexpected termination dropped')
            return

        term_future = self._terminations[cmd_session_id]
        if not term_future.done():
            self._terminations[cmd_session_id].set_result(termination)

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

    async def _init_rcb(self, rcb_conf):
        ref = iec61850.RcbRef(
            logical_device=rcb_conf['ref']['logical_device'],
            logical_node=rcb_conf['ref']['logical_node'],
            type=iec61850.RcbType[rcb_conf['ref']['type']],
            name=rcb_conf['ref']['name'])
        mlog.debug('initiating rcb %s', ref)

        get_resp = await self._conn.get_rcb_attr(
            ref, iec61850.RcbAttrType.CONF_REVISION)
        if isinstance(get_resp, iec61850.ServiceError):
            raise Exception(f'get rcb failed {get_resp}')
        else:
            conf_rev = get_resp
            if conf_rev != rcb_conf['conf_revision']:
                raise Exception(
                    f"Conf revision {conf_rev} different from "
                    f"the configuration defined {rcb_conf['conf_rev']}")

        await self._set_rcb(
            ref, ((iec61850.RcbAttrType.REPORT_ENABLE, False),))

        if ref.type == iec61850.RcbType.BUFFERED:
            if 'reservation_time' in rcb_conf:
                await self._set_rcb(
                    ref, ((iec61850.RcbAttrType.RESERVATION_TIME,
                           rcb_conf['reservation_time']),))

            entry_id = self._rcbs_entry_ids.get(rcb_conf['report_id'])
            if rcb_conf.get('purge_buffer') or entry_id is None:
                await self._set_rcb(
                    ref, ((iec61850.RcbAttrType.PURGE_BUFFER, True),))
                return

            try:
                await self._set_rcb(
                    ref, ((iec61850.RcbAttrType.ENTRY_ID, entry_id),),
                    critical=True)

            except Exception as e:
                mlog.warning('%s', e, exc_info=e)
                # try setting entry id to 0 in order to resynchronize
                await self._set_rcb(
                    ref, ((iec61850.RcbAttrType.ENTRY_ID, b'\x00'),))

        elif ref.type == iec61850.RcbType.UNBUFFERED:
            await self._set_rcb(
                ref, ((iec61850.RcbAttrType.RESERVE, True),))

        attrs = collections.deque()
        attrs.append((iec61850.RcbAttrType.TRIGGER_OPTIONS,
                      set(iec61850.TriggerCondition[i]
                          for i in rcb_conf['trigger_options'])))
        if 'conf_revision' in rcb_conf:
            attrs.append((iec61850.RcbAttrType.CONF_REVISION,
                          rcb_conf['conf_revision']))
        if 'buffer_time' in rcb_conf:
            attrs.append((iec61850.RcbAttrType.BUFFER_TIME,
                          rcb_conf['buffer_time']))
        if 'integrity_period' in rcb_conf:
            attrs.append((iec61850.RcbAttrType.INTEGRITY_PERIOD,
                          rcb_conf['integrity_period']))
        await self._set_rcb(ref, attrs)

        await self._set_rcb(
            ref, ((iec61850.RcbAttrType.REPORT_ENABLE, True),), critical=True)
        await self._set_rcb(
            ref, ((iec61850.RcbAttrType.GI, True),), critical=True)
        mlog.debug('rcb %s initiated', ref)

    async def _set_rcb(self, ref, attrs, critical=False):
        try:
            resp = await self._conn.set_rcb_attrs(ref, attrs)
            if isinstance(resp, iec61850.ServiceError):
                raise Exception(str(resp))

        except Exception as e:
            attr_names = [i[0].name for i in attrs]
            if critical:
                raise Exception(
                    f'set rcb {ref} on {attr_names} failed') from e
            else:
                mlog.warning('set rcb %s on %s failed: %s',
                             ref, attr_names, e, exc_info=e)

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
        for ds_ref, ds_value_refs in self._dyn_datasets.items():
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


def _timestamp_from_event_timestamp(timestamp):
    return iec61850.Timestamp(
        value=hat.event.common.timestamp_to_datetime(timestamp),
        leap_second=False,
        clock_failure=False,
        not_synchronized=False,
        accuracy=None)


def _dataset_ref_from_conf(ds_conf):
    if isinstance(ds_conf, str):
        return iec61850.NonPersistedDatasetRef(ds_conf)

    elif isinstance(ds_conf, dict):
        return iec61850.PersistedDatasetRef(**ds_conf)


def _command_from_event(event, cmd_conf, control_number, node):
    if cmd_conf['with_operate_time']:
        operate_time = iec61850.Timestamp(
            value=datetime.datetime.fromtimestamp(0, datetime.timezone.utc),
            leap_second=False,
            clock_failure=False,
            not_synchronized=False,
            accuracy=0)
    else:
        operate_time = None
    return iec61850.Command(
        value=_value_from_json(event.payload.data['value'], node),
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


def _data_matches_value(data_path, value_ref):
    value_path = _ref_to_path(value_ref)
    if len(data_path) == len(value_path):
        return data_path == value_path

    if len(data_path) > len(value_path):
        return data_path[:len(value_path)] == value_path

    return value_path[:len(data_path)] == data_path


def _ref_to_path(ref):
    return (ref.logical_device, ref.logical_node, *ref.names)


def _report_values_to_event(event_type_prefix, report_values, data_name,
                            data_ref, value_types_nodes):
    reasons = list(set(reason.name for rv in report_values
                       for reason in rv.reasons or []))
    data = _event_data_from_report(report_values, value_types_nodes, data_ref)
    payload = {'data': data,
               'reasons': reasons}
    return hat.event.common.RegisterEvent(
        type=(*event_type_prefix, 'gateway', 'data', data_name),
        source_timestamp=None,
        payload=hat.event.common.EventPayloadJson(payload))


def _event_data_from_report(report_values, value_types_nodes, data_ref):
    values_json = {}
    for rv in report_values:
        node = _node_from_ref(value_types_nodes, rv.ref)
        val_json = _value_to_json(rv.value, node)
        val_path = (rv.ref.logical_device, rv.ref.logical_node, *rv.ref.names)
        values_json = json.set_(values_json, val_path, val_json)
    return json.get(values_json, data_ref)


_ValueTypeNodeName = str | int


class _ValueTypeNode(typing.NamedTuple):
    name: _ValueTypeNodeName
    type: iec61850.ValueType
    children: typing.Dict[_ValueTypeNodeName, '_ValueTypeNode']


def _node_from_value_type(value_type, name=None):
    if 'logical_device' in value_type:
        return _node_from_value_type(value_type['type'],
                                     name=value_type['name'])

    if isinstance(value_type, str):
        return _ValueTypeNode(name=name,
                              type=_value_type_from_str(value_type),
                              children=[])

    if value_type['type'] == 'ARRAY':
        child_name = 0
        children = {
            child_name: _node_from_value_type(value_type['element_type'],
                                              name=child_name)}
        return _ValueTypeNode(name=name,
                              type=iec61850.ArrayValueType,
                              children=children)

    if value_type['type'] == 'STRUCT':
        for i, elm in enumerate(value_type['elements']):
            child_name = elm['name']
            children[child_name] = _node_from_value_type(elm, name=child_name)
        return _ValueTypeNode(name=name,
                              type=iec61850.StructValueType,
                              children=children)

    raise Exception('unsupported value type')


def _value_type_from_str(vt_conf):
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


def _value_type_from_ref(value_types_nodes, ref):
    node = _node_from_ref(value_types_nodes, ref)
    if node is not None:
        return _get_node_type(node)


def _get_node_type(node):
    if (node.type in iec61850.BasicValueType or
            node.type in iec61850.AcsiValueType):
        return node.type

    if node.type == iec61850.ArrayValueType:
        return iec61850.ArrayValueType(_get_node_type(node.children[0]))

    if node.type == iec61850.StructValueType:
        return iec61850.StructValueType(
            [(child.name, _get_node_type(child))
             for child in node.children.values()])


def _node_from_ref(value_types_nodes, ref):
    if isinstance(ref, iec61850.DataRef):
        ref = (ref.logical_device, ref.logical_node, ref.names[0])
    if ref not in value_types_nodes:
        return

    node = value_types_nodes[ref]
    left_names = ref.names[1:]
    i = 1
    while left_names:
        name = left_names[0]
        node = [node.children[name]
                if isinstance(name, str) else node.children[0]]
        i += 1
        left_names = name[i:]

    return node


def _value_from_json(event_value, node, value_type=None):
    value_type = node.type if node else value_type
    if value_type in iec61850.BasicValueType:
        if value_type == iec61850.BasicValueType.OCTET_STRING:
            return bytes.fromhex(event_value)
        else:
            return event_value

    if value_type == iec61850.AcsiValueType.QUALITY:
        return iec61850.Quality(
            validity=iec61850.QualityValidity[event_value['validity']],
            details=iec61850.QualityDetail[event_value['details']],
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
            accuracy=event_value['accuracy'])

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
            magnitude=_value_from_json(event_value['magnitude'], None,
                                       iec61850.AcsiValueType.ANALOGUE),
            angle=(_value_from_json(event_value['angle'], None,
                                    iec61850.AcsiValueType.ANALOGUE)
                   if 'angle' in event_value else None))

    if value_type == iec61850.AcsiValueType.STEP_POSITION:
        return iec61850.StepPosition(value=event_value['value'],
                                     transient=event_value.get('transient'))

    if value_type == iec61850.AcsiValueType.BINARY_CONTROL:
        return iec61850.BinaryControl[event_value]

    if value_type == iec61850.ArrayValueType:
        return [_value_from_json(val, node.children[0])
                for val in event_value]

    if value_type == iec61850.StructValueType:
        return {k: _value_from_json(v, node.children[k])
                for k, v in event_value.items()}

    raise Exception('unsupported value type')


def _value_to_json(data_value, node, value_type=None):
    value_type = node.type if node else value_type
    if value_type in iec61850.BasicValueType:
        if value_type == iec61850.BasicValueType.OCTET_STRING:
            return data_value.hex()

        elif value_type == iec61850.BasicValueType.BIT_STRING:
            return list(data_value)

        else:
            return data_value

    if value_type == iec61850.AcsiValueType.QUALITY:
        return {'validity': data_value.validity.name,
                'details': data_value.details.name,
                'source': data_value.source.name,
                'test': data_value.test,
                'operator_blocked': data_value.operator_blocked}

    if value_type == iec61850.AcsiValueType.TIMESTAMP:
        val = {'value': data_value.value.timestamp(),
               'leap_second': data_value.value.leap_second,
               'clock_failure': data_value.value.clock_failure,
               'not_synchronized': data_value.value.not_synchronized}
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
                                data_value.magnitude,
                                None,
                                iec61850.AcsiValueType.ANALOGUE)}
        if data_value.angle is not None:
            val['angle'] = _value_to_json(
                                data_value.angle,
                                None,
                                iec61850.AcsiValueType.ANALOGUE)
        return val

    if value_type == iec61850.AcsiValueType.STEP_POSITION:
        val = {'value': data_value.value}
        if data_value.transient is not None:
            val['transient'] = data_value.transient
        return val

    if value_type == iec61850.ArrayValueType:
        return [_value_to_json(i, node.children[0]) for i in data_value]

    if value_type == iec61850.StructValueType:
        for child_name, child_value in data_value.items():
            if child_name not in node.children:
                raise Exception(f'attr {child_name} not found in value type')

            child = node.children[child_name]
            return {child_name: _value_to_json(child_value, child)}

    raise Exception('unsupported value type')


def _value_ref_from_conf(value_ref_conf):
    return iec61850.DataRef(
        logical_device=value_ref_conf['logical_device'],
        logical_node=value_ref_conf['logical_node'],
        fc=value_ref_conf['fc'],
        names=tuple(value_ref_conf['names']))
