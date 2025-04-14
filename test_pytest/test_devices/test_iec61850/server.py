from collections.abc import Collection
import collections
import typing

from hat import aio
from hat import util
from hat.drivers import iec61850
from hat.drivers import mms
from hat.drivers import tcp
from hat.drivers.iec61850 import encoder


DataDef = encoder.DataDef

DatasetCb: typing.TypeAlias = aio.AsyncCallable[
    [iec61850.DatasetRef, Collection[iec61850.DataRef] | None],
    None]

RcbCb: typing.TypeAlias = aio.AsyncCallable[
    [iec61850.RcbRef, iec61850.RcbAttrType, iec61850.RcbAttrValue],
    None]

WriteCb: typing.TypeAlias = aio.AsyncCallable[
    [iec61850.DataRef, mms.Data],
    None]

SelectCb: typing.TypeAlias = aio.AsyncCallable[
    [iec61850.CommandRef, iec61850.Command | None],
    None]

CancelCb: typing.TypeAlias = aio.AsyncCallable[
    [iec61850.CommandRef, iec61850.Command],
    None]

OperateCb: typing.TypeAlias = aio.AsyncCallable[
    [iec61850.CommandRef, iec61850.Command],
    None]


async def create_server(addr: tcp.Address,
                        datasets: dict[iec61850.DatasetRef,
                                       Collection[iec61850.DataRef]] = {},
                        rcbs: dict[iec61850.RcbRef,
                                   dict[iec61850.RcbAttrType,
                                        iec61850.RcbAttrValue]] = {},
                        cmd_value_types: dict[iec61850.CommandRef,
                                              iec61850.ValueType] = {},
                        dataset_cb: DatasetCb | None = None,
                        rcb_cb: RcbCb | None = None,
                        write_cb: WriteCb | None = None,
                        select_cb: SelectCb | None = None,
                        cancel_cb: CancelCb | None = None,
                        operate_cb: OperateCb | None = None):
    server = Server()
    server._datasets = dict(datasets)
    server._rcbs = {k: dict(v) for k, v in rcbs.items()}
    server._cmd_value_types = cmd_value_types
    server._dataset_cb = dataset_cb
    server._rcb_cb = rcb_cb
    server._write_cb = write_cb
    server._select_cb = select_cb
    server._cancel_cb = cancel_cb
    server._operate_cb = operate_cb
    server._conn = None

    server._srv = await mms.listen(connection_cb=server._on_connection,
                                   addr=addr,
                                   request_cb=server._on_request,
                                   bind_connections=True)

    return server


class Server(aio.Resource):

    @property
    def async_group(self) -> aio.Group:
        return self._srv.async_group

    @property
    def datasets(self) -> dict[iec61850.DatasetRef,
                               Collection[iec61850.DataRef]]:
        return self._datasets

    @property
    def rcbs(self) -> dict[iec61850.RcbRef,
                           dict[iec61850.RcbAttrType, iec61850.RcbAttrValue]]:
        return self._rcbs

    @property
    def conn(self) -> mms.Connection | None:
        return self._conn

    async def send_report(self,
                          report: iec61850.Report,
                          data_defs: Collection[DataDef]):
        unconfirmed = mms.InformationReportUnconfirmed(
            specification=mms.VmdSpecificObjectName('RPT'),
            data=_report_to_mms_data(report, data_defs))

        await self._conn.send_unconfirmed(unconfirmed)

    async def send_termination(self,
                               cmd_ref: iec61850.CommandRef,
                               cmd: iec61850.Command):
        value_type = self._cmd_value_types[cmd_ref]
        mms_data = _command_to_mms_data(cmd, value_type, True)

        unconfirmed = mms.InformationReportUnconfirmed(
            specification=[
                mms.NameVariableSpecification(
                    _data_ref_to_object_name(
                        iec61850.DataRef(logical_device=cmd_ref.logical_device,
                                         logical_node=cmd_ref.logical_node,
                                         fc='CO',
                                         names=[cmd_ref.name, 'Oper'])))],
            data=[mms_data])

        await self._conn.send_unconfirmed(unconfirmed)

    def _on_connection(self, conn):
        if self._conn:
            conn.close()
            return

        self._conn = conn

    async def _on_request(self, conn, req):
        if isinstance(req, mms.DefineNamedVariableListRequest):
            return await self._process_define_named_variable_list(req)

        if isinstance(req, mms.DeleteNamedVariableListRequest):
            return await self._process_delete_named_variable_list(req)

        if isinstance(req, mms.GetNameListRequest):
            return await self._process_get_name_list(req)

        if isinstance(req, mms.GetNamedVariableListAttributesRequest):
            return await self._process_get_named_variable_list_attributes(req)

        if isinstance(req, mms.ReadRequest):
            return await self._process_read(req)

        if isinstance(req, mms.WriteRequest):
            return await self._process_write(req)

        return mms.OtherError(0)

    async def _process_define_named_variable_list(self, req):
        dataset_ref = _dataset_ref_from_object_name(req.name)
        if dataset_ref in self._datasets:
            return mms.DefinitionError.OBJECT_EXISTS

        data_refs = [_data_ref_from_object_name(i.name)
                     for i in req.specification]

        self._datasets[dataset_ref] = data_refs

        if self._dataset_cb:
            await aio.call(self._dataset_cb, dataset_ref, data_refs)

        return mms.DefineNamedVariableListResponse()

    async def _process_delete_named_variable_list(self, req):
        if len(req.names) != 1:
            return mms.OtherError(0)

        dataset_ref = _dataset_ref_from_object_name(req.names[0])
        if dataset_ref not in self._datasets:
            return mms.AccessError.OBJECT_NON_EXISTENT

        del self._datasets[dataset_ref]

        if self._dataset_cb:
            await aio.call(self._dataset_cb, dataset_ref, None)

        return mms.DeleteNamedVariableListResponse(matched=1,
                                                   deleted=1)

    async def _process_get_name_list(self, req):
        if req.object_class != mms.ObjectClass.NAMED_VARIABLE_LIST:
            return mms.OtherError(0)

        if not isinstance(req.object_scope, mms.DomainSpecificObjectScope):
            return mms.OtherError(0)

        logical_device = req.object_scope.identifier

        identifiers = [
            f'{dataset_ref.logical_node}${dataset_ref.name}'
            for dataset_ref in self._datasets.keys()
            if (isinstance(dataset_ref, iec61850.PersistedDatasetRef) and
                dataset_ref.logical_device == logical_device)]

        return mms.GetNameListResponse(identifiers=identifiers,
                                       more_follows=False)

    async def _process_get_named_variable_list_attributes(self, req):
        dataset_ref = _dataset_ref_from_object_name(req.value)
        if dataset_ref not in self._datasets:
            return mms.AccessError.OBJECT_NON_EXISTENT

        specification = [
            mms.NameVariableSpecification(_data_ref_to_object_name(data_ref))
            for data_ref in self._datasets[dataset_ref]]

        return mms.GetNamedVariableListAttributesResponse(
            mms_deletable=True,
            specification=specification)

    async def _process_read(self, req):
        if isinstance(req.value, mms.ObjectName):
            return mms.OtherError(0)

        results = collections.deque()

        for i in req.value:
            if not isinstance(i, mms.NameVariableSpecification):
                results.append(mms.DataAccessError.INVALID_ADDRESS)
                continue

            data_ref = _data_ref_from_object_name(i.name)

            if data_ref.names[-1] in {i.value for i in iec61850.RcbAttrType}:
                result = await self._process_read_rcb(data_ref)

            elif data_ref.names[-1] == 'SBO':
                result = await self._process_read_select(data_ref)

            else:
                result = mms.DataAccessError.OBJECT_UNDEFINED

            results.append(result)

        return mms.ReadResponse(results)

    async def _process_read_rcb(self, data_ref):
        if len(data_ref.names) != 2:
            return mms.DataAccessError.OBJECT_UNDEFINED

        name, attr = data_ref.names

        try:
            rcb_type = iec61850.RcbType(data_ref.fc)

        except ValueError:
            return mms.DataAccessError.OBJECT_UNDEFINED

        rcb_ref = iec61850.RcbRef(logical_device=data_ref.logical_device,
                                  logical_node=data_ref.logical_node,
                                  type=rcb_type,
                                  name=name)

        rcb = self._rcbs.get(rcb_ref)
        if rcb is None:
            return mms.DataAccessError.OBJECT_UNDEFINED

        attr_type = iec61850.RcbAttrType(attr)

        if attr_type not in rcb:
            return mms.DataAccessError.OBJECT_UNDEFINED

        return _rcb_attr_value_to_mms_data(rcb[attr_type], attr_type)

    async def _process_read_select(self, data_ref):
        if len(data_ref.names) != 2:
            return mms.DataAccessError.OBJECT_UNDEFINED

        if data_ref.fc != 'CO':
            return mms.DataAccessError.OBJECT_UNDEFINED

        cmd_ref = iec61850.CommandRef(logical_device=data_ref.logical_device,
                                      logical_node=data_ref.logical_node,
                                      name=data_ref.names[0])

        if self._select_cb:
            await aio.call(self._select_cb, cmd_ref, None)

        return mms.VisibleStringData('')

    async def _process_write(self, req):
        if isinstance(req.specification, mms.ObjectName):
            return mms.OtherError(0)

        results = collections.deque()

        for specification, mms_data in zip(req.specification, req.data):
            if not isinstance(specification, mms.NameVariableSpecification):
                results.append(mms.DataAccessError.INVALID_ADDRESS)
                continue

            data_ref = _data_ref_from_object_name(specification.name)

            if data_ref.names[-1] in {i.value for i in iec61850.RcbAttrType}:
                result = await self._process_write_rcb(data_ref, mms_data)

            elif data_ref.names[-1] == 'SBOw':
                result = await self._process_write_select(data_ref, mms_data)

            elif data_ref.names[-1] == 'Cancel':
                result = await self._process_write_cancel(data_ref, mms_data)

            elif data_ref.names[-1] == 'Oper':
                result = await self._process_write_operate(data_ref, mms_data)

            else:
                result = await self._process_write_data(data_ref, mms_data)

            results.append(result)

        return mms.WriteResponse(results)

    async def _process_write_rcb(self, data_ref, mms_data):
        if len(data_ref.names) != 2:
            return mms.DataAccessError.OBJECT_UNDEFINED

        name, attr = data_ref.names

        try:
            rcb_type = iec61850.RcbType(data_ref.fc)

        except ValueError:
            return mms.DataAccessError.OBJECT_UNDEFINED

        rcb_ref = iec61850.RcbRef(logical_device=data_ref.logical_device,
                                  logical_node=data_ref.logical_node,
                                  type=rcb_type,
                                  name=name)

        rcb = self._rcbs.get(rcb_ref)
        if rcb is None:
            return mms.DataAccessError.OBJECT_UNDEFINED

        attr_type = iec61850.RcbAttrType(attr)
        attr_value = _rcb_attr_value_from_mms_data(mms_data, attr_type)

        rcb[attr_type] = attr_value

        if self._rcb_cb:
            await aio.call(self._rcb_cb, rcb_ref, attr_type, attr_value)

    async def _process_write_select(self, data_ref, mms_data):
        if len(data_ref.names) != 2:
            return mms.DataAccessError.OBJECT_UNDEFINED

        if data_ref.fc != 'CO':
            return mms.DataAccessError.OBJECT_UNDEFINED

        cmd_ref = iec61850.CommandRef(logical_device=data_ref.logical_device,
                                      logical_node=data_ref.logical_node,
                                      name=data_ref.names[0])

        value_type = self._cmd_value_types.get(cmd_ref)
        if value_type is None:
            return mms.DataAccessError.OBJECT_UNDEFINED

        cmd = _command_from_mms_data(mms_data, value_type, True)

        if self._select_cb:
            await aio.call(self._select_cb, cmd_ref, cmd)

    async def _process_write_cancel(self, data_ref, mms_data):
        if len(data_ref.names) != 2:
            return mms.DataAccessError.OBJECT_UNDEFINED

        if data_ref.fc != 'CO':
            return mms.DataAccessError.OBJECT_UNDEFINED

        cmd_ref = iec61850.CommandRef(logical_device=data_ref.logical_device,
                                      logical_node=data_ref.logical_node,
                                      name=data_ref.names[0])

        value_type = self._cmd_value_types.get(cmd_ref)
        if value_type is None:
            return mms.DataAccessError.OBJECT_UNDEFINED

        cmd = _command_from_mms_data(mms_data, value_type, False)

        if self._cancel:
            await aio.call(self._cancel, cmd_ref, cmd)

    async def _process_write_operate(self, data_ref, mms_data):
        if len(data_ref.names) != 2:
            return mms.DataAccessError.OBJECT_UNDEFINED

        if data_ref.fc != 'CO':
            return mms.DataAccessError.OBJECT_UNDEFINED

        cmd_ref = iec61850.CommandRef(logical_device=data_ref.logical_device,
                                      logical_node=data_ref.logical_node,
                                      name=data_ref.names[0])

        value_type = self._cmd_value_types.get(cmd_ref)
        if value_type is None:
            return mms.DataAccessError.OBJECT_UNDEFINED

        cmd = _command_from_mms_data(mms_data, value_type, True)

        if self._operate_cb:
            await aio.call(self._operate_cb, cmd_ref, cmd)

    async def _process_write_data(self, data_ref, mms_data):
        if self._write_cb:
            await aio.call(self._write_cb, data_ref, mms_data)


def _dataset_ref_from_object_name(object_name):
    return encoder.dataset_ref_from_object_name(object_name)


def _dataset_ref_from_str(ref_str):
    return encoder.dataset_ref_from_str(ref_str)


def _dataset_ref_to_str(ref):
    return encoder.dataset_ref_to_str(ref)


def _data_ref_from_object_name(object_name):
    return encoder.data_ref_from_object_name(object_name)


def _data_ref_to_object_name(ref):
    return encoder.data_ref_to_object_name(ref)


def _data_ref_to_str(ref):
    return encoder.data_ref_to_str(ref)


def _value_to_mms_data(value, value_type):
    return encoder.value_to_mms_data(value, value_type)


def _value_from_mms_data(mms_data, value_type):
    return encoder.value_from_mms_data(mms_data, value_type)


def _rcb_attr_value_to_mms_data(attr_value, attr_type):
    return encoder.rcb_attr_value_to_mms_data(attr_value, attr_type)


def _rcb_attr_value_from_mms_data(mms_data, attr_type):
    return encoder.rcb_attr_value_from_mms_data(mms_data, attr_type)


def _command_to_mms_data(cmd, value_type, with_checks):
    return encoder.command_to_mms_data(cmd, value_type, with_checks)


def _command_from_mms_data(mms_data, value_type, with_checks):
    return encoder.command_from_mms_data(mms_data, value_type, with_checks)


def _report_to_mms_data(report, data_defs):
    optional_fields = set()
    segmentation = False
    elements = collections.deque()

    if report.sequence_number is not None:
        optional_fields.add(iec61850.OptionalField.SEQUENCE_NUMBER)
        elements.append(
            _value_to_mms_data(report.sequence_number,
                               iec61850.BasicValueType.UNSIGNED))

    if report.entry_time is not None:
        optional_fields.add(iec61850.OptionalField.REPORT_TIME_STAMP)
        elements.append(mms.BinaryTimeData(report.entry_time))

    if report.dataset is not None:
        optional_fields.add(iec61850.OptionalField.DATA_SET_NAME)
        elements.append(
            _value_to_mms_data(_dataset_ref_to_str(report.dataset),
                               iec61850.BasicValueType.VISIBLE_STRING))

    if report.buffer_overflow is not None:
        optional_fields.add(iec61850.OptionalField.BUFFER_OVERFLOW)
        elements.append(
            _value_to_mms_data(report.buffer_overflow,
                               iec61850.BasicValueType.BOOLEAN))

    if report.entry_id is not None:
        optional_fields.add(iec61850.OptionalField.ENTRY_ID)
        elements.append(
            _value_to_mms_data(report.entry_id,
                               iec61850.BasicValueType.OCTET_STRING))

    if report.conf_revision is not None:
        optional_fields.add(iec61850.OptionalField.CONF_REVISION)
        elements.append(
            _value_to_mms_data(report.conf_revision,
                               iec61850.BasicValueType.UNSIGNED))

    if (report.subsequence_number is not None or
            report.more_segments_follow is not None):
        segmentation = True
        elements.append(
            _value_to_mms_data(report.subsequence_number or 0,
                               iec61850.BasicValueType.UNSIGNED))
        elements.append(
            _value_to_mms_data(bool(report.more_segments_follow),
                               iec61850.BasicValueType.BOOLEAN))

    report_data = [util.first(report.data, lambda i: i.ref == data_def.ref)
                   for data_def in data_defs]

    elements.append(
        _value_to_mms_data([i is not None for i in report_data],
                           iec61850.BasicValueType.BIT_STRING))

    optional_fields.add(iec61850.OptionalField.DATA_REFERENCE)
    elements.extend(
        _value_to_mms_data(_data_ref_to_str(data.ref),
                           iec61850.BasicValueType.VISIBLE_STRING)
        for data in report_data
        if data is not None)

    elements.extend(
        _value_to_mms_data(data.value, data_def.value_type)
        for data, data_def in zip(report_data, data_defs)
        if data is not None)

    if any(data.reasons is not None
           for data in report_data
           if data is not None):
        optional_fields.add(iec61850.OptionalField.REASON_FOR_INCLUSION)
        elements.extend(
            _value_to_mms_data([False,
                                *((data.reasons and
                                   iec61850.ReasonCode(i) in data.reasons)
                                  for i in range(1, 7))],
                               iec61850.BasicValueType.BIT_STRING)
            for data in report_data
            if data is not None)

    elements.appendleft(
        _value_to_mms_data([False,
                            *(iec61850.OptionalField(i) in optional_fields
                              for i in range(1, 9)),
                            segmentation],
                           iec61850.BasicValueType.BIT_STRING))

    elements.appendleft(
        _value_to_mms_data(report.report_id,
                           iec61850.BasicValueType.VISIBLE_STRING))

    return elements
