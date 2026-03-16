#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import sys
from typing import (List,
                    Optional,
                    Tuple,
                    Type,
                    TypedDict)

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

from couchbase.logic.observability import (CollectionDetails,
                                           OpAttributeName,
                                           OpName,
                                           ServiceType)
from couchbase.logic.observability.no_op import NoOpMeter

from .meters import TestMeterType
from .value_recorders import TestValueRecorder, TestValueRecorderType


class ResetParams(TypedDict, total=False):
    op_name: Optional[OpName]
    collection_details: Optional[CollectionDetails]
    validate_error: Optional[bool]
    nested_ops: Optional[List[OpName]]
    clear_nested_ops: Optional[bool]
    error_before_dispatch: Optional[bool]
    expect_request_span: bool
    bucket_name: Optional[str]
    scope_name: Optional[str]
    collection_name: Optional[str]
    sub_op_names: Optional[List[Tuple[OpName, bool]]]

# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def _ctx(op_name: Optional[str] = None, recorder=None, extra: Optional[str] = None) -> str:
    parts = []
    if op_name is not None:
        parts.append(f'op={op_name!r}')
    if recorder is not None:
        parts.append(f'recorder={recorder.op_name!r}')
        if hasattr(recorder, 'attributes'):
            parts.append(f'attrs={list(recorder.attributes.keys())}')
    if extra:
        parts.append(extra)
    return ' | '.join(parts)


def _assert_has(d: dict, key: str, *, ctx: str = '') -> None:
    assert key in d, f'Expected attribute {key!r} to be present. {ctx}'


def _assert_not_has(d: dict, key: str, *, ctx: str = '') -> None:
    assert key not in d, f'Expected attribute {key!r} to be absent, got value={d.get(key)!r}. {ctx}'


def _assert_eq(actual, expected, *, what: str, ctx: str = '') -> None:
    assert actual == expected, f'{what}: expected={expected!r} actual={actual!r}. {ctx}'


def _assert_isinstance(val, typ, *, what: str, ctx: str = '') -> None:
    assert isinstance(val, typ), (
        f'{what}: expected type {typ.__name__!r} got {type(val).__name__!r} (value={val!r}). {ctx}'
    )


def _assert_len(seq, expected: Optional[int] = None, *,
                min_expected: Optional[int] = None,
                what: str,
                ctx: str = '') -> None:
    actual = len(seq)
    if expected is not None:
        assert actual == expected, f'{what}: expected count={expected} actual={actual}. {ctx}'
    elif min_expected is not None:
        assert actual >= min_expected, f'{what}: expected at least {min_expected} got {actual}. {ctx}'


def _get_recorder_from_meter(meter: TestMeterType,
                             op_name: Optional[OpName],
                             is_ds_op: Optional[bool] = False,
                             is_multi_op: Optional[bool] = False,
                             recorder_idx: Optional[int] = None) -> Optional[TestValueRecorderType]:
    """Fetch the target value recorer from the meter"""
    available = [(meter.recorders or {}).keys()]
    op_str = op_name.value if op_name is not None else None
    ctx = _ctx(op_name=op_str, extra=f'meter_recorders={available}')
    _assert_len(meter.recorders or [], expected=1, what='meter.recorders count', ctx=ctx)
    if recorder_idx is not None:
        recorder = meter.recorders.get(OpAttributeName.MeterOperationDuration, [None])[recorder_idx]
    else:
        recorder = meter.get_value_recorder_by_op_name(op_name)
    if is_ds_op is True:
        assert recorder is None, f'DS op found at top-level op_name={op_name.value!r}. {ctx}'
        return None
    if is_multi_op is True:
        assert recorder is None, f'Multi-op recorder found at top-level op_name={op_name.value!r}. {ctx}'
        return None

    assert recorder is not None, f'ValueRecorder not found for op={op_str!r}. {ctx}'
    return recorder


def _validate_base_metrics(recorder: TestValueRecorderType,
                           op_name: OpName,
                           recorder_type: Type[TestValueRecorderType],
                           supports_cluster_labels: bool,
                           validate_error: Optional[bool] = False) -> None:

    ctx = _ctx(op_name=op_name.value, recorder=recorder)
    _assert_isinstance(recorder, recorder_type, what='recorder type', ctx=ctx)
    _assert_eq(recorder.name, OpAttributeName.MeterOperationDuration, what='recorder.name', ctx=ctx)
    _assert_eq(recorder.op_name, op_name.value, what='recorder.op_name', ctx=ctx)
    _assert_isinstance(recorder.attributes, dict, what='recorder.attributes type', ctx=ctx)
    _assert_has(recorder.attributes, OpAttributeName.SystemName.value, ctx=ctx)
    _assert_eq(recorder.attributes[OpAttributeName.SystemName.value], 'couchbase',
               what=f'recorder attribute {OpAttributeName.SystemName.value!r}', ctx=ctx)
    _assert_eq(recorder.attributes[OpAttributeName.ReservedUnit.value], OpAttributeName.ReservedUnitSeconds.value,
               what=f'recorder attribute {OpAttributeName.ReservedUnit.value!r}', ctx=ctx)

    if supports_cluster_labels:
        _assert_isinstance(recorder.attributes.get(OpAttributeName.ClusterName.value),
                           str, what='ClusterName attribute', ctx=ctx)
        _assert_isinstance(recorder.attributes.get(OpAttributeName.ClusterUUID.value),
                           str, what='ClusterUUID attribute', ctx=ctx)
    if validate_error is True:
        _assert_has(recorder.attributes, OpAttributeName.ErrorType.value, ctx=ctx)
        _assert_isinstance(recorder.attributes[OpAttributeName.ErrorType.value],
                           str, what='ErrorType attribute', ctx=ctx)
    else:
        _assert_not_has(recorder.attributes, OpAttributeName.ErrorType.value, ctx=ctx)


class KeyValueNoOpMeterValidatorImpl:

    def __init__(self,
                 meter: TestMeterType,
                 **kwargs: Unpack[ResetParams]) -> None:
        self._meter = meter

    def _validate(self) -> None:
        _assert_isinstance(self._meter, NoOpMeter, what='meter type')
        _assert_len(self._meter.recorders, expected=0, what='meter.recorders count')

    def reset(self, **kwargs: Unpack[ResetParams]) -> None:
        pass

    def validate_kv_op(self) -> None:
        self._validate()

    def validate_ds_op(self) -> None:
        self._validate()

    def validate_multi_kv_op(self) -> None:
        self._validate()


class KeyValueMeterValidatorImpl:
    def __init__(self,
                 meter: TestMeterType,
                 collection_details,
                 supports_cluster_labels: Optional[bool] = None) -> None:
        self._meter = meter
        self._collection_details = collection_details
        self._supports_cluster_labels = supports_cluster_labels

    def _reset_to_defaults(self) -> None:
        self._op_name = None
        self._validate_error = False
        self._nested_ops = None
        self._error_before_dispatch = None
        self._expect_request_span = None
        self._sub_op_names = None

    def _validate_metrics(self,
                          recorder: TestValueRecorderType,
                          recorder_type: Type[TestValueRecorderType],
                          op_name: OpName,
                          validate_error: bool) -> None:
        _validate_base_metrics(recorder, op_name, recorder_type, self._supports_cluster_labels, validate_error)
        ctx = _ctx(op_name=op_name.value, recorder=recorder)
        _assert_eq(recorder.attributes.get(OpAttributeName.OperationName.value), op_name.value,
                   what='OperationName attribute', ctx=ctx)
        _assert_eq(recorder.attributes.get(OpAttributeName.Service.value), 'kv',
                   what='Service attribute', ctx=ctx)
        # if the op fails prior to trying to create a span (we do metrics 'things' at the same point), we won't
        # have had a chance to populate the collection details
        if self._expect_request_span is False:
            _assert_not_has(recorder.attributes, OpAttributeName.BucketName.value, ctx=ctx)
            _assert_not_has(recorder.attributes, OpAttributeName.ScopeName.value, ctx=ctx)
            _assert_not_has(recorder.attributes, OpAttributeName.CollectionName.value, ctx=ctx)
            return
        _assert_eq(recorder.attributes.get(OpAttributeName.BucketName.value), self._collection_details['bucket'],
                   what='BucketName attribute', ctx=ctx)
        _assert_eq(recorder.attributes.get(OpAttributeName.ScopeName.value), self._collection_details['scope'],
                   what='ScopeName attribute', ctx=ctx)
        _assert_eq(recorder.attributes.get(OpAttributeName.CollectionName.value),
                   self._collection_details['collection_name'],
                   what='CollectionName attribute', ctx=ctx)

    def reset(self, **kwargs: Unpack[ResetParams]) -> None:
        self._meter.clear()
        self._reset_to_defaults()

        op_name = kwargs.get('op_name')
        if op_name:
            self._op_name = op_name

        validate_error = kwargs.get('validate_error')
        if validate_error is not None:
            self._validate_error = validate_error

        nested_ops = kwargs.get('nested_ops')
        if nested_ops is not None:
            self._nested_ops = nested_ops

        sub_op_names = kwargs.get('sub_op_names')
        if sub_op_names is not None:
            self._sub_op_names = sub_op_names

        error_before_dispatch = kwargs.get('error_before_dispatch')
        if error_before_dispatch is not None:
            self._error_before_dispatch = error_before_dispatch

        expect_request_span = kwargs.get('expect_request_span')
        if expect_request_span is not None:
            self._expect_request_span = expect_request_span

    def validate_kv_op(self) -> None:
        if self._nested_ops:
            value_recorder = _get_recorder_from_meter(self._meter, self._op_name)
            self._validate_metrics(value_recorder, TestValueRecorder, self._op_name, self._validate_error)
            for nested_op in self._nested_ops:
                nested_recorder = _get_recorder_from_meter(self._meter, nested_op)
                self._validate_metrics(nested_recorder, TestValueRecorder, nested_op, self._validate_error)
        else:
            value_recorder = _get_recorder_from_meter(self._meter, self._op_name)
            self._validate_metrics(value_recorder, TestValueRecorder, self._op_name, self._validate_error)

    def validate_ds_op(self) -> None:
        ctx = _ctx(op_name=self._op_name.value)
        value_recorder = _get_recorder_from_meter(self._meter, self._op_name, is_ds_op=True)
        assert value_recorder is None, f'Expected no top-level recorder for DS op, but found one. {ctx}'
        for idx, sup_op in enumerate(self._sub_op_names):
            nested_op, with_error = sup_op
            nested_recorder = _get_recorder_from_meter(self._meter, nested_op, recorder_idx=idx)
            self._validate_metrics(nested_recorder, TestValueRecorder, nested_op, with_error)

    def validate_multi_kv_op(self) -> None:
        ctx = _ctx(op_name=self._op_name.value)

        # for multi-ops, since we ignore the top-level portion of the request and only log the nested KV ops
        # if we have an error prior to dispatch (e.g. we don't go down to the bindings) we should not have any
        # metrics recorded
        if self._error_before_dispatch is True:
            _assert_len(self._meter.recorders, expected=0, what='meter.recorders count expected to be 0')
            return

        value_recorder = _get_recorder_from_meter(self._meter, self._op_name, is_multi_op=True)
        assert value_recorder is None, f'Expected no top-level recorder for multi-op, but found one. {ctx}'
        recorder_count = len(self._meter.recorders.get(OpAttributeName.MeterOperationDuration, []))
        expected_count = len(self._nested_ops) if self._nested_ops else 0
        _assert_eq(recorder_count, expected_count,
                   what=f'meter.recorders count expected to be {expected_count}', ctx=ctx)
        for nested_op in self._nested_ops:
            nested_recorder = _get_recorder_from_meter(self._meter, nested_op)
            self._validate_metrics(nested_recorder, TestValueRecorder, nested_op, self._validate_error)


class HttpNoOpMeterValidatorImpl:
    def __init__(self,
                 meter: TestMeterType,
                 **kwargs: Unpack[ResetParams]) -> None:
        self._meter = meter

    def reset(self, **kwargs: Unpack[ResetParams]) -> None:
        pass

    def validate_http_op(self) -> None:
        _assert_isinstance(self._meter, NoOpMeter, what='meter type')
        _assert_len(self._meter.recorders, expected=0, what='meter.recorders count')


class HttpMeterValidatorImpl:
    def __init__(self,
                 meter: TestMeterType,
                 supports_cluster_labels: Optional[bool] = None) -> None:
        self._meter = meter
        self._supports_cluster_labels = supports_cluster_labels
        self._service_type = None

    def _get_service_type(self) -> ServiceType:
        if (self._op_name == OpName.AnalyticsQuery
                or self._op_name.value.startswith('manager_analytics')):
            return ServiceType.Analytics
        elif (self._op_name == OpName.Query
              or self._op_name.value.startswith('manager_query')):
            return ServiceType.Query
        elif (self._op_name == OpName.SearchQuery
              or self._op_name.value.startswith('manager_search')):
            return ServiceType.Search
        elif (self._op_name == OpName.ViewQuery
              or self._op_name.value.startswith('manager_views')):
            return ServiceType.Views
        elif self._op_name.value.startswith('manager_eventing'):
            return ServiceType.Eventing
        elif (self._op_name.value.startswith('manager_buckets')
              or self._op_name.value.startswith('manager_collections')
              or self._op_name.value.startswith('manager_users')):
            return ServiceType.Management

        raise RuntimeError(f'Unsupported op_name: {self._op_name}')

    def _reset_to_defaults(self) -> None:
        self._op_name = None
        self._service_type = None
        self._validate_error = False
        self._nested_ops = None
        self._error_before_dispatch = None
        self._expect_request_span = None
        self._bucket_name = None
        self._scope_name = None
        self._collection_name = None

    def _validate_metrics(self,
                          recorder: TestValueRecorderType,
                          recorder_type: Type[TestValueRecorderType],
                          op_name: OpName,
                          validate_error: bool) -> None:
        _validate_base_metrics(recorder, op_name, recorder_type, self._supports_cluster_labels, validate_error)
        ctx = _ctx(op_name=op_name.value, recorder=recorder)
        _assert_eq(recorder.attributes.get(OpAttributeName.OperationName.value), op_name.value,
                   what='OperationName attribute', ctx=ctx)
        _assert_eq(recorder.attributes.get(OpAttributeName.Service.value), self._service_type.value,
                   what='Service attribute', ctx=ctx)

        if self._bucket_name is not None:
            _assert_eq(recorder.attributes.get(OpAttributeName.BucketName.value), self._bucket_name,
                       what='BucketName attribute', ctx=ctx)
        else:
            _assert_not_has(recorder.attributes, OpAttributeName.BucketName.value, ctx=ctx)

        if self._scope_name is not None:
            _assert_eq(recorder.attributes.get(OpAttributeName.ScopeName.value), self._scope_name,
                       what='ScopeName attribute', ctx=ctx)
        else:
            _assert_not_has(recorder.attributes, OpAttributeName.ScopeName.value, ctx=ctx)

        if self._collection_name is not None:
            _assert_eq(recorder.attributes.get(OpAttributeName.CollectionName.value),
                       self._collection_name,
                       what='CollectionName attribute', ctx=ctx)
        else:
            _assert_not_has(recorder.attributes, OpAttributeName.CollectionName.value, ctx=ctx)

    def reset(self, **kwargs: Unpack[ResetParams]) -> None:
        self._meter.clear()
        self._reset_to_defaults()

        op_name = kwargs.get('op_name')
        if op_name:
            self._op_name = op_name

        self._service_type = self._get_service_type() if self._op_name else None

        validate_error = kwargs.get('validate_error')
        if validate_error is not None:
            self._validate_error = validate_error

        nested_ops = kwargs.get('nested_ops')
        if nested_ops is not None:
            self._nested_ops = nested_ops

        error_before_dispatch = kwargs.get('error_before_dispatch')
        if error_before_dispatch is not None:
            self._error_before_dispatch = error_before_dispatch

        expect_request_span = kwargs.get('expect_request_span')
        if expect_request_span is not None:
            self._expect_request_span = expect_request_span

        bucket_name = kwargs.get('bucket_name')
        if bucket_name is not None:
            self._bucket_name = bucket_name

        scope_name = kwargs.get('scope_name')
        if scope_name is not None:
            self._scope_name = scope_name

        collection_name = kwargs.get('collection_name')
        if collection_name is not None:
            self._collection_name = collection_name

    def validate_http_op(self) -> None:
        if self._nested_ops:
            value_recorder = _get_recorder_from_meter(self._meter, self._op_name)
            self._validate_metrics(value_recorder, TestValueRecorder, self._op_name, self._validate_error)
            for nested_op in self._nested_ops:
                nested_recorder = _get_recorder_from_meter(self._meter, nested_op)
                self._validate_metrics(nested_recorder, TestValueRecorder, nested_op, self._validate_error)
        else:
            value_recorder = _get_recorder_from_meter(self._meter, self._op_name)
            self._validate_metrics(value_recorder, TestValueRecorder, self._op_name, self._validate_error)


class KeyValueMeterValidator:
    def __init__(self,
                 meter: TestMeterType,
                 collection_details,
                 supports_cluster_labels: Optional[bool] = None) -> None:
        if isinstance(meter, NoOpMeter):
            self._impl = KeyValueNoOpMeterValidatorImpl(meter)
        else:
            self._impl = KeyValueMeterValidatorImpl(meter, collection_details, supports_cluster_labels)

    def reset(self, **kwargs: Unpack[ResetParams]) -> None:
        self._impl.reset(**kwargs)

    def validate_kv_op(self) -> None:
        self._impl.validate_kv_op()

    def validate_ds_op(self) -> None:
        self._impl.validate_ds_op()

    def validate_multi_kv_op(self) -> None:
        self._impl.validate_multi_kv_op()


class HttpMeterValidator:
    def __init__(self,
                 meter: TestMeterType,
                 supports_cluster_labels: Optional[bool] = None) -> None:
        if isinstance(meter, NoOpMeter):
            self._impl = HttpNoOpMeterValidatorImpl(meter)
        else:
            self._impl = HttpMeterValidatorImpl(meter, supports_cluster_labels)

    def reset(self, **kwargs: Unpack[ResetParams]) -> None:
        self._impl.reset(**kwargs)

    def validate_http_op(self) -> None:
        self._impl.validate_http_op()
