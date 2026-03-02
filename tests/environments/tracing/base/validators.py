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
from dataclasses import dataclass
from typing import (List,
                    Optional,
                    Tuple,
                    Type,
                    TypedDict,
                    Union)

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

from couchbase.logic.observability import (CollectionDetails,
                                           DispatchAttributeName,
                                           OpAttributeName,
                                           OpName,
                                           ServiceType)
from couchbase.logic.observability.no_op import NoOpSpan, NoOpTracer
from couchbase.logic.observability.threshold_logging import ThresholdLoggingSpanSnapshot, ThresholdLoggingTracer

from .spans import (LegacyTestSpan,
                    TestSpan,
                    TestSpanType,
                    TestThresholdLoggingSpan)
from .tracers import (LegacyTestTracer,
                      TestThresholdLoggingTracer,
                      TestTracerType)

ENCODING_OPS = [OpName.Insert, OpName.MutateIn, OpName.Replace, OpName.Upsert]


DS_OPS = set([
    OpName.ListAppend,
    OpName.ListClear,
    OpName.ListGetAll,
    OpName.ListGetAt,
    OpName.ListIndexOf,
    OpName.ListPrepend,
    OpName.ListRemoveAt,
    OpName.ListSetAt,
    OpName.ListSize,
    OpName.MapAdd,
    OpName.MapClear,
    OpName.MapExists,
    OpName.MapGet,
    OpName.MapGetAll,
    OpName.MapItems,
    OpName.MapKeys,
    OpName.MapRemove,
    OpName.MapSize,
    OpName.MapValues,
    OpName.QueueClear,
    OpName.QueuePop,
    OpName.QueuePush,
    OpName.QueueSize,
    OpName.SetAdd,
    OpName.SetClear,
    OpName.SetContains,
    OpName.SetRemove,
    OpName.SetSize,
    OpName.SetValues
])

DS_REMOVE_OPS = set([OpName.ListRemoveAt, OpName.MapRemove, OpName.QueuePop, OpName.SetRemove])

# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def _ctx(op_name: Optional[str] = None, span=None, extra: Optional[str] = None) -> str:
    parts = []
    if op_name is not None:
        parts.append(f'op={op_name!r}')
    if span is not None:
        parts.append(f'span={span.name!r}')
        if hasattr(span, '_attributes'):
            parts.append(f'attrs={list(span._attributes.keys())}')
        if hasattr(span, 'children') and span.children is not None:
            parts.append(f'children={len(span.children)}')
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


def _get_span_from_tracer(tracer, op_name, parent_span):
    """Fetch the target span from the tracer, honouring parent-span scenarios."""
    available = [s.name for s in (tracer._spans or [])]
    op_str = op_name.value if op_name is not None else None
    ctx = _ctx(op_name=op_str, extra=f'tracer_spans={available}')
    _assert_len(tracer._spans or [], expected=1, what='tracer._spans count', ctx=ctx)
    if parent_span is not None:
        assert tracer._spans[0] == parent_span, f'Expected first span to be parent_span. {ctx}'
        assert tracer._spans[0].children is not None, f'parent_span has no children. {ctx}'
        span = tracer._spans[0].children[0]
        assert span._parent_span == parent_span, f'Expected span._parent_span to match parent_span. {ctx}'
    else:
        span = tracer.get_span_by_name(op_name.value)
    assert span is not None, f'Span not found for op={op_str!r}. {ctx}'
    return span


def _validate_dispatch_span_common(span, span_type, name: str, supports_cluster_labels: bool,
                                   parent_span, ctx: str = '') -> None:
    """Validate attributes shared between KV and HTTP dispatch spans."""
    validate_request_span(span, span_type, name, supports_cluster_labels)
    _assert_eq(span._attributes.get(DispatchAttributeName.NetworkTransport.value),
               'tcp', what='dispatch NetworkTransport', ctx=ctx)
    _assert_isinstance(span._attributes.get(DispatchAttributeName.ServerAddress.value),
                       str, what='dispatch ServerAddress', ctx=ctx)
    _assert_isinstance(span._attributes.get(DispatchAttributeName.ServerPort.value),
                       int, what='dispatch ServerPort', ctx=ctx)
    _assert_isinstance(span._attributes.get(DispatchAttributeName.PeerAddress.value),
                       str, what='dispatch PeerAddress', ctx=ctx)
    _assert_isinstance(span._attributes.get(DispatchAttributeName.PeerPort.value),
                       int, what='dispatch PeerPort', ctx=ctx)
    assert span._parent_span == parent_span, (
        f'dispatch span parent mismatch: expected={parent_span!r} actual={span._parent_span!r}. {ctx}'
    )


@dataclass
class ValidateKeyValueSpanRequest:
    span: TestSpan
    op_name: OpName
    collection_details: CollectionDetails
    parent_span: Optional[TestSpan] = None
    validate_error: Optional[bool] = False
    durability: Optional[str] = None
    server_duration_gt_zero: Optional[bool] = None
    multi_op_key_count: Optional[int] = None
    skip_dispatch_validation: Optional[bool] = None
    error_before_dispatch: Optional[bool] = None
    retry_count_gt_zero: Optional[bool] = None


@dataclass
class ValidateHttpSpanRequest:
    span: TestSpan
    op_name: OpName
    parent_span: Optional[TestSpan] = None
    validate_error: Optional[bool] = False
    skip_dispatch_validation: Optional[bool] = None
    error_before_dispatch: Optional[bool] = None
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    collection_name: Optional[str] = None


class KvResetParams(TypedDict, total=False):
    full_reset: bool
    do_not_clear_spans: bool
    op_name: OpName
    collection_details: CollectionDetails
    parent_span: TestSpan
    clear_parent_span: bool
    validate_error: bool
    durability: str
    server_duration_gt_zero: bool
    multi_op_key_count: int
    nested_ops: List[OpName]
    clear_nested_ops: bool
    sub_op_names: List[Tuple[OpName, bool]]
    error_before_dispatch: bool
    expect_request_span: bool
    retry_count_gt_zero: bool


class HttpResetParams(TypedDict, total=False):
    full_reset: bool
    do_not_clear_spans: bool
    op_name: OpName
    collection_details: CollectionDetails
    parent_span: TestSpan
    clear_parent_span: bool
    validate_error: bool
    statement: str
    clear_statement: bool
    nested_ops: List[OpName]
    clear_nested_ops: bool
    error_before_dispatch: bool
    expect_request_span: bool


def validate_total_time(start_time: int, end_time: int, ctx: str = '') -> None:
    assert start_time is not None, f'start_time is None. {ctx}'
    assert start_time > 0, f'start_time={start_time} should be > 0. {ctx}'
    assert end_time is not None, f'end_time is None. {ctx}'
    assert end_time > 0, f'end_time={end_time} should be > 0. {ctx}'
    assert end_time > start_time, f'end_time={end_time} should be > start_time={start_time}. {ctx}'


def validate_request_span(span: TestSpanType,
                          span_type: Type[TestSpanType],
                          name: str,
                          supports_cluster_labels: bool,
                          validate_error: Optional[bool] = False) -> None:
    ctx = _ctx(op_name=name, span=span)
    _assert_isinstance(span, span_type, what='span type', ctx=ctx)
    _assert_eq(span.name, name, what='span.name', ctx=ctx)
    _assert_isinstance(span._attributes, dict, what='span._attributes type', ctx=ctx)
    _assert_has(span._attributes, OpAttributeName.SystemName.value, ctx=ctx)
    _assert_eq(span._attributes[OpAttributeName.SystemName.value], 'couchbase',
               what=f'span attribute {OpAttributeName.SystemName.value!r}', ctx=ctx)
    if span_type.__name__ != 'LegacyTestSpan':
        if supports_cluster_labels:
            _assert_isinstance(span._attributes.get(OpAttributeName.ClusterName.value),
                               str, what='ClusterName attribute', ctx=ctx)
            _assert_isinstance(span._attributes.get(OpAttributeName.ClusterUUID.value),
                               str, what='ClusterUUID attribute', ctx=ctx)
        expected_status = 0  # SpanStatusCode.UNSET = 0
        if validate_error is True:
            expected_status = 2  # SpanStatusCode.ERROR = 2
        _assert_eq(span._status.value, expected_status,
                   what='span status code', ctx=ctx)
    validate_total_time(span._start_time, span._end_time, ctx=ctx)


class KeyValueSpanValidatorImpl:
    def __init__(self,
                 tracer: TestTracerType,
                 collection_details: CollectionDetails,
                 supports_cluster_labels: bool,
                 op_name: Optional[OpName] = None,
                 parent_span: Optional[TestSpan] = None,
                 validate_error: Optional[bool] = False,
                 durability: Optional[str] = None,
                 server_duration_gt_zero: Optional[bool] = None,
                 multi_op_key_count: Optional[int] = None,
                 nested_ops: Optional[List[OpName]] = None,
                 sub_op_names: Optional[List[Tuple[OpName, bool]]] = None,
                 error_before_dispatch: Optional[bool] = None,
                 expect_request_span: Optional[bool] = None,
                 retry_count_gt_zero: Optional[bool] = None) -> None:
        self._tracer = tracer
        if isinstance(tracer, LegacyTestTracer):
            self._is_legacy = True
            self._span_type = LegacyTestSpan
        else:
            self._is_legacy = False
            self._span_type = TestSpan
        self._collection_details = collection_details
        self._supports_cluster_labels = supports_cluster_labels
        self._op_name = op_name
        self._parent_span = parent_span
        self._validate_error = validate_error
        self._durability = durability
        self._server_duration_gt_zero = server_duration_gt_zero
        self._multi_op_key_count = multi_op_key_count
        self._nested_ops = nested_ops
        self._sub_op_names = sub_op_names
        self._error_before_dispatch = error_before_dispatch
        self._expect_request_span = expect_request_span
        self._retry_count_gt_zero = retry_count_gt_zero

    @property
    def is_legacy(self) -> bool:
        return self._is_legacy

    def _end_parent_span(self) -> None:
        if self._parent_span is not None:
            if self._is_legacy:
                self._parent_span.finish()
            else:
                self._parent_span.end()

    def _has_multiple_ds_subdoc_subops(self) -> bool:
        if self._sub_op_names is None:
            return False
        subops = [op for op, _ in self._sub_op_names if op in [OpName.MutateIn, OpName.LookupIn]]
        return len(subops) > 1

    def _reset_to_defaults(self) -> None:
        """Reset all validator state to defaults.

        Does NOT clear spans - span clearing is controlled separately
        via the do_not_clear_spans parameter in reset().
        This is necessary for parent_span scenarios where we create
        a parent span with the tracer before resetting the validator.
        """
        self._op_name = None
        self._parent_span = None
        self._validate_error = False
        self._durability = None
        self._server_duration_gt_zero = None
        self._multi_op_key_count = None
        self._nested_ops = None
        self._sub_op_names = None
        self._error_before_dispatch = None
        self._expect_request_span = None
        self._retry_count_gt_zero = None

    def _validate_base_span(self, req: ValidateKeyValueSpanRequest) -> None:
        validate_request_span(req.span,
                              self._span_type,
                              req.op_name.value,
                              self._supports_cluster_labels,
                              validate_error=req.validate_error)
        ctx = _ctx(op_name=req.op_name.value, span=req.span)
        _assert_eq(req.span._attributes.get(OpAttributeName.OperationName.value), req.op_name.value,
                   what='OperationName attribute', ctx=ctx)
        _assert_eq(req.span._attributes.get(OpAttributeName.Service.value), 'kv',
                   what='Service attribute', ctx=ctx)
        _assert_eq(req.span._attributes.get(OpAttributeName.BucketName.value), req.collection_details['bucket'],
                   what='BucketName attribute', ctx=ctx)
        _assert_eq(req.span._attributes.get(OpAttributeName.ScopeName.value), req.collection_details['scope'],
                   what='ScopeName attribute', ctx=ctx)
        _assert_eq(req.span._attributes.get(OpAttributeName.CollectionName.value),
                   req.collection_details['collection_name'],
                   what='CollectionName attribute', ctx=ctx)

        # legacy does not support retry count attribute
        if not self.is_legacy:
            _assert_has(req.span._attributes, OpAttributeName.RetryCount.value, ctx=ctx)
            _assert_isinstance(req.span._attributes.get(OpAttributeName.RetryCount.value),
                               int,
                               what='retries attribute',
                               ctx=ctx)
            if req.retry_count_gt_zero is True:
                retry_count = req.span._attributes.get(OpAttributeName.RetryCount.value)
                assert retry_count > 0, f'RetryCount={retry_count} should be > 0. {ctx}'

        if req.durability is not None:
            _assert_eq(req.span._attributes.get(OpAttributeName.DurabilityLevel.value), req.durability,
                       what='DurabilityLevel attribute', ctx=ctx)

    def _validate_dispatch_span(self,
                                span: TestSpan,
                                name: str,
                                parent_span: TestSpan,
                                server_duration_gt_zero: Optional[bool] = True) -> None:
        ctx = _ctx(op_name=name, span=span)
        _validate_dispatch_span_common(span, self._span_type, name, self._supports_cluster_labels, parent_span, ctx=ctx)
        _assert_isinstance(span._attributes.get(DispatchAttributeName.LocalId.value),
                           str, what='dispatch LocalId', ctx=ctx)
        _assert_isinstance(span._attributes.get(DispatchAttributeName.OperationId.value),
                           str, what='dispatch OperationId', ctx=ctx)
        _assert_isinstance(span._attributes.get(DispatchAttributeName.ServerDuration.value),
                           int, what='dispatch ServerDuration', ctx=ctx)
        if server_duration_gt_zero is True:
            sd = span._attributes.get(DispatchAttributeName.ServerDuration.value)
            assert sd > 0, f'dispatch ServerDuration={sd} should be > 0. {ctx}'

    def _validate_dispatch_spans(self, req: ValidateKeyValueSpanRequest) -> None:
        dispatch_spans = [s for s in req.span.children if s.name == OpAttributeName.DispatchSpanName.value]
        ctx = _ctx(op_name=req.op_name.value, span=req.span)
        # we sometimes trigger an error w/o going down to the C++ bindings
        if not req.validate_error:
            _assert_len(dispatch_spans, min_expected=(req.multi_op_key_count or 1),
                        what='dispatch span count', ctx=ctx)
        for dispatch_span in dispatch_spans:
            self._validate_dispatch_span(dispatch_span,
                                         OpAttributeName.DispatchSpanName.value,
                                         req.span,
                                         server_duration_gt_zero=req.server_duration_gt_zero)

    def _validate_encoding_spans(self, req: ValidateKeyValueSpanRequest) -> None:
        if req.op_name in ENCODING_OPS:
            encoding_spans = [s for s in req.span.children if s.name == OpAttributeName.EncodingSpanName.value]
            ctx = _ctx(op_name=req.op_name.value, span=req.span)
            span_count = req.multi_op_key_count or 1
            # for each spec in the mutate_in op we can have an encoding span (depends on the spec)
            check_equal = True
            if req.op_name == OpName.MutateIn:
                # mutate_in remove spec does not have an encoding span
                if self._op_name in DS_REMOVE_OPS:
                    span_count = 0
                elif self._op_name in DS_OPS:
                    multiple_subdoc_subops = self._has_multiple_ds_subdoc_subops()
                    check_equal = req.validate_error is False
                    # on the retried subdoc op, we reuse the original request and therefore do not have an encoding span
                    if multiple_subdoc_subops and check_equal:
                        span_count = 0
                else:
                    check_equal = False
            if check_equal:
                _assert_len(encoding_spans, expected=span_count, what='encoding span count', ctx=ctx)
            else:
                _assert_len(encoding_spans, min_expected=span_count, what='encoding span count (min)', ctx=ctx)
            # encoding spans are the first children to be added
            encoded_spans = encoding_spans[:span_count]
            for idx, espan in enumerate(encoded_spans):
                assert req.span.children[idx] == espan, (
                    f'encoding span at index {idx} not the expected child. {ctx}'
                )
                assert espan._parent_span == req.span, (
                    f'encoding span parent mismatch at index {idx}. {ctx}'
                )
                validate_request_span(espan, self._span_type, OpAttributeName.EncodingSpanName.value,
                                      self._supports_cluster_labels)

    def _validate_span(self, req: ValidateKeyValueSpanRequest) -> None:
        ctx = _ctx(op_name=req.op_name.value, span=req.span)
        if req.parent_span is not None:
            assert req.span._parent_span == req.parent_span, (
                f'span parent mismatch: expected={req.parent_span!r} actual={req.span._parent_span!r}. {ctx}'
            )
        self._validate_base_span(req)
        # if the request errors prior to dispatch, we have not children (e.g. dispatch/encoding spans) to validate
        if req.error_before_dispatch is True:
            return
        assert req.span.children is not None, f'span has no children list. {ctx}'
        _assert_len(req.span.children, min_expected=1, what='span children count', ctx=ctx)
        if req.skip_dispatch_validation is not True:
            self._validate_dispatch_spans(req)
        if self._is_legacy is False:
            self._validate_encoding_spans(req)

    def _validate_nested_span(self,
                              req: ValidateKeyValueSpanRequest,
                              required_nested_ops: List[OpName]) -> None:
        ctx = _ctx(op_name=req.op_name.value, span=req.span,
                   extra=f'required_ops={[o.value for o in required_nested_ops]}')
        self._validate_base_span(req)
        assert req.span.children is not None, f'nested span has no children list. {ctx}'
        actual_child_names = [s.name for s in req.span.children]
        if req.multi_op_key_count is not None:
            total_child_span_count = len(required_nested_ops) * req.multi_op_key_count
        else:
            total_child_span_count = len(required_nested_ops)

        _assert_len(req.span.children, min_expected=total_child_span_count,
                    what='nested span children count',
                    ctx=_ctx(op_name=req.op_name.value, span=req.span,
                             extra=f'actual_children={actual_child_names}'))
        dispatch_spans: List[TestSpan] = []
        for op in required_nested_ops:
            child_spans = [s for s in req.span.children if s.name == op.value]
            span_count = req.multi_op_key_count or 1
            _assert_len(child_spans, min_expected=span_count,
                        what=f'child span count for op={op.value!r}', ctx=ctx)
            for child_span in child_spans:
                child_dispatch_spans = [s for s in child_span.children
                                        if s.name == OpAttributeName.DispatchSpanName.value]
                if child_dispatch_spans:
                    dispatch_spans.extend(child_dispatch_spans)
                child_req = ValidateKeyValueSpanRequest(child_span,
                                                        op,
                                                        req.collection_details,
                                                        parent_span=req.span,
                                                        validate_error=False,  # core spans shouldn't have errors
                                                        durability=req.durability,
                                                        server_duration_gt_zero=req.server_duration_gt_zero)
                self._validate_span(child_req)

        _assert_len(dispatch_spans, min_expected=total_child_span_count,
                    what='total dispatch spans across nested ops', ctx=ctx)
        # Sometimes we might have some server durations that == 0 (e.g. getAnyReplica), in that case
        # we verify that at least one of the durations is > 0. Otherwise the verification happens when
        # we validate the child's dispatch span
        if req.server_duration_gt_zero is False:
            positive_server_durations = [s for s in dispatch_spans
                                         if s._attributes[DispatchAttributeName.ServerDuration.value] > 0]
            _assert_len(positive_server_durations, min_expected=1,
                        what='dispatch spans with ServerDuration > 0', ctx=ctx)

    def _validate_tracer_and_get_span_request(self) -> ValidateKeyValueSpanRequest:
        assert self._tracer._spans is not None, 'tracer._spans is None'
        span = _get_span_from_tracer(self._tracer, self._op_name, self._parent_span)
        return ValidateKeyValueSpanRequest(span,
                                           self._op_name,
                                           self._collection_details,
                                           parent_span=self._parent_span,
                                           validate_error=self._validate_error,
                                           durability=self._durability,
                                           server_duration_gt_zero=self._server_duration_gt_zero,
                                           multi_op_key_count=self._multi_op_key_count,
                                           error_before_dispatch=self._error_before_dispatch,
                                           retry_count_gt_zero=self._retry_count_gt_zero)

    def reset(self,  # noqa: C901
              full_reset: bool = True,
              do_not_clear_spans: Optional[bool] = None,
              op_name: Optional[OpName] = None,
              collection_details: Optional[CollectionDetails] = None,
              parent_span: Optional[TestSpan] = None,
              clear_parent_span: Optional[bool] = None,
              validate_error: Optional[bool] = None,
              durability: Optional[str] = None,
              server_duration_gt_zero: Optional[bool] = None,
              multi_op_key_count: Optional[int] = None,
              nested_ops: Optional[List[OpName]] = None,
              clear_nested_ops: Optional[bool] = None,
              sub_op_names: Optional[List[Tuple[OpName, bool]]] = None,
              error_before_dispatch: Optional[bool] = None,
              expect_request_span: Optional[bool] = None,
              retry_count_gt_zero: Optional[bool] = None) -> None:
        """Reset validator state.

        Args:
            full_reset: If True (default), reset ALL state to defaults first, then apply kwargs.
                       If False, only update provided kwargs (partial update).
                       Automatically set to False when do_not_clear_spans=True.
            do_not_clear_spans: If True, don't clear spans (continuation scenario).
                               When True, automatically sets full_reset=False.
            op_name: Operation name to validate.
            collection_details: Collection details for validation.
            parent_span: Parent span for validation.
            clear_parent_span: If True, explicitly clear parent span.
            validate_error: Whether to validate error status on spans.
            durability: Durability level to validate.
            server_duration_gt_zero: Whether to validate server duration > 0.
            multi_op_key_count: Number of keys in multi-operation.
            nested_ops: List of nested operation names.
            clear_nested_ops: If True, explicitly clear nested ops.
            sub_op_names: List of sub-operation names (for data structures).
            error_before_dispatch: Whether the error occurs before dispatch (affects attr presence).
            expect_request_span: Whether to expect a request span to be present.
            retry_count_gt_zero: Whether to validate that retry count attribute is > 0.
        """
        if do_not_clear_spans is True:
            full_reset = False

        if full_reset:
            self._reset_to_defaults()

        if do_not_clear_spans is not True:
            self._tracer.clear_spans()

        if op_name:
            self._op_name = op_name
        if collection_details:
            self._collection_details = collection_details
        if parent_span:
            self._parent_span = parent_span
        elif clear_parent_span is True:
            self._parent_span = None
        if validate_error is not None:
            self._validate_error = validate_error
        if durability is not None:
            self._durability = durability
        if server_duration_gt_zero is not None:
            self._server_duration_gt_zero = server_duration_gt_zero
        if multi_op_key_count is not None:
            self._multi_op_key_count = multi_op_key_count
        if nested_ops is not None:
            self._nested_ops = nested_ops
        elif clear_nested_ops is True:
            self._nested_ops = None
        if sub_op_names is not None:
            self._sub_op_names = sub_op_names
        if error_before_dispatch is not None:
            self._error_before_dispatch = error_before_dispatch
        if expect_request_span is not None:
            self._expect_request_span = expect_request_span
        if retry_count_gt_zero is not None:
            self._retry_count_gt_zero = retry_count_gt_zero

    def validate_kv_op(self, end_parent: Optional[bool] = None) -> None:
        if end_parent is True:
            self._end_parent_span()
        # For some tests, the InvalidArgumentException can be sometimes raised prior to creating a span, so we
        # let the test indicate whether to expect a request span or not. This is due to the nature of processing
        # options.
        if self._error_before_dispatch is True and self._expect_request_span is False:
            _assert_len(self._tracer._spans, expected=0, what='tracer._spans count expected to be 0')
            return
        span_req = self._validate_tracer_and_get_span_request()
        if self._nested_ops:
            self._validate_nested_span(span_req, self._nested_ops)
        else:
            self._validate_span(span_req)

    def validate_ds_op(self, end_parent: Optional[bool] = None) -> None:
        if end_parent is True:
            self._end_parent_span()
        assert self._tracer._spans is not None, 'tracer._spans is None'
        ctx = _ctx(op_name=self._op_name.value if self._op_name else None,
                   extra=f'tracer_spans={[s.name for s in (self._tracer._spans or [])]}')
        _assert_len(self._tracer._spans, expected=1, what='tracer._spans count', ctx=ctx)
        span = self._tracer.get_span_by_name(self._op_name.value)
        assert span is not None, f'DS op span not found for op={self._op_name.value!r}. {ctx}'
        assert span._parent_span is None, (
            f'DS op span should have no parent, got parent={span._parent_span!r}. {ctx}'
        )
        _assert_len(span._children, expected=len(self._sub_op_names),
                    what='DS span children count', ctx=_ctx(op_name=self._op_name.value, span=span))

        req = ValidateKeyValueSpanRequest(span,
                                          self._op_name,
                                          self._collection_details,
                                          validate_error=self._validate_error,
                                          skip_dispatch_validation=True)
        self._validate_span(req)

        for idx, sub_op in enumerate(self._sub_op_names):
            sub_span = span._children[idx]
            _assert_eq(sub_span.name, sub_op[0].value,
                       what=f'DS sub-span name at index {idx}',
                       ctx=_ctx(op_name=self._op_name.value, span=span))
            req = ValidateKeyValueSpanRequest(sub_span,
                                              sub_op[0],
                                              self._collection_details,
                                              parent_span=span,
                                              validate_error=sub_op[1])
            self._validate_span(req)


class KeyValueNoOpSpanValidatorImpl:
    def __init__(self,
                 tracer: TestTracerType,
                 **kwargs: Unpack[KvResetParams]) -> None:
        self._tracer = tracer
        self._parent_span = kwargs.get('parent_span', None)

    @property
    def is_legacy(self) -> bool:
        return False

    def reset(self, **kwargs: Unpack[KvResetParams]) -> None:
        do_not_clear_spans = kwargs.get('do_not_clear_spans')
        full_reset = kwargs.get('full_reset', True)
        parent_span = kwargs.get('parent_span')
        clear_parent_span = kwargs.get('clear_parent_span')

        if do_not_clear_spans is True:
            full_reset = False

        if full_reset:
            self._parent_span = None

        if do_not_clear_spans is not True:
            self._tracer.clear_spans()

        if parent_span:
            self._parent_span = parent_span
        elif clear_parent_span is True:
            self._parent_span = None

    def validate_kv_op(self, end_parent: Optional[bool] = None) -> None:
        self._validate(end_parent=end_parent)

    def validate_ds_op(self, end_parent: Optional[bool] = None) -> None:
        self._validate(end_parent=end_parent)

    def _validate(self, end_parent: Optional[bool] = None) -> None:
        _assert_isinstance(self._tracer, NoOpTracer, what='tracer type')
        if self._parent_span:
            _assert_isinstance(self._parent_span, NoOpSpan, what='parent_span type')
            _assert_len(self._tracer._spans, expected=1, what='tracer._spans count (with parent_span)')
            if end_parent is True:
                self._parent_span.end()
        else:
            _assert_len(self._tracer._spans, expected=0, what='tracer._spans count (no parent_span)')


class KeyValueThresholdSpanValidatorImpl:
    def __init__(self,
                 tracer: TestTracerType,
                 **kwargs: Unpack[KvResetParams]) -> None:
        self._tracer = tracer
        self._op_name = kwargs.get('op_name', None)
        self._parent_span = kwargs.get('parent_span', None)
        self._multi_op_key_count = kwargs.get('multi_op_key_count', None)
        self._error_before_dispatch = kwargs.get('error_before_dispatch', None)
        self._expect_request_span = kwargs.get('expect_request_span', None)

    @property
    def is_legacy(self) -> bool:
        return False

    def reset(self, **kwargs: Unpack[KvResetParams]) -> None:
        do_not_clear_spans = kwargs.get('do_not_clear_spans')
        full_reset = kwargs.get('full_reset', True)
        op_name = kwargs.get('op_name')
        parent_span = kwargs.get('parent_span')
        clear_parent_span = kwargs.get('clear_parent_span')
        multi_op_key_count = kwargs.get('multi_op_key_count')
        error_before_dispatch = kwargs.get('error_before_dispatch')
        expect_request_span = kwargs.get('expect_request_span')

        if do_not_clear_spans is True:
            full_reset = False

        if full_reset:
            self._op_name = None
            self._parent_span = None
            self._multi_op_key_count = None
            self._error_before_dispatch = None
            self._expect_request_span = None

        if do_not_clear_spans is not True:
            self._tracer.clear_spans()

        if op_name:
            self._op_name = op_name
        if parent_span:
            self._parent_span = parent_span
        elif clear_parent_span is True:
            self._parent_span = None
        if multi_op_key_count is not None:
            self._multi_op_key_count = multi_op_key_count
        if error_before_dispatch is not None:
            self._error_before_dispatch = error_before_dispatch
        if expect_request_span is not None:
            self._expect_request_span = expect_request_span

    def validate_kv_op(self, end_parent: Optional[bool] = None) -> None:
        self._validate_span(end_parent=end_parent)

    def validate_ds_op(self, end_parent: Optional[bool] = None) -> None:
        self._validate_ds_span(end_parent=end_parent)

    def _collect_spans_by_name(self,
                               target_name: str,
                               current_span: TestThresholdLoggingSpan) -> List[TestThresholdLoggingSpan]:
        found_spans = []

        if current_span.name == target_name:
            found_spans.append(current_span)

        for child in current_span.children:
            found_spans.extend(self._collect_spans_by_name(target_name, child))

        return found_spans

    def _get_from_attributes(self,
                             span: TestThresholdLoggingSpan,
                             attribute_name: DispatchAttributeName,
                             default_value: Optional[Union[str, int]] = None) -> Optional[Union[str, int]]:
        return span._test_attributes.get(attribute_name.value, default_value)

    def _end_parent_span(self) -> None:
        if self._parent_span is not None:
            self._parent_span.end()

    def _validate_dispatch_spans(self, span: TestThresholdLoggingSpan) -> ThresholdLoggingSpanSnapshot:
        ctx = _ctx(op_name=self._op_name.value if self._op_name else None, span=span)
        dispatch_spans = self._collect_spans_by_name(OpAttributeName.DispatchSpanName.value, span)

        dispatch_total_time = sum((s._end_time_ns - s._start_time_ns) for s in dispatch_spans)
        dispatch_total_server_duration = sum(self._get_from_attributes(s, DispatchAttributeName.ServerDuration, 0)
                                             for s in dispatch_spans)

        last_dspan = dispatch_spans[-1]
        last_dspan_duration = last_dspan._end_time_ns - dispatch_spans[-1]._start_time_ns
        last_dspan_server_duration = self._get_from_attributes(last_dspan, DispatchAttributeName.ServerDuration, 0)
        last_dspan_local_id = self._get_from_attributes(last_dspan, DispatchAttributeName.LocalId, None)
        last_dspan_operation_id = self._get_from_attributes(last_dspan, DispatchAttributeName.OperationId, None)
        last_dspan_peer_addr = self._get_from_attributes(last_dspan, DispatchAttributeName.PeerAddress, None)
        last_dspan_peer_port = self._get_from_attributes(last_dspan, DispatchAttributeName.PeerPort, None)
        last_remote_socket = None
        if last_dspan_peer_addr is not None or last_dspan_peer_port is not None:
            address = last_dspan_peer_addr or ''
            port = last_dspan_peer_port or ''
            last_remote_socket = f'{address}:{port}'

        last_dspan_server_addr = self._get_from_attributes(last_dspan, DispatchAttributeName.ServerAddress, None)
        last_dspan_server_port = self._get_from_attributes(last_dspan, DispatchAttributeName.ServerPort, None)
        last_local_socket = None
        if last_dspan_server_addr is not None or last_dspan_server_port is not None:
            address = last_dspan_server_addr or ''
            port = last_dspan_server_port or ''
            last_local_socket = f'{address}:{port}'

        _assert_eq(dispatch_total_time, span.total_dispatch_duration_ns,
                   what='total_dispatch_duration_ns', ctx=ctx)
        _assert_eq(dispatch_total_server_duration, span.total_server_duration_ns,
                   what='total_server_duration_ns', ctx=ctx)
        _assert_eq(last_dspan_duration, span.dispatch_duration_ns,
                   what='dispatch_duration_ns', ctx=ctx)
        _assert_eq(last_dspan_server_duration, span.server_duration_ns,
                   what='server_duration_ns', ctx=ctx)
        _assert_eq(last_dspan_local_id, span.local_id, what='local_id', ctx=ctx)
        _assert_eq(last_dspan_operation_id, span.operation_id, what='operation_id', ctx=ctx)
        _assert_eq(last_remote_socket, span.remote_socket, what='remote_socket', ctx=ctx)
        _assert_eq(last_local_socket, span.local_socket, what='local_socket', ctx=ctx)

    def _validate_encoding_spans(self, span: TestThresholdLoggingSpan) -> None:
        ctx = _ctx(op_name=self._op_name.value if self._op_name else None, span=span)
        encoding_spans = self._collect_spans_by_name(OpAttributeName.EncodingSpanName.value, span)
        encoding_total_time = sum((s._end_time_ns - s._start_time_ns) for s in encoding_spans)
        _assert_eq(encoding_total_time, span.total_encode_duration_ns,
                   what='total_encode_duration_ns', ctx=ctx)

    def _maybe_get_initial_span(self) -> Optional[TestThresholdLoggingSpan]:
        _assert_isinstance(self._tracer, ThresholdLoggingTracer, what='tracer type')
        assert self._tracer._spans is not None, 'tracer._spans is None'
        # For some tests, the InvalidArgumentException can be sometimes raised prior to creating a span, so we
        # let the test indicate whether to expect a request span or not. This is due to the nature of processing
        # options.
        if self._error_before_dispatch is True and self._expect_request_span is False:
            _assert_len(self._tracer._spans, expected=0, what='tracer._spans count expected to be 0')
            return

        return _get_span_from_tracer(self._tracer, self._op_name, self._parent_span)

    def _validate_ds_span(self, end_parent: Optional[bool] = None) -> None:
        span = self._maybe_get_initial_span()
        ctx = _ctx(op_name=self._op_name.value if self._op_name else None, span=span)
        assert span is not None, f'Span not found. {ctx}'

        for child_span in span.children:
            self._validate(child_span)

        # don't think this actually matters, but lets end the parent span
        if end_parent is True:
            self._end_parent_span()

    def _validate_span(self, end_parent: Optional[bool] = None) -> None:
        span = self._maybe_get_initial_span()
        # this is okay b/c we will have already raised and assertion error if we expect to have a span
        if span is None:
            return
        self._validate(span)

        # don't think this actually matters, but lets end the parent span
        if end_parent is True:
            self._end_parent_span()

    def _validate(self, span: TestThresholdLoggingSpan) -> None:
        ctx = _ctx(op_name=self._op_name.value if self._op_name else None, span=span)
        total_duration_ns = span._end_time_ns - span._start_time_ns
        _assert_eq(total_duration_ns, span.total_duration_ns, what='total_duration_ns', ctx=ctx)
        if self._error_before_dispatch is not True:
            self._validate_dispatch_spans(span)
        if self._op_name in ENCODING_OPS:
            self._validate_encoding_spans(span)
        # we don't check thresholds for multi ops
        if self._multi_op_key_count is None:
            threshold_us = self._tracer._get_service_type_threshold(span._service_type)
            if total_duration_ns / 1000 > threshold_us:
                assert span.span_snapshot in self._tracer._over_threshold_spans, (
                    f'span expected in over_threshold_spans but not found. {ctx}'
                )
            else:
                assert span.span_snapshot in self._tracer._under_threshold_spans, (
                    f'span expected in under_threshold_spans but not found. {ctx}'
                )


class KeyValueSpanValidator:
    def __init__(self,
                 tracer: TestTracerType,
                 collection_details: CollectionDetails,
                 supports_cluster_labels: bool,
                 op_name: Optional[OpName] = None,
                 parent_span: Optional[TestSpan] = None,
                 validate_error: Optional[bool] = False,
                 durability: Optional[str] = None,
                 server_duration_gt_zero: Optional[bool] = None,
                 multi_op_key_count: Optional[int] = None,
                 nested_ops: Optional[List[OpName]] = None,
                 sub_op_names: Optional[List[Tuple[OpName, bool]]] = None,
                 error_before_dispatch: Optional[bool] = None,
                 expecte_request_span: Optional[bool] = None,
                 retry_count_gt_zero: Optional[bool] = None) -> None:
        if isinstance(tracer, NoOpTracer):
            self._impl = KeyValueNoOpSpanValidatorImpl(tracer)
        elif isinstance(tracer, TestThresholdLoggingTracer):
            self._impl = KeyValueThresholdSpanValidatorImpl(tracer)
        else:
            # we don't need to pass all these in initially...
            self._impl = KeyValueSpanValidatorImpl(tracer,
                                                   collection_details,
                                                   supports_cluster_labels,
                                                   op_name=op_name,
                                                   parent_span=parent_span,
                                                   validate_error=validate_error,
                                                   durability=durability,
                                                   server_duration_gt_zero=server_duration_gt_zero,
                                                   multi_op_key_count=multi_op_key_count,
                                                   nested_ops=nested_ops,
                                                   sub_op_names=sub_op_names,
                                                   error_before_dispatch=error_before_dispatch,
                                                   expect_request_span=expecte_request_span,
                                                   retry_count_gt_zero=retry_count_gt_zero)

    @property
    def is_legacy(self) -> bool:
        return self._impl.is_legacy

    def reset(self, **kwargs: Unpack[KvResetParams]) -> None:
        self._impl.reset(**kwargs)

    def validate_kv_op(self, end_parent: Optional[bool] = None) -> None:
        self._impl.validate_kv_op(end_parent=end_parent)

    def validate_ds_op(self, end_parent: Optional[bool] = None) -> None:
        self._impl.validate_ds_op(end_parent=end_parent)


class HttpSpanValidatorImpl:
    def __init__(self,
                 tracer: TestTracerType,
                 supports_cluster_labels: bool,
                 op_name: Optional[OpName] = None,
                 parent_span: Optional[TestSpan] = None,
                 validate_error: Optional[bool] = False,
                 statement: Optional[str] = None,
                 nested_ops: Optional[List[OpName]] = None,
                 error_before_dispatch: Optional[bool] = None,
                 expect_request_span: Optional[bool] = None,
                 bucket_name: Optional[str] = None,
                 scope_name: Optional[str] = None,
                 collection_name: Optional[str] = None) -> None:
        self._tracer = tracer
        if isinstance(tracer, LegacyTestTracer):
            self._is_legacy = True
            self._span_type = LegacyTestSpan
        else:
            self._is_legacy = False
            self._span_type = TestSpan
        self._supports_cluster_labels = supports_cluster_labels
        self._op_name = op_name
        self._service_type = None
        self._parent_span = parent_span
        self._validate_error = validate_error
        self._statement = statement
        self._nested_ops = nested_ops
        self._error_before_dispatch = error_before_dispatch
        self._expect_request_span = expect_request_span
        self._bucket_name = bucket_name
        self._scope_name = scope_name
        self._collection_name = collection_name

    @property
    def is_legacy(self) -> bool:
        return self._is_legacy

    def reset(self,  # noqa: C901
              full_reset: bool = True,
              do_not_clear_spans: Optional[bool] = None,
              op_name: Optional[OpName] = None,
              parent_span: Optional[TestSpan] = None,
              clear_parent_span: Optional[bool] = None,
              validate_error: Optional[bool] = None,
              statement: Optional[str] = None,
              clear_statement: Optional[bool] = None,
              nested_ops: Optional[List[OpName]] = None,
              clear_nested_ops: Optional[bool] = None,
              error_before_dispatch: Optional[bool] = None,
              expect_request_span: Optional[bool] = None,
              bucket_name: Optional[str] = None,
              scope_name: Optional[str] = None,
              collection_name: Optional[str] = None) -> None:
        """Reset validator state between tests.

        Args:
            full_reset: If True, reset all state fields to defaults before applying
                       any provided parameters. Defaults to True for maximum safety.
                       Automatically set to False when do_not_clear_spans=True.
            do_not_clear_spans: If True, don't clear spans (continuation scenario).
                               When True, automatically sets full_reset=False.
            op_name: New operation name to set.
            parent_span: New parent span to set.
            clear_parent_span: If True, explicitly clear the parent span.
            validate_error: Whether to validate for errors.
            statement: Query/Analytics statement to validate.
            clear_statement: If True, explicitly clear the statement.
            nested_ops: List of nested operations to validate.
            clear_nested_ops: If True, explicitly clear nested operations.
            error_before_dispatch: Whether the error occurs before dispatch (affects attr presence).
            expect_request_span: Whether to expect a request span to be present.
            bucket_name: Bucket name for validation.
            scope_name: Scope name for validation.
            collection_name: Collection name for validation.
        """
        # Automatically disable full_reset when do_not_clear_spans=True (continuation scenario)
        if do_not_clear_spans is True:
            full_reset = False

        if full_reset:
            self._reset_to_defaults()

        if op_name:
            self._op_name = op_name
        self._service_type = self._get_service_type() if self._op_name else None
        if parent_span:
            self._parent_span = parent_span
        elif clear_parent_span is True:
            self._parent_span = None
        if validate_error is not None:
            self._validate_error = validate_error
        if statement is not None:
            self._statement = statement
        elif clear_statement is True:
            self._statement = None
        if nested_ops is not None:
            self._nested_ops = nested_ops
        elif clear_nested_ops is True:
            self._nested_ops = None

        if error_before_dispatch is not None:
            self._error_before_dispatch = error_before_dispatch

        if expect_request_span is not None:
            self._expect_request_span = expect_request_span

        if do_not_clear_spans is not True:
            self._tracer.clear_spans()

        if bucket_name is not None:
            self._bucket_name = bucket_name
        if scope_name is not None:
            self._scope_name = scope_name
        if collection_name is not None:
            self._collection_name = collection_name

    def validate_http_op(self, end_parent: Optional[bool] = None) -> None:
        if end_parent is True:
            self._end_parent_span()
        # For some tests, the InvalidArgumentException can be sometimes raised prior to creating a span, so we
        # let the test indicate whether to expect a request span or not. This is due to the nature of processing
        # options.
        if self._error_before_dispatch is True and self._expect_request_span is False:
            _assert_len(self._tracer._spans, expected=0, what='tracer._spans count expected to be 0')
            return
        span_req = self._validate_tracer_and_get_span_request()
        if self._nested_ops:
            self._validate_nested_span(span_req, self._nested_ops)
        else:
            self._validate_span(span_req)

    def _reset_to_defaults(self) -> None:
        """Reset all state fields to their default values.

        This method does NOT clear spans from the tracer - span clearing
        should be done separately via the reset() method's do_not_clear_spans parameter.
        """
        self._op_name = None
        self._service_type = None
        self._collection_details = None
        self._parent_span = None
        self._validate_error = False
        self._statement = None
        self._nested_ops = None
        self._error_before_dispatch = None
        self._expect_request_span = None
        self._bucket_name = None
        self._scope_name = None
        self._collection_name = None

    def _end_parent_span(self) -> None:
        if self._parent_span is not None:
            if self._is_legacy:
                self._parent_span.finish()
            else:
                self._parent_span.end()

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

    def _validate_base_span(self, req: ValidateHttpSpanRequest) -> None:
        validate_request_span(req.span,
                              self._span_type,
                              req.op_name.value,
                              self._supports_cluster_labels,
                              validate_error=req.validate_error)
        ctx = _ctx(op_name=req.op_name.value, span=req.span)
        _assert_eq(req.span._attributes.get(OpAttributeName.OperationName.value), req.op_name.value,
                   what='OperationName attribute', ctx=ctx)
        _assert_eq(req.span._attributes.get(OpAttributeName.Service.value), self._service_type.value,
                   what='Service attribute', ctx=ctx)
        if req.bucket_name is not None:
            _assert_eq(req.span._attributes.get(OpAttributeName.BucketName.value),
                       req.bucket_name, what='BucketName attribute', ctx=ctx)
        else:
            _assert_not_has(req.span._attributes, OpAttributeName.BucketName.value, ctx=ctx)

        if req.scope_name is not None:
            _assert_eq(req.span._attributes.get(OpAttributeName.ScopeName.value),
                       req.scope_name, what='ScopeName attribute', ctx=ctx)
        else:
            _assert_not_has(req.span._attributes, OpAttributeName.ScopeName.value, ctx=ctx)

        if req.collection_name is not None:
            _assert_eq(req.span._attributes.get(OpAttributeName.CollectionName.value),
                       req.collection_name, what='CollectionName attribute', ctx=ctx)
        else:
            _assert_not_has(req.span._attributes, OpAttributeName.CollectionName.value, ctx=ctx)

        if self._statement is not None:
            _assert_eq(req.span._attributes.get(OpAttributeName.QueryStatement.value), self._statement,
                       what='QueryStatement attribute', ctx=ctx)
        else:
            _assert_not_has(req.span._attributes, OpAttributeName.QueryStatement.value, ctx=ctx)

    def _validate_dispatch_span(self, span: TestSpan, name: str, parent_span: TestSpan) -> None:
        ctx = _ctx(op_name=name, span=span)
        _validate_dispatch_span_common(span, self._span_type, name, self._supports_cluster_labels, parent_span, ctx=ctx)

    def _validate_dispatch_spans(self, req: ValidateHttpSpanRequest) -> None:
        dispatch_spans = [s for s in req.span.children if s.name == OpAttributeName.DispatchSpanName.value]
        ctx = _ctx(op_name=req.op_name.value, span=req.span)
        # we sometimes trigger an error w/o going down to the C++ bindings
        if not req.validate_error:
            _assert_len(dispatch_spans, min_expected=1, what='HTTP dispatch span count', ctx=ctx)
        for dispatch_span in dispatch_spans:
            self._validate_dispatch_span(dispatch_span,
                                         OpAttributeName.DispatchSpanName.value,
                                         req.span)

    def _validate_nested_span(self,
                              req: ValidateHttpSpanRequest,
                              required_nested_ops: List[OpName]) -> None:
        ctx = _ctx(op_name=req.op_name.value, span=req.span,
                   extra=f'required_ops={[o.value for o in required_nested_ops]}')
        self._validate_base_span(req)
        assert req.span.children is not None, f'nested HTTP span has no children list. {ctx}'
        total_child_span_count = len(required_nested_ops)
        # HTTP operations that have nested spans are operations where it is difficult to know the exact number
        # of child spans that will be created; just confirm we have atleast the number of ops specified in
        # required_nested_ops
        actual_child_names = [s.name for s in req.span.children]
        _assert_len(req.span.children, min_expected=total_child_span_count,
                    what='HTTP nested span children count (min)',
                    ctx=_ctx(op_name=req.op_name.value, span=req.span,
                             extra=f'actual_children={actual_child_names}'))
        dispatch_spans: List[TestSpan] = []
        for op in required_nested_ops:
            child_spans = [s for s in req.span.children if s.name == op.value]
            _assert_len(child_spans, min_expected=1,
                        what=f'HTTP child span count for op={op.value!r}', ctx=ctx)
            for child_span in child_spans:
                child_dispatch_spans = [s for s in child_span.children
                                        if s.name == OpAttributeName.DispatchSpanName.value]
                if child_dispatch_spans:
                    dispatch_spans.extend(child_dispatch_spans)
                child_req = ValidateHttpSpanRequest(child_span,
                                                    op,
                                                    parent_span=req.span,
                                                    # core spans shouldn't have errors
                                                    validate_error=False)
                self._validate_span(child_req)

        _assert_len(dispatch_spans, min_expected=total_child_span_count,
                    what='HTTP total dispatch spans across nested ops', ctx=ctx)

    def _validate_span(self, req: ValidateHttpSpanRequest) -> None:
        ctx = _ctx(op_name=req.op_name.value, span=req.span)
        if req.parent_span is not None:
            assert req.span._parent_span == req.parent_span, (
                f'HTTP span parent mismatch: expected={req.parent_span!r} actual={req.span._parent_span!r}. {ctx}'
            )
        self._validate_base_span(req)
        if req.error_before_dispatch is True:
            return
        assert req.span.children is not None, f'HTTP span has no children list. {ctx}'
        _assert_len(req.span.children, min_expected=1, what='HTTP span children count', ctx=ctx)
        if req.skip_dispatch_validation is not True:
            self._validate_dispatch_spans(req)

    def _validate_tracer_and_get_span_request(self) -> ValidateHttpSpanRequest:
        assert self._tracer._spans is not None, 'tracer._spans is None'
        span = _get_span_from_tracer(self._tracer, self._op_name, self._parent_span)
        return ValidateHttpSpanRequest(span,
                                       self._op_name,
                                       parent_span=self._parent_span,
                                       validate_error=self._validate_error,
                                       error_before_dispatch=self._error_before_dispatch,
                                       bucket_name=self._bucket_name,
                                       scope_name=self._scope_name,
                                       collection_name=self._collection_name)


class HttpNoOpSpanValidatorImpl:
    def __init__(self,
                 tracer: TestTracerType,
                 **kwargs: Unpack[HttpResetParams]) -> None:
        self._tracer = tracer
        self._parent_span = kwargs.get('parent_span', None)

    @property
    def is_legacy(self) -> bool:
        return False

    def reset(self, **kwargs: Unpack[HttpResetParams]) -> None:
        do_not_clear_spans = kwargs.get('do_not_clear_spans')
        full_reset = kwargs.get('full_reset', True)
        parent_span = kwargs.get('parent_span')
        clear_parent_span = kwargs.get('clear_parent_span')

        if do_not_clear_spans is True:
            full_reset = False

        if full_reset:
            self._parent_span = None

        if do_not_clear_spans is not True:
            self._tracer.clear_spans()

        if parent_span:
            self._parent_span = parent_span
        elif clear_parent_span is True:
            self._parent_span = None

    def validate_http_op(self, end_parent: Optional[bool] = None) -> None:
        _assert_isinstance(self._tracer, NoOpTracer, what='tracer type')
        if self._parent_span:
            _assert_isinstance(self._parent_span, NoOpSpan, what='parent_span type')
            _assert_len(self._tracer._spans, expected=1, what='tracer._spans count (with parent_span)')
            if end_parent is True:
                self._parent_span.end()
        else:
            _assert_len(self._tracer._spans, expected=0, what='tracer._spans count (no parent_span)')


class HttpThresholdSpanValidatorImpl:
    def __init__(self,
                 tracer: TestTracerType,
                 **kwargs: Unpack[HttpResetParams]) -> None:
        self._tracer = tracer
        self._op_name = kwargs.get('op_name', None)
        self._parent_span = kwargs.get('parent_span', None)
        self._error_before_dispatch = kwargs.get('error_before_dispatch', None)

    @property
    def is_legacy(self) -> bool:
        return False

    def reset(self, **kwargs: Unpack[HttpResetParams]) -> None:
        do_not_clear_spans = kwargs.get('do_not_clear_spans')
        full_reset = kwargs.get('full_reset', True)
        op_name = kwargs.get('op_name')
        parent_span = kwargs.get('parent_span')
        clear_parent_span = kwargs.get('clear_parent_span')
        error_before_dispatch = kwargs.get('error_before_dispatch')

        if do_not_clear_spans is True:
            full_reset = False

        if full_reset:
            self._op_name = None
            self._parent_span = None
            self._error_before_dispatch = None

        if do_not_clear_spans is not True:
            self._tracer.clear_spans()

        if op_name:
            self._op_name = op_name
        if parent_span:
            self._parent_span = parent_span
        elif clear_parent_span is True:
            self._parent_span = None

        if error_before_dispatch is not None:
            self._error_before_dispatch = error_before_dispatch

    def validate_http_op(self, end_parent: Optional[bool] = None) -> None:
        self._validate(end_parent=end_parent)

    def _collect_spans_by_name(self,
                               target_name: str,
                               current_span: TestThresholdLoggingSpan) -> List[TestThresholdLoggingSpan]:
        found_spans = []

        if current_span.name == target_name:
            found_spans.append(current_span)

        for child in current_span.children:
            found_spans.extend(self._collect_spans_by_name(target_name, child))

        return found_spans

    def _get_from_attributes(self,
                             span: TestThresholdLoggingSpan,
                             attribute_name: DispatchAttributeName,
                             default_value: Optional[Union[str, int]] = None) -> Optional[Union[str, int]]:
        return span._test_attributes.get(attribute_name.value, default_value)

    def _end_parent_span(self) -> None:
        if self._parent_span is not None:
            self._parent_span.end()

    def _validate_dispatch_spans(self, span: TestThresholdLoggingSpan) -> ThresholdLoggingSpanSnapshot:
        ctx = _ctx(op_name=self._op_name.value if self._op_name else None, span=span)
        dispatch_spans = self._collect_spans_by_name(OpAttributeName.DispatchSpanName.value, span)

        dispatch_total_time = sum((s._end_time_ns - s._start_time_ns) for s in dispatch_spans)

        last_dspan = dispatch_spans[-1]
        last_dspan_duration = last_dspan._end_time_ns - dispatch_spans[-1]._start_time_ns
        last_dspan_local_id = self._get_from_attributes(last_dspan, DispatchAttributeName.LocalId, None)
        last_dspan_operation_id = self._get_from_attributes(last_dspan, DispatchAttributeName.OperationId, None)
        last_dspan_peer_addr = self._get_from_attributes(last_dspan, DispatchAttributeName.PeerAddress, None)
        last_dspan_peer_port = self._get_from_attributes(last_dspan, DispatchAttributeName.PeerPort, None)
        last_remote_socket = None
        if last_dspan_peer_addr is not None or last_dspan_peer_port is not None:
            address = last_dspan_peer_addr or ''
            port = last_dspan_peer_port or ''
            last_remote_socket = f'{address}:{port}'

        last_dspan_server_addr = self._get_from_attributes(last_dspan, DispatchAttributeName.ServerAddress, None)
        last_dspan_server_port = self._get_from_attributes(last_dspan, DispatchAttributeName.ServerPort, None)
        last_local_socket = None
        if last_dspan_server_addr is not None or last_dspan_server_port is not None:
            address = last_dspan_server_addr or ''
            port = last_dspan_server_port or ''
            last_local_socket = f'{address}:{port}'

        _assert_eq(dispatch_total_time, span.total_dispatch_duration_ns,
                   what='total_dispatch_duration_ns', ctx=ctx)
        _assert_eq(last_dspan_duration, span.dispatch_duration_ns,
                   what='dispatch_duration_ns', ctx=ctx)
        _assert_eq(last_dspan_local_id, span.local_id, what='local_id', ctx=ctx)
        _assert_eq(last_dspan_operation_id, span.operation_id, what='operation_id', ctx=ctx)
        _assert_eq(last_remote_socket, span.remote_socket, what='remote_socket', ctx=ctx)
        _assert_eq(last_local_socket, span.local_socket, what='local_socket', ctx=ctx)

    def _validate(self, end_parent: Optional[bool] = None) -> None:
        _assert_isinstance(self._tracer, ThresholdLoggingTracer, what='tracer type')
        assert self._tracer._spans is not None, 'tracer._spans is None'
        span = _get_span_from_tracer(self._tracer, self._op_name, self._parent_span)
        ctx = _ctx(op_name=self._op_name.value if self._op_name else None, span=span)
        total_duration_ns = span._end_time_ns - span._start_time_ns
        _assert_eq(total_duration_ns, span.total_duration_ns, what='total_duration_ns', ctx=ctx)
        if self._error_before_dispatch is not True:
            self._validate_dispatch_spans(span)

        threshold_us = self._tracer._get_service_type_threshold(span._service_type)
        if total_duration_ns / 1000 > threshold_us:
            assert span.span_snapshot in self._tracer._over_threshold_spans, (
                f'span expected in over_threshold_spans but not found. {ctx}'
            )
        else:
            assert span.span_snapshot in self._tracer._under_threshold_spans, (
                f'span expected in under_threshold_spans but not found. {ctx}'
            )

        # don't think this actually matters, but lets end the parent span
        if end_parent is True:
            self._end_parent_span()


class HttpSpanValidator:
    def __init__(self,
                 tracer: TestTracerType,
                 supports_cluster_labels: bool,
                 op_name: Optional[OpName] = None,
                 parent_span: Optional[TestSpan] = None,
                 validate_error: Optional[bool] = False,
                 statement: Optional[str] = None,
                 nested_ops: Optional[List[OpName]] = None,
                 error_before_dispatch: Optional[bool] = None,
                 expect_request_span: Optional[bool] = None,
                 bucket_name: Optional[str] = None,
                 scope_name: Optional[str] = None,
                 collection_name: Optional[str] = None
                 ) -> None:
        if isinstance(tracer, NoOpTracer):
            self._impl = HttpNoOpSpanValidatorImpl(tracer)
        elif isinstance(tracer, TestThresholdLoggingTracer):
            self._impl = HttpThresholdSpanValidatorImpl(tracer)
        else:
            self._impl = HttpSpanValidatorImpl(tracer,
                                               supports_cluster_labels,
                                               op_name=op_name,
                                               parent_span=parent_span,
                                               validate_error=validate_error,
                                               statement=statement,
                                               nested_ops=nested_ops,
                                               error_before_dispatch=error_before_dispatch,
                                               expect_request_span=expect_request_span,
                                               bucket_name=bucket_name,
                                               scope_name=scope_name,
                                               collection_name=collection_name)

    @property
    def is_legacy(self) -> bool:
        return self._impl.is_legacy

    def reset(self, **kwargs: Unpack[HttpResetParams]) -> None:
        self._impl.reset(**kwargs)

    def validate_http_op(self, end_parent: Optional[bool] = None) -> None:
        self._impl.validate_http_op(end_parent=end_parent)
