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

"""Span-tree assertions for the tracing suites.

Two validators, one per shape the SDK traces: `KeyValueSpanValidator` over the key-value
operations, `HttpSpanValidator` over query, search, analytics, views and the managers.  A suite
reaches them through `TracingEnvironment`.

**Each takes its expectations at the assertion, not before the operation.**  `reset()` clears
the tracer and nothing else; the expectations are arguments to `validate_kv_op` /
`validate_http_op`.  A validator holding them across the operation instead would be a state
machine, and the cost is not the state itself but its un-setting: expectations that outlive one
assertion each need a way to be cleared individually, which is how a `reset` grows a
`full_reset`, a `clear_parent_span` and a flag per knob.  Holding nothing is what makes all of
them unnecessary, and it puts the expectation next to the assertion rather than ten lines above
the operation, which is where a reader looks for it.

**Three paths, chosen by the tracer.**  A recording tracer (current or deprecated) gets the
full tree walk.  A threshold-logging tracer gets its own bookkeeping checked instead: the
suite's question there is whether the SDK's roll-ups of dispatch, server and encoding time
match the child spans they were rolled up from, and whether the span landed on the right side
of the service threshold.  A no-op tracer gets the shortest assertion of the three, that the
SDK never asked it for a span at all.

The deprecated tracer is a fourth *tracer* but not a fourth path: it shares the tree walk with
the current one, minus the two things `CouchbaseSpan` cannot carry, a status and cluster
labels.  Where the tree walk says "not for the deprecated span", that is the reason.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import (Any,
                    Optional,
                    Union)

from couchbase.logic.observability import (CollectionDetails,
                                           DispatchAttributeName,
                                           OpAttributeName,
                                           OpName)
from couchbase.observability.tracing import SpanStatusCode
from tests.observability_ops import (DS_OPS,
                                     DS_REMOVE_OPS,
                                     ENCODING_OPS,
                                     service_for)
from tests.observability_test_impls import (PARENT_SPAN_PREFIX,
                                            InertSpan,
                                            InertTracer,
                                            LegacyRecordingSpan,
                                            LegacyRecordingTracer,
                                            RecordingSpan,
                                            TestSpanProtocol,
                                            TestSpanType,
                                            TestTracerType,
                                            ThresholdRecordingSpan,
                                            ThresholdRecordingTracer)

DISPATCH_SPAN = OpAttributeName.DispatchSpanName.value
ENCODING_SPAN = OpAttributeName.EncodingSpanName.value

# A sub-operation of a datastructure operation, and whether it was expected to fail.  The first
# call against a datastructure that does not exist yet fails, creates the document and retries,
# so a sequence of three with the first marked failed is the normal create-then-mutate shape.
DsSubOps = Sequence[tuple[OpName, bool]]

# Operations the core answers with the first sub-operation to respond, cancelling the rest.  The
# multi form does that once per key.
FIRST_RESPONSE_OPS = frozenset({OpName.GetAnyReplica, OpName.GetAnyReplicaMulti, OpName.LookupInAnyReplica})


def _describe(span: Optional[TestSpanProtocol] = None,
              op_name: Optional[OpName] = None,
              extra: Optional[str] = None) -> str:
    parts = []
    if op_name is not None:
        parts.append(f'op={op_name.value!r}')
    if span is not None:
        parts.append(f'span={span.name!r}')
        parts.append(f'attrs={sorted(span.attributes)}')
        parts.append(f'children={[c.name for c in span.children]}')
    if extra:
        parts.append(extra)
    return ' | '.join(parts)


def _expect(attributes: Mapping[str, Any],
            attribute: Union[OpAttributeName, DispatchAttributeName],
            expected: Any,
            ctx: str) -> None:
    actual = attributes.get(attribute.value)
    assert actual == expected, (
        f'span attribute {attribute.value!r}: expected {expected!r}, recorded {actual!r}. {ctx}'
    )


def _expect_absent(attributes: Mapping[str, Any], attribute: OpAttributeName, ctx: str) -> None:
    assert attribute.value not in attributes, (
        f'span attribute {attribute.value!r} should be absent, recorded '
        f'{attributes[attribute.value]!r}. {ctx}'
    )


def _expect_type(attributes: Mapping[str, Any],
                 attribute: Union[OpAttributeName, DispatchAttributeName],
                 expected_type: type,
                 ctx: str) -> None:
    actual = attributes.get(attribute.value)
    assert isinstance(actual, expected_type), (
        f'span attribute {attribute.value!r}: expected a {expected_type.__name__}, recorded '
        f'{actual!r}. {ctx}'
    )


def _expect_count(spans: Sequence[TestSpanProtocol],
                  expected: int,
                  what: str,
                  ctx: str,
                  at_least: bool = False) -> None:
    if at_least:
        assert len(spans) >= expected, (
            f'{what}: expected at least {expected}, found {len(spans)} ({[s.name for s in spans]}). {ctx}'
        )
    else:
        assert len(spans) == expected, (
            f'{what}: expected {expected}, found {len(spans)} ({[s.name for s in spans]}). {ctx}'
        )


def _children_named(span: TestSpanProtocol, name: str) -> list[TestSpanProtocol]:
    return [child for child in span.children if child.name == name]


def _descendants_named(span: TestSpanProtocol, name: str) -> list[TestSpanProtocol]:
    found: list[TestSpanProtocol] = [span] if span.name == name else []
    for child in span.children:
        found.extend(_descendants_named(child, name))
    return found


def _elapsed(span: TestSpanProtocol) -> int:
    """How long a span ran, in whatever unit its clock is in.

    The threshold-logging span keeps nanoseconds where the recording ones keep a `time_ns`
    stamp, and both arrive here through the same two fields.  A span that has not ended has no
    elapsed time to give, and reading one is a mistake worth naming rather than a `None` to
    propagate.
    """
    start, end = span.start_time, span.end_time
    assert start is not None and end is not None, (
        f'span {span.name!r} has not ended: start_time={start!r} end_time={end!r}'
    )
    return end - start


def _socket(address: Optional[Any], port: Optional[Any]) -> Optional[str]:
    """The address:port string the threshold-logging span builds from a dispatch span.

    `None` only when neither half was recorded, which is how the SDK writes it: either half
    present produces a string, with the missing half empty.
    """
    if address is None and port is None:
        return None
    return f'{address or ""}:{port or ""}'


@dataclass
class _KvExpectations:
    """What one key-value span, at any depth in the tree, should carry."""
    span: TestSpanProtocol
    op_name: OpName
    collection_details: CollectionDetails
    parent_span: Optional[TestSpanProtocol] = None
    validate_error: bool = False
    durability: Optional[str] = None
    server_duration_gt_zero: Optional[bool] = None
    multi_op_key_count: Optional[int] = None
    retry_count_gt_zero: bool = False
    error_before_dispatch: bool = False
    skip_dispatch: bool = False
    # Set only while walking underneath a datastructure operation, which is the one case where
    # how many encoding spans a `mutate_in` should have depends on what enclosed it.
    ds_op_name: Optional[OpName] = None
    ds_sub_ops: Optional[DsSubOps] = None


@dataclass
class _HttpExpectations:
    """What one http span, at any depth in the tree, should carry."""
    span: TestSpanProtocol
    op_name: OpName
    parent_span: Optional[TestSpanProtocol] = None
    validate_error: bool = False
    error_before_dispatch: bool = False
    statement: Optional[str] = None
    bucket_name: Optional[str] = None
    scope_name: Optional[str] = None
    collection_name: Optional[str] = None
    # Set for a span the C++ core created underneath an operation, rather than one the SDK
    # created from the caller's arguments.  See `_validate_base_span` for what it turns off.
    core_created: bool = False


class _SpanTreeValidator:
    """What the key-value and http validators share: the tracer, and how to read a span tree."""

    def __init__(self, tracer: TestTracerType, supports_cluster_labels: bool) -> None:
        self._tracer = tracer
        self._supports_cluster_labels = supports_cluster_labels
        self._is_legacy = isinstance(tracer, LegacyRecordingTracer)
        self._is_inert = isinstance(tracer, InertTracer)
        self._is_threshold = isinstance(tracer, ThresholdRecordingTracer)
        if self._is_legacy:
            self._span_type: type = LegacyRecordingSpan
        elif self._is_inert:
            self._span_type = InertSpan
        elif self._is_threshold:
            self._span_type = ThresholdRecordingSpan
        else:
            self._span_type = RecordingSpan

    @property
    def is_legacy(self) -> bool:
        return self._is_legacy

    def reset(self) -> None:
        """Drop every span the tracer holds, so the next assertion is about one operation."""
        self._tracer.clear_spans()

    def new_parent_span(self, op_name: OpName) -> TestSpanType:
        """Clear the tracer and open a parent span for the next operation to hang under.

        The name is not cosmetic.  A threshold-logging span asks, as it ends, whether it should
        be judged against a service threshold, and by then the only thing distinguishing a span
        the test opened from an operation the SDK ran is the name, so every parent gets the
        prefix the test span looks for.

        The concrete union rather than `TestSpanProtocol`, and that is the one place the distinction
        matters.  What comes back is handed straight to an SDK option, which types the parameter
        as the tracing protocols; the read-only surface a validator uses satisfies none of them,
        while all four concrete spans do.
        """
        self.reset()
        return self._tracer.request_span(f'{PARENT_SPAN_PREFIX}{op_name.value}_span')

    def _end_parent_span(self, parent_span: Optional[TestSpanProtocol]) -> None:
        if parent_span is None:
            return
        # `CouchbaseSpan` calls it `finish`; every span since calls it `end`.
        if isinstance(parent_span, LegacyRecordingSpan):
            parent_span.finish()
        else:
            assert isinstance(parent_span, (RecordingSpan, InertSpan, ThresholdRecordingSpan))
            parent_span.end()

    def _root_span(self, op_name: OpName, parent_span: Optional[TestSpanProtocol]) -> TestSpanProtocol:
        """The operation's own span, whether it sits on the tracer or under a parent."""
        roots = self._tracer.spans
        ctx = _describe(op_name=op_name, extra=f'tracer_roots={[s.name for s in roots]}')
        _expect_count(roots, 1, 'root spans on the tracer', ctx)
        if parent_span is None:
            span = self._tracer.get_span_by_name(op_name.value)
            assert span is not None, f'no span named {op_name.value!r} on the tracer. {ctx}'
            return span

        assert roots[0] is parent_span, (
            f'the only root span should be the parent the test opened, found {roots[0].name!r}. {ctx}'
        )
        _expect_count(parent_span.children, 1, 'children of the parent span', ctx, at_least=True)
        span = parent_span.children[0]
        assert span.parent_span is parent_span, (
            f'span {span.name!r} does not point back at the parent it was created under. {ctx}'
        )
        return span

    def _assert_no_request_span(self, parent_span: Optional[TestSpanProtocol]) -> None:
        """The operation failed so early that no span was ever created.

        A parent the test opened is still there, so what is asserted is that nothing was added
        to it rather than that the tracer is empty.
        """
        roots = self._tracer.spans
        ctx = _describe(extra=f'tracer_roots={[s.name for s in roots]}')
        if parent_span is None:
            _expect_count(roots, 0, 'root spans on the tracer', ctx)
        else:
            _expect_count(roots, 1, 'root spans on the tracer', ctx)
            _expect_count(parent_span.children, 0, 'children of the parent span', ctx)

    def _validate_inert(self, parent_span: Optional[TestSpanProtocol]) -> None:
        """The SDK never asked the no-op tracer for a span.

        A parent the test opened is the only thing that can be there, because the test opened it
        directly rather than the SDK creating it.
        """
        roots = self._tracer.spans
        ctx = _describe(extra=f'tracer_roots={len(roots)}')
        _expect_count(roots, 1 if parent_span is not None else 0, 'root spans on the no-op tracer', ctx)
        if parent_span is not None:
            assert isinstance(parent_span, InertSpan), (
                f'the no-op tracer handed back a {type(parent_span).__name__}. {ctx}'
            )
            _expect_count(parent_span.children, 0, 'children of the parent span', ctx)

    def _validate_request_span(self, span: TestSpanProtocol, name: str, validate_error: bool = False) -> None:
        """The attributes every span carries, whatever it is a span of."""
        ctx = _describe(span=span, extra=f'expected_name={name!r}')
        assert isinstance(span, self._span_type), (
            f'expected a {self._span_type.__name__}, found a {type(span).__name__}. {ctx}'
        )
        assert span.name == name, f'span name: expected {name!r}, found {span.name!r}. {ctx}'
        _expect(span.attributes, OpAttributeName.SystemName, 'couchbase', ctx)
        assert span.start_time and span.start_time > 0, f'span start_time={span.start_time!r}. {ctx}'
        assert span.end_time and span.end_time > 0, f'span end_time={span.end_time!r}. {ctx}'
        assert span.end_time > span.start_time, (
            f'span end_time={span.end_time} should be after start_time={span.start_time}. {ctx}'
        )
        if self._is_legacy:
            # `CouchbaseSpan` has no `set_status`, and the deprecated path never carried the
            # cluster labels, so neither is a thing the SDK could have written here.
            return
        if self._supports_cluster_labels:
            _expect_type(span.attributes, OpAttributeName.ClusterName, str, ctx)
            _expect_type(span.attributes, OpAttributeName.ClusterUUID, str, ctx)
        expected_status = SpanStatusCode.ERROR if validate_error else SpanStatusCode.UNSET
        assert span.status == expected_status, (
            f'span status: expected {expected_status}, found {span.status}. {ctx}'
        )

    def _validate_dispatch_span(self, span: TestSpanProtocol, parent_span: TestSpanProtocol) -> None:
        """The attributes a dispatch span carries whichever service dispatched it."""
        ctx = _describe(span=span)
        self._validate_request_span(span, DISPATCH_SPAN)
        _expect(span.attributes, DispatchAttributeName.NetworkTransport, 'tcp', ctx)
        _expect_type(span.attributes, DispatchAttributeName.ServerAddress, str, ctx)
        _expect_type(span.attributes, DispatchAttributeName.ServerPort, int, ctx)
        _expect_type(span.attributes, DispatchAttributeName.PeerAddress, str, ctx)
        _expect_type(span.attributes, DispatchAttributeName.PeerPort, int, ctx)
        assert span.parent_span is parent_span, (
            f'dispatch span does not point back at the operation that dispatched it. {ctx}'
        )

    @property
    def _threshold_tracer(self) -> ThresholdRecordingTracer:
        """The tracer, for the one path that is only reachable when it is the threshold one."""
        assert isinstance(self._tracer, ThresholdRecordingTracer)
        return self._tracer

    @staticmethod
    def _threshold_span(span: TestSpanProtocol) -> ThresholdRecordingSpan:
        """The span, for the assertions that read the SDK's own roll-ups off it."""
        assert isinstance(span, ThresholdRecordingSpan), (
            f'expected a threshold-logging span, found a {type(span).__name__}'
        )
        return span

    def _threshold_dispatch_totals(self, span: ThresholdRecordingSpan, check_server_duration: bool) -> None:
        """The SDK's dispatch roll-ups against the dispatch spans they were rolled up from.

        Every field here is written by the shipped span, not by the test implementation, which is
        the whole
        point: the arithmetic under test is the arithmetic that ships.
        """
        ctx = _describe(span=span)
        dispatch_spans = _descendants_named(span, DISPATCH_SPAN)
        _expect_count(dispatch_spans, 1, 'dispatch spans under the operation', ctx, at_least=True)

        total_dispatch = sum(_elapsed(d) for d in dispatch_spans)
        assert total_dispatch == span._total_dispatch_duration_ns, (
            f'total dispatch duration: the dispatch spans measured {total_dispatch}ns, the '
            f'operation span rolled up {span._total_dispatch_duration_ns}ns. {ctx}'
        )

        # The last dispatch is the one that succeeded, so it is the one the span reports
        # singly; the retries before it only count towards the totals.
        last = dispatch_spans[-1]
        last_duration = _elapsed(last)
        assert last_duration == span._dispatch_duration_ns, (
            f'dispatch duration: the last dispatch span measured {last_duration}ns, the '
            f'operation span reported {span._dispatch_duration_ns}ns. {ctx}'
        )
        assert last.attributes.get(DispatchAttributeName.LocalId.value) == span._local_id, (
            f'local_id does not match the last dispatch span. {ctx}'
        )
        assert last.attributes.get(DispatchAttributeName.OperationId.value) == span._operation_id, (
            f'operation_id does not match the last dispatch span. {ctx}'
        )
        remote = _socket(last.attributes.get(DispatchAttributeName.PeerAddress.value),
                         last.attributes.get(DispatchAttributeName.PeerPort.value))
        assert remote == span.remote_socket, (
            f'remote_socket: expected {remote!r}, found {span.remote_socket!r}. {ctx}'
        )
        local = _socket(last.attributes.get(DispatchAttributeName.ServerAddress.value),
                        last.attributes.get(DispatchAttributeName.ServerPort.value))
        assert local == span.local_socket, (
            f'local_socket: expected {local!r}, found {span.local_socket!r}. {ctx}'
        )

        if not check_server_duration:
            return
        total_server = sum(d.attributes.get(DispatchAttributeName.ServerDuration.value, 0) for d in dispatch_spans)
        assert total_server == span._total_server_duration_ns, (
            f'total server duration: the dispatch spans reported {total_server}ns, the '
            f'operation span rolled up {span._total_server_duration_ns}ns. {ctx}'
        )
        last_server = last.attributes.get(DispatchAttributeName.ServerDuration.value, 0)
        assert last_server == span._server_duration_ns, (
            f'server duration: the last dispatch span reported {last_server}ns, the operation '
            f'span reported {span._server_duration_ns}ns. {ctx}'
        )

    def _validate_threshold_verdict(self, span: ThresholdRecordingSpan) -> None:
        """The span landed on exactly one side of its service's threshold."""
        ctx = _describe(span=span)
        total_duration = _elapsed(span)
        assert total_duration == span._total_duration_ns, (
            f'total duration: measured {total_duration}ns, the span reported '
            f'{span._total_duration_ns}ns. {ctx}'
        )
        service_type = span._service_type
        assert service_type is not None, f'the span carries no service to judge it against. {ctx}'
        tracer = self._threshold_tracer
        threshold_us = tracer.service_threshold_us(service_type)
        if total_duration / 1000 > threshold_us:
            assert span.snapshot in tracer.over_threshold_spans, (
                f'the span took {total_duration / 1000}us against a {threshold_us}us threshold '
                f'but is not in over_threshold_spans. {ctx}'
            )
        else:
            assert span.snapshot in tracer.under_threshold_spans, (
                f'the span took {total_duration / 1000}us against a {threshold_us}us threshold '
                f'but is not in under_threshold_spans. {ctx}'
            )


class KeyValueSpanValidator(_SpanTreeValidator):
    """Asserts on the span tree one key-value operation produced.

    Constructed through `TracingEnvironment.kv_span_validator(cb_env)`.
    """

    def __init__(self,
                 tracer: TestTracerType,
                 collection_details: CollectionDetails,
                 supports_cluster_labels: bool) -> None:
        super().__init__(tracer, supports_cluster_labels)
        self._collection_details = collection_details

    def validate_kv_op(self,
                       op_name: OpName,
                       parent_span: Optional[TestSpanProtocol] = None,
                       validate_error: bool = False,
                       durability: Optional[str] = None,
                       server_duration_gt_zero: Optional[bool] = None,
                       multi_op_key_count: Optional[int] = None,
                       nested_ops: Optional[Sequence[OpName]] = None,
                       retry_count_gt_zero: bool = False,
                       error_before_dispatch: bool = False,
                       expect_request_span: bool = True) -> None:
        """One key-value operation's span, and everything underneath it.

        `nested_ops` names the operations the SDK runs underneath this one and gives each its
        own span: a replica read is a `get` plus a `get_replica`, and each of those is a full
        operation span with its own dispatch.  `multi_op_key_count` says the operation covered
        that many keys, so the counts underneath scale with it.  `server_duration_gt_zero=False`
        says at least one dispatch reported a server duration but not necessarily all of them,
        which is what a replica read looks like when one replica answers instantly.
        """
        self._end_parent_span(parent_span)
        if self._is_inert:
            self._validate_inert(parent_span)
            return
        if not expect_request_span:
            self._assert_no_request_span(parent_span)
            return

        expectations = _KvExpectations(self._root_span(op_name, parent_span),
                                       op_name,
                                       self._collection_details,
                                       parent_span=parent_span,
                                       validate_error=validate_error,
                                       durability=durability,
                                       server_duration_gt_zero=server_duration_gt_zero,
                                       multi_op_key_count=multi_op_key_count,
                                       retry_count_gt_zero=retry_count_gt_zero,
                                       error_before_dispatch=error_before_dispatch)
        if self._is_threshold:
            self._validate_threshold_kv_span(expectations)
        elif nested_ops:
            self._validate_nested(expectations, nested_ops)
        else:
            self._validate_span(expectations)

    def validate_ds_op(self,
                       ds_op_name: OpName,
                       sub_ops: DsSubOps,
                       parent_span: Optional[TestSpanProtocol] = None) -> None:
        """A datastructure operation's span, and the key-value sub-operations under it.

        `sub_ops` is the sequence of sub-operations in the order the SDK ran them, each paired
        with whether it was expected to fail.  It is asserted as a sequence: a datastructure
        operation against a document that does not exist yet fails, inserts and retries, and
        that three-step shape is the thing worth pinning.
        """
        self._end_parent_span(parent_span)
        if self._is_inert:
            self._validate_inert(parent_span)
            return

        span = self._root_span(ds_op_name, parent_span)
        ctx = _describe(span=span, op_name=ds_op_name)

        if self._is_threshold:
            # The datastructure span itself never reaches the wire, so it has no dispatch of its
            # own to roll up; each sub-operation underneath does.
            for child in span.children:
                self._validate_threshold_kv_span(_KvExpectations(child, ds_op_name, self._collection_details))
            return

        assert span.parent_span is parent_span, (
            f'datastructure span parent mismatch. {ctx}'
        )
        _expect_count(span.children, len(sub_ops), 'sub-operation spans', ctx)
        # The datastructure operation failed exactly when its last sub-operation did.  An
        # earlier one failing is normal: the create-then-retry sequence starts with a failure
        # and ends having succeeded.  Derived rather than declared, so a test cannot state the
        # sequence and the outcome inconsistently.
        self._validate_span(_KvExpectations(span,
                                            ds_op_name,
                                            self._collection_details,
                                            parent_span=parent_span,
                                            validate_error=sub_ops[-1][1],
                                            skip_dispatch=True))

        recorded = [child.name for child in span.children]
        expected = [sub_op.value for sub_op, _ in sub_ops]
        assert recorded == expected, (
            f'datastructure sub-operations: expected {expected}, found {recorded}. {ctx}'
        )
        for child, (sub_op, errored) in zip(span.children, sub_ops):
            self._validate_span(_KvExpectations(child,
                                                sub_op,
                                                self._collection_details,
                                                parent_span=span,
                                                validate_error=errored,
                                                ds_op_name=ds_op_name,
                                                ds_sub_ops=sub_ops))

    def _validate_base_span(self, expectations: _KvExpectations) -> None:
        span, op_name = expectations.span, expectations.op_name
        self._validate_request_span(span, op_name.value, validate_error=expectations.validate_error)
        ctx = _describe(span=span, op_name=op_name)
        details = expectations.collection_details
        _expect(span.attributes, OpAttributeName.OperationName, op_name.value, ctx)
        _expect(span.attributes, OpAttributeName.Service, 'kv', ctx)
        _expect(span.attributes, OpAttributeName.BucketName, details['bucket_name'], ctx)
        _expect(span.attributes, OpAttributeName.ScopeName, details['scope_name'], ctx)
        _expect(span.attributes, OpAttributeName.CollectionName, details['collection_name'], ctx)
        if expectations.durability is not None:
            _expect(span.attributes, OpAttributeName.DurabilityLevel, expectations.durability, ctx)
        if self._is_legacy:
            # The deprecated path never carried a retry count.
            return
        _expect_type(span.attributes, OpAttributeName.RetryCount, int, ctx)
        if expectations.retry_count_gt_zero:
            retries = span.attributes[OpAttributeName.RetryCount.value]
            assert retries > 0, f'expected the operation to have been retried, RetryCount={retries}. {ctx}'

    def _validate_span(self, expectations: _KvExpectations) -> None:
        span = expectations.span
        ctx = _describe(span=span, op_name=expectations.op_name)
        if expectations.parent_span is not None:
            assert span.parent_span is expectations.parent_span, (
                f'span does not point back at its expected parent. {ctx}'
            )
        self._validate_base_span(expectations)
        if expectations.error_before_dispatch:
            # Nothing reached the wire, so there is no dispatch or encoding span to look at.
            return
        _expect_count(span.children, 1, 'children of the operation span', ctx, at_least=True)
        if not expectations.skip_dispatch:
            self._validate_dispatch_spans(expectations)
        if not self._is_legacy:
            self._validate_encoding_spans(expectations)

    def _validate_dispatch_spans(self, expectations: _KvExpectations) -> None:
        span = expectations.span
        ctx = _describe(span=span, op_name=expectations.op_name)
        dispatch_spans = _children_named(span, DISPATCH_SPAN)
        if not expectations.validate_error:
            # An operation that failed may have failed before dispatching at all; one that
            # succeeded dispatched at least once per key.
            _expect_count(dispatch_spans, expectations.multi_op_key_count or 1,
                          'dispatch spans', ctx, at_least=True)
        for dispatch_span in dispatch_spans:
            self._validate_dispatch_span(dispatch_span, span)
            _expect_type(dispatch_span.attributes, DispatchAttributeName.LocalId, str, ctx)
            _expect_type(dispatch_span.attributes, DispatchAttributeName.OperationId, str, ctx)
            _expect_type(dispatch_span.attributes, DispatchAttributeName.ServerDuration, int, ctx)
            if expectations.server_duration_gt_zero:
                duration = dispatch_span.attributes[DispatchAttributeName.ServerDuration.value]
                assert duration > 0, f'dispatch ServerDuration={duration} should be above zero. {ctx}'

    def _expected_encoding_span_count(self, expectations: _KvExpectations) -> tuple[int, bool]:
        """How many encoding spans a mutation should have, and whether that is exact.

        Only `mutate_in` is ambiguous, and only because the answer depends on the specs it was
        given: a remove spec encodes nothing, a retried subdocument operation reuses the request
        it already encoded, and a `mutate_in` a test wrote by hand can carry any mix.  Where the
        count cannot be derived it is a floor rather than an equality.
        """
        count = expectations.multi_op_key_count or 1
        if expectations.op_name is not OpName.MutateIn:
            return count, True
        ds_op_name = expectations.ds_op_name
        if ds_op_name in DS_REMOVE_OPS:
            return 0, True
        if ds_op_name in DS_OPS:
            subdoc_sub_ops = [op for op, _ in (expectations.ds_sub_ops or [])
                              if op in (OpName.MutateIn, OpName.LookupIn)]
            if expectations.validate_error:
                return count, False
            # More than one subdocument sub-operation means this one is the retry, which reuses
            # the request the first attempt already encoded.
            return (0, True) if len(subdoc_sub_ops) > 1 else (count, True)
        # A `mutate_in` the test wrote itself: one encoding span per spec that encodes, and the
        # test is not asked to count its own specs.
        return count, False

    def _validate_encoding_spans(self, expectations: _KvExpectations) -> None:
        if expectations.op_name not in ENCODING_OPS:
            return
        span = expectations.span
        ctx = _describe(span=span, op_name=expectations.op_name)
        encoding_spans = _children_named(span, ENCODING_SPAN)
        count, exact = self._expected_encoding_span_count(expectations)
        _expect_count(encoding_spans, count, 'encoding spans', ctx, at_least=not exact)
        for idx, encoding_span in enumerate(encoding_spans[:count]):
            assert span.children[idx] is encoding_span, (
                f'encoding span {idx} is not child {idx}; encoding happens before dispatch, so '
                f'the encoding spans should come first. {ctx}'
            )
            assert encoding_span.parent_span is span, (
                f'encoding span {idx} does not point back at the operation that encoded. {ctx}'
            )
            self._validate_request_span(encoding_span, ENCODING_SPAN)

    def _validate_nested(self, expectations: _KvExpectations, nested_ops: Sequence[OpName]) -> None:
        """An operation whose work the SDK does through other operations, each with its own span."""
        span = expectations.span
        ctx = _describe(span=span, op_name=expectations.op_name,
                        extra=f'nested_ops={[op.value for op in nested_ops]}')
        self._validate_base_span(expectations)
        per_op_count = expectations.multi_op_key_count or 1
        _expect_count(span.children, len(nested_ops) * per_op_count,
                      'children of the nested operation span', ctx, at_least=True)

        # PYCBC-1971: when the first sub-operation answers, the SDK reads the span tree while the
        # others are still in flight, and replays them as spans with no retry count and no
        # dispatch.  Until that is fixed, those are let through, and what is asserted is that at
        # least one sub-operation, the one that answered, is complete.
        first_response = expectations.op_name in FIRST_RESPONSE_OPS
        dispatch_spans: list[TestSpanProtocol] = []
        for nested_op in nested_ops:
            child_spans = _children_named(span, nested_op.value)
            _expect_count(child_spans, per_op_count, f'spans for nested op {nested_op.value!r}',
                          ctx, at_least=True)
            for child_span in child_spans:
                if first_response and not self._is_complete_sub_op(child_span):
                    continue
                dispatch_spans.extend(_children_named(child_span, DISPATCH_SPAN))
                self._validate_span(_KvExpectations(child_span,
                                                    nested_op,
                                                    expectations.collection_details,
                                                    parent_span=span,
                                                    # The failure is the outer operation's; the
                                                    # nested ones each either worked or were not
                                                    # the one that failed.
                                                    validate_error=False,
                                                    durability=expectations.durability,
                                                    server_duration_gt_zero=expectations.server_duration_gt_zero))

        _expect_count(dispatch_spans, per_op_count if first_response else len(nested_ops) * per_op_count,
                      'dispatch spans across the nested operations', ctx, at_least=True)
        if expectations.server_duration_gt_zero is False and not first_response:
            # A replica read can be answered out of a local cache with no measurable server
            # time, so the assertion is that the operation reached a server at all.  A first
            # response operation leaves one complete sub-operation per key to look at, and that
            # one answering in under a microsecond is not a failure.
            measured = [d for d in dispatch_spans
                        if d.attributes[DispatchAttributeName.ServerDuration.value] > 0]
            _expect_count(measured, 1, 'dispatch spans reporting a server duration', ctx, at_least=True)

    def _is_complete_sub_op(self, span: TestSpanProtocol) -> bool:
        if not span.end_time or not _children_named(span, DISPATCH_SPAN):
            return False
        return self._is_legacy or OpAttributeName.RetryCount.value in span.attributes

    def _validate_threshold_kv_span(self, expectations: _KvExpectations) -> None:
        span = self._threshold_span(expectations.span)
        if not expectations.error_before_dispatch:
            if expectations.multi_op_key_count is not None:
                # A multi-operation short-circuits per-key dispatch spans as a measured
                # optimisation, so what is left to check is that the roll-ups were still
                # populated rather than what they add up to.
                self._validate_threshold_multi_op(span)
            else:
                self._threshold_dispatch_totals(span, check_server_duration=True)
        if expectations.op_name in ENCODING_OPS:
            ctx = _describe(span=span, op_name=expectations.op_name)
            encoding_spans = _descendants_named(span, ENCODING_SPAN)
            total_encode = sum(_elapsed(e) for e in encoding_spans)
            assert total_encode == span._total_encode_duration_ns, (
                f'total encode duration: the encoding spans measured {total_encode}ns, the '
                f'operation span rolled up {span._total_encode_duration_ns}ns. {ctx}'
            )
        if expectations.multi_op_key_count is None:
            self._validate_threshold_verdict(span)

    def _validate_threshold_multi_op(self, span: ThresholdRecordingSpan) -> None:
        ctx = _describe(span=span)
        for field_name, expected_type in (('_total_dispatch_duration_ns', int),
                                          ('_total_server_duration_ns', int),
                                          ('_dispatch_duration_ns', int),
                                          ('_server_duration_ns', int),
                                          ('_local_id', str),
                                          ('_operation_id', str)):
            value = getattr(span, field_name)
            assert isinstance(value, expected_type), (
                f'{field_name}: expected a {expected_type.__name__}, found {value!r}. {ctx}'
            )
        assert isinstance(span.remote_socket, str), f'remote_socket={span.remote_socket!r}. {ctx}'
        assert isinstance(span.local_socket, str), f'local_socket={span.local_socket!r}. {ctx}'


class HttpSpanValidator(_SpanTreeValidator):
    """Asserts on the span tree one http operation produced.

    Constructed through `TracingEnvironment.http_span_validator(cb_env)`.

    Where the key-value validator always expects the same bucket, scope and collection, an http
    operation carries only as much of that triple as applies to it, so each part is stated at
    the assertion and any part not stated is asserted absent.
    """

    def validate_http_op(self,
                         op_name: OpName,
                         parent_span: Optional[TestSpanProtocol] = None,
                         validate_error: bool = False,
                         statement: Optional[str] = None,
                         nested_ops: Optional[Sequence[OpName]] = None,
                         error_before_dispatch: bool = False,
                         expect_request_span: bool = True,
                         bucket_name: Optional[str] = None,
                         scope_name: Optional[str] = None,
                         collection_name: Optional[str] = None) -> None:
        self._end_parent_span(parent_span)
        if self._is_inert:
            self._validate_inert(parent_span)
            return
        if not expect_request_span:
            self._assert_no_request_span(parent_span)
            return

        expectations = _HttpExpectations(self._root_span(op_name, parent_span),
                                         op_name,
                                         parent_span=parent_span,
                                         validate_error=validate_error,
                                         error_before_dispatch=error_before_dispatch,
                                         statement=statement,
                                         bucket_name=bucket_name,
                                         scope_name=scope_name,
                                         collection_name=collection_name)
        if self._is_threshold:
            self._validate_threshold_http_span(expectations)
        elif nested_ops:
            self._validate_nested(expectations, nested_ops)
        else:
            self._validate_span(expectations)

    def _validate_base_span(self, expectations: _HttpExpectations) -> None:
        span, op_name = expectations.span, expectations.op_name
        self._validate_request_span(span, op_name.value, validate_error=expectations.validate_error)
        ctx = _describe(span=span, op_name=op_name)
        _expect(span.attributes, OpAttributeName.Service, service_for(op_name).value, ctx)
        if expectations.core_created:
            # Two assertions do not apply to a span the core opened underneath an operation.
            # Its bucket, scope and collection come from the core's own context rather than
            # from the arguments the caller passed, so the outer operation's expectations say
            # nothing about them.  And its operation name is only there under a current tracer:
            # the deprecated path does not carry it down to a core-created span (PYCBC-1919),
            # which is a gap in that path rather than something for this assertion to encode.
            return
        _expect(span.attributes, OpAttributeName.OperationName, op_name.value, ctx)
        for attribute, expected in ((OpAttributeName.BucketName, expectations.bucket_name),
                                    (OpAttributeName.ScopeName, expectations.scope_name),
                                    (OpAttributeName.CollectionName, expectations.collection_name),
                                    (OpAttributeName.QueryStatement, expectations.statement)):
            if expected is None:
                _expect_absent(span.attributes, attribute, ctx)
            else:
                _expect(span.attributes, attribute, expected, ctx)

    def _validate_span(self, expectations: _HttpExpectations) -> None:
        span = expectations.span
        ctx = _describe(span=span, op_name=expectations.op_name)
        if expectations.parent_span is not None:
            assert span.parent_span is expectations.parent_span, (
                f'span does not point back at its expected parent. {ctx}'
            )
        self._validate_base_span(expectations)
        if expectations.error_before_dispatch:
            return
        _expect_count(span.children, 1, 'children of the operation span', ctx, at_least=True)
        dispatch_spans = _children_named(span, DISPATCH_SPAN)
        if not expectations.validate_error:
            _expect_count(dispatch_spans, 1, 'dispatch spans', ctx, at_least=True)
        for dispatch_span in dispatch_spans:
            self._validate_dispatch_span(dispatch_span, span)

    def _validate_nested(self, expectations: _HttpExpectations, nested_ops: Sequence[OpName]) -> None:
        """An http operation the SDK carries out through others, each with its own span.

        Unlike the key-value case the child count is not knowable: a manager operation may poll,
        so the assertion is that each named operation happened at least once.
        """
        span = expectations.span
        ctx = _describe(span=span, op_name=expectations.op_name,
                        extra=f'nested_ops={[op.value for op in nested_ops]}')
        self._validate_base_span(expectations)
        _expect_count(span.children, len(nested_ops), 'children of the nested operation span',
                      ctx, at_least=True)

        dispatch_spans: list[TestSpanProtocol] = []
        for nested_op in nested_ops:
            child_spans = _children_named(span, nested_op.value)
            _expect_count(child_spans, 1, f'spans for nested op {nested_op.value!r}', ctx, at_least=True)
            for child_span in child_spans:
                dispatch_spans.extend(_children_named(child_span, DISPATCH_SPAN))
                self._validate_span(_HttpExpectations(child_span,
                                                      nested_op,
                                                      parent_span=span,
                                                      core_created=True))

        _expect_count(dispatch_spans, len(nested_ops),
                      'dispatch spans across the nested operations', ctx, at_least=True)

    def _validate_threshold_http_span(self, expectations: _HttpExpectations) -> None:
        span = self._threshold_span(expectations.span)
        if not expectations.error_before_dispatch:
            # An http dispatch reports no server duration, so there is nothing to roll up.
            self._threshold_dispatch_totals(span, check_server_duration=False)
        self._validate_threshold_verdict(span)


SpanValidator = Union[KeyValueSpanValidator, HttpSpanValidator]
