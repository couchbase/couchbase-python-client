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

"""The test tree's observability implementations: reporters for the unit suites, meters and
tracers for the observability suites.

Implementations rather than stand-ins, and the distinction is the reason for the name.  Nearly
every class here derives from a shipped SDK base, and the two `ThresholdRecording*` ones derive
from the SDK's own threshold-logging tracer and span, so what a suite asserts on is the
bookkeeping that ships rather than a replacement for it.  Where a mere look-alike would have
done, the class docstring says why it would not have.

**Reporters.** `LoggingMeter` and `ThresholdLoggingTracer` each start a reporter thread in
`__init__` that wakes on an emit interval and writes to a logger.  A unit test wants neither
the delay nor the output, so it stops the real reporter and swaps in one of the reporters here.

**Meters.** The metrics suites configure a cluster with a meter of their own and then read back
what the SDK recorded into it.  `RecordingMeter` keeps everything; `InertMeter` is the same
thing wearing `NoOpMeter`, so that a suite can tell the difference between the SDK recording
nothing and the SDK never being asked.  `LegacyRecordingMeter` is the third: the same idea
built on the *deprecated* `couchbase.metrics` bases, so that the canary suite can ask whether a
meter written against them is still driven at all.

**Tracers and spans.**  The same idea one level up: the tracing suites configure a cluster with
a tracer of their own and then read back the span tree the SDK built in it.  There are four,
because a tracer is the one observability object with four genuinely different implementations
to prove out, not two: `RecordingTracer` on the current `RequestTracer` base, `InertTracer`
wearing `NoOpTracer`, `ThresholdRecordingTracer` deriving from the SDK's own threshold-logging
tracer so that its bookkeeping is what gets asserted, and `LegacyRecordingTracer` on the
deprecated `couchbase.tracing` bases.

The spans are the part worth reading.  A tracer here is a few lines; its span has to be a
faithful stand-in for whatever the SDK writes into it, because the assertion is about the tree
of children, their attributes and their timings.  So each span here keeps its parent, its
children in creation order, and every attribute it is handed, under **public** names:
`attributes`, `children`, `parent_span`, `start_time`, `end_time`, `status`.  These are our
objects, so there is no reason for a validator to reach through a leading underscore to read
them, and the two spans that derive from an SDK class keep a mirror rather than fight it.

Everything here is api-agnostic: no `await`, no cluster, nothing that differs between
`couchbase`, `acouchbase` and `txcouchbase`.  That is what lets one copy serve all three
generated trees, and it is why the module sits outside `environments/` even though the metrics
and tracing halves are both reached through one.  The threshold reporter is needed by both
`couchbase/tests/threshold_logging_t.py` and the acouchbase additions file, which is the
same argument in miniature.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from typing import (TYPE_CHECKING,
                    Any,
                    Optional,
                    Protocol,
                    Union,
                    cast)

from couchbase.logic.observability import (OpAttributeName,
                                           OpName,
                                           ServiceType)
from couchbase.logic.observability._clock import now_ns
from couchbase.logic.observability.no_op import (NoOpMeter,
                                                 NoOpSpan,
                                                 NoOpTracer)
from couchbase.logic.observability.threshold_logging import (_IGNORED_MULTI_OP_SPAN_VALUES,
                                                             _IGNORED_PARENT_SPAN_VALUES,
                                                             ThresholdLoggingSpan,
                                                             ThresholdLoggingSpanSnapshot,
                                                             ThresholdLoggingTracer)
from couchbase.metrics import CouchbaseMeter, CouchbaseValueRecorder
from couchbase.observability.metrics import ValueRecorder
from couchbase.observability.tracing import (RequestSpan,
                                             RequestTracer,
                                             SpanAttributeValue,
                                             SpanStatusCode)
from couchbase.tracing import CouchbaseSpan, CouchbaseTracer

if TYPE_CHECKING:
    from couchbase.logic.options import ClusterTracingOptionsBase

# The name every parent span a suite opens must start with.  The threshold-logging span asks
# whether it should be judged against a service threshold, and a span the test opened is not an
# operation, so it must be recognisable by name alone: by the time the question is asked all
# that is left is the snapshot.
PARENT_SPAN_PREFIX = 'parent_'

# Spans the SDK creates underneath an operation.  They are part of what the operation's own
# span measures, so judging them separately would double-count.
_IGNORED_TEST_SPAN_VALUES = frozenset({OpAttributeName.DispatchSpanName.value,
                                       OpAttributeName.EncodingSpanName.value})


def _attach(span: Any, parent_span: Any, roots: list[Any]) -> None:
    """Put a new span where the validator will look for it.

    A root span goes on the tracer; a child goes on its parent, in creation order.  Every tracer
    here does the same thing, and the ordering matters: an encoding span is asserted to be the
    first child of the operation that encoded.
    """
    if parent_span is not None:
        parent_span.children.append(span)
    else:
        roots.append(span)


class RecordingReporter:
    """Stands in for `ThresholdLoggingReporter`, keeping every record it is handed.

    `add_log_record` is called from whichever thread ended the span, so the list is guarded.
    Reads in a test happen after the writing threads are joined.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records: list[tuple[Any, dict[str, Any], int]] = []

    def add_log_record(self, service_type: Any, record: dict[str, Any], total_duration: int) -> None:
        with self._lock:
            self.records.append((service_type, record, total_duration))

    def stop(self) -> None:
        pass

    def start(self) -> None:
        pass


class InertReporter:
    """Stands in for the `LoggingMeter` reporter, which a unit test only needs to not run.

    The meter's reporter has no record-keeping surface: it wakes, calls `create_report()` and
    logs the result.  Tests drive `create_report()` themselves, so the reporter here just has to
    satisfy the interface and stay still.  `stopped` is exposed so a test can assert the swap
    happened rather than assuming it.
    """

    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True

    def start(self) -> None:
        pass


class RecordingValueRecorder(ValueRecorder):
    """A real `ValueRecorder` that keeps every latency it is handed.

    Deriving from the SDK's own abstract base rather than merely resembling it: `Meter` and
    `NoOpMeter` both declare `value_recorder` returning a `ValueRecorder`, so a double that
    only looked the part could not be returned from `InertMeter` without a suppression.

    The SDK asks a meter for one of these per operation and records the elapsed time into it,
    so a suite reads `values` to prove an operation was measured and `attributes` to prove it
    was labelled.  `op_name` is lifted out of the tags at construction because that is the one
    attribute every validator dispatches on.
    """

    def __init__(self, name: str, tags: Mapping[str, str]) -> None:
        self.name = name
        self.op_name: Optional[str] = None
        if tags and OpAttributeName.OperationName.value in tags:
            self.op_name = tags[OpAttributeName.OperationName.value]
        self.attributes = tags
        self.values: list[Union[int, float]] = []

    def record_value(self, value: Union[int, float]) -> None:
        self.values.append(value)


class RecordingMeter:
    """A meter that hands out `RecordingValueRecorder`s and remembers them.

    Recorders are kept grouped by the name the SDK asked for, which in practice is only ever
    `MeterOperationDuration`.  The grouping is kept anyway: a second metric name appearing is
    exactly the kind of change a suite should notice rather than silently fold together.
    """

    def __init__(self) -> None:
        self.recorders: dict[str, list[RecordingValueRecorder]] = {}

    def value_recorder(self, name: str, tags: Mapping[str, str]) -> RecordingValueRecorder:
        recorder = RecordingValueRecorder(name, tags)
        self.recorders.setdefault(name, []).append(recorder)
        return recorder

    def get_value_recorder_by_op_name(self, op_name: OpName) -> Optional[RecordingValueRecorder]:
        named = self.recorders.get(OpAttributeName.MeterOperationDuration.value, [])
        return next((r for r in named if r.op_name == op_name.value), None)

    def clear(self) -> None:
        self.recorders = {}


class InertMeter(NoOpMeter):
    """A no-op meter that would record, if the SDK ever asked it to.

    Configuring a `NoOpMeter` makes `ObservableRequestHandler` cache an
    `ObservableRequestHandlerNoOpMeterImpl` whose `process_end` does nothing, so the meter is
    stored and then never called.  That short-circuit is the whole behaviour under test, and
    the only way to observe it from outside is a meter that *would* have recorded: `recorders`
    staying empty is then a statement about the SDK rather than about the meter.

    Subclassing `NoOpMeter` is load-bearing twice over.  The SDK dispatches on
    `isinstance(..., NoOpMeter)` to pick that impl, and the validators dispatch on the same
    check to pick their no-op assertions.
    """

    def __init__(self) -> None:
        super().__init__()
        self.recorders: dict[str, list[RecordingValueRecorder]] = {}

    def value_recorder(self, name: str, tags: Mapping[str, str]) -> RecordingValueRecorder:
        recorder = RecordingValueRecorder(name, tags)
        self.recorders.setdefault(name, []).append(recorder)
        return recorder

    def clear(self) -> None:
        self.recorders = {}


TestMeterType = Union[InertMeter, RecordingMeter]


class LegacyValueRecorder(CouchbaseValueRecorder):
    """A recorder built on the deprecated base, keeping what it was handed.

    `super().__init__()` is not ceremony here.  `Supportability.class_deprecated` works by
    wrapping `__init__`, so calling it is what runs the deprecation path this class exists to
    exercise; a subclass that skipped it would test the same shape with the deprecation itself
    bypassed.
    """

    def __init__(self, name: str, tags: Mapping[str, str]) -> None:
        super().__init__()
        self.name = name
        self.op_name: Optional[str] = tags.get(OpAttributeName.OperationName.value) if tags else None
        self.values: list[Union[int, float]] = []

    def record_value(self, value: Union[int, float]) -> None:
        self.values.append(value)


class LegacyRecordingMeter(CouchbaseMeter):
    """A meter written the way an application on the deprecated base would write one.

    `couchbase.metrics.CouchbaseMeter` has an empty body over
    `couchbase.observability.metrics.Meter`, so this reaches the SDK through exactly the same
    path `RecordingMeter` does and there is no separate cluster option selecting it.  That is
    the whole reason its suite is a canary rather than a parametrized variant: there is no
    second code path to assert on, only the question of whether the surface still works.
    """

    def __init__(self) -> None:
        super().__init__()
        self.recorders: list[LegacyValueRecorder] = []

    def value_recorder(self, name: str, tags: Mapping[str, str]) -> LegacyValueRecorder:
        recorder = LegacyValueRecorder(name, tags)
        self.recorders.append(recorder)
        return recorder

    def measured_op_names(self) -> list[Optional[str]]:
        """The operation name of every duration recorder that was handed a latency.

        Filtering on `values` rather than on the recorder merely existing is what lets one
        assertion cover both deprecated classes at once: a name appears here only if the SDK
        asked `CouchbaseMeter` for a recorder *and* called `record_value` on what it got back.
        """
        return [r.op_name for r in self.recorders
                if r.values and r.name == OpAttributeName.MeterOperationDuration.value]

    def clear(self) -> None:
        self.recorders = []


class TestSpanProtocol(Protocol):
    """The surface every span below exposes, and the only one a validator reads.

    Four span classes, three of them deriving from a different SDK base, and a validator that
    walks the tree they build.  A union of the four would make every read a question about
    which member is in hand; this says what they agree on instead, and the spans are written
    to agree rather than the validator written to cope.

    Read-only by declaration, because that is what a validator does with a span.  A plain
    instance attribute satisfies a read-only protocol member, so the implementations stay
    ordinary attributes assigned in `__init__`.
    """

    @property
    def name(self) -> str: ...

    @property
    def attributes(self) -> Mapping[str, Any]: ...

    @property
    def children(self) -> Sequence[TestSpanProtocol]: ...

    @property
    def parent_span(self) -> Optional[TestSpanProtocol]: ...

    @property
    def start_time(self) -> Optional[int]: ...

    @property
    def end_time(self) -> Optional[int]: ...

    @property
    def status(self) -> SpanStatusCode: ...


class RecordingSpan(RequestSpan):
    """The span a `RecordingTracer` hands the SDK, keeping everything written to it.

    Everything a validator reads is a plain public attribute.  `name` is the one exception and
    it is not a choice: `RequestSpan` declares it an abstract property, so a subclass cannot
    satisfy it by assigning an instance attribute of the same name.
    """

    def __init__(self,
                 name: str,
                 parent_span: Optional[RecordingSpan] = None,
                 start_time: Optional[int] = None) -> None:
        self._name = name
        self.parent_span = parent_span
        self.start_time: Optional[int] = start_time if start_time is not None else now_ns()
        self.end_time: Optional[int] = None
        self.attributes: dict[str, Any] = {}
        self.events: dict[str, Any] = {}
        self.status = SpanStatusCode.UNSET
        self.children: list[RecordingSpan] = []

    @property
    def name(self) -> str:
        return self._name

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        self.attributes[key] = value

    def set_attributes(self, attributes: Mapping[str, SpanAttributeValue]) -> None:
        self.attributes.update(attributes)

    def add_event(self,
                  name: str,
                  attributes: Optional[Mapping[str, SpanAttributeValue]] = None,
                  timestamp: Optional[int] = None) -> None:
        self.events[name] = attributes

    def set_status(self, status: SpanStatusCode) -> None:
        self.status = status

    def end(self, end_time: Optional[int] = None) -> None:
        self.end_time = end_time if end_time is not None else now_ns()


class InertSpan(NoOpSpan):
    """`NoOpSpan` wearing the shared surface, so that the no-op tree can be read like the others.

    Nothing here is ever written to, and that is the point: `NoOpSpan` implements every setter
    as a no-op, so `attributes` stays empty and `children` stays empty by construction rather
    than by the span choosing not to record.  Exposing them anyway is what lets a suite say
    "still empty" about the same fields it reads everywhere else, instead of the no-op case
    being the one shape no validator can look at.
    """

    def __init__(self,
                 name: str,
                 parent_span: Optional[InertSpan] = None,
                 start_time: Optional[int] = None) -> None:
        super().__init__(name, parent_span=parent_span, start_time=start_time)
        self.parent_span = parent_span
        self.start_time: Optional[int] = start_time
        self.end_time: Optional[int] = None
        self.attributes: dict[str, Any] = {}
        self.status = SpanStatusCode.UNSET
        self.children: list[InertSpan] = []


class ThresholdRecordingSpan(ThresholdLoggingSpan):
    """The SDK's own threshold-logging span, with the tree and the attributes kept on the side.

    The point of this one is that its bookkeeping is the SDK's, not ours: a suite asserts that
    `_total_dispatch_duration_ns` and friends add up to what the child spans actually measured,
    which is only meaningful if the arithmetic under test is the shipped arithmetic.  So this
    derives rather than reimplements, and adds only what the real span throws away: the child
    spans, and a mirror of every attribute (the real one folds each attribute into a counter
    and keeps no dictionary).

    The timing properties are aliases onto the SDK's nanosecond fields rather than a second
    copy, so the shared surface reads the same clock the roll-ups were computed from.
    """

    # The shipped span skips creating per-key child spans for a multi-operation, which is a
    # measurable win and unmeasurable from the outside.  A suite that wants to assert on those
    # children has to turn it off.
    _supports_multi_op_fast_dispatch: bool = False

    def __init__(self,
                 name: str,
                 parent_span: Optional[ThresholdRecordingSpan] = None,
                 start_time: Optional[int] = None,
                 tracer: Optional[ThresholdRecordingTracer] = None) -> None:
        super().__init__(name, parent_span=parent_span, start_time=start_time, tracer=tracer)
        self.children: list[ThresholdRecordingSpan] = []
        self.attributes: dict[str, Any] = {}

    @property
    def parent_span(self) -> Optional[ThresholdRecordingSpan]:
        parent = self._parent_span
        assert parent is None or isinstance(parent, ThresholdRecordingSpan)
        return parent

    @property
    def start_time(self) -> Optional[int]:
        return self._start_time_ns

    @property
    def end_time(self) -> Optional[int]:
        return self._end_time_ns

    @property
    def status(self) -> SpanStatusCode:
        return self._status

    @property
    def snapshot(self) -> ThresholdLoggingSpanSnapshot:
        """The snapshot the span handed its tracer as it ended.

        Only meaningful after `end`, which is the only place the shipped span builds one, and
        every read of it in a suite happens after the operation completed.
        """
        assert self._span_snapshot is not None, f'span {self._name!r} has not ended'
        return self._span_snapshot

    def end(self, end_time: Optional[int] = None) -> None:
        super().end(end_time=end_time)
        # A span the SDK creates internally is not a candidate for the threshold report, and
        # neither is a parent the test itself opened.  Checking one anyway would put a
        # non-operation into over/under_threshold_spans and break the assertion that every
        # operation span landed in exactly one of them.
        parent = self._parent_span
        if self._name.startswith(PARENT_SPAN_PREFIX) or parent is None:
            return
        ignored = (self._name in _IGNORED_MULTI_OP_SPAN_VALUES
                   or self._name in _IGNORED_TEST_SPAN_VALUES
                   or parent.name in _IGNORED_MULTI_OP_SPAN_VALUES
                   or parent.name in _IGNORED_PARENT_SPAN_VALUES)
        if not ignored and self._tracer is not None:
            self._tracer.check_threshold(self.snapshot)

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        super().set_attribute(key, value)
        self.attributes[key] = value

    def apply_core_span_attributes(self, attributes: Mapping[str, Any]) -> None:
        super().apply_core_span_attributes(attributes)
        self.attributes.update(attributes)


class LegacyRecordingSpan(CouchbaseSpan):
    """The same recording span on the deprecated `couchbase.tracing` base.

    `CouchbaseSpan` wraps a span from some other tracing library, so it takes that object and
    exposes it as `.span`; here it is its own wrapped span.  The base carries only
    `set_attribute` and `finish`, which is the whole reason the deprecated path records less:
    there is no `set_status` to call and no start-time parameter to honour.  `status` is here
    because the shared surface has it, and it stays `UNSET` forever, which is the true answer.
    """

    def __init__(self, name: str, parent_span: Optional[LegacyRecordingSpan] = None) -> None:
        super().__init__(span=self)
        self._name = name
        self.parent_span = parent_span
        self.start_time: Optional[int] = now_ns()
        self.end_time: Optional[int] = None
        self.attributes: dict[str, Any] = {}
        self.status = SpanStatusCode.UNSET
        self.children: list[LegacyRecordingSpan] = []

    @property
    def name(self) -> str:
        return self._name

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def finish(self) -> None:
        self.end_time = now_ns()


class RecordingTracer(RequestTracer):
    """Keeps every root span it is asked for; children are kept by their parent."""

    def __init__(self) -> None:
        self.spans: list[RecordingSpan] = []

    def request_span(self,
                     name: str,
                     parent_span: Optional[RequestSpan] = None,
                     start_time: Optional[int] = None) -> RecordingSpan:
        # The SDK only ever hands back a span this tracer made, so the assertion is a check on
        # the wiring rather than a narrowing convenience.
        assert parent_span is None or isinstance(parent_span, RecordingSpan)
        span = RecordingSpan(name, parent_span=parent_span, start_time=start_time)
        _attach(span, parent_span, self.spans)
        return span

    def get_span_by_name(self, name: str) -> Optional[RecordingSpan]:
        return next((s for s in self.spans if s.name == name), None)

    def clear_spans(self) -> None:
        self.spans = []


class InertTracer(NoOpTracer):
    """`NoOpTracer` that can still hand back a parent span.

    Deriving from `NoOpTracer` rather than resembling it is what makes the suite meaningful:
    the SDK picks its no-op request-handler by that type, so a double that only behaved like a
    no-op would be driven down the recording path and prove nothing.

    `request_span` is overridden to keep the spans it makes, which is what lets a test ask for
    one by name: the shipped no-op discards everything, that being the point of it.  A test
    that opens a parent span while tracing is disabled needs the span back, and that is the
    same thing an application does.
    """

    def __init__(self) -> None:
        self.spans: list[InertSpan] = []

    def request_span(self,
                     name: str,
                     parent_span: Optional[RequestSpan] = None,
                     start_time: Optional[int] = None) -> InertSpan:
        assert parent_span is None or isinstance(parent_span, InertSpan)
        span = InertSpan(name, parent_span=parent_span, start_time=start_time)
        _attach(span, parent_span, self.spans)
        return span

    def get_span_by_name(self, name: str) -> Optional[InertSpan]:
        return next((s for s in self.spans if s.name == name), None)

    def clear_spans(self) -> None:
        self.spans = []


class ThresholdRecordingTracer(ThresholdLoggingTracer):
    """The SDK's threshold-logging tracer with its reporter stilled and its verdicts kept.

    `check_threshold` is the SDK's decision point, so overriding it here records the verdict
    without changing it: every span the tracer judged lands in `over_threshold_spans` or
    `under_threshold_spans`, and a suite asserts which one.  The record still goes to the
    reporter afterwards, so the handoff is exercised too.
    """

    def __init__(self, config: Optional[dict[str, int]] = None) -> None:
        super().__init__(cast('ClusterTracingOptionsBase', config))
        self.spans: list[ThresholdRecordingSpan] = []
        self.over_threshold_spans: list[ThresholdLoggingSpanSnapshot] = []
        self.under_threshold_spans: list[ThresholdLoggingSpanSnapshot] = []
        # The real reporter wakes on an interval and logs; a suite wants the verdicts, not the
        # output, and not the wait.
        self._reporter.stop()

    def request_span(self,
                     name: str,
                     parent_span: Optional[RequestSpan] = None,
                     start_time: Optional[int] = None) -> ThresholdRecordingSpan:
        assert parent_span is None or isinstance(parent_span, ThresholdRecordingSpan)
        span = ThresholdRecordingSpan(name, parent_span=parent_span, start_time=start_time, tracer=self)
        _attach(span, parent_span, self.spans)
        return span

    def check_threshold(self, snapshot: ThresholdLoggingSpanSnapshot) -> None:
        service_type = snapshot.service_type
        if snapshot.name.startswith(PARENT_SPAN_PREFIX) or service_type is None:
            return
        service_threshold_us = self._get_service_type_threshold(service_type)
        span_total_duration_us = int(snapshot.total_duration_ns / 1000)
        if span_total_duration_us <= service_threshold_us:
            self.under_threshold_spans.append(snapshot)
        else:
            self.over_threshold_spans.append(snapshot)

        record = self._build_threshold_log_record(snapshot, span_total_duration_us)
        self._reporter.add_log_record(service_type, record, span_total_duration_us)

    def service_threshold_us(self, service_type: ServiceType) -> int:
        """The configured threshold for a service, so a suite need not reach for a private."""
        return self._get_service_type_threshold(service_type)

    def get_span_by_name(self, name: str) -> Optional[ThresholdRecordingSpan]:
        return next((s for s in self.spans if s.name == name), None)

    def clear_spans(self) -> None:
        self.spans = []
        self.over_threshold_spans = []
        self.under_threshold_spans = []


class LegacyRecordingTracer(CouchbaseTracer):
    """The same recording tracer on the deprecated `couchbase.tracing` base.

    `CouchbaseTracer` names the method `start_span`, not `request_span`.  Both are here: the
    SDK calls `start_span`, and `request_span` lets a suite open a parent span through the same
    call it would make against any of the other three tracers.
    """

    def __init__(self) -> None:
        self.spans: list[LegacyRecordingSpan] = []

    def start_span(self, name: str, parent: Optional[CouchbaseSpan] = None) -> LegacyRecordingSpan:
        assert parent is None or isinstance(parent, LegacyRecordingSpan)
        span = LegacyRecordingSpan(name, parent_span=parent)
        _attach(span, parent, self.spans)
        return span

    def request_span(self,
                     name: str,
                     parent_span: Optional[CouchbaseSpan] = None,
                     start_time: Optional[int] = None) -> LegacyRecordingSpan:
        return self.start_span(name, parent_span)

    def get_span_by_name(self, name: str) -> Optional[LegacyRecordingSpan]:
        return next((s for s in self.spans if s.name == name), None)

    def clear_spans(self) -> None:
        self.spans = []


TestSpanType = Union[RecordingSpan, InertSpan, ThresholdRecordingSpan, LegacyRecordingSpan]
TestTracerType = Union[RecordingTracer, InertTracer, ThresholdRecordingTracer, LegacyRecordingTracer]
