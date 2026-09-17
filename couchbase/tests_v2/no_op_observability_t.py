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

from typing import Optional, cast

from couchbase.logic.observability.handler import (ObservableRequestHandler,
                                                   ObservableRequestHandlerMeterImpl,
                                                   ObservableRequestHandlerNoOpMeterImpl,
                                                   ObservableRequestHandlerNoOpTracerImpl,
                                                   ObservableRequestHandlerTracerImpl)
from couchbase.logic.observability.no_op import (NoOpMeter,
                                                 NoOpSpan,
                                                 NoOpTracer)
from couchbase.logic.observability.observability_types import (MeterProtocol,
                                                               ObservabilityInstruments,
                                                               TracerProtocol,
                                                               WrappedTracer)
from couchbase.logic.operation_types import KeyValueOperationType
from couchbase.observability.tracing import RequestTracer
from tests_v2.observability_test_impls import RecordingMeter, RecordingTracer


def _instruments(tracer: Optional[RequestTracer] = None,
                 meter: Optional[MeterProtocol] = None) -> ObservabilityInstruments:
    # The cast is not about these test implementations.  `RequestTracerProtocol.request_span` declares
    # `parent_span: Optional[RequestSpanProtocol]` while the ABC every tracer derives from
    # declares the narrower `Optional[RequestSpan]`, so no subclass of the documented base
    # satisfies the protocol structurally.
    wrapped = WrappedTracer(cast(TracerProtocol, tracer if tracer is not None else NoOpTracer()), False)
    # `is_noop` mirrors what `ClusterSettings` computes, because that flag is what lets an
    # operation skip handler construction altogether: set wrongly, the handler takes a branch
    # no cluster ever takes.
    return ObservabilityInstruments(wrapped,
                                    meter if meter is not None else NoOpMeter(),
                                    is_noop=tracer is None and meter is None)


class NoOpTracerUnitTests:
    # No cb_env and no TEST_MANIFEST: the tracer is constructed and called directly, and
    # `couchbase.logic.observability.no_op` holds the only definition.  Not generated for that
    # reason.
    #
    # This is the tracer every cluster gets when nothing is configured, so it sits on the path
    # of every operation and is the one tracer no observability suite can substitute.  What it
    # owes an application that opens a parent span while tracing is off is a usable span.

    def test_request_span(self) -> None:
        span = NoOpTracer().request_span('cb.get')

        assert isinstance(span, NoOpSpan)
        assert span.is_recording is False
        assert span.span_end is None

    def test_request_span_with_parent(self) -> None:
        parent = NoOpSpan('cb.request')
        span = NoOpTracer().request_span('cb.get', parent_span=parent, start_time=1)

        # A parent and a start time are accepted and dropped: the span records nothing, so
        # there is nothing for either to be recorded on.
        assert isinstance(span, NoOpSpan)
        assert span.is_recording is False


class ObservabilityInstrumentsCacheUnitTests:
    # No cb_env and no TEST_MANIFEST, for the reason NoOpTracerUnitTests above gives.
    #
    # A no-op handler is built on every operation and there is nothing per-operation about it,
    # so one impl is shared across every op type on the same instruments.  Three cache states,
    # and the middle one is the one worth knowing: None means not yet checked, False means the
    # tracer or meter is real and there is nothing to cache, anything else is the cached impl.
    # False rather than a bool is what lets `is not False` narrow to the impl type.
    #
    # That makes it per-instance mutable state on a dataclass whose fields otherwise describe a
    # configuration, which is why the last test here is about equality and repr.

    def test_noop_impls_are_reused_across_op_types(self) -> None:
        instruments = _instruments()
        first = ObservableRequestHandler(KeyValueOperationType.Get, instruments)
        second = ObservableRequestHandler(KeyValueOperationType.Upsert, instruments)

        assert isinstance(first._tracer_impl, ObservableRequestHandlerNoOpTracerImpl)
        assert isinstance(first._meter_impl, ObservableRequestHandlerNoOpMeterImpl)
        assert first._tracer_impl is second._tracer_impl
        assert first._meter_impl is second._meter_impl
        assert first.is_noop is True and second.is_noop is True

    def test_each_instruments_instance_gets_its_own_cache(self) -> None:
        # Two clusters in one process each hold their own instruments, so a cache shared
        # across instances would hand one cluster's handler the other's impl.
        one, other = _instruments(), _instruments()
        first = ObservableRequestHandler(KeyValueOperationType.Get, one)
        second = ObservableRequestHandler(KeyValueOperationType.Get, other)

        assert first._tracer_impl is not second._tracer_impl
        assert first._meter_impl is not second._meter_impl

    def test_real_instruments_record_the_false_sentinel(self) -> None:
        instruments = _instruments(tracer=RecordingTracer(), meter=RecordingMeter())
        handler = ObservableRequestHandler(KeyValueOperationType.Get, instruments)

        # False records that there was something to check and nothing to cache, which is what
        # keeps the next handler on this instruments from checking again.
        assert instruments._cached_noop_tracer_impl is False
        assert instruments._cached_noop_meter_impl is False
        assert isinstance(handler._tracer_impl, ObservableRequestHandlerTracerImpl)
        assert isinstance(handler._meter_impl, ObservableRequestHandlerMeterImpl)
        assert handler.is_noop is False
        assert handler.with_metrics is True

    def test_cache_is_absent_from_equality_and_repr(self) -> None:
        # The cache is per-instance state, so it must not enter the value the dataclass
        # declares: two instruments configured the same way are the same configuration
        # whether or not one of them has been used.
        tracer = WrappedTracer(cast(TracerProtocol, NoOpTracer()), False)
        meter = NoOpMeter()
        warmed = ObservabilityInstruments(tracer, meter, is_noop=True)
        cold = ObservabilityInstruments(tracer, meter, is_noop=True)
        ObservableRequestHandler(KeyValueOperationType.Get, warmed)

        assert warmed._cached_noop_tracer_impl is not None
        assert cold._cached_noop_tracer_impl is None
        assert warmed == cold
        assert '_cached_noop' not in repr(warmed)
