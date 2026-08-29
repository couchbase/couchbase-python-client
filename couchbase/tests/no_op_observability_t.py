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

import pytest

from couchbase.logic.observability.handler import (ObservableRequestHandler,
                                                   ObservableRequestHandlerMeterImpl,
                                                   ObservableRequestHandlerNoOpMeterImpl,
                                                   ObservableRequestHandlerNoOpTracerImpl,
                                                   ObservableRequestHandlerTracerImpl)
from couchbase.logic.observability.no_op import (NoOpMeter,
                                                 NoOpSpan,
                                                 NoOpTracer)
from couchbase.logic.observability.observability_types import ObservabilityInstruments, WrappedTracer
from couchbase.logic.operation_types import KeyValueOperationType


class NoOpTracerTestSuite:
    TEST_MANIFEST = [
        'test_request_span',
        'test_request_span_with_parent',
    ]

    def test_request_span(self):
        """The no-op tracer should return a span rather than raise."""
        span = NoOpTracer().request_span('cb.get')

        assert isinstance(span, NoOpSpan)
        assert span.is_recording is False
        assert span.span_end is None

    def test_request_span_with_parent(self):
        """A parent span and a start time should be accepted, not rejected."""
        parent = NoOpSpan('cb.request')
        span = NoOpTracer().request_span('cb.get', parent_span=parent, start_time=1)

        assert isinstance(span, NoOpSpan)
        assert span.is_recording is False


class ClassicNoOpTracerTests(NoOpTracerTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicNoOpTracerTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicNoOpTracerTests) if valid_test_method(meth)]
        test_list = set(NoOpTracerTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')


class _StubTracer:
    """Anything that is not a NoOpTracer, so the handler takes the real branch."""

    def request_span(self, name, parent_span=None, start_time=None):
        return None


class _StubMeter:

    def value_recorder(self, name, tags):
        return None


def _instruments(tracer=None, meter=None):
    return ObservabilityInstruments(WrappedTracer(tracer or NoOpTracer(), False),
                                    meter or NoOpMeter(),
                                    is_noop=tracer is None and meter is None)


class ObservabilityInstrumentsCacheTestSuite:
    TEST_MANIFEST = [
        'test_cache_is_absent_from_equality_and_repr',
        'test_each_instruments_instance_gets_its_own_cache',
        'test_noop_impls_are_reused_across_op_types',
        'test_real_instruments_record_the_false_sentinel',
    ]

    def test_noop_impls_are_reused_across_op_types(self):
        """One no-op impl per instruments, not one per operation."""
        instruments = _instruments()
        first = ObservableRequestHandler(KeyValueOperationType.Get, instruments)
        second = ObservableRequestHandler(KeyValueOperationType.Upsert, instruments)

        assert isinstance(first._tracer_impl, ObservableRequestHandlerNoOpTracerImpl)
        assert isinstance(first._meter_impl, ObservableRequestHandlerNoOpMeterImpl)
        assert first._tracer_impl is second._tracer_impl
        assert first._meter_impl is second._meter_impl
        assert first.is_noop is True and second.is_noop is True

    def test_each_instruments_instance_gets_its_own_cache(self):
        one, other = _instruments(), _instruments()
        first = ObservableRequestHandler(KeyValueOperationType.Get, one)
        second = ObservableRequestHandler(KeyValueOperationType.Get, other)

        assert first._tracer_impl is not second._tracer_impl
        assert first._meter_impl is not second._meter_impl

    def test_real_instruments_record_the_false_sentinel(self):
        """False records that there is nothing to cache, which is what the impl branch reads."""
        instruments = _instruments(tracer=_StubTracer(), meter=_StubMeter())
        handler = ObservableRequestHandler(KeyValueOperationType.Get, instruments)

        assert instruments._cached_noop_tracer_impl is False
        assert instruments._cached_noop_meter_impl is False
        assert isinstance(handler._tracer_impl, ObservableRequestHandlerTracerImpl)
        assert isinstance(handler._meter_impl, ObservableRequestHandlerMeterImpl)
        assert handler.is_noop is False
        assert handler.with_metrics is True

    def test_cache_is_absent_from_equality_and_repr(self):
        """The cache is per-instance state, so it must not enter the value the dataclass declares."""
        tracer, meter = WrappedTracer(NoOpTracer(), False), NoOpMeter()
        warmed = ObservabilityInstruments(tracer, meter, is_noop=True)
        cold = ObservabilityInstruments(tracer, meter, is_noop=True)
        ObservableRequestHandler(KeyValueOperationType.Get, warmed)

        assert warmed._cached_noop_tracer_impl is not None
        assert cold._cached_noop_tracer_impl is None
        assert warmed == cold
        assert '_cached_noop' not in repr(warmed)


class ClassicObservabilityInstrumentsCacheTests(ObservabilityInstrumentsCacheTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicObservabilityInstrumentsCacheTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicObservabilityInstrumentsCacheTests) if valid_test_method(meth)]
        test_list = set(ObservabilityInstrumentsCacheTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')
