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

from typing import (Dict,
                    List,
                    Optional,
                    Union)

from couchbase.logic.observability.no_op import NoOpTracer
from couchbase.logic.observability.threshold_logging import ThresholdLoggingSpanSnapshot, ThresholdLoggingTracer
from couchbase.observability.tracing import RequestTracer
from couchbase.tracing import CouchbaseTracer

from .spans import (LegacyTestSpan,
                    NoOpTestSpan,
                    TestSpan,
                    TestThresholdLoggingSpan)


class LegacyTestTracer(CouchbaseTracer):

    def __init__(self) -> None:
        self._spans: List[LegacyTestSpan] = []

    def start_span(self, name: str, parent: Optional[LegacyTestSpan] = None) -> LegacyTestSpan:
        new_span = LegacyTestSpan(name, parent_span=parent)
        if parent:
            parent.children.append(new_span)
        else:
            self._spans.append(new_span)

        return new_span

    # legacy compatibility method - we used to have tests that called request_span directly on the tracer
    def request_span(self, name: str, parent: Optional[LegacyTestSpan] = None) -> LegacyTestSpan:
        return self.start_span(name, parent)

    def get_span_by_name(self, name: str) -> Optional[LegacyTestSpan]:
        return next((s for s in self._spans if s.name == name), None)

    def clear_spans(self) -> None:
        self._spans = []


# Need to extend NoOpTracer so that the ObservabilityHandler will appropriately choose the NoOp implementation.
# In terms of functionality, since the ObservableRequestHandlerNoOpImpl never calls request_span, all we need
# for testing purposes is to handle the scenario where parent spans are created.
class NoOpTestTracer(NoOpTracer):

    def __init__(self) -> None:
        self._spans: List[NoOpTestSpan] = []

    def request_span(self, name: str, parent: Optional[NoOpTestSpan] = None) -> NoOpTestSpan:
        new_span = NoOpTestSpan(name, parent_span=parent)
        if parent:
            parent.children.append(new_span)
        else:
            self._spans.append(new_span)

        return new_span

    def get_span_by_name(self, name: str) -> Optional[NoOpTestSpan]:
        return next((s for s in self._spans if s.name == name), None)

    def clear_spans(self) -> None:
        # sometimes we have parent spans that use the tracer directly (instead of going through the SDK's handler)
        self._spans = []


class TestThresholdLoggingTracer(ThresholdLoggingTracer):
    def __init__(self, config: Optional[Dict[str, int]] = None) -> None:
        super().__init__(config)
        self._spans: List[TestThresholdLoggingSpan] = []
        # we don't want to periodically log the report, we want to validate
        self._reporter.stop()
        self._under_threshold_spans = []
        self._over_threshold_spans = []

    def request_span(
        self,
        name: str,
        parent_span: Optional[TestThresholdLoggingSpan] = None,
        start_time: Optional[int] = None
    ) -> TestThresholdLoggingSpan:
        new_span = TestThresholdLoggingSpan(name, parent_span=parent_span, start_time=start_time, tracer=self)
        if parent_span:
            parent_span.children.append(new_span)
        else:
            self._spans.append(new_span)

        return new_span

    def check_threshold(self, snapshot: ThresholdLoggingSpanSnapshot) -> None:
        if snapshot.name.startswith('parent_'):
            return
        service_threshold_us = self._get_service_type_threshold(snapshot.service_type)
        # convert to micros
        span_total_duration_us = snapshot.total_duration_ns / 1000
        if span_total_duration_us <= service_threshold_us:
            self._under_threshold_spans.append(snapshot)
        else:
            self._over_threshold_spans.append(snapshot)

        threshold_log_record = self._build_threshold_log_record(snapshot, span_total_duration_us)
        self._reporter.add_log_record(snapshot.service_type, threshold_log_record, int(span_total_duration_us))

    def get_span_by_name(self, name: str) -> Optional[TestThresholdLoggingSpan]:
        return next((s for s in self._spans if s.name == name), None)

    def clear_spans(self) -> None:
        self._over_threshold_spans = []
        self._under_threshold_spans = []
        self._spans = []


class TestTracer(RequestTracer):

    def __init__(self) -> None:
        self._spans: List[TestSpan] = []

    def request_span(
        self,
        name: str,
        parent_span: Optional[TestSpan] = None,
        start_time: Optional[int] = None
    ) -> TestSpan:
        new_span = TestSpan(name, parent_span=parent_span, start_time=start_time)
        if parent_span:
            parent_span.children.append(new_span)
        else:
            self._spans.append(new_span)

        return new_span

    def get_span_by_name(self, name: str) -> Optional[TestSpan]:
        return next((s for s in self._spans if s.name == name), None)

    def clear_spans(self) -> None:
        self._spans = []


TestTracerType = Union[LegacyTestTracer, NoOpTestTracer, TestThresholdLoggingTracer, TestTracer]
