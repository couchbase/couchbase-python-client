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

from time import time_ns
from typing import (TYPE_CHECKING,
                    Any,
                    List,
                    Mapping,
                    Optional,
                    Union)

from couchbase.logic.observability.no_op import NoOpSpan
from couchbase.logic.observability.observability_types import OpAttributeName
from couchbase.logic.observability.threshold_logging import (_IGNORED_MULTI_OP_SPAN_VALUES,
                                                             _IGNORED_PARENT_SPAN_VALUES,
                                                             ThresholdLoggingSpan)
from couchbase.observability.tracing import (RequestSpan,
                                             SpanAttributeValue,
                                             SpanStatusCode)
from couchbase.tracing import CouchbaseSpan

if TYPE_CHECKING:
    from tests.environments.tracing.base.tracers import TestThresholdLoggingTracer

_IGNORED_TEST_THRESHOLD_LOGGING_SPAN_VALUES = set([OpAttributeName.DispatchSpanName.value,
                                                   OpAttributeName.EncodingSpanName.value])


class LegacyTestSpan(CouchbaseSpan):

    def __init__(self, name: str, parent_span: Optional[CouchbaseSpan] = None) -> None:
        self._name = name
        self._parent_span = parent_span
        self._start_time = time_ns()
        self._attributes = {}
        self._end_time = None
        self._children = []

    @property
    def children(self) -> List[LegacyTestSpan]:
        return self._children

    @property
    def name(self) -> str:
        return self._name

    @property
    def span(self) -> LegacyTestSpan:
        return self

    def set_attribute(self, key: str, value: Any) -> None:
        self._attributes[key] = value

    def finish(self) -> None:
        self._end_time = time_ns()


class NoOpTestSpan(NoOpSpan):

    def __init__(self,
                 name: str,
                 parent_span: Optional[NoOpSpan] = None,
                 start_time: Optional[int] = None) -> None:
        super().__init__(name, parent_span=parent_span, start_time=start_time)
        self._children = []

    @property
    def children(self) -> List[NoOpTestSpan]:
        return self._children


class TestThresholdLoggingSpan(ThresholdLoggingSpan):

    # Disable the fast-path optimization for tests so that child spans are
    # created and we can validate their attributes.
    _supports_multi_op_fast_dispatch: bool = False

    def __init__(self,
                 name: str,
                 parent_span: Optional[ThresholdLoggingSpan] = None,
                 start_time: Optional[int] = None,
                 tracer: Optional[TestThresholdLoggingTracer] = None) -> None:
        super().__init__(name, parent_span=parent_span, start_time=start_time, tracer=tracer)
        self._children = []
        self._test_attributes = {}

    @property
    def children(self) -> List[ThresholdLoggingSpan]:
        return self._children

    def end(self, end_time: Optional[int] = None) -> None:
        super().end(end_time=end_time)
        # we need this logic so that we only check non-parent spans for tests that create a parent span.
        if not self._name.startswith('parent_') and self._parent_span is not None:
            dont_check = (self._name in _IGNORED_MULTI_OP_SPAN_VALUES
                          or (self._parent_span and self._parent_span.name in _IGNORED_MULTI_OP_SPAN_VALUES)
                          or (self._name in _IGNORED_TEST_THRESHOLD_LOGGING_SPAN_VALUES)
                          or (self._parent_span.name in _IGNORED_PARENT_SPAN_VALUES))
            if not dont_check:
                self._tracer.check_threshold(self._span_snapshot)

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        super().set_attribute(key, value)
        # We add every attribute so validation has all the data to make the necessary computations
        self._test_attributes[key] = value

    def apply_core_span_attributes(self, attributes: Mapping[str, Any]) -> None:
        super().apply_core_span_attributes(attributes)
        # We add every attribute so validation has all the data to make the necessary computations
        for k, v in attributes.items():
            self._test_attributes[k] = v


class TestSpan(RequestSpan):

    def __init__(self,
                 name: str,
                 parent_span: Optional[RequestSpan] = None,
                 start_time: Optional[int] = None) -> None:
        self._name = name
        self._parent_span = parent_span
        self._start_time = start_time if start_time is not None else time_ns()
        self._attributes = {}
        self._end_time = None
        self._children = []
        self._status = SpanStatusCode.UNSET

    @property
    def children(self) -> List[TestSpan]:
        return self._children

    @property
    def name(self) -> str:
        return self._name

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        self._attributes[key] = value

    def set_attributes(self, attributes: Mapping[str, SpanAttributeValue]) -> None:
        for k, v in attributes.items():
            self.set_attribute(k, v)

    def add_event(self, name: str, value: SpanAttributeValue) -> None:
        self._events[name] = value

    def set_status(self, status: SpanStatusCode) -> None:
        self._status = status

    def end(self, end_time: Optional[int] = None) -> None:
        self._end_time = end_time if end_time is not None else time_ns()


TestSpanType = Union[LegacyTestSpan, NoOpTestSpan, TestThresholdLoggingSpan, TestSpan]
