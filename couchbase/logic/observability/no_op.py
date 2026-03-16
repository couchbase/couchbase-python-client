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

from typing import Mapping, Optional

from couchbase.observability.metrics import Meter, ValueRecorder
from couchbase.observability.tracing import (RequestSpan,
                                             RequestTracer,
                                             SpanAttributeValue,
                                             SpanStatusCode)


class NoOpValueRecorder(ValueRecorder):
    """
    **INTERNAL**
    """

    def record_value(self, value: int) -> None:
        pass


class NoOpMeter(Meter):
    """
    **INTERNAL**
    """

    def value_recorder(self, name: str, tags: Mapping[str, str]) -> ValueRecorder:
        return NoOpValueRecorder()


class NoOpSpan(RequestSpan):
    """
    **INTERNAL**
    """

    def __init__(self,
                 name: str,
                 parent_span: Optional[NoOpSpan] = None,
                 start_time: Optional[int] = None) -> None:
        pass

    @property
    def name(self) -> str:
        return ''

    @property
    def span_end(self) -> Optional[int]:
        return None

    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        pass

    def set_attributes(self, attributes: Mapping[str, SpanAttributeValue]) -> None:
        pass

    def add_event(self, name: str, value: SpanAttributeValue) -> None:
        pass

    def set_status(self, status: SpanStatusCode) -> None:
        pass

    def end(self, timestamp: Optional[int] = None) -> None:
        pass


class NoOpTracer(RequestTracer):
    """
    **INTERNAL**
    """

    def request_span(
        self,
        name: str,
        parent_span: Optional[RequestSpan] = None,
        start_time: Optional[int] = None
    ) -> RequestSpan:
        return NoOpSpan()
