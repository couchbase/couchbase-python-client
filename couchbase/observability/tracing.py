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

from abc import ABC, abstractmethod
from enum import Enum
from typing import (Mapping,
                    Optional,
                    Sequence,
                    Union)

# The core OTel Attribute Value types
SpanAttributeValue = Union[
    str,
    bool,
    int,
    float,
    Sequence[str],
    Sequence[bool],
    Sequence[int],
    Sequence[float]
]

# The common 'attributes' dictionary type
SpanAttributes = Mapping[str, SpanAttributeValue]


class SpanStatusCode(Enum):
    UNSET = 0
    OK = 1
    ERROR = 2


class RequestSpan(ABC):
    """
    Represents a single traced operation (span) in distributed tracing.

    A RequestSpan provides methods to add metadata (attributes), record events,
    set the final status, and mark the completion of an operation.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        The name of the span, describing the operation being traced.
        """

    @abstractmethod
    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        """
        Sets a single attribute (key-value pair) on the span.

        Args:
            key: The attribute key.
            value: The attribute value.
        """

    @abstractmethod
    def set_attributes(self, attributes: SpanAttributes) -> None:
        """
        Sets multiple attributes on the span.

        Args:
            attributes: A mapping of attribute keys to values.
        """

    @abstractmethod
    def add_event(self,
                  name: str,
                  attributes: Optional[SpanAttributes] = None,
                  timestamp: Optional[int] = None) -> None:
        """
        Adds a timestamped event to the span.

        Args:
            name: The event name.
            attributes: Optional attributes to associate with the event.
            timestamp: Optional timestamp for the event.
        """

    @abstractmethod
    def set_status(self, status: SpanStatusCode) -> None:
        """
        Sets the final status of the span.

        Args:
            status: The span status.
        """

    @abstractmethod
    def end(self, end_time: Optional[int]) -> None:
        """
        Marks the span as complete and records the end time.

        Args:
            end_time: Optional end time; defaults to current time if not provided.
        """


class RequestTracer(ABC):
    """
    Interface for creating and managing distributed tracing spans.

    A RequestTracer is responsible for creating new spans to track operations.
    """

    @abstractmethod
    def request_span(
        self,
        name: str,
        parent_span: Optional[RequestSpan] = None,
        start_time: Optional[int] = None
    ) -> RequestSpan:
        """
        Creates a new span to trace an operation.

        Args:
            name: The name of the span, describing the operation being traced.
            parent_span: Optional parent span for creating hierarchical traces.
            start_time: Optional start time; defaults to current time if not provided.

        Returns:
            A new RequestSpan instance.
        """
