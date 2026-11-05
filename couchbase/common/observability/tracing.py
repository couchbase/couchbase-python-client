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

__all__ = [
    'RequestSpan',
    'RequestTracer',
    'SpanAttributeValue',
    'SpanAttributes',
    'SpanStatusCode',
]

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from enum import Enum

# The core OTel Attribute Value types
SpanAttributeValue = str | bool | int | float | Sequence[str] | Sequence[bool] | Sequence[int] | Sequence[float]

# The common 'attributes' dictionary type
SpanAttributes = Mapping[str, SpanAttributeValue]


class SpanStatusCode(Enum):
    """The outcome a span records: UNSET until it is known, then OK or ERROR."""

    UNSET = 0
    OK = 1
    ERROR = 2


class RequestSpan(ABC):
    """Represents a single traced operation (span) in distributed tracing.

    A RequestSpan provides methods to add metadata (attributes), record events,
    set the final status, and mark the completion of an operation.
    """

    @property
    def is_recording(self) -> bool:
        """Whether the span records what is set on it.

        A span that is not sampled can return False, and the SDK then sets nothing on it. Defaults to True.
        """
        return True

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the operation the span traces."""

    @abstractmethod
    def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
        """Sets a single attribute (key-value pair) on the span.

        Args:
            key: The attribute key.
            value: The attribute value.
        """

    @abstractmethod
    def set_attributes(self, attributes: SpanAttributes) -> None:
        """Sets multiple attributes on the span.

        Args:
            attributes: A mapping of attribute keys to values.
        """

    @abstractmethod
    def add_event(self,
                  name: str,
                  attributes: SpanAttributes | None = None,
                  timestamp: int | None = None) -> None:
        """Add a timestamped event to the span.

        Args:
            name: The event's name.
            attributes: Attributes of the event. Defaults to None.
            timestamp: When the event happened, in nanoseconds since the epoch. Defaults to None, which is now.
        """

    @abstractmethod
    def set_status(self, status: SpanStatusCode) -> None:
        """Sets the final status of the span.

        Args:
            status: The span status.
        """

    @abstractmethod
    def end(self, end_time: int | None) -> None:
        """Mark the span complete.

        Args:
            end_time: When the operation ended, in nanoseconds since the epoch, or None for now.
        """


class RequestTracer(ABC):
    """Interface for creating and managing distributed tracing spans.

    A RequestTracer is responsible for creating new spans to track operations.
    """

    @abstractmethod
    def request_span(
        self,
        name: str,
        parent_span: RequestSpan | None = None,
        start_time: int | None = None
    ) -> RequestSpan:
        """Start a span for an operation.

        Args:
            name: The name of the operation.
            parent_span: The span of the operation this one is part of. Defaults to None.
            start_time: When the operation started, in nanoseconds since the epoch. Defaults to None, which is
                now.

        Returns:
            A new :class:`RequestSpan`.
        """
