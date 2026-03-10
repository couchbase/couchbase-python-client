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

import typing
from typing import Optional

from couchbase._version import __version__
from couchbase.observability.tracing import (RequestSpan,
                                             RequestTracer,
                                             SpanAttributes,
                                             SpanAttributeValue,
                                             SpanStatusCode)

if typing.TYPE_CHECKING:
    from opentelemetry import trace as otel_trace
    from opentelemetry.trace import TracerProvider

try:
    from opentelemetry import trace as otel_trace  # noqa: F811
    from opentelemetry.context import Context
    from opentelemetry.trace import SpanKind
    from opentelemetry.trace.status import Status as OtelStatus
    from opentelemetry.trace.status import StatusCode as OtelStatusCode
    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False


if HAS_OTEL:  # noqa: C901

    STATUS_MAP = {
        SpanStatusCode.UNSET: OtelStatusCode.UNSET,
        SpanStatusCode.OK: OtelStatusCode.OK,
        SpanStatusCode.ERROR: OtelStatusCode.ERROR,
    }

    class OtelWrapperSpan(RequestSpan):
        def __init__(self, otel_span: otel_trace.Span, name: str):
            self._otel_span = otel_span
            self._name = name

        @property
        def name(self) -> str:
            return self._name

        def set_attribute(self, key: str, value: SpanAttributeValue) -> None:
            self._otel_span.set_attribute(key, value)

        def set_attributes(self, attributes: SpanAttributes) -> None:
            self._otel_span.set_attributes(attributes)

        def add_event(
            self,
            name: str,
            attributes: Optional[SpanAttributes] = None,
            timestamp: Optional[int] = None
        ) -> None:

            self._otel_span.add_event(name, attributes, timestamp)

        def set_status(self, status: SpanStatusCode) -> None:
            otel_status = OtelStatus(STATUS_MAP.get(status, OtelStatusCode.UNSET))
            self._otel_span.set_status(otel_status)

        def end(self, end_time: Optional[int] = None) -> None:
            self._otel_span.end(end_time=end_time)

    class OtelWrapperTracer(RequestTracer):
        def __init__(self, tracer: otel_trace.Tracer):
            self._tracer = tracer

        def request_span(
            self,
            name: str,
            parent_span: Optional[RequestSpan] = None,
            start_time: Optional[int] = None
        ) -> RequestSpan:

            otel_context: Optional[Context] = None

            # if we have a parent span that is wrapped, take the raw OTel span and inject it into a new OTel context
            if parent_span and isinstance(parent_span, OtelWrapperSpan):
                raw_otel_span = parent_span._otel_span
                otel_context = otel_trace.set_span_in_context(raw_otel_span)

            span = self._tracer.start_span(
                name=name,
                context=otel_context,
                kind=SpanKind.CLIENT,
                start_time=start_time
            )

            return OtelWrapperSpan(span, name)


def get_otel_tracer(provider: Optional[TracerProvider] = None) -> RequestTracer:
    """
    Creates an OpenTelemetry wrapper tracer.

    Args:
        provider: Optional OpenTelemetry TracerProvider. If not provided,
                  falls back to the global OTel tracer provider.

    Returns:
        OtelWrapperTracer instance implementing SDK's RequestTracer interface

    Raises:
        ImportError: If OpenTelemetry is not installed
    """
    if not HAS_OTEL:
        raise ImportError(
            "OpenTelemetry is not installed. Please install with: "
            "pip install couchbase[otel]"
        )

    pkg_name = "com.couchbase.client/python"
    pkg_version = __version__

    if provider:
        otel_tracer = provider.get_tracer(pkg_name, pkg_version)
    else:
        otel_tracer = otel_trace.get_tracer(pkg_name, pkg_version)

    return OtelWrapperTracer(otel_tracer)
