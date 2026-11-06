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

#

__all__ = [
    'CouchbaseOtelSpan',
    'CouchbaseOtelTracer',
]

import sys
from typing import Any

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from opentelemetry.trace import (Span,
                                 Tracer,
                                 set_span_in_context)

from couchbase.common._tracing import CouchbaseSpan, CouchbaseTracer


class CouchbaseOtelSpan(CouchbaseSpan):
    """**DEPRECATED** A span over an OpenTelemetry span, for the deprecated tracer interface.

    .. deprecated:: 4.6.0
        Use :func:`~couchbase.observability.get_otel_tracer`, whose spans are
        :class:`~couchbase.observability.tracing.RequestSpan`.

    Args:
        otel_span (opentelemetry.trace.Span): The span it writes to.
    """

    def __init__(self, otel_span: Span) -> None:
        super().__init__(otel_span)

    @override
    def set_attribute(self, key: str, value: Any) -> None:
        self.span.set_attribute(key=key, value=value)

    @override
    def finish(self) -> None:
        self.span.end()


class CouchbaseOtelTracer(CouchbaseTracer):
    """**DEPRECATED** A tracer over an OpenTelemetry tracer, for the deprecated tracer interface.

    .. deprecated:: 4.6.0
        Use :func:`~couchbase.observability.get_otel_tracer`, which returns a
        :class:`~couchbase.observability.tracing.RequestTracer`.

    Args:
        otel_tracer (opentelemetry.trace.Tracer): The tracer it starts spans with.
    """

    def __init__(self, otel_tracer: Tracer) -> None:
        super().__init__(otel_tracer)

    def start_span(self, name: str, parent: CouchbaseSpan | None = None) -> CouchbaseOtelSpan:
        """Start a span, as a child of a parent span if one is given.

        Args:
            name (str): The name of the operation.
            parent (CouchbaseSpan, optional): The span of the operation this one is part of.

        Returns:
            :class:`CouchbaseOtelSpan`: The new span.
        """
        kwargs: dict[str, Any] = {}
        if parent:
            kwargs['context'] = set_span_in_context(parent.span)
        return CouchbaseOtelSpan(
            self._external_tracer.start_span(name, **kwargs))

    def __deepcopy__(self, memo: dict[int, Any]) -> None:
        """This prevents deepcopies, as the underlying opentelemetry tracer doesn't support a deepcopy.

        Args:
            memo: The object that we are copying into.

        Returns:
            The copy - None in this case
        """
        memo[id(self)] = None
        return None
