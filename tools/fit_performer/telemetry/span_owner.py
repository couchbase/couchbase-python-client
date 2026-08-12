from __future__ import annotations

import logging
import threading
from typing import Dict, Optional

# [if:4.6.0]
from opentelemetry import trace as otel_trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from couchbase.observability.otel_tracing import OtelWrapperSpan

# [end]


class SpanOwner:

    def __init__(self):
        self._lock = threading.Lock()
        self._spans: Dict[str, object] = {}
        self._logger = logging.getLogger(__name__)

    def get_span(self, span_id: str) -> Optional[object]:
        with self._lock:
            return self._spans.get(span_id)

    def create_span(self, tracer, request):
        parent_span = None
        if request.HasField('parent_span_id'):
            parent_span = self.get_span(request.parent_span_id)
            if parent_span is None:
                raise KeyError(f"Parent span '{request.parent_span_id}' not found")

        span = tracer.request_span(name=request.name, parent_span=parent_span)

        for key, attr in request.attributes.items():
            which = attr.WhichOneof('value')
            if which == 'value_string':
                span.set_attribute(key, attr.value_string)
            elif which == 'value_long':
                span.set_attribute(key, attr.value_long)
            elif which == 'value_boolean':
                span.set_attribute(key, attr.value_boolean)

        with self._lock:
            self._spans[request.id] = span

    def finish_span(self, request):
        with self._lock:
            span = self._spans.pop(request.id, None)

        if span is None:
            raise KeyError(f"Span '{request.id}' not found")

        span.end()

    def clear(self):
        with self._lock:
            self._spans.clear()

    # [if:4.6.0]
    def export_contexts(self) -> Dict[str, str]:
        prop = TraceContextTextMapPropagator()
        result = {}
        with self._lock:
            for span_id, span in self._spans.items():
                if hasattr(span, '_otel_span'):
                    carrier = {}
                    prop.inject(carrier, context=otel_trace.set_span_in_context(span._otel_span))
                    if 'traceparent' in carrier:
                        result[span_id] = carrier['traceparent']
        return result

    @classmethod
    def from_worker_contexts(cls, span_contexts: Dict[str, str]) -> SpanOwner:
        """Build a SpanOwner populated with remote-parent proxies from W3C traceparents."""
        owner = cls()
        prop = TraceContextTextMapPropagator()

        for span_id, traceparent in span_contexts.items():
            carrier = {'traceparent': traceparent}
            extracted_context = prop.extract(carrier)
            nrs = otel_trace.get_current_span(extracted_context)
            proxy = OtelWrapperSpan(nrs, name=span_id)
            with owner._lock:
                owner._spans[span_id] = proxy

        return owner
    # [end]
