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

"""The span each attempt of a couchbase2 request is traced with, and the trace context it sends.

An attempt is one gRPC call.  It gets a span under the operation's, named for the RPC as the gateway
names its own span for the call.  Only an OpenTelemetry span has a W3C trace context, so only under
`OtelWrapperTracer` does the call send one, which is what makes the gateway's span a child of it.

`opentelemetry-instrumentation-grpc` makes that span and sends that context itself, for every
channel opened after it is enabled.  When it was enabled before this cluster's channel opened, an
attempt makes neither and only makes the operation's span current while the call starts, which is
what puts the instrumentation's span under it.  The SDK does not depend on the package: it is
looked for among the modules already imported.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._observability.handler import WrappedSpan
from couchbase.common._observability.observability_types import (_ATTR_DISPATCH_SPAN_NAME,
                                                                 _ATTR_SYSTEM_NAME,
                                                                 _DISP_SERVER_ADDRESS,
                                                                 _DISP_SERVER_PORT,
                                                                 RequestSpanProtocol)
from couchbase.protocol.couchbase2._connstr import Endpoint

try:
    from opentelemetry import context as otel_context
    from opentelemetry import propagate as otel_propagate
    from opentelemetry import trace as otel_trace

    from couchbase.common.observability.otel_tracing import OtelWrapperSpan
    _HAS_OTEL = True
except ImportError:
    _HAS_OTEL = False

if TYPE_CHECKING:
    from contextvars import Token

    from opentelemetry.context import Context

Metadata = tuple[tuple[str, str], ...]

_GRPC_INSTRUMENTATION = 'opentelemetry.instrumentation.grpc'


def grpc_instrumented(aio: bool) -> bool:
    """Whether a channel opened now gets `opentelemetry-instrumentation-grpc`'s client interceptors."""
    module = sys.modules.get(_GRPC_INSTRUMENTATION)
    if module is None:
        return False
    instrumentor = module.GrpcAioInstrumentorClient if aio else module.GrpcInstrumentorClient
    return bool(instrumentor().is_instrumented_by_opentelemetry)


def rpc_names(stubs: Sequence[object]) -> dict[int, str]:
    """Each stub method, by `id`, to the full name of the RPC it calls, as `<service>/<method>`."""
    names: dict[int, str] = {}
    for stub in stubs:
        stub_type = type(stub)
        pb2 = sys.modules[stub_type.__module__.removesuffix('_grpc')]
        service = pb2.DESCRIPTOR.services_by_name[stub_type.__name__.removesuffix('Stub')]
        for method in service.methods:
            names[id(getattr(stub, method.name))] = f'{service.full_name}/{method.name}'
    return names


class Attempt:
    """One attempt's span, if it has one, and the metadata its call sends.

    Used as a context manager, it is current while the call starts.  A unary attempt ends when the
    block does; a stream's ends when `end` is called, or when the block raises.
    """

    __slots__ = ('metadata', '_stream', '_wrapped', '_span', '_failed', '_current', '_token')

    def __init__(self,
                 metadata: Metadata,
                 *,
                 stream: bool,
                 wrapped: WrappedSpan | None = None,
                 span: RequestSpanProtocol | None = None,
                 current: Any = None) -> None:
        self.metadata = metadata
        self._stream = stream
        self._wrapped = wrapped
        self._span = span
        self._failed = False
        # An OpenTelemetry context to make current, when the instrumentation makes the span.
        self._current = current
        self._token: Token[Context] | None = None

    def __enter__(self) -> Attempt:
        if self._current is not None:
            self._token = otel_context.attach(self._current)
        return self

    def __exit__(self,
                 exc_type: type[BaseException] | None,
                 exc_val: BaseException | None,
                 exc_tb: TracebackType | None) -> None:
        if self._token is not None:
            otel_context.detach(self._token)
            self._token = None
        if exc_val is not None:
            self.fail()
        if exc_val is not None or not self._stream:
            self.end()

    def fail(self) -> None:
        self._failed = True

    def end(self) -> None:
        if self._span is not None and self._wrapped is not None:
            self._wrapped.end_dispatch_span(self._span, self._failed)
            self._span = None


class GrpcTracing:
    """How the attempts of every request on one cluster's channel are traced.

    It is made with the cluster and filled in when the channel opens, which for an async cluster is
    at its first call.
    """

    def __init__(self, endpoint: Endpoint) -> None:
        self._endpoint = endpoint
        self._names: dict[int, str] = {}
        self._instrumented = False

    def channel_opened(self, stubs: Sequence[object], *, aio: bool) -> None:
        self._names = rpc_names(stubs)
        self._instrumented = grpc_instrumented(aio)

    def attempt(self,
                call: object,
                obs_handler: ObservableRequestHandler | None,
                metadata: Metadata,
                *,
                stream: bool) -> Attempt:
        wrapped = obs_handler.wrapped_span if obs_handler is not None else None
        if wrapped is None or not wrapped.is_recording:
            return Attempt(metadata, stream=stream)

        request_span = wrapped.request_span
        otel_span = request_span._otel_span if _HAS_OTEL and isinstance(request_span, OtelWrapperSpan) else None
        if otel_span is not None and self._instrumented:
            return Attempt(metadata, stream=stream, current=otel_trace.set_span_in_context(otel_span))

        span = wrapped.start_dispatch_span(self._names.get(id(call), _ATTR_DISPATCH_SPAN_NAME))
        span.set_attribute(_ATTR_SYSTEM_NAME, 'couchbase')
        span.set_attribute(_DISP_SERVER_ADDRESS, self._endpoint.host)
        span.set_attribute(_DISP_SERVER_PORT, self._endpoint.port)
        if otel_span is not None and isinstance(span, OtelWrapperSpan):
            carrier: dict[str, str] = {}
            otel_propagate.inject(carrier, context=otel_trace.set_span_in_context(span._otel_span))
            metadata = metadata + tuple(carrier.items())
        return Attempt(metadata, stream=stream, wrapped=wrapped, span=span)
