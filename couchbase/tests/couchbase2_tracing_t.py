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

"""Each couchbase2 attempt's span, and the trace context it sends, against an in-process gateway."""

from __future__ import annotations

import sys
import types
from collections.abc import (Callable,
                             Coroutine,
                             Iterator)
from typing import Any, cast

import grpc
import pytest

from tests.couchbase2_gateway import (Fail,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)

pytest.importorskip('opentelemetry.sdk')

from opentelemetry import trace as otel_trace  # noqa: E402
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402
from opentelemetry.trace import StatusCode  # noqa: E402

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl  # noqa: E402
from couchbase.auth import PasswordAuthenticator  # noqa: E402
from couchbase.exceptions import DocumentNotFoundException  # noqa: E402
from couchbase.observability.otel_tracing import get_otel_tracer  # noqa: E402
from couchbase.options import ClusterOptions  # noqa: E402
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl  # noqa: E402
from couchbase.protocol.couchbase2._collection_impl import CollectionImpl  # noqa: E402
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2, kv_pb2_grpc  # noqa: E402
from couchbase.protocol.couchbase2._tracing import grpc_instrumented, rpc_names  # noqa: E402
from tests.couchbase2_gateway import Certificates  # noqa: E402
from tests.event_loop import run_on_own_loop  # noqa: E402
from tests.observability_test_impls import RecordingTracer  # noqa: E402

KEY = ('travel', 'inventory', 'hotels', 'hotel_1')
UPSERT_RPC = 'couchbase.kv.v1.KvService/Upsert'
GET_RPC = 'couchbase.kv.v1.KvService/Get'
WAIT_RPC = 'couchbase.admin.query.v1.QueryAdminService/WaitForIndexOnline'
INSTRUMENTATION = 'opentelemetry.instrumentation.grpc'


@pytest.fixture(scope='module')
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Certificates:
    made = make_certificates(tmp_path_factory.mktemp('pki'))
    if made is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    return made


@pytest.fixture
def gateway() -> FakeGateway:
    return new_gateway()


@pytest.fixture
def connect(gateway: FakeGateway, certificates: Certificates) -> Iterator[Callable[..., tuple[str, ClusterOptions]]]:
    with serving(gateway, certificates) as port:
        def arguments(**kwargs: Any) -> tuple[str, ClusterOptions]:
            return (f'couchbase2://localhost:{port}',
                    ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                   trust_store_path=str(certificates.ca), **kwargs))
        yield arguments


@pytest.fixture
def exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def provider(exporter: InMemorySpanExporter) -> TracerProvider:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


@pytest.fixture
def instrumented(monkeypatch: pytest.MonkeyPatch) -> list[otel_trace.Span]:
    """What `opentelemetry-instrumentation-grpc` does to a channel, and the span current at each call.

    It patches `grpc.secure_channel` to intercept every channel opened after it is enabled, and
    reports itself enabled on its instrumentor.
    """
    seen: list[otel_trace.Span] = []

    class Interceptor(grpc.UnaryUnaryClientInterceptor):
        def intercept_unary_unary(self, continuation: Any, details: Any, request: Any) -> Any:
            seen.append(otel_trace.get_current_span())
            return continuation(details, request)

    secure_channel = grpc.secure_channel
    monkeypatch.setattr(grpc, 'secure_channel',
                        lambda *args, **kwargs: grpc.intercept_channel(secure_channel(*args, **kwargs), Interceptor()))
    monkeypatch.setitem(sys.modules, INSTRUMENTATION, _instrumentation(sync=True, aio=False))
    return seen


def _instrumentation(*, sync: bool, aio: bool) -> types.ModuleType:
    def instrumentor(enabled: bool) -> Callable[[], Any]:
        return lambda: types.SimpleNamespace(is_instrumented_by_opentelemetry=enabled)

    return cast(types.ModuleType, types.SimpleNamespace(GrpcInstrumentorClient=instrumentor(sync),
                                                        GrpcAioInstrumentorClient=instrumentor(aio)))


def _collection(cluster: ClusterImpl) -> CollectionImpl:
    return cluster.bucket(KEY[0]).scope(KEY[1]).collection(KEY[2])


def _context(span: ReadableSpan) -> otel_trace.SpanContext:
    context = span.get_span_context()
    assert context is not None
    return context


def _traceparent(span: ReadableSpan) -> str:
    context = _context(span)
    return f'00-{context.trace_id:032x}-{context.span_id:016x}-01'


def _named(exporter: InMemorySpanExporter, name: str) -> list[ReadableSpan]:
    return [span for span in exporter.get_finished_spans() if span.name == name]


def _run(coroutine: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    return run_on_own_loop(coroutine())


class Couchbase2TracingUnitTests:

    def test_each_stub_method_is_named_for_its_rpc(self) -> None:
        with grpc.insecure_channel('localhost:1') as channel:
            kv = kv_pb2_grpc.KvServiceStub(channel)
            names = rpc_names((kv,))
        assert names[id(kv.Get)] == GET_RPC
        assert names[id(kv.Upsert)] == UPSERT_RPC
        assert len(names) == len(kv_pb2.DESCRIPTOR.services_by_name['KvService'].methods)

    def test_the_instrumentation_is_read_only_once_imported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delitem(sys.modules, INSTRUMENTATION, raising=False)
        assert not grpc_instrumented(aio=False)
        monkeypatch.setitem(sys.modules, INSTRUMENTATION, _instrumentation(sync=True, aio=False))
        assert grpc_instrumented(aio=False)
        assert not grpc_instrumented(aio=True)

    def test_an_attempt_sends_the_trace_context_of_its_own_span(self,
                                                                connect: Callable[..., tuple[str, ClusterOptions]],
                                                                gateway: FakeGateway,
                                                                provider: TracerProvider,
                                                                exporter: InMemorySpanExporter) -> None:
        cluster = ClusterImpl(*connect(tracer=get_otel_tracer(provider)))
        try:
            _collection(cluster).upsert(KEY[3], {'name': 'Ritz'})
        finally:
            cluster.close_connection()
        [operation] = _named(exporter, 'upsert')
        [call] = _named(exporter, UPSERT_RPC)
        assert call.parent is not None and call.parent.span_id == _context(operation).span_id
        assert gateway.calls[0].metadata['traceparent'] == _traceparent(call)
        assert operation.attributes is not None and operation.attributes['couchbase.retries'] == 0

    def test_each_retry_is_an_attempt_with_a_span_of_its_own(self,
                                                             connect: Callable[..., tuple[str, ClusterOptions]],
                                                             gateway: FakeGateway,
                                                             provider: TracerProvider,
                                                             exporter: InMemorySpanExporter) -> None:
        gateway.failures['Upsert'] = [Fail(grpc.StatusCode.UNAVAILABLE)]
        cluster = ClusterImpl(*connect(tracer=get_otel_tracer(provider)))
        try:
            _collection(cluster).upsert(KEY[3], {'name': 'Ritz'})
        finally:
            cluster.close_connection()
        [operation] = _named(exporter, 'upsert')
        calls = _named(exporter, UPSERT_RPC)
        assert [call.status.status_code for call in calls] == [StatusCode.ERROR, StatusCode.UNSET]
        assert [c.metadata['traceparent'] for c in gateway.calls] == [_traceparent(call) for call in calls]
        assert operation.attributes is not None and operation.attributes['couchbase.retries'] == 1

    def test_a_failed_operation_records_its_exception(self,
                                                      connect: Callable[..., tuple[str, ClusterOptions]],
                                                      provider: TracerProvider,
                                                      exporter: InMemorySpanExporter) -> None:
        cluster = ClusterImpl(*connect(tracer=get_otel_tracer(provider)))
        try:
            with pytest.raises(DocumentNotFoundException):
                _collection(cluster).get(KEY[3])
        finally:
            cluster.close_connection()
        [operation] = _named(exporter, 'get')
        assert operation.status.status_code == StatusCode.ERROR
        [event] = operation.events
        assert event.name == 'exception'
        assert event.attributes is not None
        assert set(event.attributes) == {'exception.type', 'exception.message', 'exception.stacktrace'}
        assert str(event.attributes['exception.type']).endswith('.DocumentNotFoundException')
        assert 'Traceback' in str(event.attributes['exception.stacktrace'])

    def test_each_key_of_a_multi_operation_is_traced_under_it(self,
                                                              connect: Callable[..., tuple[str, ClusterOptions]],
                                                              provider: TracerProvider,
                                                              exporter: InMemorySpanExporter) -> None:
        cluster = ClusterImpl(*connect(tracer=get_otel_tracer(provider)))
        try:
            _collection(cluster).upsert_multi({'hotel_1': {'name': 'Ritz'}, 'hotel_2': {'name': 'Savoy'}})
        finally:
            cluster.close_connection()
        [operation] = _named(exporter, 'upsert_multi')
        calls = _named(exporter, UPSERT_RPC)
        assert len(calls) == 2
        assert all(call.parent is not None and call.parent.span_id == _context(operation).span_id for call in calls)

    def test_a_tracer_without_a_trace_context_sends_none(self,
                                                         connect: Callable[..., tuple[str, ClusterOptions]],
                                                         gateway: FakeGateway) -> None:
        tracer = RecordingTracer()
        cluster = ClusterImpl(*connect(tracer=tracer))
        try:
            _collection(cluster).upsert(KEY[3], {'name': 'Ritz'})
        finally:
            cluster.close_connection()
        operation = tracer.get_span_by_name('upsert')
        assert operation is not None and [c.name for c in operation.children] == ['request_encoding', UPSERT_RPC]
        assert 'traceparent' not in gateway.calls[0].metadata

    def test_the_instrumentation_makes_the_span_under_the_operation(
            self,
            connect: Callable[..., tuple[str, ClusterOptions]],
            gateway: FakeGateway,
            provider: TracerProvider,
            exporter: InMemorySpanExporter,
            instrumented: list[otel_trace.Span]) -> None:
        cluster = ClusterImpl(*connect(tracer=get_otel_tracer(provider)))
        try:
            _collection(cluster).upsert(KEY[3], {'name': 'Ritz'})
        finally:
            cluster.close_connection()
        [operation] = _named(exporter, 'upsert')
        assert not _named(exporter, UPSERT_RPC)
        assert 'traceparent' not in gateway.calls[0].metadata
        assert [span.get_span_context().span_id for span in instrumented] == [_context(operation).span_id]
        assert otel_trace.get_current_span() is otel_trace.INVALID_SPAN

    def test_an_async_attempt_sends_the_trace_context_of_its_own_span(
            self,
            connect: Callable[..., tuple[str, ClusterOptions]],
            gateway: FakeGateway,
            provider: TracerProvider,
            exporter: InMemorySpanExporter) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect(tracer=get_otel_tracer(provider)))
            try:
                await impl.bucket(KEY[0]).scope(KEY[1]).collection(KEY[2]).upsert(KEY[3], {'name': 'Ritz'})
            finally:
                await impl.close_connection()

        _run(run)
        [call] = _named(exporter, UPSERT_RPC)
        assert gateway.calls[0].metadata['traceparent'] == _traceparent(call)
