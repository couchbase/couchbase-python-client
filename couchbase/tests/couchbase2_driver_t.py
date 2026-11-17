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

import threading
import time
from collections.abc import (Callable,
                             Iterator,
                             Sequence)
from concurrent import futures
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol

import grpc
import grpc.aio
import pytest
from google.protobuf import any_pb2
from google.rpc import error_details_pb2, status_pb2

from couchbase.common._operations import Op
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CouchbaseException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  GenericErrorContext,
                                  InvalidArgumentException,
                                  RequestCanceledException,
                                  UnAmbiguousTimeoutException)
from couchbase.protocol.couchbase2._driver import (Metadata,
                                                   call_stream,
                                                   call_stream_async,
                                                   call_unary,
                                                   call_unary_async)
from couchbase.protocol.couchbase2._error_mapping import STATUS_DETAILS_KEY
from couchbase.protocol.couchbase2._request_context import RequestContext
from couchbase.protocol.couchbase2._retry import RetryReason
from tests.event_loop import run_on_own_loop

Code = grpc.StatusCode

UNARY = '/test.Scripted/Unary'
STREAM = '/test.Scripted/Stream'
METADATA: Metadata = (('authorization', 'Basic dXNlcjpwYXNz'),)


@dataclass(frozen=True)
class _Fail:
    code: Code
    trailer: bytes | None = None


class _Hang:
    """Answer nothing until the client gives up on the call."""


HANG = _Hang()
UNAVAILABLE = _Fail(Code.UNAVAILABLE)


def _locked() -> _Fail:
    detail = any_pb2.Any()
    detail.Pack(error_details_pb2.PreconditionFailure(
        violations=[error_details_pb2.PreconditionFailure.Violation(type='LOCKED')]))
    rich = status_pb2.Status(code=Code.FAILED_PRECONDITION.value[0], message='locked', details=[detail])
    return _Fail(Code.FAILED_PRECONDITION, rich.SerializeToString())


def _missing_document() -> _Fail:
    detail = any_pb2.Any()
    detail.Pack(error_details_pb2.ResourceInfo(resource_type='document'))
    rich = status_pb2.Status(code=Code.NOT_FOUND.value[0], message='not found', details=[detail])
    return _Fail(Code.NOT_FOUND, rich.SerializeToString())


UnaryStep = bytes | _Fail | _Hang
# The messages a stream sends, then how it ends: OK when None.
StreamStep = tuple[Sequence[bytes], _Fail | _Hang | None]


@dataclass
class _Attempt:
    # When the call's deadline falls, by the server's clock.
    deadline: float
    metadata: dict[str, Any]


@dataclass
class _Server:
    """Answers each call with the next step of its script, repeating the last once the script runs out."""

    unary: list[UnaryStep] = field(default_factory=list)
    stream: list[StreamStep] = field(default_factory=list)
    attempts: list[_Attempt] = field(default_factory=list)
    # Set when a call left hanging is cancelled by the client.
    cancelled: threading.Event = field(default_factory=threading.Event)

    def _next(self, script: list[Any], context: grpc.ServicerContext) -> Any:
        remaining = context.time_remaining()
        assert remaining is not None, 'the call was sent without a deadline'
        self.attempts.append(_Attempt(time.monotonic() + remaining, dict(context.invocation_metadata())))
        return script.pop(0) if len(script) > 1 else script[0]

    def _end(self, end: _Fail | _Hang, context: grpc.ServicerContext) -> None:
        if isinstance(end, _Hang):
            while context.is_active():
                time.sleep(0.01)
            self.cancelled.set()
            return
        if end.trailer is not None:
            context.set_trailing_metadata(((STATUS_DETAILS_KEY, end.trailer),))
        context.abort(end.code, 'scripted')

    def handle_unary(self, request: bytes, context: grpc.ServicerContext) -> bytes:
        step = self._next(self.unary, context)
        if isinstance(step, bytes):
            return step
        self._end(step, context)
        return b''

    def handle_stream(self, request: bytes, context: grpc.ServicerContext) -> Iterator[bytes]:
        messages, end = self._next(self.stream, context)
        yield from messages
        if end is not None:
            self._end(end, context)


@contextmanager
def _serving(server: _Server) -> Iterator[str]:
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    grpc_server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('test.Scripted', {
        'Unary': grpc.unary_unary_rpc_method_handler(server.handle_unary),
        'Stream': grpc.unary_stream_rpc_method_handler(server.handle_stream),
    }),))
    port = grpc_server.add_insecure_port('localhost:0')
    grpc_server.start()
    try:
        yield f'localhost:{port}'
    finally:
        grpc_server.stop(None)


@dataclass
class _Read:
    messages: list[bytes]
    error: CouchbaseException | None


class _Driver(Protocol):
    def unary(self, target: str, ctx: RequestContext) -> bytes: ...

    def stream(self,
               target: str,
               ctx: RequestContext,
               take: int | None = None,
               closed: Callable[[], None] = lambda: None) -> _Read:
        """Read up to `take` messages, then close the stream and call `closed` while the channel is still open.

        A stream that fails must then be finished.
        """
        ...


class _Sync:

    def unary(self, target: str, ctx: RequestContext) -> bytes:
        with grpc.insecure_channel(target) as channel:
            return call_unary(channel.unary_unary(UNARY, bytes, bytes), b'', ctx, METADATA)

    def stream(self,
               target: str,
               ctx: RequestContext,
               take: int | None = None,
               closed: Callable[[], None] = lambda: None) -> _Read:
        messages: list[bytes] = []
        with grpc.insecure_channel(target) as channel:
            stream = call_stream(channel.unary_stream(STREAM, bytes, bytes), b'', ctx, METADATA)
            try:
                for message in stream:
                    messages.append(message)
                    if len(messages) == take:
                        stream.close()
                        closed()
                        break
            except CouchbaseException as e:
                assert next(stream, None) is None
                return _Read(messages, e)
        return _Read(messages, None)


class _Async:

    def unary(self, target: str, ctx: RequestContext) -> bytes:
        async def run() -> bytes:
            async with grpc.aio.insecure_channel(target) as channel:
                return await call_unary_async(channel.unary_unary(UNARY, bytes, bytes), b'', ctx, METADATA)

        return run_on_own_loop(run())

    def stream(self,
               target: str,
               ctx: RequestContext,
               take: int | None = None,
               closed: Callable[[], None] = lambda: None) -> _Read:
        async def run() -> _Read:
            messages: list[bytes] = []
            async with grpc.aio.insecure_channel(target) as channel:
                stream = call_stream_async(channel.unary_stream(STREAM, bytes, bytes), b'', ctx, METADATA)
                try:
                    async for message in stream:
                        messages.append(message)
                        if len(messages) == take:
                            await stream.aclose()
                            closed()
                            break
                except CouchbaseException as e:
                    assert await anext(stream, None) is None
                    return _Read(messages, e)
            return _Read(messages, None)

        return run_on_own_loop(run())


@pytest.fixture(params=['sync', 'aio'])
def driver(request: pytest.FixtureRequest) -> _Driver:
    return _Sync() if request.param == 'sync' else _Async()


def _context(op: Op, timeout: float = 5.0) -> RequestContext:
    return RequestContext(op, timeout)


def _error_context(exc: BaseException | None) -> GenericErrorContext:
    assert isinstance(exc, CouchbaseException)
    assert isinstance(exc.error_context, GenericErrorContext)
    return exc.error_context


class UnaryUnitTests:

    def test_unavailable_twice_then_success(self, driver: _Driver) -> None:
        server = _Server(unary=[UNAVAILABLE, UNAVAILABLE, b'done'])
        ctx = _context(Op.Upsert)
        with _serving(server) as target:
            assert driver.unary(target, ctx) == b'done'
        assert len(server.attempts) == 3
        assert ctx.retry_attempts == 2
        assert ctx.retry_reasons == (RetryReason.SERVICE_NOT_AVAILABLE,)

    @pytest.mark.parametrize('op', [Op.Get, Op.Upsert])
    def test_locked_until_the_deadline(self, driver: _Driver, op: Op) -> None:
        server = _Server(unary=[_locked()])
        timeout = 0.3
        start = time.monotonic()
        with _serving(server) as target, pytest.raises(DocumentLockedException) as raised:
            driver.unary(target, _context(op, timeout))
        assert timeout <= time.monotonic() - start < timeout + 1.0
        # The first four controlled backoffs fit in the timeout, so at least five attempts are made.
        assert len(server.attempts) >= 5
        context = _error_context(raised.value)
        assert context.retry_attempts == len(server.attempts)
        assert context.retry_reasons == {'key_value_locked'}
        assert context.last_exception == 'DocumentLockedException(locked), caused by gRPC status FAILED_PRECONDITION'

    def test_a_failure_not_retried_is_raised_at_once(self, driver: _Driver) -> None:
        server = _Server(unary=[_missing_document(), b'done'])
        with _serving(server) as target, pytest.raises(DocumentNotFoundException) as raised:
            driver.unary(target, _context(Op.Get))
        assert len(server.attempts) == 1
        assert _error_context(raised.value).retry_attempts == 0

    def test_every_attempt_shares_one_deadline(self, driver: _Driver) -> None:
        # Five failures put their backoff between the first attempt and the last.
        server = _Server(unary=[*(UNAVAILABLE,) * 5, b'done'])
        with _serving(server) as target:
            driver.unary(target, _context(Op.Get, timeout=5.0))
        deadlines = [attempt.deadline for attempt in server.attempts]
        assert len(deadlines) == 6
        assert max(deadlines) - min(deadlines) < 0.1

    def test_every_attempt_carries_the_metadata(self, driver: _Driver) -> None:
        server = _Server(unary=[UNAVAILABLE, b'done'])
        with _serving(server) as target:
            driver.unary(target, _context(Op.Get))
        assert [attempt.metadata.get('authorization') for attempt in server.attempts] == [METADATA[0][1]] * 2

    def test_a_server_that_never_answers_times_out(self, driver: _Driver) -> None:
        server = _Server(unary=[HANG])
        with _serving(server) as target, pytest.raises(UnAmbiguousTimeoutException):
            driver.unary(target, _context(Op.Get, timeout=0.2))
        assert server.cancelled.wait(5)


class StreamUnitTests:

    def test_unavailable_before_the_first_message_is_retried(self, driver: _Driver) -> None:
        server = _Server(stream=[((), UNAVAILABLE), ((b'a', b'b'), None)])
        ctx = _context(Op.Query)
        with _serving(server) as target:
            read = driver.stream(target, ctx)
        assert read == _Read([b'a', b'b'], None)
        assert len(server.attempts) == 2
        assert ctx.retry_reasons == (RetryReason.SERVICE_NOT_AVAILABLE,)

    def test_unavailable_partway_through_cancels(self, driver: _Driver) -> None:
        server = _Server(stream=[((b'a',), UNAVAILABLE), ((b'b',), None)])
        with _serving(server) as target:
            read = driver.stream(target, _context(Op.Query))
        assert read.messages == [b'a']
        assert isinstance(read.error, RequestCanceledException)
        assert len(server.attempts) == 1

    def test_an_empty_first_message_latches(self, driver: _Driver) -> None:
        server = _Server(stream=[((b'',), UNAVAILABLE), ((b'b',), None)])
        with _serving(server) as target:
            read = driver.stream(target, _context(Op.Query))
        assert read.messages == [b'']
        assert isinstance(read.error, RequestCanceledException)
        assert len(server.attempts) == 1

    def test_partway_through_a_failure_that_does_not_retry_maps_as_usual(self, driver: _Driver) -> None:
        server = _Server(stream=[((b'a',), _Fail(Code.INVALID_ARGUMENT))])
        with _serving(server) as target:
            read = driver.stream(target, _context(Op.Query))
        assert read.messages == [b'a']
        assert isinstance(read.error, InvalidArgumentException)

    def test_the_deadline_covers_the_whole_stream(self, driver: _Driver) -> None:
        server = _Server(stream=[((b'a',), HANG)])
        with _serving(server) as target:
            read = driver.stream(target, _context(Op.Query, timeout=0.2))
        assert read.messages == [b'a']
        assert isinstance(read.error, AmbiguousTimeoutException)

    def test_a_failure_before_the_first_message_is_not_replayed(self, driver: _Driver) -> None:
        server = _Server(stream=[((), _missing_document()), ((b'a',), None)])
        with _serving(server) as target:
            read = driver.stream(target, _context(Op.Query))
        assert read.messages == []
        assert isinstance(read.error, DocumentNotFoundException)
        assert len(server.attempts) == 1

    def test_an_empty_stream(self, driver: _Driver) -> None:
        server = _Server(stream=[((), None)])
        with _serving(server) as target:
            assert driver.stream(target, _context(Op.Query)) == _Read([], None)

    def test_nothing_is_sent_until_the_first_message_is_asked_for(self) -> None:
        server = _Server(stream=[((b'a',), None)])
        with _serving(server) as target, grpc.insecure_channel(target) as channel:
            stream = call_stream(channel.unary_stream(STREAM, bytes, bytes), b'', _context(Op.Query), METADATA)
            assert server.attempts == []
            assert list(stream) == [b'a']

    def test_closing_early_cancels_the_call(self, driver: _Driver) -> None:
        server = _Server(stream=[((b'a',), HANG)])

        def closed() -> None:
            assert server.cancelled.wait(5)

        with _serving(server) as target:
            assert driver.stream(target, _context(Op.Query), take=1, closed=closed) == _Read([b'a'], None)
