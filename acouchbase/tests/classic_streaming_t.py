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

# Hand-written and neither generated nor merged.  Classic's asyncio executor for the streams, which
# awaits the core's callback and then reads on the loop, yielding it between rows.  It is driven
# through `AsyncRowIterator`, over a stand-in for the binding's connection.
from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any, cast

import pytest

from acouchbase.protocol.classic._streaming import ClassicAsyncStreamingExecutor, response_signal
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._streaming import AsyncRowIterator
from couchbase.exceptions import InternalSDKException
from couchbase.protocol.classic._pycbc_core import pycbc_connection, pycbc_exception
from couchbase.protocol.classic._streaming import QueryStreamSpec
from couchbase.serializer import DefaultJsonSerializer
from tests.test_features import TestProtocol

# What is checked here is classic's streaming executors.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)


class _Stream:
    # The core's streamed result, fully queued, as it is by the time the core calls back.

    def __init__(self, items: list[Any]) -> None:
        self._items = list(items)
        self.cancel_calls = 0
        self.read_threads: list[int] = []

    def __iter__(self) -> _Stream:
        return self

    def __next__(self) -> Any:
        self.read_threads.append(threading.get_ident())
        if not self._items:
            raise StopIteration
        return self._items.pop(0)

    def cancel(self) -> None:
        self.cancel_calls += 1


class _Connection:
    # Holds the callback, so a test chooses when the core answers and from which thread.

    def __init__(self, stream: _Stream, *, respond: bool = False, raise_exc: BaseException | None = None) -> None:
        self._stream = stream
        self._respond = respond
        # What the binding raises before it returns a stream, as on a connection not yet open.
        self._raise_exc = raise_exc
        self.callback: Any = None

    def pycbc_query(self, **kwargs: Any) -> _Stream:
        if self._raise_exc is not None:
            raise self._raise_exc
        self.callback = kwargs['callback']
        if self._respond:
            self.callback(True)
        return self._stream

    def respond_from_a_core_thread(self) -> None:
        thread = threading.Thread(target=self.callback, args=(True,))
        thread.start()
        thread.join()


class _Handler:
    # Records how the executor ends the span.

    def __init__(self) -> None:
        self.http_spans = 0
        self.exits: list[tuple[type[BaseException] | None, BaseException | None]] = []
        self.is_legacy_tracer = False
        self.legacy_request_span = None
        self.wrapper_span_name = 'wrapper'

    def create_http_span(self, **options: Any) -> None:
        self.http_spans += 1

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        self.exits.append((exc_type, exc_val))


def _async_executor(stream: _Stream,
                    *,
                    respond: bool = False,
                    binding_exc: BaseException | None = None,
                    obs_handler: _Handler | None = None) -> tuple[ClassicAsyncStreamingExecutor[Any, Any], _Connection]:
    connection = _Connection(stream, respond=respond, raise_exc=binding_exc)
    executor = ClassicAsyncStreamingExecutor(cast(pycbc_connection, connection),
                                             QueryStreamSpec({'statement': 'q'}, DefaultJsonSerializer()),
                                             asyncio.get_running_loop(),
                                             obs_handler=cast(ObservableRequestHandler | None, obs_handler))
    return executor, connection


async def _until_sent(connection: _Connection) -> None:
    while connection.callback is None:
        await asyncio.sleep(0)


class ClassicAsyncStreamingExecutorUnitTests:

    @pytest.mark.asyncio
    async def test_submit_waits_for_the_core_to_respond(self) -> None:
        executor, connection = _async_executor(_Stream([]))
        task = asyncio.ensure_future(executor.submit())
        await _until_sent(connection)
        await asyncio.sleep(0.01)
        assert task.done() is False
        connection.respond_from_a_core_thread()
        await asyncio.wait_for(task, 5)

    @pytest.mark.asyncio
    async def test_the_rows_are_read_on_the_loop(self) -> None:
        stream = _Stream([b'1', None, SimpleNamespace(raw_result={})])
        executor, connection = _async_executor(stream)
        rows = AsyncRowIterator(executor)
        task = asyncio.ensure_future(rows.execute())
        await _until_sent(connection)
        connection.respond_from_a_core_thread()
        assert await asyncio.wait_for(task, 5) == [1]
        assert set(stream.read_threads) == {threading.get_ident()}

    @pytest.mark.asyncio
    async def test_draining_streams_take_turns_on_the_loop(self) -> None:
        # Every row is queued before the core calls back, so no read waits, and only the yield in
        # each read keeps one drain from holding the loop until its last row.
        order: list[str] = []

        async def drain(name: str) -> None:
            stream = _Stream([b'1', b'2', b'3', None, SimpleNamespace(raw_result={})])
            executor, _ = _async_executor(stream, respond=True)
            async for _ in AsyncRowIterator(executor):
                order.append(name)

        await asyncio.gather(drain('a'), drain('b'))
        assert order == ['a', 'b', 'a', 'b', 'a', 'b']

    @pytest.mark.asyncio
    async def test_a_stream_stopped_without_its_end_marker_ends_as_an_error(self) -> None:
        # A cancelled core stream with no rows left stops with no `None` before it.  A StopIteration
        # cannot leave a coroutine, so the stop is taken as the end and the final read finds no item.
        stream = _Stream([])
        executor, connection = _async_executor(stream)
        task = asyncio.ensure_future(AsyncRowIterator(executor).__anext__())
        await _until_sent(connection)
        connection.respond_from_a_core_thread()
        with pytest.raises(InternalSDKException, match='without its final item'):
            await asyncio.wait_for(task, 5)
        assert stream.cancel_calls == 1

    @pytest.mark.asyncio
    async def test_cancelling_the_wait_cancels_the_stream(self) -> None:
        # Regression guard (PYCBC-1774): a query cancelled in flight cancels the core's streamed
        # result rather than leaving the server-side operation to run to its end.
        stream = _Stream([b'1', None, SimpleNamespace(raw_result={})])
        executor, connection = _async_executor(stream)
        task = asyncio.ensure_future(AsyncRowIterator(executor).__anext__())
        await _until_sent(connection)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.cancel_calls == 1
        assert stream.read_threads == []

    @pytest.mark.asyncio
    async def test_a_response_after_the_wait_was_cancelled_is_dropped(self) -> None:
        errors: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _, context: errors.append(context))
        try:
            responded, on_response = response_signal(loop)
            responded.cancel()
            on_response(True)
            await asyncio.sleep(0.01)
        finally:
            loop.set_exception_handler(None)
        assert errors == []

    @pytest.mark.asyncio
    async def test_a_binding_that_raises_ends_the_span(self) -> None:
        # The iterator over the real executor, since the iterator suite's fake records the end itself
        # and so cannot see whether the executor ends the span.
        handler = _Handler()
        err = RuntimeError('Connection not initialized')
        stream = _Stream([])
        executor, _ = _async_executor(stream, binding_exc=err, obs_handler=handler)
        rows = AsyncRowIterator(executor)
        with pytest.raises(InternalSDKException) as exc_info:
            await rows.__anext__()
        with pytest.raises(StopAsyncIteration):
            await rows.__anext__()
        assert handler.http_spans == 1
        assert handler.exits == [(InternalSDKException, exc_info.value)]
        assert stream.cancel_calls == 0

    def test_a_response_after_the_loop_closed_is_dropped(self) -> None:
        loop = asyncio.new_event_loop()
        _, on_response = response_signal(loop)
        loop.close()
        on_response(True)


class _CoreError(pycbc_exception):

    def __init__(self, err: int, strerror: str) -> None:
        self._err = err
        self._strerror = strerror

    def err(self) -> int:
        return self._err

    def err_category(self) -> str:
        return 'couchbase.common'

    def strerror(self) -> str:
        return self._strerror

    def error_context(self) -> dict[str, Any] | None:
        return None

    def error_info(self) -> Any:
        return None
