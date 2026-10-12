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

# Hand-written and neither generated nor merged.  The sync file this appears to pair with
# tests `stream_next`; this one tests `stream_anext`, a different function with a different
# teardown contract: `stream_next` ends the span and reads metadata inline, while
# `stream_anext` routes every terminal branch through `_finalize`, which additionally
# cancels the streaming result and releases the request's executor.  No sidecar either,
# because nothing here is generated to merge into: streaming_helpers_t.py is absent from the
# generator's suite table, `stream_next` being one shared function the `couchbase` and
# `txcouchbase` iterators both import unchanged.
from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from acouchbase.analytics import AsyncAnalyticsRequest
from acouchbase.n1ql import AsyncN1QLRequest
from acouchbase.search import AsyncFullTextSearchRequest
from acouchbase.views import AsyncViewRequest
from couchbase.exceptions import CouchbaseException, InternalSDKException
from couchbase.logic.streaming import stream_anext

# The four async streaming requests, which carry byte-identical copies of `_shutdown_executor`
# and `_finalize`.  All four take `(connection, loop, payload, **kwargs)`, so one construction
# shape covers the family.
ASYNC_REQUEST_TYPES = [AsyncN1QLRequest,
                       AsyncFullTextSearchRequest,
                       AsyncAnalyticsRequest,
                       AsyncViewRequest]


class _FakeStreamingResult:
    def __init__(self) -> None:
        self.cancel_calls = 0

    def cancel(self) -> None:
        self.cancel_calls += 1


class _FakeAsyncStreamingRequest:
    # `_finalize` here does what the four real implementations do, minus the streaming
    # result: end the span and release the executor.  The cancel half is asserted against the
    # real classes in AsyncStreamingFinalizeUnitTests instead, so a change to the real
    # `_finalize` cannot pass by being mirrored into the fake.

    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 *,
                 rows: list[Any] | None = None,
                 raise_exc: BaseException | None = None,
                 block_event: threading.Event | None = None
                 ) -> None:
        self._loop = loop
        self._tp_executor = ThreadPoolExecutor(1)
        self._done_streaming = False
        self._rows = list(rows or [])
        self._raise_exc = raise_exc
        self._block_event = block_event
        self.process_core_span_calls: list[BaseException | None] = []
        self.finalize_calls: list[BaseException | None] = []
        self.get_metadata_calls = 0
        self.executor_shutdown = False

    def _get_next_row(self) -> Any:
        # Runs in the executor thread, mirroring the real blocking fetch.
        if self._block_event is not None:
            self._block_event.wait()
        if self._raise_exc is not None:
            raise self._raise_exc
        if self._rows:
            return self._rows.pop(0)
        raise StopAsyncIteration

    def _process_core_span(self, exc_val: BaseException | None = None) -> None:
        self.process_core_span_calls.append(exc_val)

    def _finalize(self, exc_val: BaseException | None = None) -> None:
        self.finalize_calls.append(exc_val)
        self._shutdown_executor()

    def _shutdown_executor(self) -> None:
        if not self.executor_shutdown:
            self.executor_shutdown = True
            self._tp_executor.shutdown(wait=False)

    def _get_metadata(self) -> None:
        self.get_metadata_calls += 1


class AsyncStreamingHelperUnitTests:
    # No TEST_MANIFEST and no concrete subclass, matching the cluster-free suites in the sync
    # tree: the manifest is a parity contract and `stream_anext` has no form in the other two
    # trees.  The distinction the whole suite turns on is that mid-stream the helper ends the
    # span and leaves the request live, and on any terminal branch it finalizes, so
    # `finalize_calls` is asserted on every test including the one where it stays empty.

    @pytest.mark.asyncio
    async def test_returns_row_and_ends_span(self) -> None:
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), rows=['row-1'])
        try:
            assert await stream_anext(req) == 'row-1'
            assert req.process_core_span_calls == [None]
            assert req.finalize_calls == []
            assert req.executor_shutdown is False
        finally:
            req._tp_executor.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_stop_async_iteration_finalizes_and_reads_metadata(self) -> None:
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), rows=[])
        with pytest.raises(StopAsyncIteration):
            await stream_anext(req)
        assert req._done_streaming is True
        assert req.finalize_calls == [None]
        assert req.get_metadata_calls == 1
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_couchbase_exception_propagates_unchanged(self) -> None:
        err = CouchbaseException(message='boom')
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), raise_exc=err)
        with pytest.raises(CouchbaseException) as exc_info:
            await stream_anext(req)
        assert exc_info.value is err
        assert req.finalize_calls == [err]
        assert req.get_metadata_calls == 0
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_generic_exception_converted_to_internal(self) -> None:
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), raise_exc=ValueError('bad'))
        with pytest.raises(InternalSDKException) as exc_info:
            await stream_anext(req)
        assert 'bad' in str(exc_info.value)
        assert req.finalize_calls == [exc_info.value]
        assert req.get_metadata_calls == 0
        assert req.executor_shutdown is True

    async def _assert_propagates_unconverted(self, exc: BaseException) -> None:
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), raise_exc=exc)
        with pytest.raises(type(exc)) as exc_info:
            await stream_anext(req)
        assert exc_info.value is exc
        assert req.finalize_calls == [exc]
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_keyboard_interrupt_propagates_unconverted(self) -> None:
        await self._assert_propagates_unconverted(KeyboardInterrupt())

    @pytest.mark.asyncio
    async def test_system_exit_propagates_unconverted(self) -> None:
        # SystemExit as well as KeyboardInterrupt.  They are separate BaseException
        # subclasses, so only the pair covers the branch.
        await self._assert_propagates_unconverted(SystemExit())

    @pytest.mark.asyncio
    async def test_cancellation_finalizes_and_propagates(self) -> None:
        # Why the branch exists: CancelledError derives from BaseException, so without it a
        # cancelled task would leave the span open and the executor thread orphaned.
        block = threading.Event()
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), rows=['late-row'], block_event=block)
        task = asyncio.ensure_future(stream_anext(req))
        # Let the task reach the run_in_executor await; the worker is now blocked on the event.
        await asyncio.sleep(0.05)
        task.cancel()
        # Release the worker so the in-flight executor future can complete.  The pending
        # cancellation is then delivered into the coroutine at the await point.
        block.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(req.finalize_calls) == 1
        assert isinstance(req.finalize_calls[0], asyncio.CancelledError)
        assert req.executor_shutdown is True


class AsyncStreamingFinalizeUnitTests:
    # A separate class because the subject is different: the tests above drive `stream_anext`
    # against a fake, and these drive the teardown the four request classes each implement.
    # Parametrized over the family rather than written against the query request alone: the
    # four copies of `_finalize` are byte-identical today and nothing else would say so if one
    # drifted.  Constructing a request with a `None` connection is sound because `_finalize`
    # never reaches the connection.

    @staticmethod
    def _build(request_cls: type, loop: asyncio.AbstractEventLoop) -> Any:
        req = request_cls(None, loop, {}, obs_handler=None)
        req._streaming_result = _FakeStreamingResult()
        return req

    @pytest.mark.parametrize('request_cls', ASYNC_REQUEST_TYPES, ids=lambda c: c.__name__)
    def test_finalize_cancels_streaming_result_on_error(self, request_cls: type) -> None:
        loop = asyncio.new_event_loop()
        try:
            req = self._build(request_cls, loop)
            req._finalize(exc_val=CouchbaseException(message='boom'))
            # Abort path: the streamed result is cancelled so a worker blocked in the core can
            # unwind now rather than when the server-side operation finishes.
            assert req._streaming_result.cancel_calls == 1
            assert req._executor_shutdown is True
        finally:
            loop.close()

    @pytest.mark.parametrize('request_cls', ASYNC_REQUEST_TYPES, ids=lambda c: c.__name__)
    def test_finalize_skips_cancel_on_normal_completion(self, request_cls: type) -> None:
        loop = asyncio.new_event_loop()
        try:
            req = self._build(request_cls, loop)
            req._finalize()
            # Clean exhaustion still has trailing metadata to read off the streaming result,
            # so cancelling here would discard it.
            assert req._streaming_result.cancel_calls == 0
            assert req._executor_shutdown is True
        finally:
            loop.close()
