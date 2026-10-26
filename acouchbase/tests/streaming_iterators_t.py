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

# Hand-written and neither generated nor merged.  The sync file this appears to pair with tests
# `RowIterator`; this one tests `AsyncRowIterator`, a different class whose fetch is a coroutine
# and whose request is submitted by the first fetch rather than by `__aiter__`.  Classic's two
# asyncio executors are in classic_streaming_t.
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from couchbase.common._streaming import AsyncRowIterator, AsyncStreamingExecutor
from couchbase.exceptions import (AlreadyQueriedException,
                                  CouchbaseException,
                                  InternalSDKException)


class _FakeExecutor(AsyncStreamingExecutor[Any, str]):

    def __init__(self,
                 *,
                 rows: list[Any] | None = None,
                 raise_exc: BaseException | None = None,
                 submit_exc: BaseException | None = None,
                 block: asyncio.Event | None = None) -> None:
        self._rows = list(rows or [])
        self._raise_exc = raise_exc
        self._submit_exc = submit_exc
        self._block = block
        self._final: str | None = None
        self.submit_calls = 0
        self.next_row_calls = 0
        self.span_ends: list[BaseException | None] = []
        self.cancel_calls = 0
        self.release_calls = 0

    async def submit(self) -> None:
        self.submit_calls += 1
        if self._submit_exc is not None:
            raise self._submit_exc

    async def next_row(self) -> Any:
        self.next_row_calls += 1
        if self._block is not None:
            await self._block.wait()
        if self._raise_exc is not None:
            raise self._raise_exc
        if self._rows:
            return self._rows.pop(0)
        self._final = 'final'
        raise StopAsyncIteration

    def final_metadata(self) -> str | None:
        return self._final

    def _end_span(self, exc: BaseException | None) -> None:
        self.span_ends.append(exc)

    def _cancel(self) -> None:
        self.cancel_calls += 1

    def _release(self) -> None:
        self.release_calls += 1


class AsyncRowIteratorUnitTests:
    # No TEST_MANIFEST and no concrete subclass, matching the cluster-free suites in the sync tree:
    # the manifest is a parity contract and `AsyncRowIterator` has no form in the other two trees.

    @pytest.mark.asyncio
    async def test_the_first_fetch_submits(self) -> None:
        executor = _FakeExecutor(rows=['row-1'])
        rows = AsyncRowIterator(executor)
        assert rows.__aiter__() is rows
        assert executor.submit_calls == 0
        assert await rows.__anext__() == 'row-1'
        assert executor.submit_calls == 1
        assert rows.started_streaming is True
        assert rows.done_streaming is False
        assert executor.span_ends == []

    @pytest.mark.asyncio
    async def test_end_of_rows_finalizes_and_exposes_the_final_metadata(self) -> None:
        executor = _FakeExecutor(rows=[])
        rows = AsyncRowIterator(executor)
        with pytest.raises(StopAsyncIteration):
            await rows.__anext__()
        assert rows.done_streaming is True
        assert rows.final_metadata() == 'final'
        assert executor.span_ends == [None]
        assert executor.cancel_calls == 0
        assert executor.release_calls == 1

    @pytest.mark.asyncio
    async def test_couchbase_exception_propagates_unchanged(self) -> None:
        err = CouchbaseException(message='boom')
        executor = _FakeExecutor(raise_exc=err)
        with pytest.raises(CouchbaseException) as exc_info:
            await AsyncRowIterator(executor).__anext__()
        assert exc_info.value is err
        assert executor.span_ends == [err]
        assert executor.cancel_calls == 1
        assert executor.release_calls == 1

    @pytest.mark.asyncio
    async def test_generic_exception_converted_to_internal(self) -> None:
        executor = _FakeExecutor(raise_exc=ValueError('bad'))
        with pytest.raises(InternalSDKException) as exc_info:
            await AsyncRowIterator(executor).__anext__()
        assert 'bad' in str(exc_info.value)
        assert executor.span_ends == [exc_info.value]

    @pytest.mark.asyncio
    async def test_a_failed_submit_ends_the_iteration(self) -> None:
        # Submitted inside the fetch, so a failure to send is a terminal branch like any other.
        err = CouchbaseException(message='boom')
        executor = _FakeExecutor(rows=['row-1'], submit_exc=err)
        rows = AsyncRowIterator(executor)
        with pytest.raises(CouchbaseException):
            await rows.__anext__()
        with pytest.raises(StopAsyncIteration):
            await rows.__anext__()
        assert executor.next_row_calls == 0
        assert executor.span_ends == [err]

    @pytest.mark.parametrize('exc', [KeyboardInterrupt(), SystemExit()], ids=lambda e: type(e).__name__)
    @pytest.mark.asyncio
    async def test_interpreter_signals_propagate_unconverted(self, exc: BaseException) -> None:
        executor = _FakeExecutor(raise_exc=exc)
        with pytest.raises(type(exc)) as exc_info:
            await AsyncRowIterator(executor).__anext__()
        assert exc_info.value is exc
        assert executor.span_ends == [exc]
        assert executor.release_calls == 1

    @pytest.mark.asyncio
    async def test_error_ends_the_iteration(self) -> None:
        # Terminal, as for the sync iterator: the next call stops instead of fetching again.
        executor = _FakeExecutor(raise_exc=ValueError('bad'))
        rows = AsyncRowIterator(executor)
        with pytest.raises(InternalSDKException):
            await rows.__anext__()
        with pytest.raises(StopAsyncIteration):
            await rows.__anext__()
        assert executor.next_row_calls == 1
        assert len(executor.span_ends) == 1

    @pytest.mark.asyncio
    async def test_cancellation_finalizes_and_propagates(self) -> None:
        # Why the branch exists: CancelledError derives from BaseException, so without it a
        # cancelled task would leave the span open and the stream producing.
        block = asyncio.Event()
        executor = _FakeExecutor(rows=['late-row'], block=block)
        task = asyncio.ensure_future(AsyncRowIterator(executor).__anext__())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(executor.span_ends) == 1
        assert isinstance(executor.span_ends[0], asyncio.CancelledError)
        assert executor.cancel_calls == 1
        assert executor.release_calls == 1

    @pytest.mark.asyncio
    async def test_iterating_again_once_done_raises(self) -> None:
        rows = AsyncRowIterator(_FakeExecutor(rows=[]))
        assert [r async for r in rows] == []
        with pytest.raises(AlreadyQueriedException):
            rows.__aiter__()

    @pytest.mark.asyncio
    async def test_execute_drains_the_rows_in_a_task(self) -> None:
        executor = _FakeExecutor(rows=['row-1', 'row-2'])
        task = AsyncRowIterator(executor).execute()
        assert isinstance(task, asyncio.Task)
        assert await task == ['row-1', 'row-2']

    @pytest.mark.asyncio
    async def test_aclose_cancels_a_stream_not_drained(self) -> None:
        executor = _FakeExecutor(rows=['row-1', 'row-2'])
        rows = AsyncRowIterator(executor)
        await rows.__anext__()
        await rows.aclose()
        assert executor.cancel_calls == 1
        assert executor.span_ends == [None]
        assert executor.release_calls == 1
        with pytest.raises(StopAsyncIteration):
            await rows.__anext__()

    @pytest.mark.asyncio
    async def test_aclose_before_iterating_sends_nothing(self) -> None:
        executor = _FakeExecutor(rows=['row-1'])
        rows = AsyncRowIterator(executor)
        await rows.aclose()
        assert executor.submit_calls == 0
        assert executor.cancel_calls == 0
