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

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from acouchbase.n1ql import AsyncN1QLRequest
from couchbase.exceptions import CouchbaseException, InternalSDKException
from couchbase.logic.streaming import stream_anext


class _FakeStreamingResult:
    """Stand-in for the C-extension ``pycbc_streamed_result`` exposing the new cancel hook."""

    def __init__(self):
        self.cancel_calls = 0

    def cancel(self):
        self.cancel_calls += 1


class _FakeAsyncStreamingRequest:
    """Minimal stand-in implementing the contract ``stream_anext`` relies on, so the streaming
    iterator teardown/error handling can be exercised without a live cluster.  Records the
    teardown calls the helper makes so the tests can assert on them."""

    def __init__(self, loop, *, rows=None, raise_exc=None, block_event=None):
        self._loop = loop
        self._tp_executor = ThreadPoolExecutor(1)
        self._done_streaming = False
        self._rows = list(rows or [])
        self._raise_exc = raise_exc
        self._block_event = block_event
        # instrumentation
        self.process_core_span_calls = []
        self.finalize_calls = []
        self.get_metadata_calls = 0
        self.executor_shutdown = False

    def _get_next_row(self):
        # runs in the executor thread, mirroring the real (blocking) row fetch
        if self._block_event is not None:
            self._block_event.wait()
        if self._raise_exc is not None:
            raise self._raise_exc
        if self._rows:
            return self._rows.pop(0)
        raise StopAsyncIteration

    def _process_core_span(self, exc_val=None):
        self.process_core_span_calls.append(exc_val)

    def _finalize(self, exc_val=None):
        self.finalize_calls.append(exc_val)
        self._shutdown_executor()

    def _shutdown_executor(self):
        if not self.executor_shutdown:
            self.executor_shutdown = True
            self._tp_executor.shutdown(wait=False)

    def _get_metadata(self):
        self.get_metadata_calls += 1


class StreamingAnextTestSuite:
    TEST_MANIFEST = [
        'test_returns_row_and_ends_span',
        'test_stop_async_iteration_finalizes_and_reads_metadata',
        'test_couchbase_exception_propagates_unchanged',
        'test_generic_exception_converted_to_internal',
        'test_queue_empty_converted_with_op_name',
        'test_keyboard_interrupt_propagates_unconverted',
        'test_cancellation_finalizes_and_propagates',
        'test_finalize_cancels_streaming_result_on_error',
        'test_finalize_skips_cancel_on_normal_completion',
    ]

    @pytest.mark.asyncio
    async def test_returns_row_and_ends_span(self):
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), rows=['row-1'])
        try:
            row = await stream_anext(req, 'N1QL')
            assert row == 'row-1'
            # span ended on the (first) row, no error, and the op is NOT finalized mid-stream
            assert req.process_core_span_calls == [None]
            assert req.finalize_calls == []
            assert req.executor_shutdown is False
        finally:
            req._tp_executor.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_stop_async_iteration_finalizes_and_reads_metadata(self):
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), rows=[])
        with pytest.raises(StopAsyncIteration):
            await stream_anext(req, 'N1QL')
        assert req._done_streaming is True
        assert req.finalize_calls == [None]
        assert req.get_metadata_calls == 1
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_couchbase_exception_propagates_unchanged(self):
        err = CouchbaseException('boom')
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), raise_exc=err)
        with pytest.raises(CouchbaseException) as exc_info:
            await stream_anext(req, 'N1QL')
        assert exc_info.value is err
        assert req.finalize_calls == [err]
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_generic_exception_converted_to_internal(self):
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), raise_exc=ValueError('bad'))
        with pytest.raises(InternalSDKException):
            await stream_anext(req, 'N1QL')
        assert len(req.finalize_calls) == 1
        assert isinstance(req.finalize_calls[0], InternalSDKException)
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_queue_empty_converted_with_op_name(self):
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), raise_exc=asyncio.QueueEmpty())
        with pytest.raises(InternalSDKException) as exc_info:
            await stream_anext(req, 'Analytics')
        assert 'Analytics' in str(exc_info.value)
        assert len(req.finalize_calls) == 1
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_keyboard_interrupt_propagates_unconverted(self):
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), raise_exc=KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            await stream_anext(req, 'N1QL')
        # BaseException must be finalized for cleanup but NOT converted to a CouchbaseException
        assert len(req.finalize_calls) == 1
        assert isinstance(req.finalize_calls[0], KeyboardInterrupt)
        assert req.executor_shutdown is True

    @pytest.mark.asyncio
    async def test_cancellation_finalizes_and_propagates(self):
        # The core scenario: a task cancelled while blocked fetching the next row.
        block = threading.Event()
        req = _FakeAsyncStreamingRequest(asyncio.get_running_loop(), rows=['late-row'], block_event=block)
        task = asyncio.ensure_future(stream_anext(req, 'N1QL'))
        # let the task reach the run_in_executor await (worker is now blocked on the event)
        await asyncio.sleep(0.05)
        task.cancel()
        # release the worker so the in-flight executor future can complete; the pending
        # cancellation is then delivered into the coroutine at the await point
        block.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        # CancelledError (a BaseException) must have triggered teardown, unconverted
        assert len(req.finalize_calls) == 1
        assert isinstance(req.finalize_calls[0], asyncio.CancelledError)
        assert req.executor_shutdown is True

    def test_finalize_cancels_streaming_result_on_error(self):
        # _finalize is exercised on a real request to verify the cancel() wiring (Phase III).
        loop = asyncio.new_event_loop()
        try:
            req = AsyncN1QLRequest(None, loop, {}, obs_handler=None)
            req._streaming_result = _FakeStreamingResult()
            req._finalize(exc_val=CouchbaseException('boom'))
            # abort path: the C++ streamed result is cancelled so a blocked worker can unwind
            assert req._streaming_result.cancel_calls == 1
            assert req._executor_shutdown is True
        finally:
            loop.close()

    def test_finalize_skips_cancel_on_normal_completion(self):
        loop = asyncio.new_event_loop()
        try:
            req = AsyncN1QLRequest(None, loop, {}, obs_handler=None)
            req._streaming_result = _FakeStreamingResult()
            req._finalize()  # exc_val is None -> normal completion must NOT cancel (metadata follows)
            assert req._streaming_result.cancel_calls == 0
            assert req._executor_shutdown is True
        finally:
            loop.close()


class AsyncStreamingAnextTests(StreamingAnextTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(AsyncStreamingAnextTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(AsyncStreamingAnextTests) if valid_test_method(meth)]
        test_list = set(StreamingAnextTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')
