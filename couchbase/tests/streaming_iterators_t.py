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

from typing import Any

import pytest

from couchbase.common._streaming import RowIterator, StreamingExecutor
from couchbase.exceptions import (AlreadyQueriedException,
                                  CouchbaseException,
                                  InternalSDKException)


class _FakeExecutor(StreamingExecutor[Any, str]):
    # The iterator is pure control flow over an executor, so a fake is not a shortcut around a
    # cluster: driving a real one would need a server to produce a row and could not raise a
    # KeyboardInterrupt from inside the fetch at all.

    def __init__(self,
                 *,
                 rows: list[Any] | None = None,
                 raise_exc: BaseException | None = None,
                 submit_exc: BaseException | None = None) -> None:
        self._rows = list(rows or [])
        self._raise_exc = raise_exc
        self._submit_exc = submit_exc
        self._final: str | None = None
        self.submit_calls = 0
        self.next_row_calls = 0
        self.span_ends: list[BaseException | None] = []
        self.cancel_calls = 0
        self.release_calls = 0

    def submit(self) -> None:
        self.submit_calls += 1
        if self._submit_exc is not None:
            raise self._submit_exc

    def next_row(self) -> Any:
        self.next_row_calls += 1
        if self._raise_exc is not None:
            raise self._raise_exc
        if self._rows:
            return self._rows.pop(0)
        self._final = 'final'
        raise StopIteration

    def final_metadata(self) -> str | None:
        return self._final

    def _end_span(self, exc: BaseException | None) -> None:
        self.span_ends.append(exc)

    def _cancel(self) -> None:
        self.cancel_calls += 1

    def _release(self) -> None:
        self.release_calls += 1


def _started(executor: _FakeExecutor) -> RowIterator[Any, str]:
    return iter(RowIterator(executor))


class RowIteratorUnitTests:
    # No cb_env and no TEST_MANIFEST: `RowIterator` is the one blocking iterator every API's
    # streaming results share, Twisted's included, so a generated copy would drive the same class
    # through the same fake.  The acouchbase counterpart is hand-written, `AsyncRowIterator` being
    # a different class with a different fetch.
    #
    # Mid-stream the iterator leaves the request live, and every terminal branch finalizes it, so
    # the teardown is asserted on every test including the ones where it stays empty.

    def test_iterating_submits_once(self) -> None:
        executor = _FakeExecutor(rows=['row-1'])
        rows = RowIterator(executor)
        assert rows.started_streaming is False
        assert iter(rows) is rows
        assert iter(rows) is rows
        assert executor.submit_calls == 1
        assert rows.started_streaming is True

    def test_returns_row_and_leaves_the_request_live(self) -> None:
        executor = _FakeExecutor(rows=['row-1'])
        rows = _started(executor)
        assert next(rows) == 'row-1'
        assert rows.done_streaming is False
        assert executor.span_ends == []
        assert executor.release_calls == 0

    def test_end_of_rows_finalizes_and_exposes_the_final_metadata(self) -> None:
        executor = _FakeExecutor(rows=[])
        rows = _started(executor)
        assert rows.final_metadata() is None
        with pytest.raises(StopIteration):
            next(rows)
        assert rows.done_streaming is True
        assert rows.final_metadata() == 'final'
        assert executor.span_ends == [None]
        # A drained stream keeps its final item, so only a failure cancels.
        assert executor.cancel_calls == 0
        assert executor.release_calls == 1

    def test_couchbase_exception_propagates_unchanged(self) -> None:
        err = CouchbaseException(message='boom')
        executor = _FakeExecutor(raise_exc=err)
        with pytest.raises(CouchbaseException) as exc_info:
            next(_started(executor))
        assert exc_info.value is err
        assert executor.span_ends == [err]
        assert executor.cancel_calls == 1
        assert executor.release_calls == 1

    def test_generic_exception_converted_to_internal(self) -> None:
        executor = _FakeExecutor(raise_exc=ValueError('bad'))
        with pytest.raises(InternalSDKException) as exc_info:
            next(_started(executor))
        # The conversion keeps the original text, which is the only thing left pointing at what
        # actually failed.
        assert 'bad' in str(exc_info.value)
        assert executor.span_ends == [exc_info.value]
        assert executor.cancel_calls == 1

    @pytest.mark.parametrize('exc', [KeyboardInterrupt(), SystemExit()], ids=lambda e: type(e).__name__)
    def test_interpreter_signals_propagate_unconverted(self, exc: BaseException) -> None:
        # The `except BaseException` branch runs the teardown without rewriting the signal into a
        # CouchbaseException the caller would then have to tell apart from a real one.  Both,
        # since they are separate BaseException subclasses.
        executor = _FakeExecutor(raise_exc=exc)
        with pytest.raises(type(exc)) as exc_info:
            next(_started(executor))
        assert exc_info.value is exc
        assert executor.span_ends == [exc]
        assert executor.cancel_calls == 1
        assert executor.release_calls == 1

    def test_error_ends_the_iteration(self) -> None:
        # A failed row is terminal, as it is for a generator: the error is raised once and the
        # iterator is exhausted from then on, without fetching again.
        executor = _FakeExecutor(raise_exc=ValueError('bad'))
        rows = _started(executor)
        with pytest.raises(InternalSDKException):
            next(rows)
        with pytest.raises(StopIteration):
            next(rows)
        assert executor.next_row_calls == 1
        assert len(executor.span_ends) == 1

    def test_a_failed_submit_ends_the_iteration(self) -> None:
        # A failure to send is a terminal branch like any other, whether iterating or Twisted's
        # `start` sent the request.
        err = CouchbaseException(message='boom')
        executor = _FakeExecutor(rows=['row-1'], submit_exc=err)
        rows = RowIterator(executor)
        with pytest.raises(CouchbaseException) as exc_info:
            iter(rows)
        assert exc_info.value is err
        with pytest.raises(StopIteration):
            next(rows)
        assert executor.next_row_calls == 0
        assert executor.span_ends == [err]
        assert executor.cancel_calls == 1
        assert executor.release_calls == 1

    def test_a_failed_submit_is_converted_to_internal(self) -> None:
        executor = _FakeExecutor(submit_exc=RuntimeError('Connection not initialized'))
        rows = RowIterator(executor)
        with pytest.raises(InternalSDKException) as exc_info:
            rows.start()
        assert 'Connection not initialized' in str(exc_info.value)
        assert executor.span_ends == [exc_info.value]
        # Once failed, a second start sends nothing.
        rows.start()
        assert executor.submit_calls == 1

    def test_exhausted_iterator_keeps_stopping(self) -> None:
        executor = _FakeExecutor(rows=[])
        rows = _started(executor)
        with pytest.raises(StopIteration):
            next(rows)
        with pytest.raises(StopIteration):
            next(rows)
        assert executor.next_row_calls == 1
        assert executor.span_ends == [None]

    def test_iterating_again_once_done_raises(self) -> None:
        rows = _started(_FakeExecutor(rows=[]))
        assert list(rows) == []
        with pytest.raises(AlreadyQueriedException):
            iter(rows)

    def test_execute_drains_the_rows(self) -> None:
        executor = _FakeExecutor(rows=['row-1', 'row-2'])
        assert RowIterator(executor).execute() == ['row-1', 'row-2']
        assert executor.submit_calls == 1
        assert executor.span_ends == [None]

    def test_close_cancels_a_stream_not_drained(self) -> None:
        executor = _FakeExecutor(rows=['row-1', 'row-2'])
        rows = _started(executor)
        next(rows)
        rows.close()
        # A close is asked for, so the span ends as a success.
        assert executor.cancel_calls == 1
        assert executor.span_ends == [None]
        assert executor.release_calls == 1
        with pytest.raises(StopIteration):
            next(rows)
        assert executor.next_row_calls == 1

    def test_close_after_the_end_does_nothing(self) -> None:
        executor = _FakeExecutor(rows=[])
        rows = _started(executor)
        list(rows)
        rows.close()
        assert executor.cancel_calls == 0
        assert executor.release_calls == 1

    def test_close_before_iterating_sends_nothing(self) -> None:
        executor = _FakeExecutor(rows=['row-1'])
        rows = RowIterator(executor)
        rows.close()
        assert executor.submit_calls == 0
        assert executor.cancel_calls == 0
        with pytest.raises(AlreadyQueriedException):
            iter(rows)
