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

from couchbase.exceptions import CouchbaseException, InternalSDKException
from couchbase.logic.streaming import stream_next


class _FakeSyncStreamingRequest:
    # The helper is pure control flow over a request object, so a fake is not a shortcut
    # around a cluster: driving the real thing would need a server to produce a row and
    # could not produce a KeyboardInterrupt from inside the fetch at all.

    def __init__(self,
                 *,
                 rows: list[Any] | None = None,
                 raise_exc: BaseException | None = None
                 ) -> None:
        self._done_streaming = False
        self._rows = list(rows or [])
        self._raise_exc = raise_exc
        self.process_core_span_calls: list[BaseException | None] = []
        self.get_metadata_calls = 0

    def _get_next_row(self) -> Any:
        if self._raise_exc is not None:
            raise self._raise_exc
        if self._rows:
            return self._rows.pop(0)
        raise StopIteration

    def _process_core_span(self, exc_val: BaseException | None = None) -> None:
        self.process_core_span_calls.append(exc_val)

    def _get_metadata(self) -> None:
        self.get_metadata_calls += 1


class SyncStreamingHelperUnitTests:
    # No cb_env and no TEST_MANIFEST: `stream_next` is one function in
    # `couchbase.logic.streaming`, imported unchanged by the four `couchbase` row iterators
    # and by the four `txcouchbase` ones, so a generated Twisted copy would drive the same
    # function through the same fake.  The acouchbase form is not generated either, and
    # there the reason is stronger: `stream_anext` is a different function with a different
    # teardown contract, tested in `acouchbase/tests/streaming_helpers_t.py`.
    #
    # `_get_metadata` is asserted on every path, not only the one that calls it: metadata is
    # read from the streaming result exactly once, on clean exhaustion.

    def test_returns_row_and_ends_span(self) -> None:
        req = _FakeSyncStreamingRequest(rows=['row-1'])
        assert stream_next(req) == 'row-1'
        # Ended on the first row rather than at the end of the stream, because that is the
        # earliest point the core is known to have answered.  The call is idempotent, so
        # every subsequent row repeats it harmlessly.
        assert req.process_core_span_calls == [None]
        assert req._done_streaming is False
        assert req.get_metadata_calls == 0

    def test_stop_iteration_finalizes_and_reads_metadata(self) -> None:
        req = _FakeSyncStreamingRequest(rows=[])
        with pytest.raises(StopIteration):
            stream_next(req)
        assert req._done_streaming is True
        assert req.process_core_span_calls == [None]
        assert req.get_metadata_calls == 1

    def test_couchbase_exception_propagates_unchanged(self) -> None:
        err = CouchbaseException(message='boom')
        req = _FakeSyncStreamingRequest(raise_exc=err)
        with pytest.raises(CouchbaseException) as exc_info:
            stream_next(req)
        assert exc_info.value is err
        assert req.process_core_span_calls == [err]
        assert req._done_streaming is False
        assert req.get_metadata_calls == 0

    def test_generic_exception_converted_to_internal(self) -> None:
        req = _FakeSyncStreamingRequest(raise_exc=ValueError('bad'))
        with pytest.raises(InternalSDKException) as exc_info:
            stream_next(req)
        # The conversion goes through PYCBC_ERROR_MAP and keeps the original text, which is
        # the only thing left pointing at what actually failed.
        assert 'bad' in str(exc_info.value)
        assert req.process_core_span_calls == [exc_info.value]
        assert req.get_metadata_calls == 0

    def _assert_propagates_unconverted(self, exc: BaseException) -> None:
        # The `except BaseException` branch exists so that an interpreter-level signal ends
        # the span for cleanup without being rewritten into a CouchbaseException the caller
        # would then have to tell apart from a real one.
        req = _FakeSyncStreamingRequest(raise_exc=exc)
        with pytest.raises(type(exc)) as exc_info:
            stream_next(req)
        assert exc_info.value is exc
        assert req.process_core_span_calls == [exc]
        assert req.get_metadata_calls == 0

    def test_keyboard_interrupt_propagates_unconverted(self) -> None:
        self._assert_propagates_unconverted(KeyboardInterrupt())

    def test_system_exit_propagates_unconverted(self) -> None:
        self._assert_propagates_unconverted(SystemExit())
