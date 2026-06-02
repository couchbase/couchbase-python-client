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

import pytest

from couchbase.exceptions import CouchbaseException, InternalSDKException
from couchbase.logic.streaming import stream_next


class _FakeSyncStreamingRequest:
    """Minimal stand-in implementing the contract ``stream_next`` relies on, so the streaming
    iterator teardown/error handling can be exercised without a live cluster."""

    def __init__(self, *, rows=None, raise_exc=None):
        self._done_streaming = False
        self._rows = list(rows or [])
        self._raise_exc = raise_exc
        # instrumentation
        self.process_core_span_calls = []
        self.get_metadata_calls = 0

    def _get_next_row(self):
        if self._raise_exc is not None:
            raise self._raise_exc
        if self._rows:
            return self._rows.pop(0)
        raise StopIteration

    def _process_core_span(self, exc_val=None):
        self.process_core_span_calls.append(exc_val)

    def _get_metadata(self):
        self.get_metadata_calls += 1


class StreamingNextTestSuite:
    TEST_MANIFEST = [
        'test_returns_row_and_ends_span',
        'test_stop_iteration_finalizes_and_reads_metadata',
        'test_couchbase_exception_propagates_unchanged',
        'test_generic_exception_converted_to_internal',
        'test_keyboard_interrupt_propagates_unconverted',
        'test_system_exit_propagates_unconverted',
    ]

    def test_returns_row_and_ends_span(self):
        req = _FakeSyncStreamingRequest(rows=['row-1'])
        assert stream_next(req) == 'row-1'
        assert req.process_core_span_calls == [None]

    def test_stop_iteration_finalizes_and_reads_metadata(self):
        req = _FakeSyncStreamingRequest(rows=[])
        with pytest.raises(StopIteration):
            stream_next(req)
        assert req._done_streaming is True
        assert req.process_core_span_calls == [None]
        assert req.get_metadata_calls == 1

    def test_couchbase_exception_propagates_unchanged(self):
        err = CouchbaseException('boom')
        req = _FakeSyncStreamingRequest(raise_exc=err)
        with pytest.raises(CouchbaseException) as exc_info:
            stream_next(req)
        assert exc_info.value is err
        assert req.process_core_span_calls == [err]

    def test_generic_exception_converted_to_internal(self):
        req = _FakeSyncStreamingRequest(raise_exc=ValueError('bad'))
        with pytest.raises(InternalSDKException):
            stream_next(req)
        assert len(req.process_core_span_calls) == 1
        assert isinstance(req.process_core_span_calls[0], InternalSDKException)

    def test_keyboard_interrupt_propagates_unconverted(self):
        req = _FakeSyncStreamingRequest(raise_exc=KeyboardInterrupt())
        with pytest.raises(KeyboardInterrupt):
            stream_next(req)
        # BaseException must end the span for cleanup but NOT be converted to a CouchbaseException
        assert len(req.process_core_span_calls) == 1
        assert isinstance(req.process_core_span_calls[0], KeyboardInterrupt)

    def test_system_exit_propagates_unconverted(self):
        req = _FakeSyncStreamingRequest(raise_exc=SystemExit())
        with pytest.raises(SystemExit):
            stream_next(req)
        assert len(req.process_core_span_calls) == 1
        assert isinstance(req.process_core_span_calls[0], SystemExit)


class ClassicStreamingNextTests(StreamingNextTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicStreamingNextTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicStreamingNextTests) if valid_test_method(meth)]
        test_list = set(StreamingNextTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')
