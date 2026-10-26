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

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._observability.handler import OpAttributeOptions
from couchbase.common._observability.no_op import NoOpSpan
from couchbase.common._streaming import RowIterator
from couchbase.common.analytics import AnalyticsMetaData
from couchbase.common.exceptions import _ExceptionInfo
from couchbase.common.n1ql import QueryMetaData
from couchbase.common.views import ViewMetaData, ViewRow
from couchbase.exceptions import InternalSDKException, RequestCanceledException
from couchbase.protocol.classic._pycbc_core import pycbc_connection, pycbc_exception
from couchbase.protocol.classic._streaming import (AnalyticsStreamSpec,
                                                   ClassicStream,
                                                   ClassicStreamingExecutor,
                                                   ClassicStreamSpec,
                                                   QueryStreamSpec,
                                                   ViewStreamSpec)
from couchbase.serializer import DefaultJsonSerializer, Serializer
from tests.test_features import TestProtocol

# What is checked here is classic's streaming executors and stream specs.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)


class _CoreError(pycbc_exception):
    # A subclass of the real core type, so the isinstance checks are the ones the extension's own
    # error values meet.

    def err(self) -> int:
        return 2

    def err_category(self) -> str:
        return 'couchbase.common'

    def strerror(self) -> str:
        return 'request_canceled'

    def error_context(self) -> dict[str, Any] | None:
        return None

    def error_info(self) -> _ExceptionInfo | None:
        return None


class _Stream:
    # The core's streamed result: the rows, then `None`, then the final item.

    def __init__(self, items: list[Any]) -> None:
        self._items = list(items)
        self.cancel_calls = 0
        self.core_span = 'core-span'

    def __iter__(self) -> _Stream:
        return self

    def __next__(self) -> Any:
        if not self._items:
            raise StopIteration
        item = self._items.pop(0)
        if isinstance(item, type) and issubclass(item, StopIteration):
            raise StopIteration
        return item

    def cancel(self) -> None:
        self.cancel_calls += 1


class _Connection:

    def __init__(self, stream: _Stream, raise_exc: BaseException | None = None) -> None:
        self._stream = stream
        # What the binding raises before it returns a stream, as on a connection not yet open.
        self._raise_exc = raise_exc
        self.calls: list[dict[str, Any]] = []

    def pycbc_query(self, **kwargs: Any) -> _Stream:
        self.calls.append(kwargs)
        if self._raise_exc is not None:
            raise self._raise_exc
        return self._stream

    def pycbc_analytics_query(self, **kwargs: Any) -> _Stream:
        self.calls.append(kwargs)
        return self._stream

    def pycbc_search_query(self, **kwargs: Any) -> _Stream:
        self.calls.append(kwargs)
        return self._stream

    def pycbc_view_query(self, **kwargs: Any) -> _Stream:
        self.calls.append(kwargs)
        return self._stream


class _Handler:
    # Records what the executor asks of the observability handler.

    def __init__(self) -> None:
        self.http_spans: list[dict[str, Any]] = []
        self.meter_ends: list[BaseException | None] = []
        self.core_spans: list[tuple[Any, bool | None]] = []
        self.exits: list[tuple[type[BaseException] | None, BaseException | None]] = []
        self.is_legacy_tracer = False
        self.legacy_request_span = None
        self.wrapper_span_name = 'wrapper'

    def create_http_span(self, **options: Any) -> None:
        self.http_spans.append(options)

    def process_meter_end(self, exc_val: BaseException | None = None) -> None:
        self.meter_ends.append(exc_val)

    def process_core_span(self, core_span: Any, with_error: bool | None = False) -> None:
        self.core_spans.append((core_span, with_error))

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        self.exits.append((exc_type, exc_val))


class _Spec(ClassicStreamSpec[str, dict[str, Any]]):

    def __init__(self, params: dict[str, Any]) -> None:
        self._params = params

    def binding_call(self, connection: pycbc_connection) -> Callable[..., Any]:
        return connection.pycbc_query

    def binding_kwargs(self) -> dict[str, Any]:
        return dict(self._params)

    def span_attributes(self, kwargs: dict[str, Any]) -> OpAttributeOptions:
        return {'statement': kwargs.get('statement')}

    def decode_row(self, row: Any) -> str:
        return f'decoded:{row}'

    def decode_final_metadata(self, raw_result: dict[str, Any]) -> dict[str, Any] | None:
        return raw_result


def _final(raw_result: dict[str, Any] | None = None) -> SimpleNamespace:
    return SimpleNamespace(raw_result=raw_result or {'metadata': None})


def _executor(items: list[Any],
              params: dict[str, Any] | None = None,
              *,
              binding_exc: BaseException | None = None,
              **kwargs: Any) -> tuple[ClassicStreamingExecutor[str, dict[str, Any]], _Stream, _Connection]:
    stream = _Stream(items)
    connection = _Connection(stream, binding_exc)
    executor = ClassicStreamingExecutor(cast(pycbc_connection, connection), _Spec(params or {'statement': 'q'}),
                                        **kwargs)
    return executor, stream, connection


class ClassicStreamingExecutorUnitTests:
    # No cb_env and no TEST_MANIFEST: the classic executors are one pair of classes every API
    # builds its streaming results on, so a generated copy would drive the same code.  A stand-in
    # for the connection and its stream is what the core would hand back, and is the only way to
    # place an error value or a cancelled stream at a chosen row.

    def test_submit_takes_the_spans_out_of_the_arguments(self) -> None:
        handler = _Handler()
        parent = NoOpSpan('parent')
        executor, _, connection = _executor([], {'statement': 'q', 'span': None, 'parent_span': parent},
                                            obs_handler=cast(ObservableRequestHandler, handler))
        executor.submit()
        assert connection.calls == [{'statement': 'q', 'wrapper_span_name': 'wrapper'}]
        assert handler.http_spans == [{'statement': 'q', 'parent_span': parent, 'use_now_as_start_time': True}]

    def test_submit_without_a_handler_sends_the_arguments_alone(self) -> None:
        executor, _, connection = _executor([], {'statement': 'q', 'span': 's'})
        executor.submit()
        assert connection.calls == [{'statement': 'q'}]

    @pytest.mark.parametrize('params, default, expected', [
        ({'statement': 'q', 'timeout': 5}, 9, 5),
        ({'statement': 'q'}, 9, 9),
        ({'statement': 'q'}, None, None),
    ], ids=['the request timeout', 'the cluster timeout', 'none'])
    def test_submit_sends_the_streaming_timeout(self, params: dict[str, Any], default: Any, expected: Any) -> None:
        executor, _, connection = _executor([], params, streaming_timeout=default)
        executor.submit()
        assert connection.calls[0].get('streaming_timeout') == expected

    def test_submit_sends_the_twisted_callback(self) -> None:
        def callback(_: Any) -> None:
            pass
        executor, _, connection = _executor([], callback=callback)
        executor.submit()
        assert connection.calls[0]['callback'] is callback

    def test_submit_leaves_the_caller_arguments_alone(self) -> None:
        params = {'statement': 'q', 'parent_span': NoOpSpan('parent'), 'timeout': 5}
        expected = dict(params)
        executor, _, _ = _executor([], params, obs_handler=cast(ObservableRequestHandler, _Handler()))
        executor.submit()
        assert params == expected

    def test_rows_are_decoded_and_the_final_item_read_after_the_last(self) -> None:
        executor, _, _ = _executor(['a', 'b', None, _final({'n': 1})])
        executor.submit()
        assert executor.next_row() == 'decoded:a'
        assert executor.next_row() == 'decoded:b'
        assert executor.final_metadata() is None
        with pytest.raises(StopIteration):
            executor.next_row()
        assert executor.final_metadata() == {'n': 1}

    def test_the_first_row_ends_the_span(self) -> None:
        handler = _Handler()
        executor, _, _ = _executor(['a', 'b', None, _final()], obs_handler=cast(ObservableRequestHandler, handler))
        executor.submit()
        executor.next_row()
        executor.next_row()
        assert handler.meter_ends == [None]
        assert handler.core_spans == [('core-span', False)]

    def test_a_core_error_in_the_rows_is_raised_mapped(self) -> None:
        executor, _, _ = _executor([_CoreError()])
        executor.submit()
        with pytest.raises(RequestCanceledException):
            executor.next_row()

    def test_a_core_error_as_the_final_item_is_raised_mapped(self) -> None:
        executor, _, _ = _executor([None, _CoreError()])
        executor.submit()
        with pytest.raises(RequestCanceledException):
            executor.next_row()

    def test_a_final_item_that_cannot_be_read_is_internal(self) -> None:
        executor, _, _ = _executor([None, object()])
        executor.submit()
        with pytest.raises(InternalSDKException):
            executor.next_row()

    def test_fetch_raises_the_stop_it_is_given(self) -> None:
        stream = ClassicStream(cast(pycbc_connection, _Connection(_Stream([None, _final()]))), _Spec({}))
        stream.submit()
        with pytest.raises(StopAsyncIteration):
            stream.fetch(StopAsyncIteration)

    def test_a_stream_ended_without_its_final_item_says_so(self) -> None:
        executor, _, _ = _executor([None])
        executor.submit()
        with pytest.raises(InternalSDKException, match='without its final item'):
            executor.next_row()

    def test_a_stopped_stream_is_the_end(self) -> None:
        executor, _, _ = _executor([StopIteration, _final({'n': 1})])
        executor.submit()
        with pytest.raises(StopIteration):
            executor.next_row()
        assert executor.final_metadata() == {'n': 1}

    def test_fetching_before_submitting_is_internal(self) -> None:
        executor, _, _ = _executor(['a'])
        with pytest.raises(InternalSDKException):
            executor.next_row()

    def test_finalize_on_an_error_cancels_the_stream(self) -> None:
        handler = _Handler()
        executor, stream, _ = _executor(['a'], obs_handler=cast(ObservableRequestHandler, handler))
        executor.submit()
        err = RequestCanceledException()
        executor.finalize(err)
        assert stream.cancel_calls == 1
        assert handler.meter_ends == [err]
        assert handler.core_spans == [('core-span', True)]

    def test_finalize_on_the_end_keeps_the_stream(self) -> None:
        # A drained stream still holds its final item, which a cancel would discard.
        executor, stream, _ = _executor([None, _final()])
        executor.submit()
        executor.finalize()
        assert stream.cancel_calls == 0

    def test_close_cancels_and_ends_the_span_as_a_success(self) -> None:
        handler = _Handler()
        executor, stream, _ = _executor(['a', 'b'], obs_handler=cast(ObservableRequestHandler, handler))
        executor.submit()
        executor.close()
        assert stream.cancel_calls == 1
        assert handler.core_spans == [('core-span', False)]

    def test_close_before_submitting_does_nothing(self) -> None:
        executor, stream, _ = _executor(['a'])
        executor.close()
        assert stream.cancel_calls == 0

    def test_the_span_ends_once(self) -> None:
        handler = _Handler()
        executor, _, _ = _executor(['a', None, _final()], obs_handler=cast(ObservableRequestHandler, handler))
        executor.submit()
        executor.next_row()
        executor.finalize(RequestCanceledException())
        assert len(handler.core_spans) == 1

    def test_a_binding_that_raises_ends_the_span_as_an_error(self) -> None:
        # The span was created before the call, and no stream came back to carry a core span, so the
        # handler ends it as a request that failed before reaching the core.
        handler = _Handler()
        err = RuntimeError('Connection not initialized')
        executor, stream, _ = _executor(['a'], binding_exc=err, obs_handler=cast(ObservableRequestHandler, handler))
        with pytest.raises(RuntimeError):
            executor.submit()
        assert len(handler.http_spans) == 1
        executor.finalize(err)
        assert handler.exits == [(RuntimeError, err)]
        assert handler.meter_ends == []
        assert handler.core_spans == []
        assert stream.cancel_calls == 0

    def test_a_binding_that_raises_ends_the_span_once(self) -> None:
        handler = _Handler()
        err = RuntimeError('Connection not initialized')
        executor, _, _ = _executor(['a'], binding_exc=err, obs_handler=cast(ObservableRequestHandler, handler))
        with pytest.raises(RuntimeError):
            executor.submit()
        executor.finalize(err)
        executor.close()
        assert handler.exits == [(RuntimeError, err)]

    def test_close_before_submitting_ends_no_span(self) -> None:
        handler = _Handler()
        executor, _, _ = _executor(['a'], obs_handler=cast(ObservableRequestHandler, handler))
        executor.close()
        assert handler.exits == []
        assert handler.meter_ends == []
        assert handler.core_spans == []

    def test_a_binding_that_raises_under_the_iterator_ends_the_span(self) -> None:
        # The iterator and the executor together, since each side's fake would stand in for the other.
        handler = _Handler()
        err = RuntimeError('Connection not initialized')
        executor, _, _ = _executor(['a'], binding_exc=err, obs_handler=cast(ObservableRequestHandler, handler))
        rows = RowIterator(executor)
        with pytest.raises(InternalSDKException) as exc_info:
            iter(rows)
        assert handler.exits == [(InternalSDKException, exc_info.value)]
        with pytest.raises(StopIteration):
            next(rows)


class _TaggingSerializer(Serializer):

    def serialize(self, value: Any) -> bytes:
        raise NotImplementedError()

    def deserialize(self, value: bytes) -> Any:
        return ('tagged', value)


class QueryStreamSpecUnitTests:

    def test_calls_the_query_binding(self) -> None:
        connection = _Connection(_Stream([]))
        spec = QueryStreamSpec({}, DefaultJsonSerializer())
        assert spec.binding_call(cast(pycbc_connection, connection)) == connection.pycbc_query

    def test_kwargs_are_a_copy(self) -> None:
        params = {'statement': 'q'}
        kwargs = QueryStreamSpec(params, DefaultJsonSerializer()).binding_kwargs()
        kwargs['added'] = True
        assert params == {'statement': 'q'}

    def test_span_attributes_carry_the_statement_and_the_query_context(self) -> None:
        kwargs = {'statement': 'q', 'query_context': 'default:`b`.`s`'}
        attrs = QueryStreamSpec(kwargs, DefaultJsonSerializer()).span_attributes(kwargs)
        assert attrs['statement'] == 'q'
        assert attrs['query_params'] is kwargs
        assert attrs.get('scope_name') == 's'
        assert attrs.get('bucket_name') is not None

    def test_span_attributes_without_a_query_context(self) -> None:
        kwargs = {'statement': 'q'}
        attrs = QueryStreamSpec(kwargs, DefaultJsonSerializer()).span_attributes(kwargs)
        assert 'bucket_name' not in attrs
        assert 'scope_name' not in attrs

    def test_rows_decode_with_the_serializer_the_query_set(self) -> None:
        spec = QueryStreamSpec({'serializer': _TaggingSerializer()}, DefaultJsonSerializer())
        assert spec.decode_row(b'1') == ('tagged', b'1')

    def test_rows_fall_back_to_the_cluster_serializer(self) -> None:
        spec = QueryStreamSpec({}, _TaggingSerializer())
        assert spec.decode_row(b'1') == ('tagged', b'1')

    def test_final_metadata_is_query_metadata(self) -> None:
        spec = QueryStreamSpec({}, DefaultJsonSerializer())
        metadata = spec.decode_final_metadata({'metadata': {'request_id': 'r', 'client_context_id': 'c'}})
        assert isinstance(metadata, QueryMetaData)
        assert metadata.request_id() == 'r'

    def test_no_metadata_is_none(self) -> None:
        assert QueryStreamSpec({}, DefaultJsonSerializer()).decode_final_metadata({}) is None


class AnalyticsStreamSpecUnitTests:

    def test_calls_the_analytics_binding(self) -> None:
        connection = _Connection(_Stream([]))
        spec = AnalyticsStreamSpec({}, DefaultJsonSerializer())
        assert spec.binding_call(cast(pycbc_connection, connection)) == connection.pycbc_analytics_query

    def test_kwargs_are_a_copy(self) -> None:
        params = {'statement': 'q'}
        kwargs = AnalyticsStreamSpec(params, DefaultJsonSerializer()).binding_kwargs()
        kwargs['added'] = True
        assert params == {'statement': 'q'}

    def test_span_attributes_carry_the_statement_and_the_scope_qualifier(self) -> None:
        kwargs = {'statement': 'q', 'scope_qualifier': 'default:`b`.`s`'}
        attrs = AnalyticsStreamSpec(kwargs, DefaultJsonSerializer()).span_attributes(kwargs)
        assert attrs['statement'] == 'q'
        assert attrs['query_params'] is kwargs
        assert attrs.get('bucket_name') == 'b'
        assert attrs.get('scope_name') == 's'

    def test_a_query_context_is_not_a_scope_qualifier(self) -> None:
        kwargs = {'statement': 'q', 'query_context': 'default:`b`.`s`'}
        attrs = AnalyticsStreamSpec(kwargs, DefaultJsonSerializer()).span_attributes(kwargs)
        assert 'bucket_name' not in attrs
        assert 'scope_name' not in attrs

    def test_rows_decode_with_the_serializer_the_query_set(self) -> None:
        spec = AnalyticsStreamSpec({'serializer': _TaggingSerializer()}, DefaultJsonSerializer())
        assert spec.decode_row(b'1') == ('tagged', b'1')

    def test_rows_fall_back_to_the_cluster_serializer(self) -> None:
        spec = AnalyticsStreamSpec({}, _TaggingSerializer())
        assert spec.decode_row(b'1') == ('tagged', b'1')

    def test_final_metadata_is_analytics_metadata(self) -> None:
        spec = AnalyticsStreamSpec({}, DefaultJsonSerializer())
        metadata = spec.decode_final_metadata({'metadata': {'request_id': 'r', 'client_context_id': 'c'}})
        assert isinstance(metadata, AnalyticsMetaData)
        assert metadata.request_id() == 'r'

    def test_no_metadata_is_none(self) -> None:
        assert AnalyticsStreamSpec({}, DefaultJsonSerializer()).decode_final_metadata({}) is None


class ViewStreamSpecUnitTests:

    def test_calls_the_view_binding(self) -> None:
        connection = _Connection(_Stream([]))
        spec = ViewStreamSpec({}, DefaultJsonSerializer())
        assert spec.binding_call(cast(pycbc_connection, connection)) == connection.pycbc_view_query

    def test_kwargs_are_a_copy(self) -> None:
        encoded = {'bucket_name': 'b', 'document_name': 'dd'}
        kwargs = ViewStreamSpec(encoded, DefaultJsonSerializer()).binding_kwargs()
        kwargs['added'] = True
        assert encoded == {'bucket_name': 'b', 'document_name': 'dd'}

    def test_span_attributes_carry_the_bucket(self) -> None:
        spec = ViewStreamSpec({'bucket_name': 'b', 'document_name': 'dd'}, DefaultJsonSerializer())
        assert spec.span_attributes(spec.binding_kwargs()) == {'bucket_name': 'b'}

    def test_rows_decode_with_the_serializer_the_query_set(self) -> None:
        spec = ViewStreamSpec({'serializer': _TaggingSerializer()}, DefaultJsonSerializer())
        row = spec.decode_row({'id': 'd', 'key': '"k"', 'value': '1'})
        assert row == ViewRow(key=('tagged', b'"k"'), id='d', value=('tagged', b'1'))

    def test_rows_fall_back_to_the_cluster_serializer(self) -> None:
        spec = ViewStreamSpec({}, _TaggingSerializer())
        assert spec.decode_row({'id': 'd', 'value': '1'}).value == ('tagged', b'1')

    def test_final_metadata_reads_the_core_metadata(self) -> None:
        metadata = ViewStreamSpec({}, DefaultJsonSerializer()).decode_final_metadata(
            {'metadata': {'total_rows': 12, 'debug_info': None}})
        assert isinstance(metadata, ViewMetaData)
        assert metadata.total_rows() == 12

    def test_no_metadata_is_none(self) -> None:
        assert ViewStreamSpec({}, DefaultJsonSerializer()).decode_final_metadata({}) is None
