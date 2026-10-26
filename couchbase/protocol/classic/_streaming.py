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

"""Classic's streaming request over the core's stream, its blocking executor, and a spec per service.

The core buffers a whole response before the first row is read, sends a failed request as an error
value in the stream rather than by raising, ends the rows with ``None``, and sends the final
metadata as one more item after it.  A request given a callback is called back once all of it is
queued.  The asyncio and Twisted executors are in each package's ``protocol.classic._streaming``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from typing import (TYPE_CHECKING,
                    Any,
                    Generic,
                    TypeVar)

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._streaming import (FinalMetadataT,
                                         RowT,
                                         StreamingExecutor)
from couchbase.common.analytics import AnalyticsMetaData
from couchbase.common.exceptions import CouchbaseException, InternalSDKException
from couchbase.common.n1ql import QueryMetaData
from couchbase.common.views import ViewMetaData, ViewRow
from couchbase.protocol.classic._error_mapping import ErrorMapper
from couchbase.protocol.classic._metadata import (_analytics_metadata_from_core,
                                                  _query_metadata_from_core,
                                                  _view_metadata_from_core)
from couchbase.protocol.classic._pycbc_core import pycbc_exception as PycbcCoreException
from couchbase.protocol.classic._result import _view_row_from_core
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.common._observability.handler import OpAttributeOptions
    from couchbase.protocol.classic._pycbc_core import pycbc_connection, pycbc_streamed_result

_T = TypeVar('_T')
_RowT_co = TypeVar('_RowT_co', covariant=True)
_FinalMetadataT_co = TypeVar('_FinalMetadataT_co', covariant=True)


def next_core_row(streaming_result: Iterator[_T | PycbcCoreException | None],
                  stop: type[Exception]) -> _T:
    """Take the next raw row from a core stream, raising its error mapped and ``stop`` at the end.

    The core reports a failed request as an error value in the stream rather than by raising, and
    ends a successful one with ``None``, so neither ever reaches the row decode.
    """
    row = next(streaming_result)

    if isinstance(row, PycbcCoreException):
        raise ErrorMapper.build_exception(row)
    if row is None:
        raise stop
    return row


class ClassicStreamSpec(ABC, Generic[_RowT_co, _FinalMetadataT_co]):
    """What one service supplies to classic's executors: its call, its arguments, and its decodes."""

    @abstractmethod
    def binding_call(self, connection: pycbc_connection) -> Callable[..., pycbc_streamed_result[Any]]:
        """The binding method that starts this service's stream."""

    @abstractmethod
    def binding_kwargs(self) -> dict[str, Any]:
        """A fresh copy of the request's arguments, spans included; the executor takes the spans out."""

    @abstractmethod
    def span_attributes(self, kwargs: dict[str, Any]) -> OpAttributeOptions:
        """The span attributes this service records, read from the arguments the call is sent."""

    @abstractmethod
    def decode_row(self, row: Any) -> _RowT_co:
        """A row as the caller receives it, from the one the core sent."""

    @abstractmethod
    def decode_final_metadata(self, raw_result: dict[str, Any]) -> _FinalMetadataT_co | None:
        """The final metadata, from the core's last item."""


class ClassicStream(Generic[RowT, FinalMetadataT]):
    """One classic streaming request: its call, its span, and the reads of its rows and final item.

    Each side's executor holds one and decides only how to wait for the core.
    """

    def __init__(self,
                 connection: pycbc_connection,
                 spec: ClassicStreamSpec[RowT, FinalMetadataT],
                 *,
                 obs_handler: ObservableRequestHandler | None = None,
                 streaming_timeout: Any = None) -> None:
        self._connection = connection
        self._spec = spec
        self._obs_handler = obs_handler
        self._streaming_timeout = streaming_timeout
        self._streamed_result: pycbc_streamed_result[Any] | None = None
        self._final_metadata: FinalMetadataT | None = None
        self._submitted = False
        self._span_ended = False

    def submit(self, callback: Callable[[Any], None] | None = None) -> None:
        """Send the request.  The core calls ``callback``, if given, once the response is queued."""
        self._submitted = True
        kwargs = self._spec.binding_kwargs()
        # The handler creates the span, so the spans the caller gave are its parents, not arguments.
        span = kwargs.pop('span', None)
        parent_span = kwargs.pop('parent_span', None)
        if self._obs_handler:
            # The request is lazy, so its span starts when it is sent rather than when it was built.
            attrs = self._spec.span_attributes(kwargs)
            attrs['parent_span'] = ObservableRequestHandler.maybe_get_parent_span(span=span, parent_span=parent_span)
            attrs['use_now_as_start_time'] = True
            self._obs_handler.create_http_span(**attrs)

        streaming_timeout = kwargs.get('timeout', self._streaming_timeout)
        if streaming_timeout:
            kwargs['streaming_timeout'] = streaming_timeout
        if callback:
            kwargs['callback'] = callback

        if self._obs_handler:
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if self._obs_handler.is_legacy_tracer:
                legacy_request_span = self._obs_handler.legacy_request_span
                if legacy_request_span:
                    kwargs['parent_span'] = legacy_request_span
            else:
                kwargs['wrapper_span_name'] = self._obs_handler.wrapper_span_name

        self._streamed_result = self._spec.binding_call(self._connection)(**kwargs)

    def fetch(self, stop: type[Exception]) -> RowT:
        """The next row, reading the final metadata and raising ``stop`` after the last.

        A stream that stops without its end marker raises ``stop`` too, and the final read decides
        whether that was an error: a `StopIteration` cannot leave a coroutine, which turns it into
        a `RuntimeError`, so the async side could not tell it from a failure otherwise.
        """
        try:
            row = next_core_row(self._require_streamed_result(), stop)
        except (stop, StopIteration):
            self._final_metadata = self._read_final_metadata()
            raise stop from None
        return self._spec.decode_row(row)

    def final_metadata(self) -> FinalMetadataT | None:
        return self._final_metadata

    def end_span(self, exc: BaseException | None) -> None:
        """End the span and meter, once.  A request never sent has neither."""
        if self._span_ended or not self._submitted:
            return
        self._span_ended = True
        if self._obs_handler and self._streamed_result is None:
            # The binding raised, so there is no core span to end the span with.
            self._obs_handler.__exit__(type(exc) if exc is not None else None, exc, None)
        elif self._obs_handler and self._streamed_result:
            self._obs_handler.process_meter_end(exc_val=exc)
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if self._obs_handler.is_legacy_tracer:
                # the handler knows how to handle this legacy situation (essentially just ends the span)
                self._obs_handler.process_core_span(None)
            elif hasattr(self._streamed_result, 'core_span'):
                self._obs_handler.process_core_span(self._streamed_result.core_span, with_error=(exc is not None))

    def cancel(self) -> None:
        """Unblock a read still waiting on the core, rather than wait for the whole server-side operation.

        Only an error or a close cancels, so a drained stream keeps its final item.
        """
        if self._streamed_result is not None:
            self._streamed_result.cancel()

    def _read_final_metadata(self) -> FinalMetadataT | None:
        try:
            response = next(self._require_streamed_result(), None)
            if isinstance(response, PycbcCoreException):
                raise ErrorMapper.build_exception(response)
            if response is None:
                raise InternalSDKException('The stream ended without its final item.')
            return self._spec.decode_final_metadata(response.raw_result)
        except CouchbaseException:
            raise
        except Exception as ex:
            raise InternalSDKException(str(ex))

    def _require_streamed_result(self) -> pycbc_streamed_result[Any]:
        if self._streamed_result is None:
            raise InternalSDKException('The streaming request was not submitted.')
        return self._streamed_result


class ClassicStreamingExecutor(StreamingExecutor[RowT, FinalMetadataT]):
    """Classic's blocking executor: each read waits on the core, the GIL released."""

    def __init__(self,
                 connection: pycbc_connection,
                 spec: ClassicStreamSpec[RowT, FinalMetadataT],
                 *,
                 obs_handler: ObservableRequestHandler | None = None,
                 streaming_timeout: Any = None,
                 callback: Callable[[Any], None] | None = None) -> None:
        self._stream = ClassicStream(connection,
                                     spec,
                                     obs_handler=obs_handler,
                                     streaming_timeout=streaming_timeout)
        # Twisted's: it hands out the rows only once the core has called it.
        self._callback = callback

    def submit(self) -> None:
        self._stream.submit(self._callback)

    def next_row(self) -> RowT:
        row = self._stream.fetch(StopIteration)
        # The first row is the earliest point the buffered core is known to have answered.
        self._stream.end_span(None)
        return row

    def final_metadata(self) -> FinalMetadataT | None:
        return self._stream.final_metadata()

    def _end_span(self, exc: BaseException | None) -> None:
        self._stream.end_span(exc)

    def _cancel(self) -> None:
        self._stream.cancel()


class QueryStreamSpec(ClassicStreamSpec[Any, QueryMetaData]):

    def __init__(self, params: dict[str, Any], default_serializer: Serializer) -> None:
        self._params = params
        self._serializer: Serializer = params.get('serializer', None) or default_serializer

    def binding_call(self, connection: pycbc_connection) -> Callable[..., pycbc_streamed_result[Any]]:
        return connection.pycbc_query

    def binding_kwargs(self) -> dict[str, Any]:
        return dict(self._params)

    def span_attributes(self, kwargs: dict[str, Any]) -> OpAttributeOptions:
        attrs: OpAttributeOptions = {'statement': kwargs.get('statement'), 'query_params': kwargs}
        query_context = kwargs.get('query_context', None)
        if query_context:
            components = ObservableRequestHandler.get_query_context_components(query_context)
            if components is not None:
                attrs['bucket_name'], attrs['scope_name'] = components
        return attrs

    def decode_row(self, row: Any) -> Any:
        return self._serializer.deserialize(row)

    def decode_final_metadata(self, raw_result: dict[str, Any]) -> QueryMetaData | None:
        record = _query_metadata_from_core(raw_result.get('metadata', None))
        return None if record is None else QueryMetaData(record)


class AnalyticsStreamSpec(ClassicStreamSpec[Any, AnalyticsMetaData]):

    def __init__(self, params: dict[str, Any], default_serializer: Serializer) -> None:
        self._params = params
        self._serializer: Serializer = params.get('serializer', None) or default_serializer

    def binding_call(self, connection: pycbc_connection) -> Callable[..., pycbc_streamed_result[Any]]:
        return connection.pycbc_analytics_query

    def binding_kwargs(self) -> dict[str, Any]:
        return dict(self._params)

    def span_attributes(self, kwargs: dict[str, Any]) -> OpAttributeOptions:
        attrs: OpAttributeOptions = {'statement': kwargs.get('statement'), 'query_params': kwargs}
        scope_qualifier = kwargs.get('scope_qualifier', None)
        if scope_qualifier:
            components = ObservableRequestHandler.get_query_context_components(scope_qualifier, is_analytics=True)
            if components is not None:
                attrs['bucket_name'], attrs['scope_name'] = components
        return attrs

    def decode_row(self, row: Any) -> Any:
        return self._serializer.deserialize(row)

    def decode_final_metadata(self, raw_result: dict[str, Any]) -> AnalyticsMetaData | None:
        record = _analytics_metadata_from_core(raw_result.get('metadata', None))
        return None if record is None else AnalyticsMetaData(record)


class ViewStreamSpec(ClassicStreamSpec[ViewRow, ViewMetaData]):

    def __init__(self, encoded_query: dict[str, Any], default_serializer: Serializer) -> None:
        self._encoded_query = encoded_query
        self._serializer: Serializer = encoded_query.get('serializer', None) or default_serializer

    def binding_call(self, connection: pycbc_connection) -> Callable[..., pycbc_streamed_result[Any]]:
        return connection.pycbc_view_query

    def binding_kwargs(self) -> dict[str, Any]:
        return dict(self._encoded_query)

    def span_attributes(self, kwargs: dict[str, Any]) -> OpAttributeOptions:
        return {'bucket_name': kwargs['bucket_name']}

    def decode_row(self, row: Any) -> ViewRow:
        return _view_row_from_core(row, self._serializer)

    def decode_final_metadata(self, raw_result: dict[str, Any]) -> ViewMetaData | None:
        record = _view_metadata_from_core(raw_result.get('metadata', None))
        return None if record is None else ViewMetaData(record)
