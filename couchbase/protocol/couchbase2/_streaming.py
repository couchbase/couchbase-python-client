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

"""Rows off a gateway stream, and the blocking executor that reads them.

Rows stream lazily.  One message holds several rows, so one message is buffered at a time, and what
follows the rows is read once they run out.  Each service reads its own messages: `QueryRows` here
reads a query's, and the executor reads any service's through its `Rows`.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from typing import (TYPE_CHECKING,
                    Any,
                    Generic,
                    Protocol,
                    TypeVar)

from couchbase.common._streaming import StreamingExecutor
from couchbase.common.n1ql import QueryMetaData
from couchbase.protocol.couchbase2._metadata import query_metadata

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler
    from couchbase.common._observability.handler import OpAttributeOptions
    from couchbase.common.n1ql import _QueryMetaDataRecord
    from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2
    from couchbase.serializer import Serializer

RowT = TypeVar('RowT')
MetaT = TypeVar('MetaT')
MessageT = TypeVar('MessageT')
_RowT_co = TypeVar('_RowT_co', covariant=True)
_MetaT_co = TypeVar('_MetaT_co', covariant=True)
_MessageT_contra = TypeVar('_MessageT_contra', contravariant=True)


class Rows(Protocol[_RowT_co, _MetaT_co, _MessageT_contra]):
    """The rows of the message being read, and what follows them once the stream ends."""

    def take(self, message: _MessageT_contra) -> None: ...

    def __bool__(self) -> bool: ...

    def next_row(self) -> _RowT_co: ...

    def final_metadata(self) -> _MetaT_co | None: ...


class QueryRows:
    """A query's rows, and its metadata once a message has carried it.

    A stream that ends with no metadata is not an error: the gateway may send none, and the final
    metadata is then `None`.  A stream that ends with a status other than success is not an error
    either.  The gateway reports a failed query as the call's status, and an unset status reads as
    `RUNNING`, so the field cannot tell a failure from a gateway that left it out.
    """

    def __init__(self, serializer: Serializer) -> None:
        self._serializer = serializer
        self._rows: deque[bytes] = deque()
        self._metadata: _QueryMetaDataRecord | None = None

    def take(self, message: query_pb2.QueryResponse) -> None:
        self._rows.extend(message.rows)
        record = query_metadata(message)
        if record is not None:
            self._metadata = record

    def __bool__(self) -> bool:
        return bool(self._rows)

    def next_row(self) -> Any:
        return self._serializer.deserialize(self._rows.popleft())

    def final_metadata(self) -> QueryMetaData | None:
        return None if self._metadata is None else QueryMetaData(self._metadata)


def end_span(obs_handler: ObservableRequestHandler, exc: BaseException | None) -> None:
    """End a streaming request's span and meter, as its context manager would have."""
    obs_handler.__exit__(None if exc is None else type(exc), exc, None if exc is None else exc.__traceback__)


class StreamExecutor(StreamingExecutor[RowT, MetaT], Generic[RowT, MetaT, MessageT]):
    """The blocking executor: each read waits on the stream, and the span covers it start to end.

    `open_stream` sends the request; it is called at submit, so the request's deadline starts then.
    Without an `obs_handler` the caller owns the span, as Twisted's does.
    """

    def __init__(self,
                 open_stream: Callable[[], Iterator[MessageT]],
                 rows: Rows[RowT, MetaT, MessageT],
                 *,
                 obs_handler: ObservableRequestHandler | None = None,
                 span_attributes: OpAttributeOptions | None = None) -> None:
        self._open_stream = open_stream
        self._rows = rows
        self._obs_handler = obs_handler
        self._span_attributes: OpAttributeOptions = span_attributes or {}
        self._stream: Iterator[MessageT] | None = None
        self._final_metadata: MetaT | None = None
        self._span_ended = obs_handler is None

    def submit(self) -> None:
        if self._obs_handler is not None:
            self._obs_handler.create_http_span(**self._span_attributes)
        self._stream = self._open_stream()

    def next_row(self) -> RowT:
        if self._stream is None:
            raise StopIteration
        while not self._rows:
            message = next(self._stream, None)
            if message is None:
                self._final_metadata = self._rows.final_metadata()
                raise StopIteration
            self._rows.take(message)
        return self._rows.next_row()

    def final_metadata(self) -> MetaT | None:
        return self._final_metadata

    def _end_span(self, exc: BaseException | None) -> None:
        if self._span_ended or self._obs_handler is None:
            return
        self._span_ended = True
        end_span(self._obs_handler, exc)

    def _cancel(self) -> None:
        close = getattr(self._stream, 'close', None)
        if close is not None:
            close()
