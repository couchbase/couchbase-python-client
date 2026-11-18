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

"""Rows off the gateway's `grpc.aio` stream: the asyncio executor, and the read Twisted makes whole.

Each row is awaited as its message arrives, so nothing is buffered beyond one message.
"""

from __future__ import annotations

import asyncio
from collections.abc import (AsyncGenerator,
                             Awaitable,
                             Callable)
from typing import (TYPE_CHECKING,
                    Generic,
                    TypeVar)

from couchbase.common._streaming import AsyncStreamingExecutor, RowIterator
from couchbase.protocol.couchbase2._streaming import (Rows,
                                                      StreamExecutor,
                                                      end_span)

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler
    from couchbase.common._observability.handler import OpAttributeOptions

RowT = TypeVar('RowT')
MetaT = TypeVar('MetaT')
MessageT = TypeVar('MessageT')


class AsyncStreamExecutor(AsyncStreamingExecutor[RowT, MetaT], Generic[RowT, MetaT, MessageT]):
    """`open_stream` sends the request; it is awaited at submit, so the request's deadline starts then."""

    def __init__(self,
                 open_stream: Callable[[], Awaitable[AsyncGenerator[MessageT, None]]],
                 rows: Rows[RowT, MetaT, MessageT],
                 *,
                 obs_handler: ObservableRequestHandler,
                 span_attributes: OpAttributeOptions) -> None:
        self._open_stream = open_stream
        self._rows = rows
        self._obs_handler = obs_handler
        self._span_attributes = span_attributes
        self._stream: AsyncGenerator[MessageT, None] | None = None
        self._final_metadata: MetaT | None = None
        self._span_ended = False
        self._closing: asyncio.Future[None] | None = None

    async def submit(self) -> None:
        self._obs_handler.create_http_span(**self._span_attributes)
        self._stream = await self._open_stream()

    async def next_row(self) -> RowT:
        if self._stream is None:
            raise StopAsyncIteration
        # A message carries many rows, so without this a drain would hold the loop to its last row.
        await asyncio.sleep(0)
        while not self._rows:
            try:
                message = await self._stream.__anext__()
            except StopAsyncIteration:
                self._final_metadata = self._rows.final_metadata()
                raise
            self._rows.take(message)
        return self._rows.next_row()

    def final_metadata(self) -> MetaT | None:
        return self._final_metadata

    async def close(self) -> None:
        if self._stream is not None:
            await self._stream.aclose()
        self._end_span(None)
        self._release()

    def _end_span(self, exc: BaseException | None) -> None:
        if self._span_ended:
            return
        self._span_ended = True
        end_span(self._obs_handler, exc)

    def _cancel(self) -> None:
        # An error raised by the stream has already closed it; one raised by a row's decode leaves it
        # suspended, and closing it cancels the call.  Closing is a coroutine, so it is scheduled.
        if self._stream is None:
            return
        try:
            self._closing = asyncio.ensure_future(self._stream.aclose())
        except RuntimeError:
            # No running loop, so the call is cancelled when the stream is collected.
            pass


async def buffered_rows(open_stream: Callable[[], Awaitable[AsyncGenerator[MessageT, None]]],
                        rows: Rows[RowT, MetaT, MessageT]) -> RowIterator[RowT, MetaT]:
    """The whole of a stream, read before its rows are handed out.

    Twisted hands out a blocking result, so its rows have to be read before it exists or iterating
    them would block the reactor.  The rows are decoded as they are iterated.
    """
    stream = await open_stream()
    messages = [message async for message in stream]
    return RowIterator(StreamExecutor(lambda: iter(messages), rows))
