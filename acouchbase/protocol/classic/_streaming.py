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

"""Classic's asyncio executor, and the wait on the core's callback that Twisted shares."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from couchbase.common._streaming import (AsyncStreamingExecutor,
                                         FinalMetadataT,
                                         RowT)
from couchbase.protocol.classic._streaming import ClassicStream, ClassicStreamSpec

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler
    from couchbase.protocol.classic._pycbc_core import pycbc_connection


def response_signal(loop: asyncio.AbstractEventLoop) -> tuple[asyncio.Future[None], Callable[[Any], None]]:
    """A future on ``loop``, and the core callback that resolves it once the response is queued.

    The core queues every row, the end marker and the final item, then calls the callback from an
    IO thread, so once the future resolves no read of the stream waits on the core.
    """
    responded: asyncio.Future[None] = loop.create_future()

    def _resolve() -> None:
        # Cancelled by a caller that stopped waiting, which the core cannot know.
        if not responded.done():
            responded.set_result(None)

    def _on_response(_: Any) -> None:
        try:
            loop.call_soon_threadsafe(_resolve)
        except RuntimeError:
            # The loop closed with the request in flight, so nothing is waiting for it.
            pass

    return responded, _on_response


class ClassicAsyncStreamingExecutor(AsyncStreamingExecutor[RowT, FinalMetadataT]):
    """Classic's asyncio executor: submit awaits the core's callback, and the reads then never wait.

    The core queues a whole response before it calls back, so no thread is needed.  A protocol
    whose core hands out rows as they arrive needs a wait per row instead.
    """

    def __init__(self,
                 connection: pycbc_connection,
                 spec: ClassicStreamSpec[RowT, FinalMetadataT],
                 loop: asyncio.AbstractEventLoop,
                 *,
                 obs_handler: ObservableRequestHandler | None = None,
                 streaming_timeout: Any = None) -> None:
        self._stream = ClassicStream(connection,
                                     spec,
                                     obs_handler=obs_handler,
                                     streaming_timeout=streaming_timeout)
        self._loop = loop

    async def submit(self) -> None:
        responded, on_response = response_signal(self._loop)
        self._stream.submit(on_response)
        await responded

    async def next_row(self) -> RowT:
        # The rows are already queued, so without this a drain would hold the loop to the last row.
        await asyncio.sleep(0)
        row = self._stream.fetch(StopAsyncIteration)
        self._stream.end_span(None)
        return row

    def final_metadata(self) -> FinalMetadataT | None:
        return self._stream.final_metadata()

    def _end_span(self, exc: BaseException | None) -> None:
        self._stream.end_span(exc)

    def _cancel(self) -> None:
        self._stream.cancel()
