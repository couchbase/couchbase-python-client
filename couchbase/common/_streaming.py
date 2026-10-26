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

"""The streaming contract: a protocol owns how rows are fetched, and these iterators own the rest.

A streaming result has two channels.  The rows, and one ``final_metadata`` value, typed by the
service, which exists only once the rows are done: query, analytics and views carry their
metadata there, search its metadata and its facets together, and a range scan nothing.

The iterators are the single point of error handling and teardown for every streaming result.
Each terminal branch, the end of the rows and every error alike, marks the iterator done and
finalizes the executor, so an error ends the iteration as a raising generator does: it is
raised once, and every later call raises the stop exception without fetching.

``asyncio.CancelledError``, ``KeyboardInterrupt`` and ``SystemExit`` derive from
``BaseException`` rather than ``Exception``, so each iterator has an ``except BaseException``
branch: a cancellation still ends the span and cancels the stream, and is re-raised unchanged.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from couchbase.common.exceptions import (AlreadyQueriedException,
                                         CouchbaseException,
                                         InternalSDKException)

RowT = TypeVar('RowT')
FinalMetadataT = TypeVar('FinalMetadataT')
# An executor only produces rows and metadata, so one yielding a narrower metadata type stands in where the
# iterator is typed with a wider one.
_RowT_co = TypeVar('_RowT_co', covariant=True)
_FinalMetadataT_co = TypeVar('_FinalMetadataT_co', covariant=True)


class _ExecutorBase(ABC, Generic[_RowT_co, _FinalMetadataT_co]):

    @abstractmethod
    def final_metadata(self) -> _FinalMetadataT_co | None:
        """The value read after the last row, and ``None`` until the rows are done."""

    def finalize(self, exc: BaseException | None = None) -> None:
        """End the request: its span, then on an error the stream, then what the side holds."""
        self._end_span(exc)
        if exc is not None:
            self._cancel()
        self._release()

    @abstractmethod
    def _end_span(self, exc: BaseException | None) -> None:
        """End the span and meter, once.  When to call it beyond `finalize` is the executor's choice."""

    @abstractmethod
    def _cancel(self) -> None:
        """Stop the server producing.  A no-op on a request not yet submitted."""

    def _release(self) -> None:
        """Release what this side holds for the fetch, which is nothing unless it says otherwise."""

    def _close(self) -> None:
        # A close is a cancel the caller asked for, so the span ends without an error.
        self._cancel()
        self._end_span(None)
        self._release()


class StreamingExecutor(_ExecutorBase[_RowT_co, _FinalMetadataT_co]):
    """How one protocol fetches the rows of one streaming request, for the blocking APIs."""

    @abstractmethod
    def submit(self) -> None:
        """Send the request.  Called once, before the first row."""

    @abstractmethod
    def next_row(self) -> _RowT_co:
        """The next row, or `StopIteration` once there are none, by which time `final_metadata` is set."""

    def close(self) -> None:
        """Abandon the stream: cancel it, and end the span as a success."""
        self._close()


class AsyncStreamingExecutor(_ExecutorBase[_RowT_co, _FinalMetadataT_co]):
    """How one protocol fetches the rows of one streaming request, for asyncio.

    The fetch is a coroutine by contract.  How it waits is the protocol's: an executor thread over
    a blocking core, a future resolved from a core callback, or a native async stream.
    """

    @abstractmethod
    async def submit(self) -> None:
        """Send the request, on the loop that will await the rows.  Called once, before the first row."""

    @abstractmethod
    async def next_row(self) -> _RowT_co:
        """The next row, or `StopAsyncIteration` once there are none, by which time `final_metadata` is set."""

    async def close(self) -> None:
        """Abandon the stream: cancel it, and end the span as a success."""
        self._close()


class RowIterator(Generic[RowT, FinalMetadataT]):
    """The rows of a streaming result, for the blocking APIs."""

    def __init__(self, executor: StreamingExecutor[RowT, FinalMetadataT]) -> None:
        self._executor = executor
        self._started = False
        self._done = False

    @property
    def started_streaming(self) -> bool:
        return self._started

    @property
    def done_streaming(self) -> bool:
        return self._done

    def start(self) -> None:
        """Submit the request, if it has not been.  Iterating does this; Twisted does it first."""
        if self._started:
            return
        self._started = True
        try:
            self._executor.submit()
        except CouchbaseException as ex:
            self._terminate(ex)
            raise
        except Exception as ex:
            err = InternalSDKException(str(ex))
            self._terminate(err)
            raise err
        except BaseException as ex:
            self._terminate(ex)
            raise

    def final_metadata(self) -> FinalMetadataT | None:
        return self._executor.final_metadata()

    def execute(self) -> list[RowT]:
        return list(self)

    def close(self) -> None:
        """Stop iterating early.  A stream not yet drained is cancelled, and later calls stop."""
        if self._done:
            return
        self._done = True
        if self._started:
            self._executor.close()

    def __iter__(self) -> RowIterator[RowT, FinalMetadataT]:
        if self._done:
            raise AlreadyQueriedException()
        self.start()
        return self

    def __next__(self) -> RowT:
        if self._done:
            raise StopIteration
        try:
            return self._executor.next_row()
        except StopIteration:
            self._terminate()
            raise
        except CouchbaseException as ex:
            self._terminate(ex)
            raise
        except Exception as ex:
            err = InternalSDKException(str(ex))
            self._terminate(err)
            raise err
        except BaseException as ex:
            self._terminate(ex)
            raise

    def _terminate(self, exc: BaseException | None = None) -> None:
        self._done = True
        self._executor.finalize(exc)


class AsyncRowIterator(Generic[RowT, FinalMetadataT]):
    """The rows of a streaming result, for asyncio.  The request is submitted by the first fetch."""

    def __init__(self, executor: AsyncStreamingExecutor[RowT, FinalMetadataT]) -> None:
        self._executor = executor
        self._started = False
        self._done = False

    @property
    def started_streaming(self) -> bool:
        return self._started

    @property
    def done_streaming(self) -> bool:
        return self._done

    def final_metadata(self) -> FinalMetadataT | None:
        return self._executor.final_metadata()

    def execute(self) -> asyncio.Task[list[RowT]]:
        async def _execute() -> list[RowT]:
            return [r async for r in self]

        return asyncio.create_task(_execute())

    async def aclose(self) -> None:
        """Stop iterating early.  A stream not yet drained is cancelled, and later calls stop."""
        if self._done:
            return
        self._done = True
        if self._started:
            await self._executor.close()

    def __aiter__(self) -> AsyncRowIterator[RowT, FinalMetadataT]:
        if self._done:
            raise AlreadyQueriedException()
        return self

    async def __anext__(self) -> RowT:
        if self._done:
            raise StopAsyncIteration
        try:
            if not self._started:
                self._started = True
                await self._executor.submit()
            return await self._executor.next_row()
        except StopAsyncIteration:
            self._terminate()
            raise
        except CouchbaseException as ex:
            self._terminate(ex)
            raise
        except Exception as ex:
            err = InternalSDKException(str(ex))
            self._terminate(err)
            raise err
        except BaseException as ex:
            self._terminate(ex)
            raise

    def _terminate(self, exc: BaseException | None = None) -> None:
        self._done = True
        self._executor.finalize(exc)
