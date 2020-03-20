import asyncio

from couchbase import QueryResult, AnalyticsResult
from couchbase_core.asynchronous.view import AsyncViewBase
from couchbase.asynchronous import AsyncViewResult
from couchbase_core.asynchronous.n1ql import AsyncN1QLRequest
from couchbase_core.asynchronous.rowsbase import AsyncRowsBase


class AioBase:
    def __init__(self):
        self.__local_done = False
        self.__accum = asyncio.Queue()
        self._future = asyncio.Future()
        self.start()
        self.raw.rows_per_call = 1000

    @property
    @asyncio.coroutine
    def future(self):
        yield from self._future
        self._future = None

    def __iter__(self):
        if self._future is not None:
            raise ValueError("yield from result.future before calling non-async for.")

        yield from iter(self.__accum.get_nowait, None)

    def __aiter__(self):
        if self._future is None:
            raise ValueError("do not yield from result.future before calling async for.")

        return self

    @asyncio.coroutine
    def __anext__(self):
        try:
            out = None
            if self._future.done() and not self.__accum.empty():
                out = self.__accum.get_nowait()
            elif not self._future.done():
                out = yield from self.__accum.get()

            if out is None:
                raise StopAsyncIteration

            return out
        except asyncio.queues.QueueEmpty:
            raise StopAsyncIteration

    def on_rows(self, rowiter):
        for row in rowiter:
            self.__accum.put_nowait(row)

    def on_done(self):
        self._future.set_result(None)
        self.__accum.put_nowait(None)

    def on_error(self, ex):
        self._future.set_exception(ex)
        self.__accum.put_nowait(None)


class AView(AioBase, AsyncViewBase):

    def __init__(self, *args, **kwargs):
        AsyncViewBase.__init__(self, *args, **kwargs)
        AioBase.__init__(self)


class AN1QLRequest(AioBase, AsyncN1QLRequest):

    def __init__(self, *args, **kwargs):
        AsyncN1QLRequest.__init__(self, *args, **kwargs)
        AioBase.__init__(self)


class AViewResult(AioBase, AsyncViewResult):
    def __init__(self, *args, **kwargs):
        AsyncViewBase.__init__(self, *args, **kwargs)
        AioBase.__init__(self)


class AsyncQueryResult(AsyncRowsBase, QueryResult):
    def __init__(self, *args, **kwargs):
        QueryResult.__init__(self, *args, **kwargs)


class AsyncAnalyticsResult(AsyncRowsBase, AnalyticsResult):
    def __init__(self, *args, **kwargs):
        AnalyticsResult.__init__(self, *args, **kwargs)


class AQueryResult(AioBase, AsyncQueryResult):
    def __init__(self, *args, **kwargs):
        AsyncQueryResult.__init__(self, *args, **kwargs)
        AioBase.__init__(self)


class AAnalyticsResult(AioBase, AsyncAnalyticsResult):
    def __init__(self, *args, **kwargs):
        AsyncAnalyticsResult.__init__(self, *args, **kwargs)
        AioBase.__init__(self)
