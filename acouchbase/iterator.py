import asyncio
from couchbase.async.view import AsyncViewBase
from couchbase.async.n1ql import AsyncN1QLRequest

class AioBase:
    def __init__(self):
        self.__local_done = False
        self.__accumulator = asyncio.Queue()
        self._future = asyncio.Future()
        self.start()
        self.raw.rows_per_call = 1000

    @property
    @asyncio.coroutine
    def future(self):
        yield from self._future
        self.__accumulator = [j for j in iter(self.__accumulator.get_nowait, None) if j is not None]

    @asyncio.coroutine
    def __iter__(self):
        if isinstance(self.__accumulator, asyncio.Queue):
            raise ValueError("yield from result.future before calling non-async for.")

        yield from self.__accumulator

    @asyncio.coroutine
    def __aiter__(self):
        if not isinstance(self.__accumulator, asyncio.Queue):
            raise ValueError("do not yield from result.future before calling async for.")

        return self

    @asyncio.coroutine
    def __anext__(self):
        try:
            out = None
            if self._future.done() and not self.__accumulator.empty():
                out = self.__accumulator.get_nowait()
            elif not self._future.done():
                out = yield from self.__accumulator.get()

            if out is None:
                raise StopAsyncIteration

            return out
        except asyncio.queues.QueueEmpty:
            raise StopAsyncIteration

    def on_rows(self, rowiter):
        for row in rowiter:
            self.__accumulator.put_nowait(row)

    def on_done(self):
        self._future.set_result(None)
        self.__accumulator.put_nowait(None)

    def on_error(self, ex):
        self._future.set_exception(ex)
        self.__accumulator.put_nowait(None)


class ViewRowProcessor(AioBase, AsyncViewBase):

    def __init__(self, *args, **kwargs):
        AsyncViewBase.__init__(self, *args, **kwargs)
        AioBase.__init__(self)


class N1QLRowProcessor(AioBase, AsyncN1QLRequest):

    def __init__(self, *args, **kwargs):
        AsyncN1QLRequest.__init__(self, *args, **kwargs)
        AioBase.__init__(self)


