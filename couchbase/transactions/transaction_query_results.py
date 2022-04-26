import asyncio
import logging

from couchbase.transcoder import DefaultJsonSerializer
log = logging.getLogger(__name__)


class TransactionQueryResults:
    # we could fake an async rows with something like this, and wrap
    # the rows with it.  But, this seems pointless for now, since the
    # transactions lib doesn't stream the results.   However, if we decide
    # to update the transactions to support it, maybe we should have the
    # correct interface?
    class RowIter:
        def __init__(self, rows):
            self._loop = asyncio.get_event_loop()
            self._queue = asyncio.Queue(loop=self._loop)
            for r in rows:
                self._queue.put_nowait(r)

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._queue.empty():
                log.debug('done iterating over rows')
                raise StopAsyncIteration
            row = await self._queue.get()
            return row

    def __init__(self,
                 res    # type: str
                 ):

        self._res = DefaultJsonSerializer().deserialize(res)

    def rows(self):
        return self._res.get("results")

    def __str__(self):
        return f'TransactionQueryResult{{res={self._res}}}'
