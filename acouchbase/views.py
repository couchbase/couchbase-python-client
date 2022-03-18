import asyncio
from typing import Awaitable

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)
from couchbase.logic.views import ViewQuery  # noqa: F401
from couchbase.logic.views import ViewRequestLogic


class AsyncViewRequest(ViewRequestLogic):
    def __init__(self,
                 connection,
                 loop,
                 encoded_query,
                 **kwargs
                 ):
        super().__init__(connection, encoded_query, **kwargs)

    @classmethod
    def generate_view_request(cls, connection, loop, encoded_query, **kwargs):
        return cls(connection, loop, encoded_query, **kwargs)

    def _get_metadata(self):
        try:
            views_response = next(self._streaming_result)
            self._set_metadata(views_response)
        except StopAsyncIteration:
            pass

    # async def _get_metadata(self):
    #     # print('setting metadata')
    #     if self._query_request_ftr.done():
    #         if self._query_request_ftr.exception():
    #             print('raising exception')
    #             raise self._query_request_ftr.exception()
    #         else:
    #             self._set_metadata()
    #     else:
    #         await self._query_request_ftr
    #         self._set_metadata()

    def execute(self) -> Awaitable[None]:
        async def _execute():
            return [r async for r in self]

        return asyncio.create_task(_execute())

    def __aiter__(self):
        if self.done_streaming:
            # @TODO(jc): better exception
            raise Exception("Previously iterated over results.")

        if not self.started_streaming:
            self._submit_query()

        return self

    async def _get_next_row(self):
        if self.done_streaming is True:
            return

        row = next(self._streaming_result)
        if row is None:
            raise StopAsyncIteration
        # this should allow the event loop to pick up something else
        await self._rows.put(self.serializer.deserialize(row))

    async def __anext__(self):
        try:
            await self._get_next_row()
            return self._rows.get_nowait()
        except asyncio.QueueEmpty:
            self._done_streaming = True
            self._get_metadata()
            # TODO:  don't think this is right...
            raise StopAsyncIteration
        except StopAsyncIteration:
            self._done_streaming = True
            self._get_metadata()
            raise
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            print(f'base exception: {ex}')
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            print(exc_cls.__name__)
            excptn = exc_cls(str(ex))
            raise excptn

    # def __aiter__(self):
    #     if self._query_request_ftr is not None and self._query_request_ftr.done():
    #         # @TODO(jc): better exception
    #         raise Exception("Previously iterated over results.")

    #     if self._query_request_ftr is None:
    #         self._submit_query()

    #     return self

    # async def _get_next_row(self):
    #     if self._done_streaming is True:
    #         return

    #     try:
    #         # print('get next row')
    #         row = next(self._streaming_result)
    #         # this should allow the event loop to pick up something else
    #         await self._rows.put(row)
    #         # simulate random 'slowness'
    #         # from random import random
    #         # await asyncio.sleep(random())
    #     except StopIteration:
    #         self._done_streaming = True

    # async def __anext__(self):
    #     try:
    #         if self._query_request_ftr.done() and self._query_request_ftr.exception():
    #             raise self._query_request_ftr.exception()

    #         await self._get_next_row()
    #         return self._rows.get_nowait()
    #     except asyncio.QueueEmpty:
    #         await self._get_metadata()
    #         raise StopAsyncIteration
    #     except CouchbaseException as ex:
    #         raise ex
    #     except Exception as ex:
    #         print(f'base exception: {ex}')
    #         exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
    #         print(exc_cls.__name__)
    #         excptn = exc_cls(str(ex))
    #         raise excptn
