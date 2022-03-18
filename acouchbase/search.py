import asyncio
from typing import Awaitable

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)
from couchbase.logic.search import SearchQueryBuilder  # noqa: F401
from couchbase.logic.search import (SearchRequestLogic,
                                    SearchRow,
                                    SearchRowLocations)


class AsyncSearchRequest(SearchRequestLogic):
    def __init__(self,
                 connection,
                 loop,
                 encoded_query,
                 **kwargs
                 ):
        super().__init__(connection, encoded_query, **kwargs)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @classmethod
    def generate_search_request(cls, connection, loop, encoded_query, **kwargs):
        return cls(connection, loop, encoded_query, **kwargs)

    def _get_metadata(self):
        try:
            query_response = next(self._streaming_result)
            self._set_metadata(query_response)
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

        deserialized_row = self.serializer.deserialize(row)
        if issubclass(self.row_factory, SearchRow):
            locations = deserialized_row.get('locations', None)
            if locations:
                locations = SearchRowLocations(locations)
            deserialized_row['locations'] = locations
            await self._rows.put(self.row_factory(**deserialized_row))
        else:
            await self._rows.put(deserialized_row)

    async def __anext__(self):
        try:
            await self._get_next_row()
            return self._rows.get_nowait()
        except asyncio.QueueEmpty:
            self._done_streaming = True
            self._get_metadata()
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
