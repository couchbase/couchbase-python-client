import asyncio

from twisted.internet.defer import Deferred

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)
from couchbase.logic.views import ViewRequestLogic


class ViewRequest(ViewRequestLogic):
    def __init__(self,
                 connection,
                 loop,
                 encoded_query,
                 **kwargs
                 ):
        super().__init__(connection, loop, encoded_query, **kwargs)

    @classmethod
    def generate_view_request(cls, connection, loop, encoded_query, **kwargs):
        return cls(connection, loop, encoded_query, **kwargs)

    def execute_query(self):
        if self._query_request_ftr is not None and self._query_request_ftr.done():
            # @TODO(jc): better exception
            raise Exception("Previously iterated over results.")

        if self._query_request_ftr is None:
            self._submit_query()

        self._query_d = Deferred.fromFuture(self._query_request_ftr)
        return self._query_d

    def _get_metadata(self):
        if self._query_request_ftr.done():
            if self._query_request_ftr.exception():
                print('raising exception')
                raise self._query_request_ftr.exception()
            else:
                self._set_metadata()
        else:
            self._loop.run_until_complete(self._query_request_ftr)
            self._set_metadata()
            # print(self._query_request_result)

    def __iter__(self):
        if self._query_request_ftr is not None and self._query_request_ftr.done():
            # @TODO(jc): better exception
            raise Exception("Previously iterated over results.")

        if self._query_request_ftr is None:
            self._submit_query()

        return self

    def _get_next_row(self):
        if self._done_streaming is True:
            return

        try:
            row = next(self._streaming_result)
            self._rows.put_nowait(row)
        except StopIteration:
            self._done_streaming = True

    def __next__(self):
        try:
            if self._query_request_ftr.done() and self._query_request_ftr.exception():
                raise self._query_request_ftr.exception()

            self._get_next_row()
            return self._rows.get_nowait()
        except asyncio.QueueEmpty:
            self._get_metadata()
            raise StopIteration
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            print(f'base exception: {ex}')
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            print(exc_cls.__name__)
            excptn = exc_cls(str(ex))
            raise excptn
