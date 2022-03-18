import asyncio

from twisted.internet.defer import Deferred

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)
from couchbase.logic.n1ql import QueryRequestLogic


class N1QLRequest(QueryRequestLogic):
    def __init__(self,
                 connection,
                 loop,
                 query_params,
                 row_factory=lambda x: x,
                 **kwargs
                 ):
        super().__init__(connection, loop, query_params, row_factory=row_factory, **kwargs)
        self._query_d = None

    @classmethod
    def generate_n1ql_request(cls, connection, loop, query_params, row_factory=lambda x: x):
        return cls(connection, loop, query_params, row_factory=row_factory)

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
            # @TODO:  don't think this is reachable...
            self._loop.run_until_complete(self._query_request_ftr)
            self._set_metadata()

    def __iter__(self):
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
