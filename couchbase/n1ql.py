from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)
from couchbase.logic.n1ql import N1QLQuery  # noqa: F401
from couchbase.logic.n1ql import QueryError  # noqa: F401
from couchbase.logic.n1ql import QueryMetaData  # noqa: F401
from couchbase.logic.n1ql import QueryMetrics  # noqa: F401
from couchbase.logic.n1ql import QueryProfile  # noqa: F401
from couchbase.logic.n1ql import QueryScanConsistency  # noqa: F401
from couchbase.logic.n1ql import QueryStatus  # noqa: F401
from couchbase.logic.n1ql import QueryWarning  # noqa: F401
from couchbase.logic.n1ql import QueryRequestLogic


class N1QLRequest(QueryRequestLogic):
    def __init__(self,
                 connection,
                 query_params,
                 row_factory=lambda x: x,
                 **kwargs
                 ):
        super().__init__(connection, query_params, row_factory=row_factory, **kwargs)

    @classmethod
    def generate_n1ql_request(cls, connection, query_params, row_factory=lambda x: x):
        return cls(connection, query_params, row_factory=row_factory)

    def execute(self):
        return [r for r in list(self)]

    def _get_metadata(self):
        try:
            query_response = next(self._streaming_result)
            self._set_metadata(query_response)
        except StopIteration:
            pass
        # if self._query_request_ftr.done():
        #     if self._query_request_ftr.exception():
        #         print('raising exception')
        #         raise self._query_request_ftr.exception()
        #     else:
        #         self._set_metadata()
        # else:
        #     self._loop.run_until_complete(self._query_request_ftr)
        #     self._set_metadata()
            # print(self._query_request_result)

    def __iter__(self):
        # if self._query_request_ftr is not None and self._query_request_ftr.done():
        if self.done_streaming:
            # @TODO(jc): better exception
            raise Exception("Previously iterated over results.")

        # if self._query_request_ftr is None:
        if not self.started_streaming:
            self._submit_query()

        return self

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        # try:
        row = next(self._streaming_result)
        if row is None:
            raise StopIteration

        return self.serializer.deserialize(row)
        # except StopIteration:
        #     self._done_streaming = True
        #     self._get_metadata()

    def __next__(self):
        try:
            # if self._query_request_ftr.done() and self._query_request_ftr.exception():
            #     raise self._query_request_ftr.exception()
            return self._get_next_row()
            # return self._rows.get_nowait()
        # except asyncio.QueueEmpty:
        #     self._get_metadata()
        #     raise StopIteration
        except StopIteration:
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
