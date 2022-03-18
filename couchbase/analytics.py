from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)
from couchbase.logic.analytics import AnalyticsError  # noqa: F401
from couchbase.logic.analytics import AnalyticsMetaData  # noqa: F401
from couchbase.logic.analytics import AnalyticsMetrics  # noqa: F401
from couchbase.logic.analytics import AnalyticsQuery  # noqa: F401
from couchbase.logic.analytics import AnalyticsScanConsistency  # noqa: F401
from couchbase.logic.analytics import AnalyticsStatus  # noqa: F401
from couchbase.logic.analytics import AnalyticsWarning  # noqa: F401
from couchbase.logic.analytics import AnalyticsRequestLogic


class AnalyticsRequest(AnalyticsRequestLogic):
    def __init__(self,
                 connection,
                 query_params,
                 row_factory=lambda x: x,
                 **kwargs
                 ):
        super().__init__(connection, query_params, row_factory=row_factory, **kwargs)

    @classmethod
    def generate_analytics_request(cls, connection, query_params, row_factory=lambda x: x):
        return cls(connection, query_params, row_factory=row_factory)

    def execute(self):
        return [r for r in list(self)]

    def _get_metadata(self):
        try:
            analytics_response = next(self._streaming_result)
            self._set_metadata(analytics_response)
        except StopIteration:
            pass

    # def _get_metadata(self):
    #     if self._query_request_ftr.done():
    #         if self._query_request_ftr.exception():
    #             print('raising exception')
    #             raise self._query_request_ftr.exception()
    #         else:
    #             self._set_metadata()
    #     else:
    #         self._loop.run_until_complete(self._query_request_ftr)
    #         self._set_metadata()

    def __iter__(self):
        if self.done_streaming:
            # @TODO(jc): better exception
            raise Exception("Previously iterated over results.")

        if not self.started_streaming:
            self._submit_query()

        return self

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        row = next(self._streaming_result)
        if row is None:
            raise StopIteration

        return self.serializer.deserialize(row)

    def __next__(self):
        try:
            return self._get_next_row()
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

    # def __iter__(self):
    #     if self._query_request_ftr is not None and self._query_request_ftr.done():
    #         # @TODO(jc): better exception
    #         raise Exception("Previously iterated over results.")

    #     if self._query_request_ftr is None:
    #         self._submit_query()

    #     return self

    # def _get_next_row(self):
    #     if self._done_streaming is True:
    #         return

    #     try:
    #         row = next(self._streaming_result)
    #         self._rows.put_nowait(row)
    #     except StopIteration:
    #         self._done_streaming = True

    # def __next__(self):
    #     try:
    #         if self._query_request_ftr.done() and self._query_request_ftr.exception():
    #             raise self._query_request_ftr.exception()

    #         self._get_next_row()
    #         return self._rows.get_nowait()
    #     except asyncio.QueueEmpty:
    #         self._get_metadata()
    #         raise StopIteration
    #     except CouchbaseException as ex:
    #         raise ex
    #     except Exception as ex:
    #         print(f'base exception: {ex}')
    #         exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
    #         print(exc_cls.__name__)
    #         excptn = exc_cls(str(ex))
    #         raise excptn
