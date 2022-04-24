import json

from twisted.internet.defer import Deferred

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  AlreadyQueriedException,
                                  CouchbaseException,
                                  ErrorMapper,
                                  ExceptionMap)
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.search import (SearchRequestLogic,
                                    SearchRow,
                                    SearchRowLocations)


class SearchRequest(SearchRequestLogic):
    def __init__(self,
                 connection,
                 loop,
                 encoded_query,
                 **kwargs
                 ):
        super().__init__(connection, loop, encoded_query, **kwargs)

    @classmethod
    def generate_search_request(cls, connection, loop, encoded_query, **kwargs):
        return cls(connection, loop, encoded_query, **kwargs)

    def execute_search_query(self) -> Deferred:
        # if self._query_request_ftr is not None and self._query_request_ftr.done():
        if self.done_streaming:
            raise AlreadyQueriedException()

        if self._query_request_ftr is None:
            self._query_request_ftr = self.loop.create_future()
            self._submit_query(callback=self._on_query_complete)
            self._query_d = Deferred.fromFuture(self._query_request_ftr)

        return self._query_d

    def _on_query_complete(self, result):
        print(f'_on_query_callback: {result}')
        self._loop.call_soon_threadsafe(self._query_request_ftr.set_result, result)

    def _get_metadata(self):
        try:
            search_response = next(self._streaming_result)
            self._set_metadata(search_response)
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn

    def __iter__(self):
        return self

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        row = next(self._streaming_result)
        if isinstance(row, CouchbaseBaseException):
            raise ErrorMapper.build_exception(row)
        # should only be None one query request is complete and _no_ errors found
        if row is None:
            raise StopIteration

        # TODO:  until streaming, a dict is returned, no deserializing...
        # deserialized_row = self.serializer.deserialize(row)
        deserialized_row = row
        if issubclass(self.row_factory, SearchRow):
            locations = deserialized_row.get('locations', None)
            if locations:
                locations = SearchRowLocations(locations)
            deserialized_row['locations'] = locations

            fields = deserialized_row.get('fields', None)
            if fields and isinstance(fields, str):
                fields = json.loads(fields)
            deserialized_row['fields'] = fields

            explanation = deserialized_row.get('explanation', None)
            if explanation and isinstance(explanation, str):
                explanation = json.loads(explanation)
            deserialized_row['explanation'] = explanation

            return self.row_factory(**deserialized_row)
        else:
            return deserialized_row

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
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn
