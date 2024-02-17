#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  AlreadyQueriedException,
                                  CouchbaseException,
                                  ErrorMapper,
                                  ExceptionMap)
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.views import ViewErrorMode  # noqa: F401
from couchbase.logic.views import ViewMetaData  # noqa: F401
from couchbase.logic.views import ViewOrdering  # noqa: F401
from couchbase.logic.views import ViewQuery  # noqa: F401
from couchbase.logic.views import ViewScanConsistency  # noqa: F401
from couchbase.logic.views import ViewRequestLogic, ViewRow


class ViewRequest(ViewRequestLogic):
    def __init__(self,
                 connection,
                 encoded_query,
                 **kwargs
                 ):
        super().__init__(connection, encoded_query, **kwargs)

    @classmethod
    def generate_view_request(cls, connection, encoded_query, **kwargs):
        return cls(connection, encoded_query, **kwargs)

    def execute(self):
        return [r for r in list(self)]

    def _get_metadata(self):
        try:
            # @TODO:  PYCBC-1524
            views_response = next(self._streaming_result)
            self._set_metadata(views_response)
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn

    def __iter__(self):
        if self.done_streaming:
            raise AlreadyQueriedException()

        if not self.started_streaming:
            self._submit_query()

        return self

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        try:
            row = next(self._streaming_result)
        except StopIteration:
            # @TODO:  PYCBC-1524
            row = next(self._streaming_result)

        if isinstance(row, CouchbaseBaseException):
            raise ErrorMapper.build_exception(row)
        # should only be None one query request is complete and _no_ errors found
        if row is None:
            raise StopIteration

        # TODO:  until streaming, a dict is returned, no deserializing...
        # deserialized_row = self.serializer.deserialize(row)
        deserialized_row = row
        if issubclass(self.row_factory, ViewRow):
            if hasattr(self.row_factory, 'from_json'):
                return self.row_factory.from_json(deserialized_row)
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
