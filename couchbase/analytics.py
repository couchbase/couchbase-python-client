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
from couchbase.logic.analytics import AnalyticsError  # noqa: F401
from couchbase.logic.analytics import AnalyticsMetaData  # noqa: F401
from couchbase.logic.analytics import AnalyticsMetrics  # noqa: F401
from couchbase.logic.analytics import AnalyticsQuery  # noqa: F401
from couchbase.logic.analytics import AnalyticsScanConsistency  # noqa: F401
from couchbase.logic.analytics import AnalyticsStatus  # noqa: F401
from couchbase.logic.analytics import AnalyticsWarning  # noqa: F401
from couchbase.logic.analytics import AnalyticsRequestLogic
from couchbase.logic.supportability import Supportability


class AnalyticsRequest(AnalyticsRequestLogic):
    def __init__(self,
                 connection,
                 query_params,
                 row_factory=lambda x: x,
                 **kwargs
                 ):
        super().__init__(connection, query_params, row_factory=row_factory, **kwargs)

    @classmethod
    def generate_analytics_request(cls, connection, query_params, row_factory=lambda x: x, **kwargs):
        return cls(connection, query_params, row_factory=row_factory, **kwargs)

    def execute(self):
        return [r for r in list(self)]

    def _get_metadata(self):
        try:
            analytics_response = next(self._streaming_result)
            self._set_metadata(analytics_response)
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

        row = next(self._streaming_result)
        if isinstance(row, CouchbaseBaseException):
            raise ErrorMapper.build_exception(row)
        # should only be None one query request is complete and _no_ errors found
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
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn


"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead use:
    * All options should be imported from `couchbase.options`.

"""

from couchbase.logic.options import AnalyticsOptionsBase  # nopep8 # isort:skip # noqa: E402


@Supportability.import_deprecated('couchbase.analytics', 'couchbase.options')
class AnalyticsOptions(AnalyticsOptionsBase):  # noqa: F811
    pass
