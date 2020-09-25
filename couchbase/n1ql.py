#
# Copyright 2019, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import enum
from datetime import timedelta
from typing import *

from couchbase.options import UnsignedInt64
from couchbase_core import iterable_wrapper, JSON
from couchbase.exceptions import QueryException
from couchbase_core.n1ql import N1QLRequest


class QueryStatus(enum.Enum):
    RUNNING = ()
    SUCCESS = ()
    ERRORS = ()
    COMPLETED = ()
    STOPPED = ()
    TIMEOUT = ()
    CLOSED = ()
    FATAL = ()
    ABORTED = ()
    UNKNOWN = ()


class QueryWarning(object):
    def __init__(self, raw_warning):
        self._raw_warning = raw_warning

    def code(self):
        # type: (...) -> int
        return self._raw_warning.get('code')

    def message(self):
        # type: (...) -> str
        return self._raw_warning.get('msg')


class QueryMetrics(object):
    def __init__(self,
                 parent  # type: QueryResult
                 ):
        self._parentquery = parent

    @property
    def _raw_metrics(self):
        return self._parentquery.metrics

    def _as_timedelta(self, time_str):
        return self._parentquery._duration_as_timedelta(self._raw_metrics.get(time_str))

    def elapsed_time(self):
        # type: (...) -> timedelta
        return self._as_timedelta('elapsedTime')

    def execution_time(self):
        # type: (...) -> timedelta
        return self._as_timedelta('executionTime')

    def sort_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('sortCount', 0))

    def result_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('resultCount', 0))

    def result_size(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('resultSize', 0))

    def mutation_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('mutationCount', 0))

    def error_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('errorCount', 0))

    def warning_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('warningCount', 0))


class QueryMetaData(object):
    def __init__(self,
                 parent  # type: QueryResult
                 ):
        self._parentquery_for_metadata = parent

    def request_id(self):
        # type: (...) -> str
        return self._parentquery_for_metadata.meta.get('requestID')

    def client_context_id(self):
        # type: (...) -> str
        return self._parentquery_for_metadata.meta.get('clientContextID')

    def signature(self):
        # type: (...) -> Optional[JSON]
        return self._parentquery_for_metadata.meta.get('signature')

    def status(self):
        # type: (...) -> QueryStatus
        return QueryStatus[self._parentquery_for_metadata.meta.get('status').upper()]

    def warnings(self):
        # type: (...) -> List[QueryWarning]
        return list(map(QueryWarning, self._parentquery_for_metadata.meta.get('warnings', [])))

    def metrics(self):
        # type: (...) -> Optional[QueryMetrics]
        return QueryMetrics(self._parentquery_for_metadata)

    def profile(self):
        # type: (...) -> Optional[JSON]
        return self._parentquery_for_metadata.profile


class QueryResult(iterable_wrapper(N1QLRequest)):
    def __init__(self,
                 params, parent, **kwargs
                 ):
        # type (...)->None
        super(QueryResult, self).__init__(params, parent, **kwargs)

    def metadata(self  # type: QueryResult
                 ):
        # type: (...) -> QueryMetaData
        return QueryMetaData(self)

    def _respond_to_timedelta(self, conv_query):
        first_entry = next(iter(conv_query), None)
        nanoseconds = first_entry.get('$1', None) if first_entry else None

        if nanoseconds is None:
            raise Exception("Cannot get result from first entry {} of query response {}".format(first_entry, conv_query.rows()))
        return timedelta(seconds=nanoseconds * 1e-9)

    def _duration_as_timedelta(self,
                               metrics_str):
        try:
            conv_query = self._parent.query(r'select str_to_duration("{}");'.format(metrics_str), timeout=timedelta(seconds=5))
            return self._respond_to_timedelta(conv_query)
        except Exception as e:
            raise QueryException.pyexc("Not able to get result in nanoseconds", inner=e)

