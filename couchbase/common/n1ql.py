#  Copyright 2016-2026. Couchbase, Inc.
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

from __future__ import annotations

__all__ = [
    'QueryError',
    'QueryMetaData',
    'QueryMetrics',
    'QueryProblem',
    'QueryProfile',
    'QueryScanConsistency',
    'QueryStatus',
    'QueryWarning',
]

from dataclasses import asdict, dataclass
from datetime import timedelta
from enum import Enum

from couchbase.common._utils import JSONType
from couchbase.common.options import UnsignedInt64


class QueryStatus(Enum):
    """Represents the status of a query."""
    RUNNING = "running"
    SUCCESS = "success"
    ERRORS = "errors"
    COMPLETED = "completed"
    STOPPED = "stopped"
    TIMEOUT = "timeout"
    CLOSED = "closed"
    FATAL = "fatal"
    ABORTED = "aborted"
    UNKNOWN = "unknown"

    @classmethod
    def from_str(cls, value: str) -> QueryStatus:
        """**INTERNAL** The member for a status the server sent, or ``UNKNOWN`` for one it does not name."""
        # The status is sent by the server, so a value this SDK cannot name comes from a newer
        # server rather than from a mistake by the caller, and it reads as UNKNOWN.
        if isinstance(value, str):
            for member in cls:
                if member.value == value:
                    return member
        return cls.UNKNOWN


# The records below hold one response's metadata in a shape no protocol owns. Each protocol fills
# them from its own wire format, and the public classes that follow read them without knowing
# which protocol did. Durations are integer nanoseconds, because protocols that each converted to
# timedelta would round differently. A record carries every field either protocol sends, whether
# or not an accessor reads it, and a protocol that does not send one fills it with None, or an
# empty tuple for a list.

@dataclass(frozen=True)
class _QueryProblemRecord:
    code: int
    message: str
    reason: int | None
    retry: bool | None


@dataclass(frozen=True)
class _QueryMetricsRecord:
    elapsed_time_ns: int
    execution_time_ns: int
    result_count: int
    result_size: int
    mutation_count: int
    sort_count: int
    error_count: int
    warning_count: int


@dataclass(frozen=True)
class _QueryMetaDataRecord:
    request_id: str
    client_context_id: str
    status: QueryStatus
    signature: JSONType | None
    profile: JSONType | None
    warnings: tuple[_QueryProblemRecord, ...]
    errors: tuple[_QueryProblemRecord, ...]
    metrics: _QueryMetricsRecord | None


class QueryProblem(object):
    """A warning or error the query service reported for a query."""

    def __init__(self, record: _QueryProblemRecord) -> None:
        self._record = record

    def code(self) -> int:
        """Get the service's code for the problem.

        Returns:
            int: The service's code for the problem.
        """
        return self._record.code

    def message(self) -> str:
        """Get the service's description of the problem.

        Returns:
            str: The service's description of the problem.
        """
        return self._record.message

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class QueryWarning(QueryProblem):
    """A warning the query service reported for a query."""


class QueryError(QueryProblem):
    """An error the query service reported for a query."""


class QueryMetrics(object):
    """The metrics the query service reported for a query."""

    def __init__(self, record: _QueryMetricsRecord) -> None:
        self._record = record

    def elapsed_time(self) -> timedelta:
        """Get the total amount of time spent running the query.

        Returns:
            timedelta: The total amount of time spent running the query.
        """
        return timedelta(microseconds=self._record.elapsed_time_ns / 1000)

    def execution_time(self) -> timedelta:
        """Get the total amount of time spent executing the query.

        Returns:
            timedelta: The total amount of time spent executing the query.
        """
        return timedelta(microseconds=self._record.execution_time_ns / 1000)

    def sort_count(self) -> UnsignedInt64:
        """Get the total number of rows which were part of the sorting for the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of rows which were part of the
            sorting for the query.
        """
        return UnsignedInt64(self._record.sort_count)

    def result_count(self) -> UnsignedInt64:
        """Get the total number of rows which were part of the result set.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of rows which were part of the result set.
        """
        return UnsignedInt64(self._record.result_count)

    def result_size(self) -> UnsignedInt64:
        """Get the total number of bytes which were generated as part of the result set.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of bytes which were generated as
            part of the result set.
        """
        return UnsignedInt64(self._record.result_size)

    def mutation_count(self) -> UnsignedInt64:
        """Get the total number of rows which were altered by the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of rows which were altered by the query.
        """
        return UnsignedInt64(self._record.mutation_count)

    def error_count(self) -> UnsignedInt64:
        """Get the total number of errors which were encountered during the execution of the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of errors which were encountered during
            the execution of the query.
        """
        return UnsignedInt64(self._record.error_count)

    def warning_count(self) -> UnsignedInt64:
        """Get the total number of warnings which were encountered during the execution of the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of warnings which were encountered during
            the execution of the query.
        """
        return UnsignedInt64(self._record.warning_count)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class QueryMetaData:
    """The metadata the query service returned with a query's rows."""

    def __init__(self, record: _QueryMetaDataRecord) -> None:
        self._record = record

    def request_id(self) -> str:
        """Get the request ID which is associated with the executed query.

        Returns:
            str: The request ID which is associated with the executed query.
        """
        return self._record.request_id

    def client_context_id(self) -> str:
        """Get the client context ID which is associated with the executed query.

        Returns:
            str: The client context ID which is associated with the executed query.
        """
        return self._record.client_context_id

    def status(self) -> QueryStatus:
        """Get the status of the query at the time the query metadata was generated.

        Returns:
            :class:`.QueryStatus`: The status of the query at the time the query metadata was generated.
        """
        return self._record.status

    def signature(self) -> JSONType | None:
        """Get the signature of the query.

        Returns:
            JSONType | None: The signature of the query.
        """
        return self._record.signature

    def warnings(self) -> list[QueryWarning]:
        """Get warnings that occurred during the execution of the query.

        Returns:
            list[:class:`.QueryWarning`]: Any warnings that occurred during the execution of the query.
        """
        return [QueryWarning(w) for w in self._record.warnings]

    def errors(self) -> list[QueryError]:
        """Get errors that occurred during the execution of the query.

        Returns:
            list[:class:`.QueryError`]: Any errors that occurred during the execution of the query.
        """
        return [QueryError(e) for e in self._record.errors]

    def metrics(self) -> QueryMetrics | None:
        """Get the various metrics which are made available by the query engine.

        Returns:
            :class:`.QueryMetrics` | None: The query's metrics, if the service sent them.
        """
        if self._record.metrics is None:
            return None
        return QueryMetrics(self._record.metrics)

    def profile(self) -> JSONType | None:
        """Get the various profiling details that were generated during execution of the query.

        Returns:
            JSONType | None: Profiling details.
        """
        return self._record.profile

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class QueryScanConsistency(Enum):
    """Represents the various scan consistency options that are available when querying against the query service."""

    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"
    AT_PLUS = "at_plus"


class QueryProfile(Enum):
    """Specifies the profiling mode for a query."""

    OFF = "off"
    PHASES = "phases"
    TIMINGS = "timings"
