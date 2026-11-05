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
    'AnalyticsError',
    'AnalyticsMetaData',
    'AnalyticsMetrics',
    'AnalyticsProblem',
    'AnalyticsScanConsistency',
    'AnalyticsStatus',
    'AnalyticsWarning',
]

from dataclasses import asdict, dataclass
from datetime import timedelta
from enum import Enum

from couchbase.common._utils import JSONType
from couchbase.common.options import UnsignedInt64


class AnalyticsStatus(Enum):
    """Represents the status of an analytics query."""
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
    def from_str(cls, value: str) -> AnalyticsStatus:
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
class _AnalyticsProblemRecord:
    code: int
    message: str


@dataclass(frozen=True)
class _AnalyticsMetricsRecord:
    elapsed_time_ns: int
    execution_time_ns: int
    result_count: int
    result_size: int
    error_count: int
    processed_objects: int
    warning_count: int
    mutation_count: int | None
    sort_count: int | None


@dataclass(frozen=True)
class _AnalyticsMetaDataRecord:
    request_id: str
    client_context_id: str
    status: AnalyticsStatus
    signature: JSONType | None
    warnings: tuple[_AnalyticsProblemRecord, ...]
    errors: tuple[_AnalyticsProblemRecord, ...]
    metrics: _AnalyticsMetricsRecord | None


class AnalyticsProblem(object):
    """A warning or error the analytics service reported for a query."""

    def __init__(self, record: _AnalyticsProblemRecord) -> None:
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


class AnalyticsWarning(AnalyticsProblem):
    """A warning the analytics service reported for a query."""


class AnalyticsError(AnalyticsProblem):
    """An error the analytics service reported for a query."""


class AnalyticsMetrics(object):
    """The metrics the analytics service reported for a query."""

    def __init__(self, record: _AnalyticsMetricsRecord) -> None:
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

    def result_count(self) -> UnsignedInt64:
        """Get the total number of rows which were part of the result set.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of rows which were part of the result set.
        """
        return UnsignedInt64(self._record.result_count)

    def result_size(self) -> UnsignedInt64:
        """Get the total number of bytes which were generated as part of the result set.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of bytes which were generated as part of the
                result set.
        """
        return UnsignedInt64(self._record.result_size)

    def error_count(self) -> UnsignedInt64:
        """Get the total number of errors which were encountered during the execution of the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of errors which were encountered during the
                execution of the query.
        """
        return UnsignedInt64(self._record.error_count)

    def processed_objects(self) -> UnsignedInt64:
        """Get the total number of objects the query processed.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of objects the query processed.
        """
        return UnsignedInt64(self._record.processed_objects)

    def warning_count(self) -> UnsignedInt64:
        """Get the total number of warnings which were encountered during the execution of the query.

        Returns:
            :class:`~couchbase.options.UnsignedInt64`: The total number of warnings which were encountered during the
                execution of the query.
        """
        return UnsignedInt64(self._record.warning_count)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class AnalyticsMetaData:
    """The metadata the analytics service returned with a query's rows."""

    def __init__(self, record: _AnalyticsMetaDataRecord) -> None:
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

    def status(self) -> AnalyticsStatus:
        """Get the status of the query at the time the query metadata was generated.

        Returns:
            :class:`.AnalyticsStatus`: The status of the query at the time the query metadata was generated.
        """
        return self._record.status

    def signature(self) -> JSONType | None:
        """Get the signature of the query.

        Returns:
            JSONType | None: The signature of the query, if the service sent one.
        """
        return self._record.signature

    def warnings(self) -> list[AnalyticsWarning]:
        """Get warnings that occurred during the execution of the query.

        Returns:
            list[:class:`.AnalyticsWarning`]: Any warnings that occurred during the execution of the query.
        """
        return [AnalyticsWarning(w) for w in self._record.warnings]

    def errors(self) -> list[AnalyticsError]:
        """Get errors that occurred during the execution of the query.

        Returns:
            list[:class:`.AnalyticsError`]: Any errors that occurred during the execution of the query.
        """
        return [AnalyticsError(e) for e in self._record.errors]

    def metrics(self) -> AnalyticsMetrics | None:
        """Get the metrics the analytics service reported for the query.

        Returns:
            :class:`.AnalyticsMetrics` | None: The query's metrics, if the service sent them.
        """
        if self._record.metrics is None:
            return None
        return AnalyticsMetrics(self._record.metrics)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class AnalyticsScanConsistency(Enum):
    """How current the data an analytics query reads must be.

    ``NOT_BOUNDED`` reads what the service has already ingested, which is fastest but may miss recent
    mutations.  ``REQUEST_PLUS`` waits until the service has ingested every mutation made before the query.
    """

    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"
