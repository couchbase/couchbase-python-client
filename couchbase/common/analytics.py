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
    """
    Represents the status of an analytics query.
    """
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
    def __init__(self, record: _AnalyticsProblemRecord) -> None:
        self._record = record

    def code(self) -> int:
        return self._record.code

    def message(self) -> str:
        return self._record.message

    def __repr__(self) -> str:
        return f'{type(self).__name__}:{asdict(self._record)}'


class AnalyticsWarning(AnalyticsProblem):
    pass


class AnalyticsError(AnalyticsProblem):
    pass


class AnalyticsMetrics(object):

    def __init__(self, record: _AnalyticsMetricsRecord) -> None:
        self._record = record

    def elapsed_time(self) -> timedelta:
        return timedelta(microseconds=self._record.elapsed_time_ns / 1000)

    def execution_time(self) -> timedelta:
        return timedelta(microseconds=self._record.execution_time_ns / 1000)

    def result_count(self) -> UnsignedInt64:
        return UnsignedInt64(self._record.result_count)

    def result_size(self) -> UnsignedInt64:
        return UnsignedInt64(self._record.result_size)

    def error_count(self) -> UnsignedInt64:
        return UnsignedInt64(self._record.error_count)

    def processed_objects(self) -> UnsignedInt64:
        return UnsignedInt64(self._record.processed_objects)

    def warning_count(self) -> UnsignedInt64:
        return UnsignedInt64(self._record.warning_count)

    def __repr__(self) -> str:
        return f'AnalyticsMetrics:{asdict(self._record)}'


class AnalyticsMetaData:
    def __init__(self, record: _AnalyticsMetaDataRecord) -> None:
        self._record = record

    def request_id(self) -> str:
        return self._record.request_id

    def client_context_id(self) -> str:
        return self._record.client_context_id

    def status(self) -> AnalyticsStatus:
        return self._record.status

    def signature(self) -> JSONType | None:
        return self._record.signature

    def warnings(self) -> list[AnalyticsWarning]:
        return [AnalyticsWarning(w) for w in self._record.warnings]

    def errors(self) -> list[AnalyticsError]:
        return [AnalyticsError(e) for e in self._record.errors]

    def metrics(self) -> AnalyticsMetrics | None:
        if self._record.metrics is None:
            return None
        return AnalyticsMetrics(self._record.metrics)

    def __repr__(self) -> str:
        return f'AnalyticsMetaData:{asdict(self._record)}'


class AnalyticsScanConsistency(Enum):
    """
    For use with :attr:`~.AnalyticsQuery.consistency`, will allow cached
    values to be returned. This will improve performance but may not
    reflect the latest data in the server.
    """
    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"
