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
    'ClusterState',
    'EndpointDiagnosticsReport',
    'EndpointPingReport',
    'EndpointState',
    'PingState',
    'ServiceType',
]

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

from couchbase.common._supportability import Supportability


class EndpointState(Enum):
    Disconnected = "disconnected"
    Connecting = "connecting"
    Connected = "connected"
    Disconnecting = "disconnecting"


class ClusterState(Enum):
    Online = "online"
    Degraded = "degraded"
    Offline = "offline"


class ServiceType(Enum):
    Analytics = "analytics"
    KeyValue = "key_value"
    Eventing = "eventing"
    Management = "management"
    Query = "query"
    Search = "search"
    View = "view"


class PingState(Enum):
    OK = 'ok'
    TIMEOUT = 'timeout'
    ERROR = 'error'


@dataclass(frozen=True)
class _EndpointDiagnosticsRecord:
    """One endpoint of a diagnostics report.

    ``service_type`` and ``state`` hold the values of :class:`ServiceType` and :class:`EndpointState`.
    """
    service_type: str
    id: str
    remote: str
    local: str
    state: str
    bucket: str | None
    details: str | None
    last_activity_us: int | None


@dataclass(frozen=True)
class _EndpointPingRecord:
    """One endpoint of a ping report.

    ``service_type`` and ``state`` hold the values of :class:`ServiceType` and :class:`PingState`.
    """
    service_type: str
    id: str
    remote: str
    local: str
    state: str
    bucket: str | None
    error: str | None
    latency_us: int | None


class EndpointDiagnosticsReport:
    def __init__(self, record: _EndpointDiagnosticsRecord) -> None:
        self._record = record

    @property
    def type(self) -> ServiceType:
        """Endpoint point service type

        .. deprecated:: 4.0.0
            Use :attr:`service_type` instead.

        Returns:
            ServiceType: Endpoint Service Type
        """
        Supportability.class_property_deprecated('EndpointDiagnosticsReport.type',
                                                 'EndpointDiagnosticsReport.service_type')
        return self.service_type

    @property
    def service_type(self) -> ServiceType:
        return ServiceType(self._record.service_type)

    @property
    def id(self) -> str | None:
        return self._record.id

    @property
    def local(self) -> str | None:
        return self._record.local

    @property
    def remote(self) -> str | None:
        return self._record.remote

    @property
    def namespace(self) -> str | None:
        return self._record.bucket

    @property
    def last_activity(self) -> timedelta | None:
        """Endpoint point last activity in us

        .. deprecated:: 4.0.0
            Use :attr:`last_activity_us` instead.

        Returns:
            timedelta: last activity in us
        """
        Supportability.class_property_deprecated('EndpointDiagnosticsReport.last_activity',
                                                 'EndpointDiagnosticsReport.last_activity_us')
        return self.last_activity_us

    @property
    def last_activity_us(self) -> timedelta | None:
        if self._record.last_activity_us is None:
            return None
        return timedelta(microseconds=self._record.last_activity_us)

    @property
    def state(self) -> EndpointState:
        return EndpointState(self._record.state)

    def as_dict(self) -> dict[str, str | int]:
        r = self._record
        output = {'type': r.service_type,
                  'id': r.id,
                  'remote': r.remote,
                  'local': r.local,
                  'state': r.state,
                  'bucket': r.bucket,
                  'details': r.details,
                  'last_activity_us': r.last_activity_us}
        return {k: v for k, v in output.items() if v is not None}


class EndpointPingReport:

    def __init__(self, record: _EndpointPingRecord) -> None:
        self._record = record

    @property
    def service_type(self) -> ServiceType:
        return ServiceType(self._record.service_type)

    @property
    def id(self) -> str | None:
        return self._record.id

    @property
    def local(self) -> str | None:
        return self._record.local

    @property
    def remote(self) -> str | None:
        return self._record.remote

    @property
    def namespace(self) -> str | None:
        return self._record.bucket

    @property
    def error(self) -> str | None:
        return self._record.error

    @property
    def latency(self) -> timedelta | None:
        if self._record.latency_us is None:
            return None
        return timedelta(microseconds=self._record.latency_us)

    @property
    def state(self) -> PingState:
        return PingState(self._record.state)

    def as_dict(self) -> dict[str, str | int]:
        r = self._record
        output = {'type': r.service_type,
                  'id': r.id,
                  'remote': r.remote,
                  'local': r.local,
                  'state': r.state,
                  'bucket': r.bucket,
                  'error': r.error,
                  'latency_us': r.latency_us}
        return {k: v for k, v in output.items() if v is not None}

    def __repr__(self) -> str:
        return "EndpointPingReport:{}".format(self.as_dict())
