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
    """The state of one endpoint's connection, in a diagnostics report."""

    Disconnected = "disconnected"
    Connecting = "connecting"
    Connected = "connected"
    Disconnecting = "disconnecting"


class ClusterState(Enum):
    """The state of the SDK's connections, summed over the endpoints of a diagnostics report.

    ``Online`` when every endpoint is connected, ``Offline`` when none is, and ``Degraded`` otherwise.
    """

    Online = "online"
    Degraded = "degraded"
    Offline = "offline"


class ServiceType(Enum):
    """A Couchbase Server service, as an endpoint in a diagnostics or ping report names it."""

    Analytics = "analytics"
    KeyValue = "key_value"
    Eventing = "eventing"
    Management = "management"
    Query = "query"
    Search = "search"
    View = "view"


class PingState(Enum):
    """The outcome of pinging one endpoint."""

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
    """One endpoint of a :class:`~couchbase.result.DiagnosticsResult`."""

    def __init__(self, record: _EndpointDiagnosticsRecord) -> None:
        self._record = record

    @property
    def type(self) -> ServiceType:
        """The service the endpoint belongs to.

        .. deprecated:: 4.0.0
            Use :attr:`service_type` instead.
        """
        Supportability.class_property_deprecated('EndpointDiagnosticsReport.type',
                                                 'EndpointDiagnosticsReport.service_type')
        return self.service_type

    @property
    def service_type(self) -> ServiceType:
        """The service the endpoint belongs to."""
        return ServiceType(self._record.service_type)

    @property
    def id(self) -> str:
        """The SDK's identifier for the endpoint's connection."""
        return self._record.id

    @property
    def local(self) -> str:
        """The local address of the connection."""
        return self._record.local

    @property
    def remote(self) -> str:
        """The address of the node the endpoint connects to."""
        return self._record.remote

    @property
    def namespace(self) -> str | None:
        """The bucket the endpoint is bound to, if it is bound to one."""
        return self._record.bucket

    @property
    def last_activity(self) -> timedelta | None:
        """The time since the endpoint last sent or received anything, if known.

        .. deprecated:: 4.0.0
            Use :attr:`last_activity_us` instead.
        """
        Supportability.class_property_deprecated('EndpointDiagnosticsReport.last_activity',
                                                 'EndpointDiagnosticsReport.last_activity_us')
        return self.last_activity_us

    @property
    def last_activity_us(self) -> timedelta | None:
        """The time since the endpoint last sent or received anything, if known."""
        if self._record.last_activity_us is None:
            return None
        return timedelta(microseconds=self._record.last_activity_us)

    @property
    def state(self) -> EndpointState:
        """The state of the endpoint's connection."""
        return EndpointState(self._record.state)

    def as_dict(self) -> dict[str, str | int]:
        """Get the report as a dict, leaving out the fields that are not set.

        Returns:
            dict[str, str | int]: The report's fields, by name.
        """
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.as_dict()})'


class EndpointPingReport:
    """One endpoint of a :class:`~couchbase.result.PingResult`."""

    def __init__(self, record: _EndpointPingRecord) -> None:
        self._record = record

    @property
    def service_type(self) -> ServiceType:
        """The service the endpoint belongs to."""
        return ServiceType(self._record.service_type)

    @property
    def id(self) -> str:
        """The SDK's identifier for the endpoint's connection."""
        return self._record.id

    @property
    def local(self) -> str:
        """The local address of the connection."""
        return self._record.local

    @property
    def remote(self) -> str:
        """The address of the node the endpoint connects to."""
        return self._record.remote

    @property
    def namespace(self) -> str | None:
        """The bucket the endpoint is bound to, if it is bound to one."""
        return self._record.bucket

    @property
    def error(self) -> str | None:
        """Why the ping failed, if it did."""
        return self._record.error

    @property
    def latency(self) -> timedelta | None:
        """How long the ping took, if it completed."""
        if self._record.latency_us is None:
            return None
        return timedelta(microseconds=self._record.latency_us)

    @property
    def state(self) -> PingState:
        """The outcome of the ping."""
        return PingState(self._record.state)

    def as_dict(self) -> dict[str, str | int]:
        """Get the report as a dict, leaving out the fields that are not set.

        Returns:
            dict[str, str | int]: The report's fields, by name.
        """
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
        return f'{type(self).__name__}({self.as_dict()})'
