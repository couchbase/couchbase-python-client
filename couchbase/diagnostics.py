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

from datetime import timedelta
from enum import Enum
from typing import (Any,
                    Dict,
                    Optional)


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
    View = "views"
    KeyValue = "kv"
    Query = "query"
    Search = "search"
    Analytics = "analytics"
    Management = "mgmt"


class PingState(Enum):
    OK = 'ok'
    TIMEOUT = 'timeout'
    ERROR = 'error'


class EndpointDiagnosticsReport:
    def __init__(self,
                 service_type,  # type: ServiceType
                 source  # type: Dict[str, Any]
                 ):
        self._src = source
        self._service_type = service_type

    @property
    def type(self) -> ServiceType:
        """**DEPRECATED** user service_type

        Endpoint point service type

        Returns:
            ServiceType: Endpoint Service Type
        """
        return self._service_type

    @property
    def service_type(self) -> ServiceType:
        return self._service_type

    @property
    def id(self) -> str:
        return self._src.get('id', None)

    @property
    def local(self) -> str:
        return self._src.get('local', None)

    @property
    def remote(self) -> str:
        return self._src.get('remote', None)

    @property
    def namespace(self) -> str:
        # was 'scope', now 'namespace'
        return self._src.get('namespace', None)

    @property
    def last_activity(self) -> timedelta:
        """**DEPRECATED** user last_activity_us

        Endpoint point last activity in us

        Returns:
            timedelta: last activity in us
        """
        return timedelta(microseconds=self._src.get('last_activity_us', None))

    @property
    def last_activity_us(self) -> timedelta:
        return timedelta(microseconds=self._src.get('last_activity_us', None))

    @property
    def state(self) -> EndpointState:
        return EndpointState(self._src.get('state', None))

    def as_dict(self) -> dict:
        return self._src


class EndpointPingReport:

    def __init__(self,
                 service_type,  # type: ServiceType
                 source  # type: Dict[str, Any]
                 ):
        self._src_ping = source
        self._service_type = service_type

    @property
    def service_type(self) -> ServiceType:
        return self._service_type

    @property
    def id(self) -> str:
        return self._src_ping.get('id', None)

    @property
    def local(self) -> str:
        return self._src_ping.get('local', None)

    @property
    def remote(self) -> str:
        return self._src_ping.get('remote', None)

    @property
    def namespace(self) -> Optional[str]:
        # was 'scope', now 'namespace'
        return self._src_ping.get(
            'namespace', self._src_ping.get('scope', None))

    @property
    def error(self) -> Optional[str]:
        return self._src_ping.get('error', None)

    @property
    def latency(self) -> timedelta:
        return timedelta(microseconds=self._src_ping.get('latency_us', None))

    @property
    def state(self) -> PingState:
        return PingState(self._src_ping.get('state', None))

    def as_dict(self) -> Dict[str, Any]:
        return self._src_ping

    def __repr__(self):
        return "EndpointPingReport:{}".format(self._src_ping)
