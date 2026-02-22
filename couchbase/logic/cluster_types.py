#  Copyright 2016-2023. Couchbase, Inc.
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

from dataclasses import dataclass, fields
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    Optional,
                    Set)

from couchbase.logic.operation_types import (ClusterMgmtOperationType,
                                             ClusterOperationType,
                                             StreamingOperationType)

if TYPE_CHECKING:
    from couchbase.analytics import AnalyticsQuery
    from couchbase.diagnostics import ClusterState
    from couchbase.n1ql import N1QLQuery
    from couchbase.search import SearchQueryBuilder

# we have these params on the top-level pycbc_core request
# OPARG_SKIP_LIST = ['timeout']


@dataclass
class ClusterRequest:

    def req_to_dict(self,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = {
            field.name: getattr(self, field.name)
            for field in fields(self)
            if getattr(self, field.name) is not None
        }

        if callback is not None:
            op_kwargs['callback'] = callback

        if errback is not None:
            op_kwargs['errback'] = errback

        return op_kwargs


@dataclass
class AnalyticsQueryRequest:
    analytics_query: AnalyticsQuery
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return StreamingOperationType.AnalyticsQuery.value


@dataclass
class CloseConnectionRequest(ClusterRequest):
    @property
    def op_name(self) -> str:
        return ClusterOperationType.Close.value


@dataclass
class ClusterInfoRequest(ClusterRequest):
    @property
    def op_name(self) -> str:
        return ClusterMgmtOperationType.ClusterDescribe.value


@dataclass
class CreateConnectionRequest:
    connstr: str
    auth: Dict[str, Any]
    options: Dict[str, Any]

    @property
    def op_name(self) -> str:
        return ClusterOperationType.Connect.value

    def req_to_dict(self,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = {
            'connstr': self.connstr,
            'auth': self.auth,
            'options': self.options
        }

        if callback is not None:
            op_kwargs['callback'] = callback

        if errback is not None:
            op_kwargs['errback'] = errback

        return op_kwargs


@dataclass
class DiagnosticsRequest(ClusterRequest):
    report_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ClusterOperationType.Diagnostics.value


@dataclass
class GetConnectionInfoRequest(ClusterRequest):
    @property
    def op_name(self) -> str:
        return ClusterOperationType.GetConnectionInfo.value


@dataclass
class PingRequest(ClusterRequest):
    services: Set[str]
    report_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ClusterOperationType.Ping.value


@dataclass
class QueryRequest:
    n1ql_query: N1QLQuery
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return StreamingOperationType.Query


@dataclass
class SearchQueryRequest:
    query_builder: SearchQueryBuilder
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return StreamingOperationType.SearchQuery


@dataclass
class UpdateCredentialsRequest(ClusterRequest):
    auth: Dict[str, Any]

    @property
    def op_name(self) -> str:
        return ClusterOperationType.UpdateCredentials.value


@dataclass
class WaitUntilReadyRequest:
    timeout: timedelta
    desired_state: ClusterState
    service_types: Set[str]

    @property
    def op_name(self) -> str:
        return ClusterOperationType.WaitUntilReady.value
