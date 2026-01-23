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
                    List,
                    Optional,
                    Set)

from couchbase.logic.operation_types import ClusterOperationType

if TYPE_CHECKING:
    from couchbase.analytics import AnalyticsQuery
    from couchbase.diagnostics import ClusterState
    from couchbase.logic.client_adapter import PyCapsuleType
    from couchbase.n1ql import N1QLQuery
    from couchbase.search import SearchQueryBuilder

# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['timeout']


@dataclass
class ClusterRequest:
    # TODO: maybe timeout isn't optional, but defaults to default timeout?
    #       otherwise that makes inheritance tricky w/ child classes having required params

    def req_to_dict(self,
                    conn: PyCapsuleType,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
        op_kwargs = {
            'conn': conn,
        }

        if callback is not None:
            op_kwargs['callback'] = callback

        if errback is not None:
            op_kwargs['errback'] = errback

        if hasattr(self, 'timeout') and getattr(self, 'timeout') is not None:
            op_kwargs['timeout'] = getattr(self, 'timeout')

        op_kwargs.update(**{
            field.name: getattr(self, field.name)
            for field in fields(self)
            if field.name not in OPARG_SKIP_LIST and getattr(self, field.name) is not None
        })

        return op_kwargs


@dataclass
class AnalyticsQueryRequest:
    analytics_query: AnalyticsQuery
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ClusterOperationType.AnalyicsQuery


@dataclass
class CloseConnectionRequest(ClusterRequest):
    @property
    def op_name(self) -> str:
        return ClusterOperationType.CloseConnection.value


@dataclass
class ClusterInfoRequest(ClusterRequest):
    mgmt_op: int
    op_type: int

    @property
    def op_name(self) -> str:
        return ClusterOperationType.GetClusterInfo.value


@dataclass
class CreateConnectionRequest:
    connstr: str
    auth: Dict[str, Any]
    options: Dict[str, Any]

    @property
    def op_name(self) -> str:
        return ClusterOperationType.CreateConnection.value

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
    op_type: int
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
    op_type: int
    service_types: List[str]
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
        return ClusterOperationType.Query


@dataclass
class SearchQueryRequest:
    query_builder: SearchQueryBuilder
    num_workers: Optional[int] = None

    @property
    def op_name(self) -> str:
        return ClusterOperationType.SearchQuery


@dataclass
class UpdateCredentialsRequest(ClusterRequest):
    auth: Dict[str, Any]

    @property
    def op_name(self) -> str:
        return ClusterOperationType.UpdateCredentials.value


@dataclass
class WaitUntilReadyRequest:
    timeout: timedelta
    desired_state: ClusterState = None
    service_types: Set[str] = None

    @property
    def op_name(self) -> str:
        return ClusterOperationType.WaitUntilReady.value
