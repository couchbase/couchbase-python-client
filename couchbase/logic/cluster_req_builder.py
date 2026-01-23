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

from datetime import timedelta
from typing import TYPE_CHECKING, Union

from couchbase.analytics import AnalyticsQuery
from couchbase.diagnostics import ClusterState, ServiceType
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.cluster_types import (AnalyticsQueryRequest,
                                           ClusterInfoRequest,
                                           DiagnosticsRequest,
                                           GetConnectionInfoRequest,
                                           PingRequest,
                                           QueryRequest,
                                           SearchQueryRequest,
                                           UpdateCredentialsRequest,
                                           WaitUntilReadyRequest)
from couchbase.n1ql import N1QLQuery
from couchbase.options import forward_args
from couchbase.pycbc_core import (cluster_mgmt_operations,
                                  mgmt_operations,
                                  operations)
from couchbase.search import (SearchQuery,
                              SearchQueryBuilder,
                              SearchRequest)

if TYPE_CHECKING:
    from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator


class ClusterRequestBuilder:

    def __init__(self) -> None:
        pass

    def build_analytics_query_request(self,
                                      statement: str,
                                      *options: object,
                                      **kwargs: object) -> AnalyticsQueryRequest:
        num_workers = kwargs.pop('num_workers', None)
        req = AnalyticsQueryRequest(AnalyticsQuery.create_query_object(statement, *options, **kwargs))
        if num_workers:
            req.num_workers = num_workers
        return req

    def build_cluster_info_request(self) -> ClusterInfoRequest:
        return ClusterInfoRequest(mgmt_operations.CLUSTER.value, cluster_mgmt_operations.GET_CLUSTER_INFO.value)

    def build_diagnostics_request(self, *options: object, **kwargs: object) -> DiagnosticsRequest:
        # TODO: OptionsProcessor
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = DiagnosticsRequest(operations.DIAGNOSTICS.value, **final_args)
        if timeout:
            req.timeout = timeout
        return req

    def build_get_connection_info_request(self) -> GetConnectionInfoRequest:
        return GetConnectionInfoRequest(mgmt_operations.CLUSTER.value, cluster_mgmt_operations.GET_CLUSTER_INFO.value)

    def build_ping_request(self, *options: object, **kwargs: object) -> PingRequest:
        # TODO: OptionsProcessor
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)

        service_types = final_args.pop('service_types', None)
        if not service_types:
            service_types = list(
                map(lambda st: st.value, [ServiceType(st.value) for st in ServiceType]))

        if not isinstance(service_types, (list, set)):
            raise InvalidArgumentException('Service types must be a list/set.')

        service_types = list(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))

        req = PingRequest(operations.PING.value, service_types, **final_args)
        if timeout:
            req.timeout = timeout

        return req

    def build_query_request(self, statement: str, *options: object, **kwargs: object) -> QueryRequest:
        num_workers = kwargs.pop('num_workers', None)
        req = QueryRequest(N1QLQuery.create_query_object(statement, *options, **kwargs))
        if num_workers:
            req.num_workers = num_workers
        return req

    def build_search_request(self,
                             index: str,
                             query: Union[SearchQuery, SearchRequest],
                             *options: object,
                             **kwargs: object) -> SearchQueryRequest:
        num_workers = kwargs.pop('num_workers', None)
        if isinstance(query, SearchQuery):
            req = SearchQueryRequest(SearchQueryBuilder.create_search_query_object(index, query, *options, **kwargs))
        else:
            req = SearchQueryRequest(SearchQueryBuilder.create_search_query_from_request(
                index, query, *options, **kwargs))
        if num_workers:
            req.num_workers = num_workers
        return req

    def build_udpate_credential_request(
            self, authenticator: Union[CertificateAuthenticator, PasswordAuthenticator]) -> UpdateCredentialsRequest:
        return UpdateCredentialsRequest(authenticator.as_dict())

    def build_wait_until_ready_request(self,
                                       timeout: timedelta,
                                       *options: object,
                                       **kwargs: object) -> WaitUntilReadyRequest:
        final_args = forward_args(kwargs, *options)
        service_types = final_args.get("service_types", None)
        if not service_types:
            service_types = [ServiceType(st.value) for st in ServiceType]

        desired_state = final_args.get("desired_state", ClusterState.Online)
        service_types_set = set(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))
        return WaitUntilReadyRequest(timeout, desired_state, service_types_set)
