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

from datetime import timedelta
from typing import TYPE_CHECKING

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._supportability import Supportability
from couchbase.common.diagnostics import ClusterState, ServiceType
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.options import (AnalyticsOptions,
                                      DiagnosticsOptions,
                                      PingOptions,
                                      QueryOptions,
                                      SearchOptions,
                                      WaitUntilReadyOptions)
from couchbase.common.search_queries import SearchQuery
from couchbase.common.search_request import SearchRequest
from couchbase.protocol.classic._analytics import AnalyticsQuery
from couchbase.protocol.classic._cluster_types import (AnalyticsQueryRequest,
                                                       ClusterInfoRequest,
                                                       ClusterOptionsBlock,
                                                       DiagnosticsRequest,
                                                       GetConnectionInfoRequest,
                                                       PingRequest,
                                                       QueryRequest,
                                                       SearchQueryRequest,
                                                       UpdateCredentialsRequest,
                                                       WaitUntilReadyRequest)
from couchbase.protocol.classic._n1ql import N1QLQuery
from couchbase.protocol.classic._search import SearchQueryBuilder
from couchbase.protocol.classic._wire import check_streaming_args, forward_args

if TYPE_CHECKING:
    from couchbase.auth import (CertificateAuthenticator,
                                JwtAuthenticator,
                                PasswordAuthenticator)


class ClusterRequestBuilder:

    def __init__(self, option_validation: OptionValidationMode | None = None) -> None:
        self._option_validation = option_validation

    def build_analytics_query_request(self,
                                      statement: str,
                                      obs_handler: ObservableRequestHandler,
                                      *options: object,
                                      **kwargs: object) -> AnalyticsQueryRequest:
        # Accepted and discarded; the statement's builder would send it as a named parameter.
        kwargs.pop('num_workers', None)
        check_streaming_args(options, kwargs, block=ClusterOptionsBlock.Analytics, options_type=AnalyticsOptions,
                             mode=self._option_validation)
        return AnalyticsQueryRequest(AnalyticsQuery.create_query_object(statement, *options, **kwargs), obs_handler)

    def build_cluster_info_request(self) -> ClusterInfoRequest:
        return ClusterInfoRequest()

    def build_diagnostics_request(self,
                                  options: DiagnosticsOptions | None = None,
                                  **kwargs: object) -> DiagnosticsRequest:
        final_args = forward_args(kwargs, options, block=ClusterOptionsBlock.Diagnostics, mode=self._option_validation)
        timeout = final_args.pop('timeout', None)
        req = DiagnosticsRequest(**final_args)
        if timeout:
            req.timeout = timeout
        return req

    def build_get_connection_info_request(self) -> GetConnectionInfoRequest:
        return GetConnectionInfoRequest()

    def build_ping_request(self, options: PingOptions | None = None, **kwargs: object) -> PingRequest:
        final_args = forward_args(kwargs, options, block=ClusterOptionsBlock.Ping, mode=self._option_validation)
        timeout = final_args.pop('timeout', None)

        service_types = final_args.pop('service_types', None)
        if not service_types:
            service_types = list(
                map(lambda st: st.value, [ServiceType(st.value) for st in ServiceType]))

        if not isinstance(service_types, (list, set)):
            raise InvalidArgumentException('Service types must be a list/set.')

        services = set(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))

        req = PingRequest(services, **final_args)
        if timeout:
            req.timeout = timeout

        return req

    def build_query_request(self,
                            statement: str,
                            obs_handler: ObservableRequestHandler,
                            *options: object,
                            **kwargs: object) -> QueryRequest:
        # Accepted and discarded; the statement's builder would send it as a named parameter.
        kwargs.pop('num_workers', None)
        check_streaming_args(options, kwargs, block=ClusterOptionsBlock.Query, options_type=QueryOptions,
                             mode=self._option_validation)
        return QueryRequest(N1QLQuery.create_query_object(statement, *options, **kwargs), obs_handler)

    def build_search_request(self,
                             index: str,
                             request: SearchRequest | SearchQuery,
                             obs_handler: ObservableRequestHandler,
                             options: SearchOptions | None = None,
                             **kwargs: object) -> SearchQueryRequest:
        check_streaming_args((options,), kwargs, block=ClusterOptionsBlock.Search, options_type=SearchOptions,
                             mode=self._option_validation)
        if isinstance(request, SearchQuery):
            Supportability.method_param_type_deprecated('search', 'request', 'SearchQuery', 'SearchRequest')
            query_builder = SearchQueryBuilder.create_search_query_object(index, request, options, **kwargs)
        else:
            query_builder = SearchQueryBuilder.create_search_query_from_request(index, request, options, **kwargs)

        return SearchQueryRequest(query_builder, obs_handler)

    def build_search_query_request(self,
                                   index: str,
                                   query: SearchQuery,
                                   obs_handler: ObservableRequestHandler,
                                   options: SearchOptions | None = None,
                                   **kwargs: object) -> SearchQueryRequest:
        check_streaming_args((options,), kwargs, block=ClusterOptionsBlock.Search, options_type=SearchOptions,
                             mode=self._option_validation)
        if isinstance(query, SearchRequest):
            raise InvalidArgumentException(
                message=('A SearchRequest is not a valid search_query() query. '
                         'Pass it to search() instead.'))

        query_builder = SearchQueryBuilder.create_search_query_object(index, query, options, **kwargs)

        return SearchQueryRequest(query_builder, obs_handler)

    def build_update_credential_request(
            self, authenticator: CertificateAuthenticator | JwtAuthenticator | PasswordAuthenticator
    ) -> UpdateCredentialsRequest:
        return UpdateCredentialsRequest(authenticator.as_dict())

    def build_wait_until_ready_request(self,
                                       timeout: timedelta,
                                       options: WaitUntilReadyOptions | None = None,
                                       **kwargs: object) -> WaitUntilReadyRequest:
        final_args = forward_args(kwargs, options, block=ClusterOptionsBlock.WaitUntilReady,
                                  mode=self._option_validation)
        service_types = final_args.get("service_types", None)
        if not service_types:
            service_types = [ServiceType(st.value) for st in ServiceType]

        desired_state = final_args.get("desired_state", ClusterState.Online)
        service_types_set = set(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))
        return WaitUntilReadyRequest(timeout, desired_state, service_types_set)
