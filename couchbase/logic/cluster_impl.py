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

import time
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    Tuple,
                    Union)

from couchbase.analytics import AnalyticsRequest
from couchbase.exceptions import ServiceUnavailableException, UnAmbiguousTimeoutException
from couchbase.logic.client_adapter import ClientAdapter
from couchbase.logic.cluster_req_builder import ClusterRequestBuilder
from couchbase.logic.cluster_settings import ClusterSettings
from couchbase.logic.cluster_types import CreateConnectionRequest, GetConnectionInfoRequest
from couchbase.logic.pycbc_core import pycbc_connection
from couchbase.n1ql import N1QLRequest
from couchbase.result import (AnalyticsResult,
                              ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult,
                              QueryResult,
                              SearchResult)
from couchbase.search import FullTextSearchRequest
from couchbase.transactions import Transactions

if TYPE_CHECKING:
    from couchbase.logic.cluster_types import (AnalyticsQueryRequest,
                                               ClusterInfoRequest,
                                               DiagnosticsRequest,
                                               PingRequest,
                                               QueryRequest,
                                               SearchQueryRequest,
                                               UpdateCredentialsRequest,
                                               WaitUntilReadyRequest)
    from couchbase.serializer import Serializer


class ClusterImpl:
    def __init__(self, connstr: str, *options: object, **kwargs: object) -> None:
        skip_connect = kwargs.pop('skip_connect', None)
        self._cluster_settings = ClusterSettings.build_cluster_settings(connstr, *options, **kwargs)
        connect_request = CreateConnectionRequest(self._cluster_settings.connstr,
                                                  self._cluster_settings.auth,
                                                  self._cluster_settings.cluster_options)
        self._client_adapter = ClientAdapter(connect_request, skip_connect=skip_connect)
        self._request_builder = ClusterRequestBuilder()
        self._cluster_info: Optional[ClusterInfoResult] = None
        self._transactions: Optional[Transactions] = None

    @property
    def client_adapter(self) -> ClientAdapter:
        """**INTERNAL**"""
        return self._client_adapter

    @property
    def cluster_info(self) -> Optional[ClusterInfoResult]:
        """**INTERNAL**"""
        return self._cluster_info

    @property
    def cluster_settings(self) -> ClusterSettings:
        """**INTERNAL**"""
        return self._cluster_settings

    @property
    def connected(self) -> bool:
        """**INTERNAL**"""
        return self._client_adapter.connected

    @property
    def connection(self) -> pycbc_connection:
        """**INTERNAL**"""
        return self._client_adapter.connection

    @property
    def default_serializer(self) -> Serializer:
        """**INTERNAL**"""
        return self._cluster_settings.default_serializer

    @property
    def is_developer_preview(self) -> Optional[bool]:
        """**INTERNAL**"""
        if self._cluster_info:
            return False
        return None

    @property
    def request_builder(self) -> ClusterRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    @property
    def server_version(self) -> Optional[str]:
        """**INTERNAL**"""
        if self._cluster_info:
            return self._cluster_info.server_version

        return None

    @property
    def server_version_short(self) -> Optional[float]:
        """**INTERNAL**"""
        if self._cluster_info:
            return self._cluster_info.server_version_short

        return None

    @property
    def server_version_full(self) -> Optional[str]:
        """**INTERNAL**"""
        if self._cluster_info:
            return self._cluster_info.server_version_full

        return None

    @property
    def transactions(self) -> Transactions:
        """**INTERNAL**"""
        if not self._transactions:
            self._transactions = Transactions(self)
        return self._transactions

    def analytics_query(self, req: AnalyticsQueryRequest) -> AnalyticsResult:
        """**INTERNAL**"""
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('analytics_timeout', None)
        return AnalyticsResult(AnalyticsRequest.generate_analytics_request(self._client_adapter.connection,
                                                                           req.analytics_query.params,
                                                                           default_serializer=self.default_serializer,
                                                                           streaming_timeout=streaming_timeout))

    def close_connection(self) -> None:
        """**INTERNAL**"""
        self._client_adapter.close_connection()

    def diagnostics(self, req: DiagnosticsRequest) -> DiagnosticsResult:
        """**INTERNAL**"""
        return DiagnosticsResult(self._client_adapter.execute_cluster_request(req))

    def get_cluster_info(self, req: ClusterInfoRequest) -> ClusterInfoResult:
        """**INTERNAL**"""
        if not self.connected:
            raise RuntimeError('Cannot get cluster info until a connection is established.')
        try:
            res = self._client_adapter.execute_cluster_request(req)
            cluster_info = ClusterInfoResult(res)
            self._cluster_info = cluster_info
            return cluster_info
        except ServiceUnavailableException as ex:
            ex._message = ('If using Couchbase Server < 6.6, '
                           'a bucket needs to be opened prior to cluster level operations')
            raise ex from None

    def get_connection_info(self) -> Dict[str, Any]:
        """**INTERNAL**"""
        res = self._client_adapter.execute_cluster_request(GetConnectionInfoRequest())
        if 'credentials' in res:
            if 'allowed_sasl_mechanisms' not in res['credentials']:
                res['credentials']['allowed_sasl_mechanisms'] = []
        return res

    def ping(self, req: PingRequest) -> PingResult:
        """**INTERNAL**"""
        return PingResult(self._client_adapter.execute_cluster_request(req))

    def query(self, req: QueryRequest) -> QueryResult:
        """**INTERNAL**"""
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('query_timeout', None)
        return QueryResult(N1QLRequest.generate_n1ql_request(self._client_adapter.connection,
                                                             req.n1ql_query.params,
                                                             default_serializer=self.default_serializer,
                                                             streaming_timeout=streaming_timeout))

    def search(self, req: SearchQueryRequest) -> SearchResult:
        """**INTERNAL**"""
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('search_timeout', None)
        return SearchResult(FullTextSearchRequest.generate_search_request(self._client_adapter.connection,
                                                                          req.query_builder.as_encodable(),
                                                                          default_serializer=self.default_serializer,
                                                                          streaming_timeout=streaming_timeout))

    def update_credentials(self, req: UpdateCredentialsRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_cluster_request(req)
        self._cluster_settings.auth = req.auth

    def wait_until_ready(self, req: WaitUntilReadyRequest) -> None:
        """**INTERNAL**"""
        current_time = time.monotonic()
        deadline = current_time + req.timeout.total_seconds()
        delay = 0.1  # seconds

        diag_req = self._request_builder.build_diagnostics_request()
        ping_req = self._request_builder.build_ping_request(service_types=req.service_types)

        while True:
            diag_res = self.diagnostics(diag_req)
            endpoint_svc_types = set(map(lambda st: st.value, diag_res.endpoints.keys()))
            if not endpoint_svc_types.issuperset(req.service_types):
                self.ping(ping_req)
                diag_res = self.diagnostics(diag_req)

            if diag_res.state == req.desired_state:
                break

            current_time = time.monotonic()
            if deadline < (current_time + delay):
                raise UnAmbiguousTimeoutException('Desired state not found.')
            time.sleep(delay)

    def _get_connection_opts(self,
                             auth_only: Optional[bool] = None,
                             conn_only: Optional[bool] = None
                             ) -> Union[Dict[str, Any], Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]:
        """Get connection related options

        **INTERNAL** not intended for use in public API.

        Args:
            auth_only (bool, optional): Set to True to return only auth options. Defaults to False.
            conn_only (bool, optional): Set to True to return only cluster options. Defaults to False.

        Returns:
            Union[Dict[str, Any], Dict[str, Any], Tuple[Dict[str, Any], Dict[str, Any]]]: Either the
                cluster auth, cluster options or a tuple of both the cluster auth and cluster options.
        """
        if auth_only is True:
            return self._cluster_settings.auth
        if conn_only is True:
            return self._cluster_settings.cluster_options
        return self._cluster_settings.auth, self._cluster_settings.cluster_options
