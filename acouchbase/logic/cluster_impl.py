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
from asyncio import AbstractEventLoop, sleep
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional)

from acouchbase.analytics import AsyncAnalyticsRequest
from acouchbase.logic.client_adapter import AsyncClientAdapter
from acouchbase.n1ql import AsyncN1QLRequest
from acouchbase.search import AsyncFullTextSearchRequest
from acouchbase.transactions import Transactions
from couchbase.exceptions import ServiceUnavailableException, UnAmbiguousTimeoutException
from couchbase.logic.cluster_impl import ClusterSettings
from couchbase.logic.cluster_req_builder import ClusterRequestBuilder
from couchbase.logic.cluster_types import CreateConnectionRequest, GetConnectionInfoRequest
from couchbase.logic.observability import ObservabilityInstruments
from couchbase.logic.operation_types import ClusterOperationType
from couchbase.logic.pycbc_core import pycbc_connection
from couchbase.result import (AnalyticsResult,
                              ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult,
                              QueryResult,
                              SearchResult)
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.logic.cluster_types import (AnalyticsQueryRequest,
                                               ClusterInfoRequest,
                                               DiagnosticsRequest,
                                               PingRequest,
                                               QueryRequest,
                                               SearchQueryRequest,
                                               UpdateCredentialsRequest,
                                               WaitUntilReadyRequest)


class AsyncClusterImpl:
    def __init__(self, connstr: str, *options: object, **kwargs: object) -> None:
        loop: Optional[AbstractEventLoop] = kwargs.pop('loop', None)
        loop_validator = kwargs.pop('loop_validator', None)
        kwargs['_default_timeouts'] = pycbc_connection.pycbc_get_default_timeouts()
        self._cluster_settings = ClusterSettings.build_cluster_settings(connstr, *options, **kwargs)
        connect_request = CreateConnectionRequest(self._cluster_settings.connstr,
                                                  self._cluster_settings.auth,
                                                  self._cluster_settings.cluster_options)
        # A connection is made when we create the client adapter, but it is an async operation that we cannot await
        # b/c the call needs to happen when we initialize a cluster (new cluster -> new client adapter). We await
        # the create connection future in whichever operation comes next.
        self._client_adapter = AsyncClientAdapter(connect_request, loop=loop, loop_validator=loop_validator)
        self._cluster_settings.set_observability_cluster_labels_callable(
            self._client_adapter.binding_map.op_map[ClusterOperationType.GetClusterLabels.value])
        self._request_builder = ClusterRequestBuilder()
        self._cluster_info: Optional[ClusterInfoResult] = None
        self._transactions: Optional[Transactions] = None

    @property
    def client_adapter(self) -> AsyncClientAdapter:
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
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL**"""
        return self._client_adapter.loop

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
        if self._cluster_info:
            return self._cluster_info.server_version_short

        return None

    @property
    def server_version_full(self) -> Optional[str]:
        if self._cluster_info:
            return self._cluster_info.server_version_full

        return None

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._cluster_settings.observability_instruments

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
        return AnalyticsResult(AsyncAnalyticsRequest.generate_analytics_request(self._client_adapter.connection,
                                                                                self.loop,
                                                                                req.analytics_query.params,
                                                                                default_serializer=self.default_serializer,  # noqa: E501
                                                                                streaming_timeout=streaming_timeout,
                                                                                obs_handler=req.obs_handler,
                                                                                num_workers=req.num_workers))

    async def close_connection(self) -> None:
        """**INTERNAL**"""
        try:
            from couchbase.logic.observability import ThresholdLoggingTracer
            tracer = self._cluster_settings.tracer.tracer
            if isinstance(tracer, ThresholdLoggingTracer):
                # shutdown the tracer's reporter
                tracer.close()
        except Exception:  # nosec
            # Don't raise exceptions during shutdown
            pass
        await self._client_adapter.close_connection()

    async def diagnostics(self, req: DiagnosticsRequest) -> DiagnosticsResult:
        """**INTERNAL**"""
        await self._client_adapter.wait_until_connected()
        res = await self._client_adapter.execute_cluster_request(req)
        return DiagnosticsResult(res)

    async def get_cluster_info(self, req: ClusterInfoRequest) -> ClusterInfoResult:
        """**INTERNAL**"""
        await self._client_adapter.wait_until_connected()

        try:
            res = await self._client_adapter.execute_cluster_request(req)
            cluster_info = ClusterInfoResult(res)
            self._cluster_info = cluster_info
            return cluster_info
        except ServiceUnavailableException as ex:
            ex._message = ('If using Couchbase Server < 6.6, '
                           'a bucket needs to be opened prior to cluster level operations')
            raise

    def get_connection_info(self) -> Dict[str, Any]:
        """**INTERNAL**"""
        return self._client_adapter.execute_cluster_request_sync(GetConnectionInfoRequest())

    async def ping(self, req: PingRequest) -> PingResult:
        """**INTERNAL**"""
        await self._client_adapter.wait_until_connected()
        res = await self._client_adapter.execute_cluster_request(req)
        return PingResult(res)

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
        return QueryResult(AsyncN1QLRequest.generate_n1ql_request(self._client_adapter.connection,
                                                                  self._client_adapter.loop,
                                                                  req.n1ql_query.params,
                                                                  default_serializer=self.default_serializer,
                                                                  streaming_timeout=streaming_timeout,
                                                                  obs_handler=req.obs_handler,
                                                                  num_workers=req.num_workers))

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
        return SearchResult(AsyncFullTextSearchRequest.generate_search_request(self._client_adapter.connection,
                                                                               self.loop,
                                                                               req.query_builder.as_encodable(),
                                                                               default_serializer=self.default_serializer,  # noqa: E501
                                                                               streaming_timeout=streaming_timeout,
                                                                               obs_handler=req.obs_handler,
                                                                               num_workers=req.num_workers))

    def update_credentials(self, req: UpdateCredentialsRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_cluster_request_sync(req)
        self._cluster_settings.auth = req.auth

    async def wait_until_connected(self) -> None:
        """**INTERNAL**"""
        await self._client_adapter.wait_until_connected()

    async def wait_until_ready(self, req: WaitUntilReadyRequest) -> None:
        """**INTERNAL**"""
        current_time = time.monotonic()
        deadline = current_time + req.timeout.total_seconds()
        delay = 0.1  # seconds

        diag_req = self._request_builder.build_diagnostics_request()
        ping_req = self._request_builder.build_ping_request(service_types=req.service_types)

        while True:
            diag_res = await self.diagnostics(diag_req)
            endpoint_svc_types = set(map(lambda st: st.value, diag_res.endpoints.keys()))
            if not endpoint_svc_types.issuperset(req.service_types):
                await self.ping(ping_req)
                diag_res = await self.diagnostics(diag_req)

            if diag_res.state == req.desired_state:
                break

            current_time = time.monotonic()
            if deadline < (current_time + delay):
                raise UnAmbiguousTimeoutException('Desired state not found.')
            await sleep(delay)
