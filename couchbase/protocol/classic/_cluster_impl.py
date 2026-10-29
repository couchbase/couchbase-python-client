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

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from couchbase.common._cluster_settings import ClusterSettings
from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common._streaming import RowIterator
from couchbase.common.exceptions import ServiceUnavailableException, UnAmbiguousTimeoutException
from couchbase.common.options import ClusterOptions
from couchbase.common.result import (ClusterInfoResult,
                                     DiagnosticsResult,
                                     PingResult)
from couchbase.protocol.classic._bindings import ConnectionBinding
from couchbase.protocol.classic._bucket_impl import BucketImpl
from couchbase.protocol.classic._client_adapter import ClientAdapter
from couchbase.protocol.classic._cluster_req_builder import ClusterRequestBuilder
from couchbase.protocol.classic._cluster_types import (CreateConnectionRequest,
                                                       GetConnectionInfoRequest,
                                                       StreamingTimeouts,
                                                       cluster_streaming_timeouts,
                                                       connect_options,
                                                       timeout_ms)
from couchbase.protocol.classic._management._analytics_mgmt_impl import AnalyticsMgmtImpl
from couchbase.protocol.classic._management._bucket_mgmt_impl import BucketMgmtImpl
from couchbase.protocol.classic._management._eventing_function_mgmt_impl import EventingFunctionMgmtImpl
from couchbase.protocol.classic._management._query_index_mgmt_impl import QueryIndexMgmtImpl
from couchbase.protocol.classic._management._search_index_mgmt_impl import SearchIndexMgmtImpl
from couchbase.protocol.classic._management._user_mgmt_impl import UserMgmtImpl
from couchbase.protocol.classic._pycbc_core import pycbc_connection, pycbc_hdr_histogram
from couchbase.protocol.classic._result import (_cluster_info_from_core,
                                                _diagnostics_from_core,
                                                _ping_from_core)
from couchbase.protocol.classic._streaming import (AnalyticsStreamSpec,
                                                   ClassicStreamingExecutor,
                                                   QueryStreamSpec,
                                                   SearchStreamSpec)
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from couchbase.transactions import Transactions

if TYPE_CHECKING:
    from datetime import timedelta

    from couchbase.auth import (CertificateAuthenticator,
                                JwtAuthenticator,
                                PasswordAuthenticator)
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          DiagnosticsOptions,
                                          PingOptions,
                                          QueryOptions,
                                          SearchOptions,
                                          WaitUntilReadyOptions)
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.protocol.classic._cluster_types import (DiagnosticsRequest,
                                                           PingRequest,
                                                           SearchQueryRequest)
    from couchbase.serializer import Serializer

log = logging.getLogger(__name__)


def normalize_connection_info(res: dict[str, Any]) -> dict[str, Any]:
    """**INTERNAL**

    The binding's cluster_credentials to_py omits allowed_sasl_mechanisms when the vector is
    empty as well as when it is unset, the same rule it applies to the string fields beside it.
    An empty list is reachable through PasswordAuthenticator, so a missing key has to be read
    back as one rather than left absent.
    """
    if 'credentials' in res and 'allowed_sasl_mechanisms' not in res['credentials']:
        res['credentials']['allowed_sasl_mechanisms'] = []
    return res


class ClusterImpl:

    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: object) -> None:
        skip_connect = kwargs.pop('skip_connect', None)
        kwargs['_histogram_factory'] = pycbc_hdr_histogram
        self._cluster_settings = ClusterSettings.build_cluster_settings(connstr, *options, **kwargs)
        connect_request = CreateConnectionRequest.from_settings(self._cluster_settings)
        self._client_adapter = ClientAdapter(connect_request, skip_connect=skip_connect,
                                             option_validation=self._cluster_settings.option_validation)
        self._cluster_settings.set_observability_cluster_labels_callable(
            self._client_adapter.binding_map.op_map[ConnectionBinding.GetClusterLabels.value])
        self._request_builder = ClusterRequestBuilder(self._cluster_settings.option_validation)
        self._streaming_timeouts = cluster_streaming_timeouts(self._cluster_settings)
        self._kv_durable_timeout = timeout_ms(self._cluster_settings, 'kv_durable_timeout')
        self._cluster_info: ClusterInfoResult | None = None
        self._transactions: Transactions | None = None
        # Guards the lazy init of self._transactions in the transactions property
        self._transactions_lock = threading.Lock()

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

    @property
    def client_adapter(self) -> ClientAdapter:
        """**INTERNAL**"""
        return self._client_adapter

    @property
    def cluster_info(self) -> ClusterInfoResult | None:
        """**INTERNAL**"""
        return self._cluster_info

    @property
    def cluster_settings(self) -> ClusterSettings:
        """**INTERNAL**"""
        return self._cluster_settings

    @property
    def streaming_timeouts(self) -> StreamingTimeouts:
        """**INTERNAL**"""
        return self._streaming_timeouts

    @property
    def kv_durable_timeout(self) -> int | None:
        """**INTERNAL** In milliseconds, or `None` where the cluster sets none."""
        return self._kv_durable_timeout

    @property
    def connect_options(self) -> dict[str, Any]:
        """**INTERNAL** What the binding is handed at connect."""
        return connect_options(self._cluster_settings)

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
    def is_developer_preview(self) -> bool | None:
        """**INTERNAL**"""
        if self._cluster_info:
            return False
        return None

    @property
    def request_builder(self) -> ClusterRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    @property
    def server_version(self) -> str | None:
        """**INTERNAL**"""
        if self._cluster_info:
            return self._cluster_info.server_version

        return None

    @property
    def server_version_short(self) -> float | None:
        """**INTERNAL**"""
        if self._cluster_info:
            return self._cluster_info.server_version_short

        return None

    @property
    def server_version_full(self) -> str | None:
        """**INTERNAL**"""
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
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        if self._transactions is None:
            with self._transactions_lock:
                if self._transactions is None:
                    self._transactions = Transactions(self)
        return self._transactions

    def bucket(self, name: str) -> BucketImpl:
        """**INTERNAL**"""
        return BucketImpl(name, self)

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AnalyticsResult:
        obs_handler = ObservableRequestHandler(Op.AnalyticsQuery, self.observability_instruments)
        req = self._request_builder.build_analytics_query_request(statement, obs_handler, *options, **kwargs)
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('analytics_timeout', None)
        return AnalyticsResult(RowIterator(ClassicStreamingExecutor(self._client_adapter.connection,
                                                                    AnalyticsStreamSpec(req.analytics_query.params,
                                                                                        self.default_serializer),
                                                                    obs_handler=req.obs_handler,
                                                                    streaming_timeout=streaming_timeout)))

    def bucket_mgmt(self) -> BucketMgmtImpl:
        """**INTERNAL**"""
        return BucketMgmtImpl(self._client_adapter, self.observability_instruments)

    def analytics_mgmt(self) -> AnalyticsMgmtImpl:
        """**INTERNAL**"""
        return AnalyticsMgmtImpl(self._client_adapter, self.observability_instruments)

    def query_index_mgmt(self) -> QueryIndexMgmtImpl:
        """**INTERNAL**"""
        return QueryIndexMgmtImpl(self._client_adapter, self.observability_instruments)

    def user_mgmt(self) -> UserMgmtImpl:
        """**INTERNAL**"""
        return UserMgmtImpl(self._client_adapter, self.observability_instruments)

    def search_index_mgmt(self) -> SearchIndexMgmtImpl:
        """**INTERNAL**"""
        return SearchIndexMgmtImpl(self._client_adapter, self.observability_instruments)

    def eventing_function_mgmt(self) -> EventingFunctionMgmtImpl:
        """**INTERNAL**"""
        return EventingFunctionMgmtImpl(self._client_adapter, self.observability_instruments)

    def close_connection(self) -> None:
        """**INTERNAL**"""

        # Swap under the lock, close outside it.  A first access holds the lock across
        # Transactions(), so an unlocked read here can see None mid-construction and skip
        # teardown, stranding a live instance on a closed cluster with only GC to close it.
        # close() is a network round-trip and must not be held under the lock.
        with self._transactions_lock:
            txns, self._transactions = self._transactions, None

        # Ahead of the adapter close: the core cannot remove its client record once the
        # connection is gone, and burns the full retry budget failing to.
        if txns is not None:
            try:
                # Synchronous, but PYCBC-1798 releases the GIL, so only this thread blocks.
                txns.close()
            except Exception:
                # Must not skip the connection close below and leak the core IO thread.
                log.warning('Error closing transactions while closing the cluster.', exc_info=True)

        try:
            from couchbase.common._observability import LoggingMeter, ThresholdLoggingTracer
            instruments = self._cluster_settings.observability_instruments
            tracer = instruments.tracer.tracer
            if isinstance(tracer, ThresholdLoggingTracer):
                # shutdown the tracer's reporter
                tracer.close()
            meter = instruments.meter
            if isinstance(meter, LoggingMeter):
                # shutdown the meter's reporter
                meter.close()
        except Exception:  # nosec
            # Don't raise exceptions during shutdown
            pass
        self._client_adapter.close_connection()

    def diagnostics(self, opts: DiagnosticsOptions | None = None, **kwargs: Any) -> DiagnosticsResult:
        return self._diagnostics(self._request_builder.build_diagnostics_request(opts, **kwargs))

    def get_cluster_info(self) -> ClusterInfoResult:
        if not self.connected:
            raise RuntimeError('Cannot get cluster info until a connection is established.')
        try:
            res = self._client_adapter.execute_cluster_request(self._request_builder.build_cluster_info_request())
            cluster_info = ClusterInfoResult(_cluster_info_from_core(res.raw_result))
            self._cluster_info = cluster_info
            return cluster_info
        except ServiceUnavailableException as ex:
            ex._message = ('If using Couchbase Server < 6.6, '
                           'a bucket needs to be opened prior to cluster level operations')
            raise ex from None

    def get_connection_info(self) -> dict[str, Any]:
        """**INTERNAL**"""
        return normalize_connection_info(
            self._client_adapter.execute_cluster_request(GetConnectionInfoRequest()))

    def ping(self, opts: PingOptions | None = None, **kwargs: Any) -> PingResult:
        return self._ping(self._request_builder.build_ping_request(opts, **kwargs))

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> QueryResult:
        obs_handler = ObservableRequestHandler(Op.Query, self.observability_instruments)
        req = self._request_builder.build_query_request(statement, obs_handler, *options, **kwargs)
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('query_timeout', None)
        return QueryResult(RowIterator(ClassicStreamingExecutor(self._client_adapter.connection,
                                                                QueryStreamSpec(req.n1ql_query.params,
                                                                                self.default_serializer),
                                                                obs_handler=req.obs_handler,
                                                                streaming_timeout=streaming_timeout)))

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> SearchResult:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_query_request(index, query, obs_handler, options, **kwargs)
        return self._search(req)

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> SearchResult:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_request(index, request, obs_handler, options, **kwargs)
        return self._search(req)

    def _search(self, req: SearchQueryRequest) -> SearchResult:
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('search_timeout', None)
        return SearchResult(RowIterator(ClassicStreamingExecutor(self._client_adapter.connection,
                                                                 SearchStreamSpec(req.query_builder.as_encodable(),
                                                                                  self.default_serializer),
                                                                 obs_handler=req.obs_handler,
                                                                 streaming_timeout=streaming_timeout)))

    def update_credentials(self,
                           authenticator: CertificateAuthenticator | JwtAuthenticator | PasswordAuthenticator
                           ) -> None:
        req = self._request_builder.build_update_credential_request(authenticator)
        self._client_adapter.execute_cluster_request(req)
        self._cluster_settings.auth = req.auth

    def wait_until_ready(self, timeout: timedelta, opts: WaitUntilReadyOptions | None = None, **kwargs: Any) -> None:
        req = self._request_builder.build_wait_until_ready_request(timeout, opts, **kwargs)
        current_time = time.monotonic()
        deadline = current_time + req.timeout.total_seconds()
        delay = 0.1  # seconds

        diag_req = self._request_builder.build_diagnostics_request()
        ping_req = self._request_builder.build_ping_request(service_types=req.service_types)

        while True:
            diag_res = self._diagnostics(diag_req)
            endpoint_svc_types = set(map(lambda st: st.value, diag_res.endpoints.keys()))
            if not endpoint_svc_types.issuperset(req.service_types):
                self._ping(ping_req)
                diag_res = self._diagnostics(diag_req)

            if diag_res.state == req.desired_state:
                break

            current_time = time.monotonic()
            if deadline < (current_time + delay):
                raise UnAmbiguousTimeoutException('Desired state not found.')
            time.sleep(delay)

    def _diagnostics(self, req: DiagnosticsRequest) -> DiagnosticsResult:
        return DiagnosticsResult(_diagnostics_from_core(self._client_adapter.execute_cluster_request(req).raw_result))

    def _ping(self, req: PingRequest) -> PingResult:
        return PingResult(_ping_from_core(self._client_adapter.execute_cluster_request(req).raw_result))

    def _get_connection_opts(self,
                             auth_only: bool | None = None,
                             conn_only: bool | None = None
                             ) -> dict[str, Any] | tuple[dict[str, Any], dict[str, Any]]:
        """Get connection related options

        **INTERNAL** not intended for use in public API.

        Args:
            auth_only (bool, optional): Set to True to return only auth options. Defaults to False.
            conn_only (bool, optional): Set to True to return only cluster options. Defaults to False.

        Returns:
            dict[str, Any] | tuple[dict[str, Any], dict[str, Any]]: Either the
                cluster auth, cluster options or a tuple of both the cluster auth and cluster options.
        """
        if auth_only is True:
            return self._cluster_settings.auth
        if conn_only is True:
            return self.connect_options
        return self._cluster_settings.auth, self.connect_options
