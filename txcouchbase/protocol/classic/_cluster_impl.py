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

import asyncio
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

from twisted.internet.defer import Deferred

from acouchbase import get_event_loop
from acouchbase.protocol.classic._cluster_impl import AsyncClusterImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.result import (ClusterInfoResult,
                                     DiagnosticsResult,
                                     PingResult)
from couchbase.protocol.classic._streaming import (AnalyticsStreamSpec,
                                                   QueryStreamSpec,
                                                   SearchStreamSpec)
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.protocol.classic._bucket_impl import TxBucketImpl
from txcouchbase.protocol.classic._deferred import defer
from txcouchbase.protocol.classic._management._analytics_mgmt_impl import TxAnalyticsMgmtImpl
from txcouchbase.protocol.classic._management._bucket_mgmt_impl import TxBucketMgmtImpl
from txcouchbase.protocol.classic._management._query_index_mgmt_impl import TxQueryIndexMgmtImpl
from txcouchbase.protocol.classic._management._search_index_mgmt_impl import TxSearchIndexMgmtImpl
from txcouchbase.protocol.classic._management._user_mgmt_impl import TxUserMgmtImpl
from txcouchbase.protocol.classic._streaming import submit_rows

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from datetime import timedelta

    from twisted.internet.interfaces import IReactorCore

    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          ClusterOptions,
                                          DiagnosticsOptions,
                                          PingOptions,
                                          QueryOptions,
                                          SearchOptions,
                                          WaitUntilReadyOptions)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.protocol.classic._cluster_types import SearchQueryRequest


class TxClusterImpl(AsyncClusterImpl):

    def __init__(self,
                 connstr: str,
                 *options: ClusterOptions,
                 **kwargs: object) -> None:
        # `loop` is spelled out so a caller's is refused: the validator supplies the reactor's loop.
        super().__init__(connstr, *options, loop=None, loop_validator=self._validate_loop, **kwargs)

    def bucket(self, name: str) -> TxBucketImpl:
        """**INTERNAL**"""
        return TxBucketImpl(name, self)

    def analytics_query_deferred(self,
                                 statement: str,
                                 *options: AnalyticsOptions | JSONType,
                                 **kwargs: Any) -> Deferred[AnalyticsResult]:
        obs_handler = ObservableRequestHandler(Op.AnalyticsQuery, self.observability_instruments)
        req = self._request_builder.build_analytics_query_request(statement, obs_handler, *options, **kwargs)
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute an analytics query prior to establishing a connection.')
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('analytics_timeout', None)
        rows, responded = submit_rows(self.connection,
                                      self.loop,
                                      AnalyticsStreamSpec(req.analytics_query.params, self.default_serializer),
                                      obs_handler=req.obs_handler,
                                      streaming_timeout=streaming_timeout)
        return responded.addCallback(lambda _: AnalyticsResult(rows))

    def bucket_mgmt(self) -> TxBucketMgmtImpl:
        """**INTERNAL**"""
        return TxBucketMgmtImpl(self._client_adapter, self.observability_instruments)

    def analytics_mgmt(self) -> TxAnalyticsMgmtImpl:
        """**INTERNAL**"""
        return TxAnalyticsMgmtImpl(self._client_adapter, self.observability_instruments)

    def query_index_mgmt(self) -> TxQueryIndexMgmtImpl:
        """**INTERNAL**"""
        return TxQueryIndexMgmtImpl(self._client_adapter, self.observability_instruments)

    def user_mgmt(self) -> TxUserMgmtImpl:
        """**INTERNAL**"""
        return TxUserMgmtImpl(self._client_adapter, self.observability_instruments)

    def search_index_mgmt(self) -> TxSearchIndexMgmtImpl:
        """**INTERNAL**"""
        return TxSearchIndexMgmtImpl(self._client_adapter, self.observability_instruments)

    def close_connection_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().close_connection()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def diagnostics_deferred(self,
                             opts: DiagnosticsOptions | None = None,
                             **kwargs: Any) -> Deferred[DiagnosticsResult]:
        return defer(self.loop,
                     None,
                     lambda: self._diagnostics(self._request_builder.build_diagnostics_request(opts, **kwargs)))

    def get_cluster_info_deferred(self) -> Deferred[ClusterInfoResult]:
        return defer(self.loop, None, self.get_cluster_info)

    def ping_deferred(self, opts: PingOptions | None = None, **kwargs: Any) -> Deferred[PingResult]:
        return defer(self.loop, None, lambda: self._ping(self._request_builder.build_ping_request(opts, **kwargs)))

    def query_deferred(self,
                       statement: str,
                       *options: QueryOptions | JSONType,
                       **kwargs: Any) -> Deferred[QueryResult]:
        obs_handler = ObservableRequestHandler(Op.Query, self.observability_instruments)
        req = self._request_builder.build_query_request(statement, obs_handler, *options, **kwargs)
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a query prior to establishing a connection.')

        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('query_timeout', None)
        rows, responded = submit_rows(self.connection,
                                      self.loop,
                                      QueryStreamSpec(req.n1ql_query.params, self.default_serializer),
                                      obs_handler=req.obs_handler,
                                      streaming_timeout=streaming_timeout)
        return responded.addCallback(lambda _: QueryResult(rows))

    def search_query_deferred(self,
                              index: str,
                              query: SearchQuery,
                              options: SearchOptions | None = None,
                              **kwargs: Any) -> Deferred[SearchResult]:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_query_request(index, query, obs_handler, options, **kwargs)
        return self._search_deferred(req)

    def search_deferred(self,
                        index: str,
                        request: SearchRequest | SearchQuery,
                        options: SearchOptions | None = None,
                        **kwargs: Any) -> Deferred[SearchResult]:
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        req = self._request_builder.build_search_request(index, request, obs_handler, options, **kwargs)
        return self._search_deferred(req)

    def _search_deferred(self, req: SearchQueryRequest) -> Deferred[SearchResult]:
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a search prior to establishing a connection.')
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('search_timeout', None)
        rows, responded = submit_rows(self.connection,
                                      self.loop,
                                      SearchStreamSpec(req.query_builder.as_encodable(), self.default_serializer),
                                      obs_handler=req.obs_handler,
                                      streaming_timeout=streaming_timeout)
        return responded.addCallback(lambda _: SearchResult(rows))

    def wait_until_ready_deferred(self,
                                  timeout: timedelta,
                                  opts: WaitUntilReadyOptions | None = None,
                                  **kwargs: Any) -> Deferred[None]:
        return defer(self.loop,
                     None,
                     lambda: self._wait_until_ready(
                         self._request_builder.build_wait_until_ready_request(timeout, opts, **kwargs)))

    def wait_until_connected_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().wait_until_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def _validate_loop(self, loop: AbstractEventLoop | None = None) -> AbstractEventLoop:
        """**INTERNAL**"""
        if not loop:
            loop = get_event_loop()

        from twisted.internet import reactor
        if hasattr(reactor, '_asyncioEventloop') and reactor._asyncioEventloop is loop:
            # We're in Twisted context - check reactor.running instead
            if not cast('IReactorCore', reactor).running:
                raise RuntimeError('Reactor is not running.')
        else:
            msg = ('The asyncio event loop has not been installed.  Be sure to import txcouchbase prior to'
                   ' importing the Twisted reactor. Importing txcouchbase will setup and install the event'
                   ' loop using Twisted\'s asyncioreactor.')
            raise RuntimeError(msg)

        return loop
