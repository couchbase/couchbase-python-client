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
from typing import TYPE_CHECKING, Optional

from twisted.internet.defer import Deferred

from acouchbase import get_event_loop
from acouchbase.logic.cluster_impl import AsyncClusterImpl
from couchbase.result import (AnalyticsResult,
                              ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.analytics import AnalyticsRequest
from txcouchbase.n1ql import N1QLRequest
from txcouchbase.search import FullTextSearchRequest

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase.logic.cluster_types import (AnalyticsQueryRequest,
                                               ClusterInfoRequest,
                                               DiagnosticsRequest,
                                               PingRequest,
                                               QueryRequest,
                                               SearchQueryRequest,
                                               WaitUntilReadyRequest)


class TxClusterImpl(AsyncClusterImpl):

    def __init__(self, connstr: str, *options: object, **kwargs: object) -> None:
        kwargs['loop_validator'] = self._validate_loop
        super().__init__(connstr, *options, **kwargs)

    def analytics_query_deferred(self, req: AnalyticsQueryRequest) -> Deferred[AnalyticsResult]:
        """**INTERNAL**"""
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute an analytics query prior to establishing a connection.')
        # If the analytics_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the analytics_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's analytics_timeout (set here). If the cluster
        # also does not specify an analytics_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::analytics_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('analytics_timeout', None)
        q_req = AnalyticsRequest.generate_analytics_request(self.connection,
                                                            self.loop,
                                                            req.analytics_query.params,
                                                            default_serializer=self.default_serializer,
                                                            streaming_timeout=streaming_timeout,
                                                            obs_handler=req.obs_handler)
        d = Deferred()

        def _on_ok(_):
            d.callback(AnalyticsResult(q_req))

        def _on_err(exc):
            d.errback(exc)

        query_d = q_req.execute_analytics_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def close_connection_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().close_connection()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def diagnostics_deferred(self, req: DiagnosticsRequest) -> Deferred[DiagnosticsResult]:
        """**INTERNAL**"""
        coro = super().diagnostics(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_cluster_info_deferred(self, req: ClusterInfoRequest) -> Deferred[ClusterInfoResult]:
        """**INTERNAL**"""
        coro = super().get_cluster_info(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def ping_deferred(self, req: PingRequest) -> Deferred[PingResult]:
        """**INTERNAL**"""
        coro = super().ping(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def query_deferred(self, req: QueryRequest) -> Deferred[QueryResult]:
        """**INTERNAL**"""
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a query prior to establishing a connection.')

        # If the n1ql_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the n1ql_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's query_timeout (set here). If the cluster
        # also does not specify a query_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::query_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('query_timeout', None)
        q_req = N1QLRequest.generate_n1ql_request(self.connection,
                                                  self.loop,
                                                  req.n1ql_query.params,
                                                  default_serializer=self.default_serializer,
                                                  streaming_timeout=streaming_timeout,
                                                  obs_handler=req.obs_handler)

        d = Deferred()

        def _on_ok(_):
            d.callback(QueryResult(q_req))

        def _on_err(exc):
            d.errback(exc)

        query_d = q_req.execute_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def search_deferred(self, req: SearchQueryRequest) -> Deferred[SearchResult]:
        """**INTERNAL**"""
        if not self.connected:
            raise RuntimeError('Cannot attempt to execute a search prior to establishing a connection.')
        # If the search_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the search_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's search_timeout (set here). If the cluster
        # also does not specify a search_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::search_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('search_timeout', None)
        q_req = FullTextSearchRequest.generate_search_request(self.connection,
                                                              self.loop,
                                                              req.query_builder.as_encodable(),
                                                              default_serializer=self.default_serializer,
                                                              streaming_timeout=streaming_timeout,
                                                              obs_handler=req.obs_handler)
        d = Deferred()

        def _on_ok(_):
            d.callback(SearchResult(q_req))

        def _on_err(exc):
            d.errback(exc)

        query_d = q_req.execute_search_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def wait_until_ready_deferred(self, req: WaitUntilReadyRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().wait_until_ready(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def wait_until_connected_deferred(self) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().wait_until_connected()
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def _validate_loop(self, loop: Optional[AbstractEventLoop] = None) -> None:
        """**INTERNAL**"""
        if not loop:
            loop = get_event_loop()

        from twisted.internet import reactor
        if hasattr(reactor, '_asyncioEventloop') and reactor._asyncioEventloop is loop:
            # We're in Twisted context - check reactor.running instead
            if not reactor.running:
                raise RuntimeError('Reactor is not running.')
        else:
            msg = ('The asyncio event loop has not been installed.  Be sure to import txcouchbase prior to'
                   ' importing the Twisted reactor. Importing txcouchbase will setup and install the event'
                   ' loop using Twisted\'s asyncioreactor.')
            raise RuntimeError(msg)

        return loop
