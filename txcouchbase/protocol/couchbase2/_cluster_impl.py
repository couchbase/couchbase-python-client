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

"""The Twisted cluster over the gateway: the asyncio cluster, on the reactor's loop, answering in Deferreds.

A query's or a search's Deferred fires once the whole stream has been read, since the result it
carries is the blocking one and iterating it must not block the reactor.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

from twisted.internet.defer import Deferred

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from acouchbase.protocol.couchbase2._streaming import buffered_rows
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._core import unsupported
from couchbase.protocol.couchbase2._health import wait_until_serving_async
from couchbase.protocol.couchbase2._search import SearchRows
from couchbase.protocol.couchbase2._streaming import QueryRows
from couchbase.result import QueryResult, SearchResult
from txcouchbase.protocol.couchbase2._bucket_impl import TxBucketImpl
from txcouchbase.protocol.couchbase2._deferred import defer
from txcouchbase.protocol.couchbase2._management._bucket_mgmt_impl import TxBucketMgmtImpl

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
    from couchbase.common.result import (ClusterInfoResult,
                                         DiagnosticsResult,
                                         PingResult)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.protocol.couchbase2._cluster_types import QueryRequest, SearchQueryRequest
    from couchbase.result import AnalyticsResult


def reactor_loop(_: AbstractEventLoop | None = None) -> AbstractEventLoop:
    """The loop the running reactor drives, which is the only loop a Twisted cluster may use."""
    from twisted.internet import reactor
    loop: AbstractEventLoop | None = getattr(reactor, '_asyncioEventloop', None)
    if loop is None:
        raise RuntimeError('The asyncio event loop has not been installed.  Be sure to import txcouchbase prior to'
                           ' importing the Twisted reactor. Importing txcouchbase will setup and install the event'
                           ' loop using Twisted\'s asyncioreactor.')
    if not cast('IReactorCore', reactor).running:
        raise RuntimeError('Reactor is not running.')
    return loop


class TxClusterImpl(AsyncClusterImpl):

    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: Any) -> None:
        # `loop` is spelled out so a caller's is refused: the validator supplies the reactor's loop.
        super().__init__(connstr, *options, loop=None, loop_validator=reactor_loop, **kwargs)

    def bucket(self, name: str) -> TxBucketImpl:
        """**INTERNAL**"""
        self._core.ensure_open()
        return TxBucketImpl(name, self)

    def bucket_mgmt(self) -> TxBucketMgmtImpl:
        """**INTERNAL**"""
        return TxBucketMgmtImpl(self)

    def close_connection_deferred(self) -> Deferred[None]:
        return Deferred.fromFuture(asyncio.ensure_future(self.close_connection(), loop=self.loop))

    def wait_until_connected_deferred(self) -> Deferred[None]:
        return Deferred.fromFuture(asyncio.ensure_future(self.wait_until_connected(), loop=self.loop))

    def query_deferred(self,
                       statement: str,
                       *options: QueryOptions | JSONType,
                       **kwargs: Any) -> Deferred[QueryResult]:
        def build() -> QueryRequest:
            self.core.ensure_open()
            return self._request_builder.build_query_request(statement, *options, **kwargs)

        return self.query_deferred_from(build)

    def query_deferred_from(self, build: Callable[[], QueryRequest]) -> Deferred[QueryResult]:
        """**INTERNAL** A query the cluster or a scope builds at the call, read whole before its Deferred fires."""
        obs_handler = ObservableRequestHandler(Op.Query, self.observability_instruments)

        async def read(req: QueryRequest) -> QueryResult:
            return QueryResult(await buffered_rows(lambda: self.open_query(req, obs_handler),
                                                   QueryRows(req.serializer)))

        def start() -> Coroutine[Any, Any, QueryResult]:
            req = build()
            obs_handler.create_http_span(**req.span_attributes)
            return read(req)

        return defer(self.loop, obs_handler, start)

    def get_cluster_info_deferred(self) -> Deferred[ClusterInfoResult]:
        raise unsupported('get_cluster_info')

    def ping_deferred(self, opts: PingOptions | None = None, **kwargs: Any) -> Deferred[PingResult]:
        raise unsupported('ping')

    def diagnostics_deferred(self,
                             opts: DiagnosticsOptions | None = None,
                             **kwargs: Any) -> Deferred[DiagnosticsResult]:
        raise unsupported('diagnostics')

    def wait_until_ready_deferred(self,
                                  timeout: timedelta,
                                  opts: WaitUntilReadyOptions | None = None,
                                  **kwargs: Any) -> Deferred[None]:
        self.core.ensure_open()
        req = self._request_builder.build_wait_until_ready_request(timeout, opts, **kwargs)

        async def wait() -> None:
            stubs = await self._stubs()
            await wait_until_serving_async(stubs.health,
                                           self.core.context(Op.WaitUntilReady,
                                                             req.timeout,
                                                             obs_handler=None))

        return defer(self.loop, None, wait)

    def analytics_query_deferred(self,
                                 statement: str,
                                 *options: AnalyticsOptions | JSONType,
                                 **kwargs: Any) -> Deferred[AnalyticsResult]:
        raise unsupported('analytics_query')

    def search_query_deferred(self,
                              index: str,
                              query: SearchQuery,
                              options: SearchOptions | None = None,
                              **kwargs: Any) -> Deferred[SearchResult]:
        def build() -> SearchQueryRequest:
            self.core.ensure_open()
            return self._request_builder.build_search_query_request(index, query, options, **kwargs)

        return self.search_deferred_from(build)

    def search_deferred(self,
                        index: str,
                        request: SearchRequest | SearchQuery,
                        options: SearchOptions | None = None,
                        **kwargs: Any) -> Deferred[SearchResult]:
        def build() -> SearchQueryRequest:
            self.core.ensure_open()
            return self._request_builder.build_search_request(index, request, options, **kwargs)

        return self.search_deferred_from(build)

    def search_deferred_from(self, build: Callable[[], SearchQueryRequest]) -> Deferred[SearchResult]:
        """**INTERNAL** A search the cluster or a scope builds at the call, read whole before its Deferred fires."""
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)

        async def read(req: SearchQueryRequest) -> SearchResult:
            return SearchResult(await buffered_rows(lambda: self.open_search(req, obs_handler), SearchRows(req)))

        def start() -> Coroutine[Any, Any, SearchResult]:
            req = build()
            obs_handler.create_http_span(**req.span_attributes)
            return read(req)

        return defer(self.loop, obs_handler, start)
