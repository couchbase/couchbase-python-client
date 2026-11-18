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

"""The asyncio cluster over the gateway: one `grpc.aio.Channel`, and the operations couchbase2 provides.

The channel is built by the first operation rather than at construction.  A `grpc.aio` channel binds
to the loop running when it is made, and building its credentials can block for a TLS handshake,
which is done off the loop.  Any operation couchbase2 does not provide raises
`FeatureUnavailableException`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn,
                    cast)

from acouchbase.protocol.couchbase2._bucket_impl import AsyncBucketImpl
from acouchbase.protocol.couchbase2._management._bucket_mgmt_impl import AsyncBucketMgmtImpl
from acouchbase.protocol.couchbase2._management._query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from acouchbase.protocol.couchbase2._management._search_index_mgmt_impl import AsyncSearchIndexMgmtImpl
from acouchbase.protocol.couchbase2._streaming import AsyncStreamExecutor
from acouchbase.result import AsyncQueryResult, AsyncSearchResult
from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common._streaming import AsyncRowIterator
from couchbase.protocol.couchbase2._channel import channel_arguments, open_aio_channel
from couchbase.protocol.couchbase2._cluster_req_builder import ClusterRequestBuilder
from couchbase.protocol.couchbase2._connectivity import ConnectivityLog, watch_async
from couchbase.protocol.couchbase2._core import ClusterCore, unsupported
from couchbase.protocol.couchbase2._driver import call_stream_async
from couchbase.protocol.couchbase2._generated.admin.bucket.v1 import bucket_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.collection.v1 import collection_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2_grpc as query_admin_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2_grpc as search_admin_pb2_grpc
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2_grpc
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2_grpc
from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2_grpc
from couchbase.protocol.couchbase2._health import health_check_async, wait_until_serving_async
from couchbase.protocol.couchbase2._search import SearchRows
from couchbase.protocol.couchbase2._streaming import QueryRows

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from datetime import timedelta

    import grpc.aio
    from google.protobuf.message import Message

    from acouchbase.result import AsyncAnalyticsResult
    from acouchbase.transactions import Transactions
    from couchbase.auth import (CertificateAuthenticator,
                                JwtAuthenticator,
                                PasswordAuthenticator)
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
    from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2
    from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2


@dataclass(frozen=True)
class _Stubs:
    channel: grpc.aio.Channel
    kv: kv_pb2_grpc.KvServiceAsyncStub
    query: query_pb2_grpc.QueryServiceAsyncStub
    search: search_pb2_grpc.SearchServiceAsyncStub
    collection_admin: collection_pb2_grpc.CollectionAdminServiceAsyncStub
    bucket_admin: bucket_pb2_grpc.BucketAdminServiceAsyncStub
    query_admin: query_admin_pb2_grpc.QueryAdminServiceAsyncStub
    search_admin: search_admin_pb2_grpc.SearchAdminServiceAsyncStub
    health: grpc.aio.UnaryUnaryMultiCallable[Message, int]
    watch: asyncio.Task[None]


class AsyncClusterImpl:

    def __init__(self,
                 connstr: str,
                 *options: ClusterOptions,
                 loop: AbstractEventLoop | None = None,
                 loop_validator: Callable[[AbstractEventLoop | None], AbstractEventLoop] | None = None,
                 **kwargs: Any) -> None:
        self._core = ClusterCore(connstr, *options, **kwargs)
        self._request_builder = ClusterRequestBuilder(self._core)
        self._supplied_loop = loop
        self._loop: AbstractEventLoop | None = None
        self._loop_validator = loop_validator
        self._open: _Stubs | None = None
        self._opening = asyncio.Lock()

    @property
    def core(self) -> ClusterCore:
        """**INTERNAL**"""
        return self._core

    @property
    def loop(self) -> AbstractEventLoop:
        if self._loop is None:
            loop = self._supplied_loop
            if self._loop_validator:
                loop = self._loop_validator(loop)
            elif loop is None:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    raise RuntimeError('Event loop is not running.') from None
            if not loop.is_running():
                raise RuntimeError('Event loop is not running.')
            self._loop = loop
        return self._loop

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._core.observability_instruments

    @property
    def transactions(self) -> Transactions:
        raise unsupported('Transactions')

    async def kv(self) -> kv_pb2_grpc.KvServiceAsyncStub:
        """**INTERNAL** The key-value stub, on a channel built the first time any stub is asked for."""
        return (await self._stubs()).kv

    async def query_service(self) -> query_pb2_grpc.QueryServiceAsyncStub:
        """**INTERNAL**"""
        return (await self._stubs()).query

    async def search_service(self) -> search_pb2_grpc.SearchServiceAsyncStub:
        """**INTERNAL**"""
        return (await self._stubs()).search

    async def collection_admin(self) -> collection_pb2_grpc.CollectionAdminServiceAsyncStub:
        """**INTERNAL**"""
        return (await self._stubs()).collection_admin

    async def bucket_admin(self) -> bucket_pb2_grpc.BucketAdminServiceAsyncStub:
        """**INTERNAL**"""
        return (await self._stubs()).bucket_admin

    async def query_admin(self) -> query_admin_pb2_grpc.QueryAdminServiceAsyncStub:
        """**INTERNAL**"""
        return (await self._stubs()).query_admin

    async def search_admin(self) -> search_admin_pb2_grpc.SearchAdminServiceAsyncStub:
        """**INTERNAL**"""
        return (await self._stubs()).search_admin

    async def _stubs(self) -> _Stubs:
        if self._open is not None:
            return self._open
        async with self._opening:
            if self._open is None:
                config = self._core.channel_config
                loop = asyncio.get_running_loop()
                credentials, options = await loop.run_in_executor(None, channel_arguments, config)
                self._core.ensure_open()
                channel = open_aio_channel(config, credentials, options)
                # The async stub classes exist only in the stubs; at runtime one class serves
                # both channel kinds.
                self._open = _Stubs(channel,
                                    cast('kv_pb2_grpc.KvServiceAsyncStub', kv_pb2_grpc.KvServiceStub(channel)),
                                    cast('query_pb2_grpc.QueryServiceAsyncStub',
                                         query_pb2_grpc.QueryServiceStub(channel)),
                                    cast('search_pb2_grpc.SearchServiceAsyncStub',
                                         search_pb2_grpc.SearchServiceStub(channel)),
                                    cast('collection_pb2_grpc.CollectionAdminServiceAsyncStub',
                                         collection_pb2_grpc.CollectionAdminServiceStub(channel)),
                                    cast('bucket_pb2_grpc.BucketAdminServiceAsyncStub',
                                         bucket_pb2_grpc.BucketAdminServiceStub(channel)),
                                    cast('query_admin_pb2_grpc.QueryAdminServiceAsyncStub',
                                         query_admin_pb2_grpc.QueryAdminServiceStub(channel)),
                                    cast('search_admin_pb2_grpc.SearchAdminServiceAsyncStub',
                                         search_admin_pb2_grpc.SearchAdminServiceStub(channel)),
                                    health_check_async(channel),
                                    watch_async(channel,
                                                ConnectivityLog(config.endpoint.target,
                                                                lambda: self._core.last_request)))
                open_stubs = self._open
                self._core.tracing.channel_opened((open_stubs.kv,
                                                   open_stubs.query,
                                                   open_stubs.search,
                                                   open_stubs.collection_admin,
                                                   open_stubs.bucket_admin,
                                                   open_stubs.query_admin,
                                                   open_stubs.search_admin),
                                                  aio=True)
            return self._open

    async def close_connection(self) -> None:
        if self._core.closed:
            return
        self._core.close()
        if self._open is not None:
            await self._open.channel.close()
            await self._open.watch

    async def wait_until_connected(self) -> None:
        """Build the channel.  It connects on its first call, as the sync channel does."""
        self._core.ensure_open()
        await self._stubs()

    def bucket(self, name: str) -> AsyncBucketImpl:
        """**INTERNAL**"""
        self._core.ensure_open()
        return AsyncBucketImpl(name, self)

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> AsyncQueryResult:
        self._core.ensure_open()
        return self.run_query(self._request_builder.build_query_request(statement, *options, **kwargs))

    def run_query(self, req: QueryRequest) -> AsyncQueryResult:
        """**INTERNAL** A query the cluster or a scope built, sent once its rows are first read."""
        obs_handler = ObservableRequestHandler(Op.Query, self.observability_instruments)
        return AsyncQueryResult(AsyncRowIterator(AsyncStreamExecutor(lambda: self.open_query(req, obs_handler),
                                                                     QueryRows(req.serializer),
                                                                     obs_handler=obs_handler,
                                                                     span_attributes=req.span_attributes)))

    async def open_query(self,
                         req: QueryRequest,
                         obs_handler: ObservableRequestHandler | None) -> AsyncGenerator[query_pb2.QueryResponse, None]:
        """**INTERNAL** Send a built query; its deadline starts here."""
        self._core.ensure_open()
        service = await self.query_service()
        ctx = self._core.context(Op.Query,
                                 req.timeout,
                                 readonly=req.readonly,
                                 obs_handler=obs_handler)
        return call_stream_async(service.Query, req.message, ctx, self._core.metadata)

    async def get_cluster_info(self) -> ClusterInfoResult:
        raise unsupported('get_cluster_info')

    async def ping(self, opts: PingOptions | None = None, **kwargs: Any) -> PingResult:
        raise unsupported('ping')

    async def diagnostics(self, opts: DiagnosticsOptions | None = None, **kwargs: Any) -> DiagnosticsResult:
        raise unsupported('diagnostics')

    def update_credentials(self,
                           authenticator: CertificateAuthenticator | JwtAuthenticator | PasswordAuthenticator
                           ) -> None:
        self._core.update_credentials(authenticator)

    async def wait_until_ready(self,
                               timeout: timedelta,
                               opts: WaitUntilReadyOptions | None = None,
                               **kwargs: Any) -> None:
        self._core.ensure_open()
        req = self._request_builder.build_wait_until_ready_request(timeout, opts, **kwargs)
        stubs = await self._stubs()
        await wait_until_serving_async(stubs.health,
                                       self._core.context(Op.WaitUntilReady,
                                                          req.timeout,
                                                          obs_handler=None))

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AsyncAnalyticsResult:
        raise unsupported('analytics_query')

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> AsyncSearchResult:
        self._core.ensure_open()
        return self.run_search(self._request_builder.build_search_query_request(index, query, options, **kwargs))

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> AsyncSearchResult:
        self._core.ensure_open()
        return self.run_search(self._request_builder.build_search_request(index, request, options, **kwargs))

    def run_search(self, req: SearchQueryRequest) -> AsyncSearchResult:
        """**INTERNAL** A search the cluster or a scope built, sent once its rows are first read."""
        obs_handler = ObservableRequestHandler(Op.SearchQuery, self.observability_instruments)
        return AsyncSearchResult(AsyncRowIterator(AsyncStreamExecutor(lambda: self.open_search(req, obs_handler),
                                                                      SearchRows(req),
                                                                      obs_handler=obs_handler,
                                                                      span_attributes=req.span_attributes)))

    async def open_search(
        self,
        req: SearchQueryRequest,
        obs_handler: ObservableRequestHandler | None
    ) -> AsyncGenerator[search_pb2.SearchQueryResponse, None]:
        """**INTERNAL** Send a built search; its deadline starts here."""
        self._core.ensure_open()
        service = await self.search_service()
        ctx = self._core.context(Op.SearchQuery, req.timeout, obs_handler=obs_handler)
        return call_stream_async(service.SearchQuery, req.message, ctx, self._core.metadata)

    def bucket_mgmt(self) -> AsyncBucketMgmtImpl:
        """**INTERNAL**"""
        return AsyncBucketMgmtImpl(self)

    def user_mgmt(self) -> NoReturn:
        raise unsupported('User management')

    def query_index_mgmt(self) -> AsyncQueryIndexMgmtImpl:
        """**INTERNAL**"""
        return AsyncQueryIndexMgmtImpl(self)

    def analytics_mgmt(self) -> NoReturn:
        raise unsupported('Analytics management')

    def search_index_mgmt(self) -> AsyncSearchIndexMgmtImpl:
        """**INTERNAL**"""
        return AsyncSearchIndexMgmtImpl(self)

    def eventing_function_mgmt(self) -> NoReturn:
        raise unsupported('Eventing function management')
