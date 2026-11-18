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

"""An asyncio query index manager over the gateway's query admin service.

Each operation's request is built, then sent, so Twisted can build at the call and send in a coroutine.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.exceptions import (AmbiguousTimeoutException,
                                         CouchbaseException,
                                         UnAmbiguousTimeoutException)
from couchbase.common.management._query_index_mgmt_args import WATCH_POLL_SECONDS, watch_timed_out
from couchbase.protocol.couchbase2._driver import call_unary_async
from couchbase.protocol.couchbase2._management._query_index_mgmt_req_builder import QueryIndexMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._query_index_mgmt_types import query_indexes, still_deferred

if TYPE_CHECKING:
    import grpc.aio

    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
    from couchbase.common.management.options import (BuildDeferredQueryIndexOptions,
                                                     CreatePrimaryQueryIndexOptions,
                                                     CreateQueryIndexOptions,
                                                     DropPrimaryQueryIndexOptions,
                                                     DropQueryIndexOptions,
                                                     GetAllQueryIndexOptions,
                                                     WatchQueryIndexOptions)
    from couchbase.common.management.queries import QueryIndex
    from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2
    from couchbase.protocol.couchbase2._management._query_index_mgmt_types import (QueryIndexMgmtRequest,
                                                                                   WatchIndexesRequest)

Req = TypeVar('Req')
Resp = TypeVar('Resp')


class AsyncQueryIndexMgmtImpl:

    def __init__(self, cluster_impl: AsyncClusterImpl, collection_context: tuple[str, str] | None = None) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = QueryIndexMgmtRequestBuilder(cluster_impl.core, collection_context)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def request_builder(self) -> QueryIndexMgmtRequestBuilder:
        """**INTERNAL** The builder, once the cluster is known to be open."""
        self._cluster_impl.core.ensure_open()
        return self._request_builder

    async def create_index(self,
                           bucket_name: str,
                           index_name: str,
                           keys: Iterable[str],
                           options: CreateQueryIndexOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.QueryIndexCreate, self.observability_instruments) as obs_handler:
            await self.send_create_index(
                self.request_builder().build_create_index_request(bucket_name,
                                                                  index_name,
                                                                  keys,
                                                                  obs_handler,
                                                                  options,
                                                                  **kwargs))

    async def create_primary_index(self,
                                   bucket_name: str,
                                   options: CreatePrimaryQueryIndexOptions | None = None,
                                   **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.QueryIndexCreate, self.observability_instruments) as obs_handler:
            await self.send_create_primary_index(
                self.request_builder().build_create_primary_index_request(bucket_name, obs_handler, options, **kwargs))

    async def drop_index(self,
                         bucket_name: str,
                         index_name: str,
                         options: DropQueryIndexOptions | None = None,
                         **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.QueryIndexDrop, self.observability_instruments) as obs_handler:
            await self.send_drop_index(
                self.request_builder().build_drop_index_request(bucket_name,
                                                                index_name,
                                                                obs_handler,
                                                                options,
                                                                **kwargs))

    async def drop_primary_index(self,
                                 bucket_name: str,
                                 options: DropPrimaryQueryIndexOptions | None = None,
                                 **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.QueryIndexDrop, self.observability_instruments) as obs_handler:
            await self.send_drop_primary_index(
                self.request_builder().build_drop_primary_index_request(bucket_name, obs_handler, options, **kwargs))

    async def get_all_indexes(self,
                              bucket_name: str,
                              options: GetAllQueryIndexOptions | None = None,
                              **kwargs: Any) -> list[QueryIndex]:
        async with ObservableRequestHandler.create(Op.QueryIndexGetAll, self.observability_instruments) as obs_handler:
            return await self.send_get_all_indexes(
                self.request_builder().build_get_all_indexes_request(bucket_name, obs_handler, options, **kwargs))

    async def build_deferred_indexes(self,
                                     bucket_name: str,
                                     options: BuildDeferredQueryIndexOptions | None = None,
                                     **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.QueryIndexBuildDeferred,
                                                   self.observability_instruments) as obs_handler:
            await self.send_build_deferred_indexes(
                self.request_builder().build_build_deferred_indexes_request(bucket_name,
                                                                            obs_handler,
                                                                            options,
                                                                            **kwargs))

    async def watch_indexes(self,
                            bucket_name: str,
                            index_names: Iterable[str],
                            options: WatchQueryIndexOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.QueryIndexWatchIndexes,
                                                   self.observability_instruments) as obs_handler:
            await self.send_watch_indexes(
                self.request_builder().build_watch_indexes_request(bucket_name,
                                                                   index_names,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs),
                obs_handler)

    async def send_create_index(self, req: QueryIndexMgmtRequest[query_pb2.CreateIndexRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.query_admin()
        await self._send(Op.QueryIndexCreate, stub.CreateIndex, req)

    async def send_create_primary_index(self,
                                        req: QueryIndexMgmtRequest[query_pb2.CreatePrimaryIndexRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.query_admin()
        await self._send(Op.QueryIndexCreate, stub.CreatePrimaryIndex, req)

    async def send_drop_index(self, req: QueryIndexMgmtRequest[query_pb2.DropIndexRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.query_admin()
        await self._send(Op.QueryIndexDrop, stub.DropIndex, req)

    async def send_drop_primary_index(self, req: QueryIndexMgmtRequest[query_pb2.DropPrimaryIndexRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.query_admin()
        await self._send(Op.QueryIndexDrop, stub.DropPrimaryIndex, req)

    async def send_get_all_indexes(self,
                                   req: QueryIndexMgmtRequest[query_pb2.GetAllIndexesRequest]) -> list[QueryIndex]:
        """**INTERNAL**"""
        return await self._get_all(req.message, req.timeout, req.obs_handler)

    async def send_build_deferred_indexes(self,
                                          req: QueryIndexMgmtRequest[query_pb2.BuildDeferredIndexesRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.query_admin()
        await self._send(Op.QueryIndexBuildDeferred, stub.BuildDeferredIndexes, req)

    async def send_watch_indexes(self, req: WatchIndexesRequest, obs_handler: ObservableRequestHandler | None) -> None:
        """**INTERNAL** Wait on each watched index in turn, as the sync impl does."""
        deadline = time.monotonic() + req.timeout
        for wait in req.waits:
            while not await self._wait(wait, deadline, obs_handler):
                if deadline < time.monotonic() + WATCH_POLL_SECONDS:
                    raise watch_timed_out()
                await asyncio.sleep(WATCH_POLL_SECONDS)

    async def _wait(self,
                    message: query_pb2.WaitForIndexOnlineRequest,
                    deadline: float,
                    obs_handler: ObservableRequestHandler | None) -> bool:
        """Whether the index came online; False when it is still deferred."""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.query_admin()
        try:
            async with ObservableRequestHandler.create(Op.QueryIndexWaitForIndexOnline,
                                                       self.observability_instruments) as sub_obs_handler:
                if sub_obs_handler and obs_handler:
                    sub_obs_handler.create_http_span(parent_span=obs_handler.wrapped_span)
                ctx = core.context(Op.QueryIndexWaitForIndexOnline,
                                   deadline - time.monotonic(),
                                   readonly=True,
                                   obs_handler=sub_obs_handler)
                await call_unary_async(stub.WaitForIndexOnline, message, ctx, core.metadata)
        except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
            raise watch_timed_out() from None
        except CouchbaseException as exc:
            if still_deferred(exc):
                return False
            raise
        return True

    async def _get_all(self,
                       message: query_pb2.GetAllIndexesRequest,
                       timeout: float,
                       obs_handler: ObservableRequestHandler | None) -> list[QueryIndex]:
        core = self._cluster_impl.core
        stub = await self._cluster_impl.query_admin()
        ctx = core.context(Op.QueryIndexGetAll, timeout, readonly=True, obs_handler=obs_handler)
        return query_indexes(await call_unary_async(stub.GetAllIndexes, message, ctx, core.metadata))

    async def _send(self,
                    op: Op,
                    rpc: grpc.aio.UnaryUnaryMultiCallable[Req, Resp],
                    req: QueryIndexMgmtRequest[Req]) -> Resp:
        core = self._cluster_impl.core
        ctx = core.context(op, req.timeout, obs_handler=req.obs_handler)
        return await call_unary_async(rpc, req.message, ctx, core.metadata)
