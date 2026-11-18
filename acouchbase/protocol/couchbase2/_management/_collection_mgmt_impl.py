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

"""An asyncio collection manager over the gateway's collection admin service.

Each operation's request is built, then sent, so Twisted can build at the call and send in a coroutine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._driver import call_unary_async
from couchbase.protocol.couchbase2._management._collection_mgmt_req_builder import CollectionMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._collection_mgmt_types import (CollectionMgmtRequest,
                                                                              history_unavailable,
                                                                              scope_specs)

if TYPE_CHECKING:
    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
    from couchbase.common.management.collections import ScopeSpec, UpdateCollectionSettings
    from couchbase.common.management.options import (CreateScopeOptions,
                                                     DropScopeOptions,
                                                     GetAllScopesOptions,
                                                     UpdateCollectionOptions)
    from couchbase.protocol.couchbase2._generated.admin.collection.v1 import collection_pb2


class AsyncCollectionMgmtImpl:

    def __init__(self, bucket_name: str, cluster_impl: AsyncClusterImpl) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = CollectionMgmtRequestBuilder(cluster_impl.core, bucket_name)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def request_builder(self) -> CollectionMgmtRequestBuilder:
        """**INTERNAL** The builder, once the cluster is known to be open."""
        self._cluster_impl.core.ensure_open()
        return self._request_builder

    async def create_scope(self, scope_name: str, options: CreateScopeOptions | None = None, **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.ScopeCreate, self.observability_instruments) as obs_handler:
            await self.send_create_scope(
                self.request_builder().build_create_scope_request(scope_name, obs_handler, options, **kwargs))

    async def drop_scope(self, scope_name: str, options: DropScopeOptions | None = None, **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.ScopeDrop, self.observability_instruments) as obs_handler:
            await self.send_drop_scope(
                self.request_builder().build_drop_scope_request(scope_name, obs_handler, options, **kwargs))

    async def get_all_scopes(self, options: GetAllScopesOptions | None = None, **kwargs: Any) -> list[ScopeSpec]:
        async with ObservableRequestHandler.create(Op.ScopeGetAll, self.observability_instruments) as obs_handler:
            return await self.send_get_all_scopes(
                self.request_builder().build_get_all_scopes_request(obs_handler, options, **kwargs))

    async def update_collection(self,
                                scope_name: str,
                                collection_name: str,
                                settings: UpdateCollectionSettings,
                                options: UpdateCollectionOptions | None = None,
                                **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.CollectionUpdate, self.observability_instruments) as obs_handler:
            await self.send_update_collection(
                self.request_builder().build_update_collection_request(scope_name,
                                                                       collection_name,
                                                                       settings,
                                                                       obs_handler,
                                                                       options,
                                                                       **kwargs))

    async def create_collection(self, *args: Any, **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.CollectionCreate, self.observability_instruments) as obs_handler:
            await self.send_create_collection(
                self.request_builder().build_create_collection_request(*args, obs_handler=obs_handler, **kwargs))

    async def drop_collection(self, *args: Any, **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.CollectionDrop, self.observability_instruments) as obs_handler:
            await self.send_drop_collection(
                self.request_builder().build_drop_collection_request(*args, obs_handler=obs_handler, **kwargs))

    async def send_create_scope(self, req: CollectionMgmtRequest[collection_pb2.CreateScopeRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.collection_admin()
        ctx = core.context(Op.ScopeCreate, req.timeout, obs_handler=req.obs_handler)
        await call_unary_async(stub.CreateScope, req.message, ctx, core.metadata)

    async def send_drop_scope(self, req: CollectionMgmtRequest[collection_pb2.DeleteScopeRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.collection_admin()
        ctx = core.context(Op.ScopeDrop, req.timeout, obs_handler=req.obs_handler)
        await call_unary_async(stub.DeleteScope, req.message, ctx, core.metadata)

    async def send_get_all_scopes(self,
                                  req: CollectionMgmtRequest[collection_pb2.ListCollectionsRequest]
                                  ) -> list[ScopeSpec]:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.collection_admin()
        ctx = core.context(Op.ScopeGetAll, req.timeout, obs_handler=req.obs_handler)
        return scope_specs(await call_unary_async(stub.ListCollections, req.message, ctx, core.metadata))

    async def send_update_collection(self,
                                     req: CollectionMgmtRequest[collection_pb2.UpdateCollectionRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.collection_admin()
        ctx = core.context(Op.CollectionUpdate, req.timeout, obs_handler=req.obs_handler)
        with history_unavailable():
            await call_unary_async(stub.UpdateCollection, req.message, ctx, core.metadata)

    async def send_create_collection(self,
                                     req: CollectionMgmtRequest[collection_pb2.CreateCollectionRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.collection_admin()
        ctx = core.context(Op.CollectionCreate, req.timeout, obs_handler=req.obs_handler)
        with history_unavailable():
            await call_unary_async(stub.CreateCollection, req.message, ctx, core.metadata)

    async def send_drop_collection(self,
                                   req: CollectionMgmtRequest[collection_pb2.DeleteCollectionRequest]) -> None:
        """**INTERNAL**"""
        core = self._cluster_impl.core
        stub = await self._cluster_impl.collection_admin()
        ctx = core.context(Op.CollectionDrop, req.timeout, obs_handler=req.obs_handler)
        await call_unary_async(stub.DeleteCollection, req.message, ctx, core.metadata)
