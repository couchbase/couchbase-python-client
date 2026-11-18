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

"""A sync collection manager over the gateway's collection admin service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._driver import call_unary
from couchbase.protocol.couchbase2._management._collection_mgmt_req_builder import CollectionMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._collection_mgmt_types import history_unavailable, scope_specs

if TYPE_CHECKING:
    from couchbase.common.management.collections import ScopeSpec, UpdateCollectionSettings
    from couchbase.common.management.options import (CreateScopeOptions,
                                                     DropScopeOptions,
                                                     GetAllScopesOptions,
                                                     UpdateCollectionOptions)
    from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl


class CollectionMgmtImpl:

    def __init__(self, bucket_name: str, cluster_impl: ClusterImpl) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = CollectionMgmtRequestBuilder(cluster_impl.core, bucket_name)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def create_scope(self, scope_name: str, options: CreateScopeOptions | None = None, **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.ScopeCreate, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_create_scope_request(scope_name, obs_handler, options, **kwargs)
            ctx = core.context(Op.ScopeCreate, req.timeout, obs_handler=req.obs_handler)
            call_unary(self._cluster_impl.collection_admin.CreateScope, req.message, ctx, core.metadata)

    def drop_scope(self, scope_name: str, options: DropScopeOptions | None = None, **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.ScopeDrop, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_scope_request(scope_name, obs_handler, options, **kwargs)
            ctx = core.context(Op.ScopeDrop, req.timeout, obs_handler=req.obs_handler)
            call_unary(self._cluster_impl.collection_admin.DeleteScope, req.message, ctx, core.metadata)

    def get_all_scopes(self, options: GetAllScopesOptions | None = None, **kwargs: Any) -> list[ScopeSpec]:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.ScopeGetAll, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_scopes_request(obs_handler, options, **kwargs)
            ctx = core.context(Op.ScopeGetAll, req.timeout, obs_handler=req.obs_handler)
            return scope_specs(call_unary(self._cluster_impl.collection_admin.ListCollections,
                                          req.message,
                                          ctx,
                                          core.metadata))

    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          options: UpdateCollectionOptions | None = None,
                          **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.CollectionUpdate, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_update_collection_request(scope_name,
                                                                        collection_name,
                                                                        settings,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            ctx = core.context(Op.CollectionUpdate, req.timeout, obs_handler=req.obs_handler)
            with history_unavailable():
                call_unary(self._cluster_impl.collection_admin.UpdateCollection, req.message, ctx, core.metadata)

    def create_collection(self, *args: Any, **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.CollectionCreate, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_create_collection_request(*args, obs_handler=obs_handler, **kwargs)
            ctx = core.context(Op.CollectionCreate, req.timeout, obs_handler=req.obs_handler)
            with history_unavailable():
                call_unary(self._cluster_impl.collection_admin.CreateCollection, req.message, ctx, core.metadata)

    def drop_collection(self, *args: Any, **kwargs: Any) -> None:
        core = self._cluster_impl.core
        core.ensure_open()
        with ObservableRequestHandler.create(Op.CollectionDrop, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_collection_request(*args, obs_handler=obs_handler, **kwargs)
            ctx = core.context(Op.CollectionDrop, req.timeout, obs_handler=req.obs_handler)
            call_unary(self._cluster_impl.collection_admin.DeleteCollection, req.message, ctx, core.metadata)
