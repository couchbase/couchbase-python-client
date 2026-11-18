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

from __future__ import annotations

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.couchbase2._management._collection_mgmt_impl import AsyncCollectionMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.couchbase2._deferred import defer

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.collections import ScopeSpec, UpdateCollectionSettings
    from couchbase.common.management.options import (CreateScopeOptions,
                                                     DropScopeOptions,
                                                     GetAllScopesOptions,
                                                     UpdateCollectionOptions)


class TxCollectionMgmtImpl(AsyncCollectionMgmtImpl):

    def create_scope_deferred(self,
                              scope_name: str,
                              options: CreateScopeOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.ScopeCreate, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_create_scope(
                self.request_builder().build_create_scope_request(scope_name, obs_handler, options, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def drop_scope_deferred(self,
                            scope_name: str,
                            options: DropScopeOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.ScopeDrop, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_drop_scope(
                self.request_builder().build_drop_scope_request(scope_name, obs_handler, options, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def get_all_scopes_deferred(self,
                                options: GetAllScopesOptions | None = None,
                                **kwargs: Any) -> Deferred[list[ScopeSpec]]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.ScopeGetAll, self.observability_instruments)

        def start() -> Coroutine[Any, Any, list[ScopeSpec]]:
            return self.send_get_all_scopes(
                self.request_builder().build_get_all_scopes_request(obs_handler, options, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def update_collection_deferred(self,
                                   scope_name: str,
                                   collection_name: str,
                                   settings: UpdateCollectionSettings,
                                   options: UpdateCollectionOptions | None = None,
                                   **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.CollectionUpdate, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_update_collection(
                self.request_builder().build_update_collection_request(scope_name,
                                                                       collection_name,
                                                                       settings,
                                                                       obs_handler,
                                                                       options,
                                                                       **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def create_collection_deferred(self, *args: Any, **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.CollectionCreate, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_create_collection(
                self.request_builder().build_create_collection_request(*args, obs_handler=obs_handler, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def drop_collection_deferred(self, *args: Any, **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.CollectionDrop, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_drop_collection(
                self.request_builder().build_drop_collection_request(*args, obs_handler=obs_handler, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)
