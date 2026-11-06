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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._collection_mgmt_impl import AsyncCollectionMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

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
        obs_handler = ObservableRequestHandler(Op.ScopeCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_scope_request(self._bucket_name,
                                                                   scope_name,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            return self._create_scope(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_scope_deferred(self,
                            scope_name: str,
                            options: DropScopeOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.ScopeDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_scope_request(self._bucket_name,
                                                                 scope_name,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            return self._drop_scope(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_scopes_deferred(self,
                                options: GetAllScopesOptions | None = None,
                                **kwargs: Any) -> Deferred[Iterable[ScopeSpec]]:
        obs_handler = ObservableRequestHandler(Op.ScopeGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[ScopeSpec]]:
            req = self._request_builder.build_get_all_scopes_request(self._bucket_name, obs_handler, options, **kwargs)
            return self._get_all_scopes(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def update_collection_deferred(self,
                                   scope_name: str,
                                   collection_name: str,
                                   settings: UpdateCollectionSettings,
                                   options: UpdateCollectionOptions | None = None,
                                   **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.CollectionUpdate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_update_collection_request(self._bucket_name,
                                                                        scope_name,
                                                                        collection_name,
                                                                        settings,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            return self._update_collection(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def create_collection_deferred(self, *args: Any, **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.CollectionCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_collection_request(self._bucket_name,
                                                                        *args,
                                                                        obs_handler=obs_handler,
                                                                        **kwargs)
            return self._create_collection(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_collection_deferred(self, *args: Any, **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.CollectionDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_collection_request(self._bucket_name,
                                                                      *args,
                                                                      obs_handler=obs_handler,
                                                                      **kwargs)
            return self._drop_collection(req, obs_handler)

        return defer(self.loop, obs_handler, start)
