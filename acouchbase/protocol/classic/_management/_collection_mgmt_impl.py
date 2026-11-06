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

from asyncio import AbstractEventLoop
from collections.abc import Iterable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.collections import CollectionSpec, ScopeSpec
from couchbase.protocol.classic._management._collection_mgmt_req_builder import CollectionMgmtRequestBuilder

if TYPE_CHECKING:
    from acouchbase.protocol.classic._client_adapter import AsyncClientAdapter
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.collections import UpdateCollectionSettings
    from couchbase.common.management.options import (CreateScopeOptions,
                                                     DropScopeOptions,
                                                     GetAllScopesOptions,
                                                     UpdateCollectionOptions)
    from couchbase.protocol.classic._management._collection_mgmt_types import (CreateCollectionRequest,
                                                                               CreateScopeRequest,
                                                                               DropCollectionRequest,
                                                                               DropScopeRequest,
                                                                               GetAllScopesRequest,
                                                                               UpdateCollectionRequest)


class AsyncCollectionMgmtImpl:
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 bucket_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._bucket_name = bucket_name
        self._request_builder = CollectionMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL** The loop the txcouchbase impl runs these coroutines on."""
        return self._client_adapter.loop

    async def create_scope(self,
                           scope_name: str,
                           options: CreateScopeOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.ScopeCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_scope_request(self._bucket_name,
                                                                   scope_name,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            await self._create_scope(req, obs_handler)

    async def drop_scope(self,
                         scope_name: str,
                         options: DropScopeOptions | None = None,
                         **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.ScopeDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_scope_request(self._bucket_name,
                                                                 scope_name,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            await self._drop_scope(req, obs_handler)

    async def get_all_scopes(self,
                             options: GetAllScopesOptions | None = None,
                             **kwargs: Any) -> Iterable[ScopeSpec]:
        async with ObservableRequestHandler(Op.ScopeGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_scopes_request(self._bucket_name, obs_handler, options, **kwargs)
            return await self._get_all_scopes(req, obs_handler)

    async def update_collection(self,
                                scope_name: str,
                                collection_name: str,
                                settings: UpdateCollectionSettings,
                                options: UpdateCollectionOptions | None = None,
                                **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.CollectionUpdate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_update_collection_request(self._bucket_name,
                                                                        scope_name,
                                                                        collection_name,
                                                                        settings,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            await self._update_collection(req, obs_handler)

    async def create_collection(self, *args: Any, **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.CollectionCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_collection_request(self._bucket_name,
                                                                        *args,
                                                                        obs_handler=obs_handler,
                                                                        **kwargs)
            await self._create_collection(req, obs_handler)

    async def drop_collection(self, *args: Any, **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.CollectionDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_collection_request(self._bucket_name,
                                                                      *args,
                                                                      obs_handler=obs_handler,
                                                                      **kwargs)
            await self._drop_collection(req, obs_handler)

    # The execute half of each operation.  The txcouchbase impl builds the request itself, inside
    # the `start()` it hands to `defer()`, and returns one of these as the coroutine to run.
    async def _create_scope(self, req: CreateScopeRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _drop_scope(self, req: DropScopeRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _get_all_scopes(self, req: GetAllScopesRequest, obs_handler: ObservableRequestHandler) -> list[ScopeSpec]:
        res = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_scopes = res.raw_result['manifest']['scopes']
        return [ScopeSpec(s['name'],
                          [CollectionSpec(c['name'],
                                          s['name'],
                                          timedelta(seconds=c['max_expiry']),
                                          history=c.get('history'))
                           for c in s['collections']])
                for s in raw_scopes]

    async def _update_collection(self, req: UpdateCollectionRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _create_collection(self, req: CreateCollectionRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _drop_collection(self, req: DropCollectionRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
