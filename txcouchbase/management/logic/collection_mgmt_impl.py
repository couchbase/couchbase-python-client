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
from typing import TYPE_CHECKING, List

from twisted.internet.defer import Deferred

from acouchbase.management.logic.collection_mgmt_impl import AsyncCollectionMgmtImpl
from couchbase.management.logic.collection_mgmt_req_types import (CreateCollectionRequest,
                                                                  CreateScopeRequest,
                                                                  DropCollectionRequest,
                                                                  DropScopeRequest,
                                                                  GetAllScopesRequest,
                                                                  ScopeSpec,
                                                                  UpdateCollectionRequest)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter


class TxCollectionMgmtImpl(AsyncCollectionMgmtImpl):
    def __init__(self, client_adapter: AsyncClientAdapter) -> Deferred[None]:
        super().__init__(client_adapter)

    def create_collection_deferred(self, req: CreateCollectionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_collection(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_scope_deferred(self, req: CreateScopeRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_scope(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_collection_deferred(self, req: DropCollectionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_collection(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_scope_deferred(self, req: DropScopeRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_scope(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_scopes_deferred(self, req: GetAllScopesRequest) -> Deferred[List[ScopeSpec]]:
        """**INTERNAL**"""
        coro = super().get_all_scopes(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def update_collection_deferred(self, req: UpdateCollectionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().update_collection(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
