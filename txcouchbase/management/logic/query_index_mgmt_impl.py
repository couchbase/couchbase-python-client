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

from acouchbase.management.logic.query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from couchbase.management.logic.query_index_mgmt_req_types import (BuildDeferredIndexesRequest,
                                                                   CreateIndexRequest,
                                                                   DropIndexRequest,
                                                                   GetAllIndexesRequest,
                                                                   QueryIndex,
                                                                   WatchIndexesRequest)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter


class TxQueryIndexMgmtImpl(AsyncQueryIndexMgmtImpl):
    def __init__(self, client_adapter: AsyncClientAdapter) -> Deferred[None]:
        super().__init__(client_adapter)

    def create_index_deferred(self, req: CreateIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def create_primary_index_deferred(self, req: CreateIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().create_primary_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_index_deferred(self, req: DropIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_primary_index_deferred(self, req: DropIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_primary_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_indexes_deferred(self, req: GetAllIndexesRequest) -> Deferred[List[QueryIndex]]:
        """**INTERNAL**"""
        coro = super().get_all_indexes(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def build_deferred_indexes_deferred(self, req: BuildDeferredIndexesRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().build_deferred_indexes(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def watch_indexes_deferred(self, req: WatchIndexesRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().watch_indexes(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
