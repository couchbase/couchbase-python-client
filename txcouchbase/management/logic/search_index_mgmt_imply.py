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
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred

from acouchbase.management.logic.search_index_mgmt_impl import AsyncSearchIndexMgmtImpl
from couchbase.management.logic.search_index_mgmt_types import SearchIndex

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.logic.search_index_mgmt_types import (AllowQueryingRequest,
                                                                    AnalyzeDocumentRequest,
                                                                    DisallowQueryingRequest,
                                                                    DropIndexRequest,
                                                                    FreezePlanRequest,
                                                                    GetAllIndexesRequest,
                                                                    GetAllIndexStatsRequest,
                                                                    GetIndexedDocumentsCountRequest,
                                                                    GetIndexRequest,
                                                                    GetIndexStatsRequest,
                                                                    PauseIngestRequest,
                                                                    ResumeIngestRequest,
                                                                    UnfreezePlanRequest,
                                                                    UpsertIndexRequest)


class TxSearchIndexMgmtImpl(AsyncSearchIndexMgmtImpl):
    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        super().__init__(client_adapter)

    def allow_querying_deferred(self, req: AllowQueryingRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().allow_querying(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def analyze_document_deferred(self, req: AnalyzeDocumentRequest) -> Deferred[Dict[str, Any]]:
        """**INTERNAL**"""
        coro = super().analyze_documents(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_index_deferred(self, req: DropIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def disallow_querying_deferred(self, req: DisallowQueryingRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().disallow_querying(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def freeze_plan_deferred(self, req: FreezePlanRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().freeze_plan(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_indexes_deferred(self, req: GetAllIndexesRequest) -> Deferred[Iterable[SearchIndex]]:
        """**INTERNAL**"""
        coro = super().get_all_indexes(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_index_stats_deferred(self, req: GetAllIndexStatsRequest) -> Deferred[Dict[str, Any]]:
        """**INTERNAL**"""
        coro = super().get_all_index_stats(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_indexed_documents_count_deferred(self, req: GetIndexedDocumentsCountRequest) -> Deferred[int]:
        """**INTERNAL**"""
        coro = super().get_indexed_documents_count(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_index_deferred(self, req: GetIndexRequest) -> Deferred[SearchIndex]:
        """**INTERNAL**"""
        coro = super().get_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_index_stats_deferred(self, req: GetIndexStatsRequest) -> Deferred[Dict[str, Any]]:
        """**INTERNAL**"""
        coro = super().get_index_stats(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def pause_ingest_deferred(self, req: PauseIngestRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().pause_ingest(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def resume_ingest_deferred(self, req: ResumeIngestRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().resume_ingest(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def unfreeze_plan_deferred(self, req: UnfreezePlanRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().unfreeze_plan(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_index_deferred(self, req: UpsertIndexRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_index(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
