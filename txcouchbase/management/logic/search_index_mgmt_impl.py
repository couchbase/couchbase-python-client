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
from twisted.python.failure import Failure

from acouchbase.management.logic.search_index_mgmt_impl import AsyncSearchIndexMgmtImpl
from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
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
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        super().__init__(client_adapter, observability_instruments)

    def _finish_span(self, result, obs_handler: ObservableRequestHandler):
        """Callback to properly end the span on success or failure."""
        if isinstance(result, Failure):
            exc = result.value
            obs_handler.__exit__(type(exc), exc, exc.__traceback__)
            return result
        else:
            obs_handler.__exit__(None, None, None)
            return result

    def allow_querying_deferred(self,
                                req: AllowQueryingRequest,
                                obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().allow_querying(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def analyze_document_deferred(self,
                                  req: AnalyzeDocumentRequest,
                                  obs_handler: ObservableRequestHandler) -> Deferred[Dict[str, Any]]:
        """**INTERNAL**"""
        coro = super().analyze_document(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_index_deferred(self,
                            req: DropIndexRequest,
                            obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_index(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def disallow_querying_deferred(self,
                                   req: DisallowQueryingRequest,
                                   obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().disallow_querying(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def freeze_plan_deferred(self,
                             req: FreezePlanRequest,
                             obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().freeze_plan(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_indexes_deferred(self,
                                 req: GetAllIndexesRequest,
                                 obs_handler: ObservableRequestHandler) -> Deferred[Iterable[SearchIndex]]:
        """**INTERNAL**"""
        coro = super().get_all_indexes(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_index_stats_deferred(self,
                                     req: GetAllIndexStatsRequest,
                                     obs_handler: ObservableRequestHandler) -> Deferred[Dict[str, Any]]:
        """**INTERNAL**"""
        coro = super().get_all_index_stats(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_indexed_documents_count_deferred(self,
                                             req: GetIndexedDocumentsCountRequest,
                                             obs_handler: ObservableRequestHandler) -> Deferred[int]:
        """**INTERNAL**"""
        coro = super().get_indexed_documents_count(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_index_deferred(self,
                           req: GetIndexRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[SearchIndex]:
        """**INTERNAL**"""
        coro = super().get_index(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_index_stats_deferred(self,
                                 req: GetIndexStatsRequest,
                                 obs_handler: ObservableRequestHandler) -> Deferred[Dict[str, Any]]:
        """**INTERNAL**"""
        coro = super().get_index_stats(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def pause_ingest_deferred(self,
                              req: PauseIngestRequest,
                              obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().pause_ingest(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def resume_ingest_deferred(self,
                               req: ResumeIngestRequest,
                               obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().resume_ingest(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def unfreeze_plan_deferred(self,
                               req: UnfreezePlanRequest,
                               obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().unfreeze_plan(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_index_deferred(self,
                              req: UpsertIndexRequest,
                              obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_index(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
