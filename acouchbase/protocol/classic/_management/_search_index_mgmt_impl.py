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

import json
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.search import SearchIndex
from couchbase.protocol.classic._management._search_index_mgmt_req_builder import SearchIndexMgmtRequestBuilder

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from acouchbase.protocol.classic._client_adapter import AsyncClientAdapter
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.options import (AllowQueryingSearchIndexOptions,
                                                     AnalyzeDocumentSearchIndexOptions,
                                                     DisallowQueryingSearchIndexOptions,
                                                     DropSearchIndexOptions,
                                                     FreezePlanSearchIndexOptions,
                                                     GetAllSearchIndexesOptions,
                                                     GetAllSearchIndexStatsOptions,
                                                     GetSearchIndexedDocumentsCountOptions,
                                                     GetSearchIndexOptions,
                                                     GetSearchIndexStatsOptions,
                                                     PauseIngestSearchIndexOptions,
                                                     ResumeIngestSearchIndexOptions,
                                                     UnfreezePlanSearchIndexOptions,
                                                     UpsertSearchIndexOptions)
    from couchbase.protocol.classic._management._search_index_mgmt_types import (AllowQueryingRequest,
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


class AsyncSearchIndexMgmtImpl:
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments,
                 scope_context: tuple[str, str] | None = None) -> None:
        self._client_adapter = client_adapter
        self._request_builder = SearchIndexMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments
        self._scope_context = scope_context

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL** The loop the txcouchbase impl runs these coroutines on."""
        return self._client_adapter.loop

    async def upsert_index(self,
                           index: SearchIndex,
                           options: UpsertSearchIndexOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexUpsert, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_index_request(index,
                                                                   self._scope_context,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            await self._upsert_index(req, obs_handler)

    async def drop_index(self,
                         index_name: str,
                         options: DropSearchIndexOptions | None = None,
                         **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_index_request(index_name,
                                                                 self._scope_context,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            await self._drop_index(req, obs_handler)

    async def get_index(self,
                        index_name: str,
                        options: GetSearchIndexOptions | None = None,
                        **kwargs: Any) -> SearchIndex:
        async with ObservableRequestHandler(Op.SearchIndexGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_index_request(index_name,
                                                                self._scope_context,
                                                                obs_handler,
                                                                options,
                                                                **kwargs)
            return await self._get_index(req, obs_handler)

    async def get_all_indexes(self,
                              options: GetAllSearchIndexesOptions | None = None,
                              **kwargs: Any) -> Iterable[SearchIndex]:
        async with ObservableRequestHandler(Op.SearchIndexGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_indexes_request(self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            return await self._get_all_indexes(req, obs_handler)

    async def get_indexed_documents_count(self,
                                          index_name: str,
                                          options: GetSearchIndexedDocumentsCountOptions | None = None,
                                          **kwargs: Any) -> int:
        async with ObservableRequestHandler(Op.SearchIndexGetDocumentsCount,
                                            self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                  self._scope_context,
                                                                                  obs_handler,
                                                                                  options,
                                                                                  **kwargs)
            return await self._get_indexed_documents_count(req, obs_handler)

    async def pause_ingest(self,
                           index_name: str,
                           options: PauseIngestSearchIndexOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexPauseIngest, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_pause_ingest_request(index_name,
                                                                   self._scope_context,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            await self._pause_ingest(req, obs_handler)

    async def resume_ingest(self,
                            index_name: str,
                            options: ResumeIngestSearchIndexOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexResumeIngest, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_resume_ingest_request(index_name,
                                                                    self._scope_context,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            await self._resume_ingest(req, obs_handler)

    async def allow_querying(self,
                             index_name: str,
                             options: AllowQueryingSearchIndexOptions | None = None,
                             **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexAllowQuerying,
                                            self._observability_instruments) as obs_handler:
            req = self._request_builder.build_allow_querying_request(index_name,
                                                                     self._scope_context,
                                                                     obs_handler,
                                                                     options,
                                                                     **kwargs)
            await self._allow_querying(req, obs_handler)

    async def disallow_querying(self,
                                index_name: str,
                                options: DisallowQueryingSearchIndexOptions | None = None,
                                **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexDisallowQuerying,
                                            self._observability_instruments) as obs_handler:
            req = self._request_builder.build_disallow_querying_request(index_name,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            await self._disallow_querying(req, obs_handler)

    async def freeze_plan(self,
                          index_name: str,
                          options: FreezePlanSearchIndexOptions | None = None,
                          **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexFreezePlan, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_freeze_plan_request(index_name,
                                                                  self._scope_context,
                                                                  obs_handler,
                                                                  options,
                                                                  **kwargs)
            await self._freeze_plan(req, obs_handler)

    async def unfreeze_plan(self,
                            index_name: str,
                            options: UnfreezePlanSearchIndexOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.SearchIndexUnfreezePlan, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_unfreeze_plan_request(index_name,
                                                                    self._scope_context,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            await self._unfreeze_plan(req, obs_handler)

    async def analyze_document(self,
                               index_name: str,
                               document: Any,
                               options: AnalyzeDocumentSearchIndexOptions | None = None,
                               **kwargs: Any) -> dict[str, Any]:
        async with ObservableRequestHandler(Op.SearchIndexAnalyzeDocument,
                                            self._observability_instruments) as obs_handler:
            req = self._request_builder.build_analyze_document_request(index_name,
                                                                       document,
                                                                       self._scope_context,
                                                                       obs_handler,
                                                                       options,
                                                                       **kwargs)
            return await self._analyze_document(req, obs_handler)

    async def get_index_stats(self,
                              index_name: str,
                              options: GetSearchIndexStatsOptions | None = None,
                              **kwargs: Any) -> dict[str, Any]:
        async with ObservableRequestHandler(Op.SearchIndexGetStats, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_index_stats_request(index_name,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            return await self._get_index_stats(req, obs_handler)

    async def get_all_index_stats(self,
                                  options: GetAllSearchIndexStatsOptions | None = None,
                                  **kwargs: Any) -> dict[str, Any]:
        async with ObservableRequestHandler(Op.SearchGetStats, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_index_stats_request(obs_handler, options, **kwargs)
            return await self._get_all_index_stats(req, obs_handler)

    # The execute half of each operation.  The txcouchbase impl builds the request itself, inside
    # the `start()` it hands to `defer()`, and returns one of these as the coroutine to run.
    async def _allow_querying(self, req: AllowQueryingRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _analyze_document(
        self,
        req: AnalyzeDocumentRequest,
        obs_handler: ObservableRequestHandler,
    ) -> dict[str, Any]:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        analysis = ret.raw_result['analysis']
        status = ret.raw_result['status']
        return {
            'analysis': json.loads(analysis),
            'status': status
        }

    async def _drop_index(
        self,
        req: DropIndexRequest,
        obs_handler: ObservableRequestHandler,
    ) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _disallow_querying(
        self,
        req: DisallowQueryingRequest,
        obs_handler: ObservableRequestHandler,
    ) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _freeze_plan(
        self,
        req: FreezePlanRequest,
        obs_handler: ObservableRequestHandler,
    ) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _get_all_indexes(
        self,
        req: GetAllIndexesRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Iterable[SearchIndex]:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_indexes = ret.raw_result['indexes']
        return [SearchIndex.from_server(idx) for idx in raw_indexes]

    async def _get_all_index_stats(
        self,
        req: GetAllIndexStatsRequest,
        obs_handler: ObservableRequestHandler,
    ) -> dict[str, Any]:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_stats = ret.raw_result['stats']
        return json.loads(raw_stats)

    async def _get_indexed_documents_count(
        self,
        req: GetIndexedDocumentsCountRequest,
        obs_handler: ObservableRequestHandler,
    ) -> int:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        return ret.raw_result['count']

    async def _get_index(self, req: GetIndexRequest, obs_handler: ObservableRequestHandler) -> SearchIndex:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_index = ret.raw_result['index']
        return SearchIndex.from_server(raw_index)

    async def _get_index_stats(self,
                               req: GetIndexStatsRequest,
                               obs_handler: ObservableRequestHandler) -> dict[str, Any]:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_stats = ret.raw_result['stats']
        return json.loads(raw_stats)

    async def _pause_ingest(self, req: PauseIngestRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _resume_ingest(self, req: ResumeIngestRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _unfreeze_plan(self, req: UnfreezePlanRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _upsert_index(self, req: UpsertIndexRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
