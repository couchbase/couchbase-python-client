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

import json
from asyncio import AbstractEventLoop
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from couchbase.management.logic.search_index_mgmt_req_builder import SearchIndexMgmtRequestBuilder
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


class AsyncSearchIndexMgmtImpl:
    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        self._client_adapter = client_adapter
        self._request_builder = SearchIndexMgmtRequestBuilder()

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL**"""
        return self._client_adapter.loop

    @property
    def request_builder(self) -> SearchIndexMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    async def allow_querying(self, req: AllowQueryingRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def analyze_document(self, req: AnalyzeDocumentRequest) -> Dict[str, Any]:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        output = {}
        analysis = ret.raw_result.get('analysis', None)
        if analysis:
            output['analysis'] = json.loads(analysis)
        status = ret.raw_result.get('status', None)
        if status:
            output['status'] = status

        return output

    async def drop_index(self, req: DropIndexRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def disallow_querying(self, req: DisallowQueryingRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def freeze_plan(self, req: FreezePlanRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def get_all_indexes(self, req: GetAllIndexesRequest) -> Iterable[SearchIndex]:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        indexes = []
        raw_indexes = ret.raw_result.get('indexes', None)
        if raw_indexes:
            indexes = [SearchIndex.from_server(idx) for idx in raw_indexes]

        return indexes

    async def get_all_index_stats(self, req: GetAllIndexStatsRequest) -> Dict[str, Any]:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        raw_stats = ret.raw_result.get('stats', None)
        stats = None
        if raw_stats:
            stats = json.loads(raw_stats)

        return stats

    async def get_indexed_documents_count(self, req: GetIndexedDocumentsCountRequest) -> int:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        return ret.raw_result.get('count', 0)

    async def get_index(self, req: GetIndexRequest) -> SearchIndex:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        raw_index = ret.raw_result.get('index', None)
        index = None
        if raw_index:
            index = SearchIndex.from_server(raw_index)

        return index

    async def get_index_stats(self, req: GetIndexStatsRequest) -> Dict[str, Any]:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        raw_stats = ret.raw_result.get('stats', None)
        stats = None
        if raw_stats:
            stats = json.loads(raw_stats)

        return stats

    async def pause_ingest(self, req: PauseIngestRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def resume_ingest(self, req: ResumeIngestRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def unfreeze_plan(self, req: UnfreezePlanRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def upsert_index(self, req: UpsertIndexRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)
