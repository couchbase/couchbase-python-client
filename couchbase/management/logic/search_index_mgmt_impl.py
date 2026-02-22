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
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.management.logic.search_index_mgmt_req_builder import SearchIndexMgmtRequestBuilder
from couchbase.management.logic.search_index_mgmt_types import SearchIndex

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter
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


class SearchIndexMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = SearchIndexMgmtRequestBuilder()
        self._observability_instruments = observability_instruments

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._observability_instruments

    @property
    def request_builder(self) -> SearchIndexMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    def allow_querying(self, req: AllowQueryingRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def analyze_document(self, req: AnalyzeDocumentRequest, obs_handler: ObservableRequestHandler) -> Dict[str, Any]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        analysis = ret.raw_result['analysis']
        status = ret.raw_result['status']
        return {
            'analysis': json.loads(analysis),
            'status': status
        }

    def drop_index(self, req: DropIndexRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def disallow_querying(self, req: DisallowQueryingRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def freeze_plan(self, req: FreezePlanRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_indexes(
        self,
        req: GetAllIndexesRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Iterable[SearchIndex]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_indexes = ret.raw_result['indexes']
        return [SearchIndex.from_server(idx) for idx in raw_indexes]

    def get_all_index_stats(
        self,
        req: GetAllIndexStatsRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Dict[str, Any]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_stats = ret.raw_result['stats']
        return json.loads(raw_stats)

    def get_indexed_documents_count(
        self,
        req: GetIndexedDocumentsCountRequest,
        obs_handler: ObservableRequestHandler,
    ) -> int:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        return ret.raw_result['count']

    def get_index(self, req: GetIndexRequest, obs_handler: ObservableRequestHandler) -> SearchIndex:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_index = ret.raw_result['index']
        return SearchIndex.from_server(raw_index)

    def get_index_stats(self, req: GetIndexStatsRequest, obs_handler: ObservableRequestHandler) -> Dict[str, Any]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_stats = ret.raw_result['stats']
        return json.loads(raw_stats)

    def pause_ingest(self, req: PauseIngestRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def resume_ingest(self, req: ResumeIngestRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def unfreeze_plan(self, req: UnfreezePlanRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def upsert_index(self, req: UpsertIndexRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
