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
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.search import SearchIndex
from couchbase.protocol.classic._management._search_index_mgmt_req_builder import SearchIndexMgmtRequestBuilder

if TYPE_CHECKING:
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
    from couchbase.protocol.classic._client_adapter import ClientAdapter


class SearchIndexMgmtImpl:
    def __init__(self,
                 client_adapter: ClientAdapter,
                 observability_instruments: ObservabilityInstruments,
                 scope_context: tuple[str, str] | None = None) -> None:
        self._client_adapter = client_adapter
        self._request_builder = SearchIndexMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments
        self._scope_context = scope_context

    def upsert_index(self,
                     index: SearchIndex,
                     options: UpsertSearchIndexOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexUpsert, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_index_request(index,
                                                                   self._scope_context,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_index(self,
                   index_name: str,
                   options: DropSearchIndexOptions | None = None,
                   **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_index_request(index_name,
                                                                 self._scope_context,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_index(self,
                  index_name: str,
                  options: GetSearchIndexOptions | None = None,
                  **kwargs: Any) -> SearchIndex:
        with ObservableRequestHandler(Op.SearchIndexGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_index_request(index_name,
                                                                self._scope_context,
                                                                obs_handler,
                                                                options,
                                                                **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_index = ret.raw_result['index']
            return SearchIndex.from_server(raw_index)

    def get_all_indexes(self,
                        options: GetAllSearchIndexesOptions | None = None,
                        **kwargs: Any) -> Iterable[SearchIndex]:
        with ObservableRequestHandler(Op.SearchIndexGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_indexes_request(self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_indexes = ret.raw_result['indexes']
            return [SearchIndex.from_server(idx) for idx in raw_indexes]

    def get_indexed_documents_count(self,
                                    index_name: str,
                                    options: GetSearchIndexedDocumentsCountOptions | None = None,
                                    **kwargs: Any) -> int:
        with ObservableRequestHandler(Op.SearchIndexGetDocumentsCount, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                  self._scope_context,
                                                                                  obs_handler,
                                                                                  options,
                                                                                  **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            return ret.raw_result['count']

    def pause_ingest(self,
                     index_name: str,
                     options: PauseIngestSearchIndexOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexPauseIngest, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_pause_ingest_request(index_name,
                                                                   self._scope_context,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def resume_ingest(self,
                      index_name: str,
                      options: ResumeIngestSearchIndexOptions | None = None,
                      **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexResumeIngest, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_resume_ingest_request(index_name,
                                                                    self._scope_context,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def allow_querying(self,
                       index_name: str,
                       options: AllowQueryingSearchIndexOptions | None = None,
                       **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexAllowQuerying, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_allow_querying_request(index_name,
                                                                     self._scope_context,
                                                                     obs_handler,
                                                                     options,
                                                                     **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def disallow_querying(self,
                          index_name: str,
                          options: DisallowQueryingSearchIndexOptions | None = None,
                          **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexDisallowQuerying, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_disallow_querying_request(index_name,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def freeze_plan(self,
                    index_name: str,
                    options: FreezePlanSearchIndexOptions | None = None,
                    **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexFreezePlan, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_freeze_plan_request(index_name,
                                                                  self._scope_context,
                                                                  obs_handler,
                                                                  options,
                                                                  **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def unfreeze_plan(self,
                      index_name: str,
                      options: UnfreezePlanSearchIndexOptions | None = None,
                      **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.SearchIndexUnfreezePlan, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_unfreeze_plan_request(index_name,
                                                                    self._scope_context,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def analyze_document(self,
                         index_name: str,
                         document: Any,
                         options: AnalyzeDocumentSearchIndexOptions | None = None,
                         **kwargs: Any) -> dict[str, Any]:
        with ObservableRequestHandler(Op.SearchIndexAnalyzeDocument, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_analyze_document_request(index_name,
                                                                       document,
                                                                       self._scope_context,
                                                                       obs_handler,
                                                                       options,
                                                                       **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            analysis = ret.raw_result['analysis']
            status = ret.raw_result['status']
            return {
                'analysis': json.loads(analysis),
                'status': status
            }

    def get_index_stats(self,
                        index_name: str,
                        options: GetSearchIndexStatsOptions | None = None,
                        **kwargs: Any) -> dict[str, Any]:
        with ObservableRequestHandler(Op.SearchIndexGetStats, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_index_stats_request(index_name,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_stats = ret.raw_result['stats']
            return json.loads(raw_stats)

    def get_all_index_stats(self,
                            options: GetAllSearchIndexStatsOptions | None = None,
                            **kwargs: Any) -> dict[str, Any]:
        with ObservableRequestHandler(Op.SearchGetStats, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_index_stats_request(obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_stats = ret.raw_result['stats']
            return json.loads(raw_stats)
