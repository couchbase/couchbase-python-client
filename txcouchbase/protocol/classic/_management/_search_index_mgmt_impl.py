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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._search_index_mgmt_impl import AsyncSearchIndexMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

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
    from couchbase.common.management.search import SearchIndex


class TxSearchIndexMgmtImpl(AsyncSearchIndexMgmtImpl):

    def upsert_index_deferred(self,
                              index: SearchIndex,
                              options: UpsertSearchIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexUpsert, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_upsert_index_request(index,
                                                                   self._scope_context,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            return self._upsert_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_index_deferred(self,
                            index_name: str,
                            options: DropSearchIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_index_request(index_name,
                                                                 self._scope_context,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            return self._drop_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_index_deferred(self,
                           index_name: str,
                           options: GetSearchIndexOptions | None = None,
                           **kwargs: Any) -> Deferred[SearchIndex]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexGet, self._observability_instruments)

        def start() -> Coroutine[Any, Any, SearchIndex]:
            req = self._request_builder.build_get_index_request(index_name,
                                                                self._scope_context,
                                                                obs_handler,
                                                                options,
                                                                **kwargs)
            return self._get_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_indexes_deferred(self,
                                 options: GetAllSearchIndexesOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[SearchIndex]]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[SearchIndex]]:
            req = self._request_builder.build_get_all_indexes_request(self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            return self._get_all_indexes(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_indexed_documents_count_deferred(self,
                                             index_name: str,
                                             options: GetSearchIndexedDocumentsCountOptions | None = None,
                                             **kwargs: Any) -> Deferred[int]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexGetDocumentsCount, self._observability_instruments)

        def start() -> Coroutine[Any, Any, int]:
            req = self._request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                  self._scope_context,
                                                                                  obs_handler,
                                                                                  options,
                                                                                  **kwargs)
            return self._get_indexed_documents_count(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def pause_ingest_deferred(self,
                              index_name: str,
                              options: PauseIngestSearchIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexPauseIngest, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_pause_ingest_request(index_name,
                                                                   self._scope_context,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            return self._pause_ingest(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def resume_ingest_deferred(self,
                               index_name: str,
                               options: ResumeIngestSearchIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexResumeIngest, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_resume_ingest_request(index_name,
                                                                    self._scope_context,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            return self._resume_ingest(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def allow_querying_deferred(self,
                                index_name: str,
                                options: AllowQueryingSearchIndexOptions | None = None,
                                **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexAllowQuerying, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_allow_querying_request(index_name,
                                                                     self._scope_context,
                                                                     obs_handler,
                                                                     options,
                                                                     **kwargs)
            return self._allow_querying(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def disallow_querying_deferred(self,
                                   index_name: str,
                                   options: DisallowQueryingSearchIndexOptions | None = None,
                                   **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexDisallowQuerying, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_disallow_querying_request(index_name,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            return self._disallow_querying(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def freeze_plan_deferred(self,
                             index_name: str,
                             options: FreezePlanSearchIndexOptions | None = None,
                             **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexFreezePlan, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_freeze_plan_request(index_name,
                                                                  self._scope_context,
                                                                  obs_handler,
                                                                  options,
                                                                  **kwargs)
            return self._freeze_plan(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def unfreeze_plan_deferred(self,
                               index_name: str,
                               options: UnfreezePlanSearchIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexUnfreezePlan, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_unfreeze_plan_request(index_name,
                                                                    self._scope_context,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            return self._unfreeze_plan(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def analyze_document_deferred(self,
                                  index_name: str,
                                  document: Any,
                                  options: AnalyzeDocumentSearchIndexOptions | None = None,
                                  **kwargs: Any) -> Deferred[dict[str, Any]]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexAnalyzeDocument, self._observability_instruments)

        def start() -> Coroutine[Any, Any, dict[str, Any]]:
            req = self._request_builder.build_analyze_document_request(index_name,
                                                                       document,
                                                                       self._scope_context,
                                                                       obs_handler,
                                                                       options,
                                                                       **kwargs)
            return self._analyze_document(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_index_stats_deferred(self,
                                 index_name: str,
                                 options: GetSearchIndexStatsOptions | None = None,
                                 **kwargs: Any) -> Deferred[dict[str, Any]]:
        obs_handler = ObservableRequestHandler(Op.SearchIndexGetStats, self._observability_instruments)

        def start() -> Coroutine[Any, Any, dict[str, Any]]:
            req = self._request_builder.build_get_index_stats_request(index_name,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            return self._get_index_stats(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_index_stats_deferred(self,
                                     options: GetAllSearchIndexStatsOptions | None = None,
                                     **kwargs: Any) -> Deferred[dict[str, Any]]:
        obs_handler = ObservableRequestHandler(Op.SearchGetStats, self._observability_instruments)

        def start() -> Coroutine[Any, Any, dict[str, Any]]:
            req = self._request_builder.build_get_all_index_stats_request(obs_handler, options, **kwargs)
            return self._get_all_index_stats(req, obs_handler)

        return defer(self.loop, obs_handler, start)
