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

from collections.abc import (Callable,
                             Coroutine,
                             Iterable)
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from acouchbase.protocol.couchbase2._management._search_index_mgmt_impl import AsyncSearchIndexMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._core import unsupported
from txcouchbase.protocol.couchbase2._deferred import defer

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

T = TypeVar('T')


class TxSearchIndexMgmtImpl(AsyncSearchIndexMgmtImpl):

    def upsert_index_deferred(self,
                              index: SearchIndex,
                              options: UpsertSearchIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexUpsert, lambda obs_handler: self.send_upsert_index(
            self.request_builder().build_upsert_index_request(index, obs_handler, options, **kwargs)))

    def drop_index_deferred(self,
                            index_name: str,
                            options: DropSearchIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexDrop, lambda obs_handler: self.send_drop_index(
            self.request_builder().build_drop_index_request(index_name, obs_handler, options, **kwargs)))

    def get_index_deferred(self,
                           index_name: str,
                           options: GetSearchIndexOptions | None = None,
                           **kwargs: Any) -> Deferred[SearchIndex]:
        return self._deferred(Op.SearchIndexGet, lambda obs_handler: self.send_get_index(
            self.request_builder().build_get_index_request(index_name, obs_handler, options, **kwargs)))

    def get_all_indexes_deferred(self,
                                 options: GetAllSearchIndexesOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[SearchIndex]]:
        def start(obs_handler: ObservableRequestHandler | None) -> Coroutine[Any, Any, Iterable[SearchIndex]]:
            return self.send_get_all_indexes(
                self.request_builder().build_get_all_indexes_request(obs_handler, options, **kwargs))

        return self._deferred(Op.SearchIndexGetAll, start)

    def get_indexed_documents_count_deferred(self,
                                             index_name: str,
                                             options: GetSearchIndexedDocumentsCountOptions | None = None,
                                             **kwargs: Any) -> Deferred[int]:
        def start(obs_handler: ObservableRequestHandler | None) -> Coroutine[Any, Any, int]:
            return self.send_get_indexed_documents_count(
                self.request_builder().build_get_indexed_documents_count_request(index_name,
                                                                                 obs_handler,
                                                                                 options,
                                                                                 **kwargs))

        return self._deferred(Op.SearchIndexGetDocumentsCount, start)

    def pause_ingest_deferred(self,
                              index_name: str,
                              options: PauseIngestSearchIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexPauseIngest, lambda obs_handler: self.send_pause_ingest(
            self.request_builder().build_pause_ingest_request(index_name, obs_handler, options, **kwargs)))

    def resume_ingest_deferred(self,
                               index_name: str,
                               options: ResumeIngestSearchIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexResumeIngest, lambda obs_handler: self.send_resume_ingest(
            self.request_builder().build_resume_ingest_request(index_name, obs_handler, options, **kwargs)))

    def allow_querying_deferred(self,
                                index_name: str,
                                options: AllowQueryingSearchIndexOptions | None = None,
                                **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexAllowQuerying, lambda obs_handler: self.send_allow_querying(
            self.request_builder().build_allow_querying_request(index_name, obs_handler, options, **kwargs)))

    def disallow_querying_deferred(self,
                                   index_name: str,
                                   options: DisallowQueryingSearchIndexOptions | None = None,
                                   **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexDisallowQuerying, lambda obs_handler: self.send_disallow_querying(
            self.request_builder().build_disallow_querying_request(index_name, obs_handler, options, **kwargs)))

    def freeze_plan_deferred(self,
                             index_name: str,
                             options: FreezePlanSearchIndexOptions | None = None,
                             **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexFreezePlan, lambda obs_handler: self.send_freeze_plan(
            self.request_builder().build_freeze_plan_request(index_name, obs_handler, options, **kwargs)))

    def unfreeze_plan_deferred(self,
                               index_name: str,
                               options: UnfreezePlanSearchIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        return self._deferred(Op.SearchIndexUnfreezePlan, lambda obs_handler: self.send_unfreeze_plan(
            self.request_builder().build_unfreeze_plan_request(index_name, obs_handler, options, **kwargs)))

    def analyze_document_deferred(self,
                                  index_name: str,
                                  document: Any,
                                  options: AnalyzeDocumentSearchIndexOptions | None = None,
                                  **kwargs: Any) -> Deferred[dict[str, Any]]:
        return self._deferred(Op.SearchIndexAnalyzeDocument, lambda obs_handler: self.send_analyze_document(
            self.request_builder().build_analyze_document_request(index_name,
                                                                  document,
                                                                  obs_handler,
                                                                  options,
                                                                  **kwargs)))

    def get_index_stats_deferred(self,
                                 index_name: str,
                                 options: GetSearchIndexStatsOptions | None = None,
                                 **kwargs: Any) -> Deferred[dict[str, Any]]:
        raise unsupported('get_index_stats')

    def get_all_index_stats_deferred(self,
                                     options: GetAllSearchIndexStatsOptions | None = None,
                                     **kwargs: Any) -> Deferred[dict[str, Any]]:
        raise unsupported('get_all_index_stats')

    def _deferred(self,
                  op: Op,
                  start: Callable[[ObservableRequestHandler | None], Coroutine[Any, Any, T]]) -> Deferred[T]:
        """`start`'s coroutine as a Deferred, built at the call under a span for `op`."""
        obs_handler = ObservableRequestHandler.create_or_none(op, self.observability_instruments)
        return defer(self._cluster_impl.loop, obs_handler, lambda: start(obs_handler))
