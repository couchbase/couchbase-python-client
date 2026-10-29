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

"""What a protocol's search index management impl must provide to the search index manager facades.

One impl serves both facades, `SearchIndexManager` and `ScopeSearchIndexManager`: the cluster impl builds it with no
scope context and the scope impl with its bucket and scope names.  The SDK imports this module only under
`TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

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


class SearchIndexMgmtInterface(Protocol):
    def upsert_index(self,
                     index: SearchIndex,
                     options: UpsertSearchIndexOptions | None = None,
                     **kwargs: Any) -> None: ...

    def drop_index(self,
                   index_name: str,
                   options: DropSearchIndexOptions | None = None,
                   **kwargs: Any) -> None: ...

    def get_index(self,
                  index_name: str,
                  options: GetSearchIndexOptions | None = None,
                  **kwargs: Any) -> SearchIndex: ...

    def get_all_indexes(self,
                        options: GetAllSearchIndexesOptions | None = None,
                        **kwargs: Any) -> Iterable[SearchIndex]: ...

    def get_indexed_documents_count(self,
                                    index_name: str,
                                    options: GetSearchIndexedDocumentsCountOptions | None = None,
                                    **kwargs: Any) -> int: ...

    def pause_ingest(self,
                     index_name: str,
                     options: PauseIngestSearchIndexOptions | None = None,
                     **kwargs: Any) -> None: ...

    def resume_ingest(self,
                      index_name: str,
                      options: ResumeIngestSearchIndexOptions | None = None,
                      **kwargs: Any) -> None: ...

    def allow_querying(self,
                       index_name: str,
                       options: AllowQueryingSearchIndexOptions | None = None,
                       **kwargs: Any) -> None: ...

    def disallow_querying(self,
                          index_name: str,
                          options: DisallowQueryingSearchIndexOptions | None = None,
                          **kwargs: Any) -> None: ...

    def freeze_plan(self,
                    index_name: str,
                    options: FreezePlanSearchIndexOptions | None = None,
                    **kwargs: Any) -> None: ...

    def unfreeze_plan(self,
                      index_name: str,
                      options: UnfreezePlanSearchIndexOptions | None = None,
                      **kwargs: Any) -> None: ...

    def analyze_document(self,
                         index_name: str,
                         document: Any,
                         options: AnalyzeDocumentSearchIndexOptions | None = None,
                         **kwargs: Any) -> dict[str, Any]: ...

    def get_index_stats(self,
                        index_name: str,
                        options: GetSearchIndexStatsOptions | None = None,
                        **kwargs: Any) -> dict[str, Any]: ...

    def get_all_index_stats(self,
                            options: GetAllSearchIndexStatsOptions | None = None,
                            **kwargs: Any) -> dict[str, Any]: ...


class AsyncSearchIndexMgmtInterface(Protocol):
    async def upsert_index(self,
                           index: SearchIndex,
                           options: UpsertSearchIndexOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def drop_index(self,
                         index_name: str,
                         options: DropSearchIndexOptions | None = None,
                         **kwargs: Any) -> None: ...

    async def get_index(self,
                        index_name: str,
                        options: GetSearchIndexOptions | None = None,
                        **kwargs: Any) -> SearchIndex: ...

    async def get_all_indexes(self,
                              options: GetAllSearchIndexesOptions | None = None,
                              **kwargs: Any) -> Iterable[SearchIndex]: ...

    async def get_indexed_documents_count(self,
                                          index_name: str,
                                          options: GetSearchIndexedDocumentsCountOptions | None = None,
                                          **kwargs: Any) -> int: ...

    async def pause_ingest(self,
                           index_name: str,
                           options: PauseIngestSearchIndexOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def resume_ingest(self,
                            index_name: str,
                            options: ResumeIngestSearchIndexOptions | None = None,
                            **kwargs: Any) -> None: ...

    async def allow_querying(self,
                             index_name: str,
                             options: AllowQueryingSearchIndexOptions | None = None,
                             **kwargs: Any) -> None: ...

    async def disallow_querying(self,
                                index_name: str,
                                options: DisallowQueryingSearchIndexOptions | None = None,
                                **kwargs: Any) -> None: ...

    async def freeze_plan(self,
                          index_name: str,
                          options: FreezePlanSearchIndexOptions | None = None,
                          **kwargs: Any) -> None: ...

    async def unfreeze_plan(self,
                            index_name: str,
                            options: UnfreezePlanSearchIndexOptions | None = None,
                            **kwargs: Any) -> None: ...

    async def analyze_document(self,
                               index_name: str,
                               document: Any,
                               options: AnalyzeDocumentSearchIndexOptions | None = None,
                               **kwargs: Any) -> dict[str, Any]: ...

    async def get_index_stats(self,
                              index_name: str,
                              options: GetSearchIndexStatsOptions | None = None,
                              **kwargs: Any) -> dict[str, Any]: ...

    async def get_all_index_stats(self,
                                  options: GetAllSearchIndexStatsOptions | None = None,
                                  **kwargs: Any) -> dict[str, Any]: ...


class TxSearchIndexMgmtInterface(Protocol):
    def upsert_index_deferred(self,
                              index: SearchIndex,
                              options: UpsertSearchIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def drop_index_deferred(self,
                            index_name: str,
                            options: DropSearchIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]: ...

    def get_index_deferred(self,
                           index_name: str,
                           options: GetSearchIndexOptions | None = None,
                           **kwargs: Any) -> Deferred[SearchIndex]: ...

    def get_all_indexes_deferred(self,
                                 options: GetAllSearchIndexesOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[SearchIndex]]: ...

    def get_indexed_documents_count_deferred(self,
                                             index_name: str,
                                             options: GetSearchIndexedDocumentsCountOptions | None = None,
                                             **kwargs: Any) -> Deferred[int]: ...

    def pause_ingest_deferred(self,
                              index_name: str,
                              options: PauseIngestSearchIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def resume_ingest_deferred(self,
                               index_name: str,
                               options: ResumeIngestSearchIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]: ...

    def allow_querying_deferred(self,
                                index_name: str,
                                options: AllowQueryingSearchIndexOptions | None = None,
                                **kwargs: Any) -> Deferred[None]: ...

    def disallow_querying_deferred(self,
                                   index_name: str,
                                   options: DisallowQueryingSearchIndexOptions | None = None,
                                   **kwargs: Any) -> Deferred[None]: ...

    def freeze_plan_deferred(self,
                             index_name: str,
                             options: FreezePlanSearchIndexOptions | None = None,
                             **kwargs: Any) -> Deferred[None]: ...

    def unfreeze_plan_deferred(self,
                               index_name: str,
                               options: UnfreezePlanSearchIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]: ...

    def analyze_document_deferred(self,
                                  index_name: str,
                                  document: Any,
                                  options: AnalyzeDocumentSearchIndexOptions | None = None,
                                  **kwargs: Any) -> Deferred[dict[str, Any]]: ...

    def get_index_stats_deferred(self,
                                 index_name: str,
                                 options: GetSearchIndexStatsOptions | None = None,
                                 **kwargs: Any) -> Deferred[dict[str, Any]]: ...

    def get_all_index_stats_deferred(self,
                                     options: GetAllSearchIndexStatsOptions | None = None,
                                     **kwargs: Any) -> Deferred[dict[str, Any]]: ...
