#  Copyright 2016-2022. Couchbase, Inc.
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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.management.logic.search_index_logic import SearchIndex, SearchIndexManagerLogic

if TYPE_CHECKING:
    from couchbase.management.options import (AllowQueryingSearchIndexOptions,
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


class SearchIndexManager(SearchIndexManagerLogic):
    def __init__(self, connection, loop):
        super().__init__(connection)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Dict[str, Any]
                     ) -> Deferred[None]:

        return Deferred.fromFuture(super().upsert_index(index, *options, **kwargs))

    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Dict[str, Any]
                   ) -> Deferred[None]:

        return Deferred.fromFuture(super().drop_index(index_name, *options, **kwargs))

    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Dict[str, Any]
                  ) -> Deferred[SearchIndex]:

        return Deferred.fromFuture(super().get_index(index_name, *options, **kwargs))

    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[Iterable[SearchIndex]]:
        return Deferred.fromFuture(super().get_all_indexes(*options, **kwargs))

    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Dict[str, Any]
                                    ) -> Deferred[int]:
        return Deferred.fromFuture(super().get_indexed_documents_count(index_name, *options, **kwargs))

    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Dict[str, Any]
                     ) -> Deferred[None]:
        return Deferred.fromFuture(super().pause_ingest(index_name, *options, **kwargs))

    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Deferred[None]:
        return Deferred.fromFuture(super().resume_ingest(index_name, *options, **kwargs))

    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Dict[str, Any]
                       ) -> Deferred[None]:
        return Deferred.fromFuture(super().allow_querying(index_name, *options, **kwargs))

    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Dict[str, Any]
                          ) -> Deferred[None]:
        return Deferred.fromFuture(super().disallow_querying(index_name, *options, **kwargs))

    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Deferred[None]:
        return Deferred.fromFuture(super().freeze_plan(index_name, *options, **kwargs))

    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Deferred[None]:
        return Deferred.fromFuture(super().unfreeze_plan(index_name, *options, **kwargs))

    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Dict[str, Any]
                         ) -> Deferred[Dict[str, Any]]:
        return Deferred.fromFuture(super().analyze_document(index_name, document, *options, **kwargs))

    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[Dict[str, Any]]:
        return Deferred.fromFuture(super().get_index_stats(index_name, *options, **kwargs))

    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Dict[str, Any]
                            ) -> Deferred[Dict[str, Any]]:
        return Deferred.fromFuture(super().get_all_index_stats(*options, **kwargs))
