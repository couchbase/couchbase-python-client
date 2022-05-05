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

from typing import (Any,
                    Dict,
                    Iterable)

from couchbase.management.logic.search_index_logic import SearchIndex, SearchIndexManagerLogic
from couchbase.management.logic.wrappers import BlockingMgmtWrapper, ManagementType

# @TODO:  lets deprecate import of options from couchbase.management.search
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
    def __init__(self, connection):
        super().__init__(connection)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Dict[str, Any]
                     ) -> None:

        return super().upsert_index(index, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Dict[str, Any]
                   ) -> None:

        return super().drop_index(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(SearchIndex, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Dict[str, Any]
                  ) -> SearchIndex:

        return super().get_index(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(SearchIndex, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Iterable[SearchIndex]:
        return super().get_all_indexes(*options, **kwargs)

    @BlockingMgmtWrapper.block(int, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Dict[str, Any]
                                    ) -> int:
        return super().get_indexed_documents_count(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Dict[str, Any]
                     ) -> None:
        return super().pause_ingest(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> None:
        return super().resume_ingest(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Dict[str, Any]
                       ) -> None:
        return super().allow_querying(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Dict[str, Any]
                          ) -> None:
        return super().disallow_querying(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> None:
        return super().freeze_plan(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> None:
        return super().unfreeze_plan(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Dict[str, Any]
                         ) -> Dict[str, Any]:
        return super().analyze_document(index_name, document, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Dict[str, Any]:
        return super().get_index_stats(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Dict[str, Any]
                            ) -> Dict[str, Any]:
        return super().get_all_index_stats(*options, **kwargs)
