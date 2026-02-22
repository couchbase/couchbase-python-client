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

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import SearchIndexMgmtOperationType
from couchbase.management.logic.search_index_mgmt_types import SearchIndex
from txcouchbase.management.logic.search_index_mgmt_impl import TxSearchIndexMgmtImpl

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
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


class SearchIndexManager:

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._impl = TxSearchIndexMgmtImpl(client_adapter, observability_instruments)
        self._scope_context = None

    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Dict[str, Any]
                     ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexUpsert
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_upsert_index_request(index,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        *options,
                                                                        **kwargs)
            d = self._impl.upsert_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Dict[str, Any]
                   ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_index_request(index_name,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      *options,
                                                                      **kwargs)
            d = self._impl.drop_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Dict[str, Any]
                  ) -> Deferred[SearchIndex]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGet
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_index_request(index_name,
                                                                     self._scope_context,
                                                                     obs_handler,
                                                                     *options,
                                                                     **kwargs)
            d = self._impl.get_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[Iterable[SearchIndex]]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_indexes_request(self._scope_context,
                                                                           obs_handler,
                                                                           *options,
                                                                           **kwargs)
            d = self._impl.get_all_indexes_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Dict[str, Any]
                                    ) -> Deferred[int]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGetDocumentsCount
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                       self._scope_context,
                                                                                       obs_handler,
                                                                                       *options,
                                                                                       **kwargs)
            d = self._impl.get_indexed_documents_count_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Dict[str, Any]
                     ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlIngest
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=True)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_pause_ingest_request(index_name,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        *options,
                                                                        **kwargs)
            d = self._impl.pause_ingest_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlIngest
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=False)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_resume_ingest_request(index_name,
                                                                         self._scope_context,
                                                                         obs_handler,
                                                                         *options,
                                                                         **kwargs)
            d = self._impl.resume_ingest_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Dict[str, Any]
                       ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=True)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_allow_querying_request(index_name,
                                                                          self._scope_context,
                                                                          obs_handler,
                                                                          *options,
                                                                          **kwargs)
            d = self._impl.allow_querying_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Dict[str, Any]
                          ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=False)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_disallow_querying_request(index_name,
                                                                             self._scope_context,
                                                                             obs_handler,
                                                                             *options,
                                                                             **kwargs)
            d = self._impl.disallow_querying_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlPlanFreeze
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=True)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_freeze_plan_request(index_name,
                                                                       self._scope_context,
                                                                       obs_handler,
                                                                       *options,
                                                                       **kwargs)
            d = self._impl.freeze_plan_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlPlanFreeze
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=False)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_unfreeze_plan_request(index_name,
                                                                         self._scope_context,
                                                                         obs_handler,
                                                                         *options,
                                                                         **kwargs)
            d = self._impl.unfreeze_plan_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Dict[str, Any]
                         ) -> Deferred[Dict[str, Any]]:
        op_type = SearchIndexMgmtOperationType.SearchIndexAnalyzeDocument
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_analyze_document_request(index_name,
                                                                            document,
                                                                            self._scope_context,
                                                                            obs_handler,
                                                                            *options,
                                                                            **kwargs)
            d = self._impl.analyze_document_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[Dict[str, Any]]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGetStats
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_index_stats_request(index_name,
                                                                           self._scope_context,
                                                                           obs_handler,
                                                                           *options,
                                                                           **kwargs)
            d = self._impl.get_index_stats_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Dict[str, Any]
                            ) -> Deferred[Dict[str, Any]]:
        op_type = SearchIndexMgmtOperationType.SearchGetStats
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_index_stats_request(obs_handler, *options, **kwargs)
            d = self._impl.get_all_index_stats_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise


class ScopeSearchIndexManager:

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 bucket_name: str,
                 scope_name: str,
                 observability_instruments: ObservabilityInstruments) -> None:
        self._impl = TxSearchIndexMgmtImpl(client_adapter, observability_instruments)
        self._scope_context = bucket_name, scope_name

    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Dict[str, Any]
                     ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexUpsert
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_upsert_index_request(index,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        *options,
                                                                        **kwargs)
            d = self._impl.upsert_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Dict[str, Any]
                   ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexDrop
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_drop_index_request(index_name,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      *options,
                                                                      **kwargs)
            d = self._impl.drop_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Dict[str, Any]
                  ) -> Deferred[SearchIndex]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGet
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_index_request(index_name,
                                                                     self._scope_context,
                                                                     obs_handler,
                                                                     *options,
                                                                     **kwargs)
            d = self._impl.get_index_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[Iterable[SearchIndex]]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGetAll
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_indexes_request(self._scope_context,
                                                                           obs_handler,
                                                                           *options,
                                                                           **kwargs)
            d = self._impl.get_all_indexes_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Dict[str, Any]
                                    ) -> Deferred[int]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGetDocumentsCount
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                       self._scope_context,
                                                                                       obs_handler,
                                                                                       *options,
                                                                                       **kwargs)
            d = self._impl.get_indexed_documents_count_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Dict[str, Any]
                     ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlIngest
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=True)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_pause_ingest_request(index_name,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        *options,
                                                                        **kwargs)
            d = self._impl.pause_ingest_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlIngest
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=False)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_resume_ingest_request(index_name,
                                                                         self._scope_context,
                                                                         obs_handler,
                                                                         *options,
                                                                         **kwargs)
            d = self._impl.resume_ingest_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Dict[str, Any]
                       ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=True)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_allow_querying_request(index_name,
                                                                          self._scope_context,
                                                                          obs_handler,
                                                                          *options,
                                                                          **kwargs)
            d = self._impl.allow_querying_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Dict[str, Any]
                          ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=False)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_disallow_querying_request(index_name,
                                                                             self._scope_context,
                                                                             obs_handler,
                                                                             *options,
                                                                             **kwargs)
            d = self._impl.disallow_querying_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlPlanFreeze
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=True)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_freeze_plan_request(index_name,
                                                                       self._scope_context,
                                                                       obs_handler,
                                                                       *options,
                                                                       **kwargs)
            d = self._impl.freeze_plan_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> Deferred[None]:
        op_type = SearchIndexMgmtOperationType.SearchIndexControlPlanFreeze
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments, op_type_toggle=False)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_unfreeze_plan_request(index_name,
                                                                         self._scope_context,
                                                                         obs_handler,
                                                                         *options,
                                                                         **kwargs)
            d = self._impl.unfreeze_plan_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Dict[str, Any]
                         ) -> Deferred[Dict[str, Any]]:
        op_type = SearchIndexMgmtOperationType.SearchIndexAnalyzeDocument
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_analyze_document_request(index_name,
                                                                            document,
                                                                            self._scope_context,
                                                                            obs_handler,
                                                                            *options,
                                                                            **kwargs)
            d = self._impl.analyze_document_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Deferred[Dict[str, Any]]:
        op_type = SearchIndexMgmtOperationType.SearchIndexGetStats
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_index_stats_request(index_name,
                                                                           self._scope_context,
                                                                           obs_handler,
                                                                           *options,
                                                                           **kwargs)
            d = self._impl.get_index_stats_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise

    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Dict[str, Any]
                            ) -> Deferred[Dict[str, Any]]:
        op_type = SearchIndexMgmtOperationType.SearchGetStats
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        obs_handler.__enter__()
        try:
            req = self._impl.request_builder.build_get_all_index_stats_request(obs_handler, *options, **kwargs)
            d = self._impl.get_all_index_stats_deferred(req, obs_handler)
            d.addBoth(self._impl._finish_span, obs_handler)
            return d
        except Exception as e:
            obs_handler.__exit__(type(e), e, e.__traceback__)
            raise
