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

"""A sync search index manager over the gateway's search admin service.

The service has no index statistics, so both stats calls raise `FeatureUnavailableException`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._core import unsupported
from couchbase.protocol.couchbase2._driver import call_unary
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2
from couchbase.protocol.couchbase2._management._search_index_mgmt_req_builder import SearchIndexMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._search_index_mgmt_types import (analysis,
                                                                                found_index,
                                                                                search_index)

if TYPE_CHECKING:
    import grpc

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
    from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
    from couchbase.protocol.couchbase2._management._search_index_mgmt_types import SearchIndexMgmtRequest

Req = TypeVar('Req')
Resp = TypeVar('Resp')


class SearchIndexMgmtImpl:

    def __init__(self, cluster_impl: ClusterImpl, scope_context: tuple[str, str] | None = None) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = SearchIndexMgmtRequestBuilder(cluster_impl.core, scope_context)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def upsert_index(self,
                     index: SearchIndex,
                     options: UpsertSearchIndexOptions | None = None,
                     **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexUpsert, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_index_request(index, obs_handler, options, **kwargs)
            admin = self._cluster_impl.search_admin
            if isinstance(req.message, search_pb2.UpdateIndexRequest):
                self._send(Op.SearchIndexUpsert,
                           admin.UpdateIndex,
                           req.message,
                           req.timeout,
                           req.obs_handler)
            else:
                self._send(Op.SearchIndexUpsert,
                           admin.CreateIndex,
                           req.message,
                           req.timeout,
                           req.obs_handler)

    def drop_index(self,
                   index_name: str,
                   options: DropSearchIndexOptions | None = None,
                   **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexDrop, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_index_request(index_name, obs_handler, options, **kwargs)
            self._send_req(Op.SearchIndexDrop, self._cluster_impl.search_admin.DeleteIndex, req)

    def get_index(self,
                  index_name: str,
                  options: GetSearchIndexOptions | None = None,
                  **kwargs: Any) -> SearchIndex:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexGet, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_index_request(index_name, obs_handler, options, **kwargs)
            return found_index(req.message.name,
                               self._send_req(Op.SearchIndexGet,
                                              self._cluster_impl.search_admin.GetIndex,
                                              req))

    def get_all_indexes(self,
                        options: GetAllSearchIndexesOptions | None = None,
                        **kwargs: Any) -> Iterable[SearchIndex]:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexGetAll, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_indexes_request(obs_handler, options, **kwargs)
            response = self._send_req(Op.SearchIndexGetAll,
                                      self._cluster_impl.search_admin.ListIndexes,
                                      req)
            return [search_index(index) for index in response.indexes]

    def get_indexed_documents_count(self,
                                    index_name: str,
                                    options: GetSearchIndexedDocumentsCountOptions | None = None,
                                    **kwargs: Any) -> int:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexGetDocumentsCount,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                  obs_handler,
                                                                                  options,
                                                                                  **kwargs)
            return self._send_req(Op.SearchIndexGetDocumentsCount,
                                  self._cluster_impl.search_admin.GetIndexedDocumentsCount,
                                  req).count

    def pause_ingest(self,
                     index_name: str,
                     options: PauseIngestSearchIndexOptions | None = None,
                     **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexPauseIngest, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_pause_ingest_request(index_name, obs_handler, options, **kwargs)
            self._send_req(Op.SearchIndexPauseIngest,
                           self._cluster_impl.search_admin.PauseIndexIngest,
                           req)

    def resume_ingest(self,
                      index_name: str,
                      options: ResumeIngestSearchIndexOptions | None = None,
                      **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexResumeIngest,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_resume_ingest_request(index_name, obs_handler, options, **kwargs)
            self._send_req(Op.SearchIndexResumeIngest,
                           self._cluster_impl.search_admin.ResumeIndexIngest,
                           req)

    def allow_querying(self,
                       index_name: str,
                       options: AllowQueryingSearchIndexOptions | None = None,
                       **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexAllowQuerying,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_allow_querying_request(index_name, obs_handler, options, **kwargs)
            self._send_req(Op.SearchIndexAllowQuerying,
                           self._cluster_impl.search_admin.AllowIndexQuerying,
                           req)

    def disallow_querying(self,
                          index_name: str,
                          options: DisallowQueryingSearchIndexOptions | None = None,
                          **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexDisallowQuerying,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_disallow_querying_request(index_name, obs_handler, options, **kwargs)
            self._send_req(Op.SearchIndexDisallowQuerying,
                           self._cluster_impl.search_admin.DisallowIndexQuerying,
                           req)

    def freeze_plan(self,
                    index_name: str,
                    options: FreezePlanSearchIndexOptions | None = None,
                    **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexFreezePlan, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_freeze_plan_request(index_name, obs_handler, options, **kwargs)
            self._send_req(Op.SearchIndexFreezePlan,
                           self._cluster_impl.search_admin.FreezeIndexPlan,
                           req)

    def unfreeze_plan(self,
                      index_name: str,
                      options: UnfreezePlanSearchIndexOptions | None = None,
                      **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexUnfreezePlan,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_unfreeze_plan_request(index_name, obs_handler, options, **kwargs)
            self._send_req(Op.SearchIndexUnfreezePlan,
                           self._cluster_impl.search_admin.UnfreezeIndexPlan,
                           req)

    def analyze_document(self,
                         index_name: str,
                         document: Any,
                         options: AnalyzeDocumentSearchIndexOptions | None = None,
                         **kwargs: Any) -> dict[str, Any]:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.SearchIndexAnalyzeDocument,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_analyze_document_request(index_name,
                                                                       document,
                                                                       obs_handler,
                                                                       options,
                                                                       **kwargs)
            return analysis(self._send_req(Op.SearchIndexAnalyzeDocument,
                                           self._cluster_impl.search_admin.AnalyzeDocument,
                                           req))

    def get_index_stats(self,
                        index_name: str,
                        options: GetSearchIndexStatsOptions | None = None,
                        **kwargs: Any) -> dict[str, Any]:
        raise unsupported('get_index_stats')

    def get_all_index_stats(self,
                            options: GetAllSearchIndexStatsOptions | None = None,
                            **kwargs: Any) -> dict[str, Any]:
        raise unsupported('get_all_index_stats')

    def _send_req(self,
                  op: Op,
                  rpc: grpc.UnaryUnaryMultiCallable[Req, Resp],
                  req: SearchIndexMgmtRequest[Req]) -> Resp:
        return self._send(op, rpc, req.message, req.timeout, req.obs_handler)

    def _send(self,
              op: Op,
              rpc: grpc.UnaryUnaryMultiCallable[Req, Resp],
              message: Req,
              timeout: float,
              obs_handler: ObservableRequestHandler | None) -> Resp:
        core = self._cluster_impl.core
        ctx = core.context(op, timeout, obs_handler=obs_handler)
        return call_unary(rpc, message, ctx, core.metadata)
