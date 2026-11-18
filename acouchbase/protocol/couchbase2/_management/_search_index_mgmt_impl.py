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

"""An asyncio search index manager over the gateway's search admin service.

Each operation's request is built, then sent, so Twisted can build at the call and send in a coroutine.
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
from couchbase.protocol.couchbase2._driver import call_unary_async
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2
from couchbase.protocol.couchbase2._management._search_index_mgmt_req_builder import SearchIndexMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._search_index_mgmt_types import (analysis,
                                                                                found_index,
                                                                                search_index)

if TYPE_CHECKING:
    import grpc.aio

    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
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
    from couchbase.protocol.couchbase2._management._search_index_mgmt_types import SearchIndexMgmtRequest

Req = TypeVar('Req')
Resp = TypeVar('Resp')


class AsyncSearchIndexMgmtImpl:

    def __init__(self, cluster_impl: AsyncClusterImpl, scope_context: tuple[str, str] | None = None) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = SearchIndexMgmtRequestBuilder(cluster_impl.core, scope_context)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def request_builder(self) -> SearchIndexMgmtRequestBuilder:
        """**INTERNAL** The builder, once the cluster is known to be open."""
        self._cluster_impl.core.ensure_open()
        return self._request_builder

    async def upsert_index(self,
                           index: SearchIndex,
                           options: UpsertSearchIndexOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexUpsert, self.observability_instruments) as obs_handler:
            await self.send_upsert_index(
                self.request_builder().build_upsert_index_request(index, obs_handler, options, **kwargs))

    async def drop_index(self,
                         index_name: str,
                         options: DropSearchIndexOptions | None = None,
                         **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexDrop, self.observability_instruments) as obs_handler:
            await self.send_drop_index(
                self.request_builder().build_drop_index_request(index_name, obs_handler, options, **kwargs))

    async def get_index(self,
                        index_name: str,
                        options: GetSearchIndexOptions | None = None,
                        **kwargs: Any) -> SearchIndex:
        async with ObservableRequestHandler.create(Op.SearchIndexGet, self.observability_instruments) as obs_handler:
            return await self.send_get_index(
                self.request_builder().build_get_index_request(index_name, obs_handler, options, **kwargs))

    async def get_all_indexes(self,
                              options: GetAllSearchIndexesOptions | None = None,
                              **kwargs: Any) -> Iterable[SearchIndex]:
        async with ObservableRequestHandler.create(Op.SearchIndexGetAll, self.observability_instruments) as obs_handler:
            return await self.send_get_all_indexes(
                self.request_builder().build_get_all_indexes_request(obs_handler, options, **kwargs))

    async def get_indexed_documents_count(self,
                                          index_name: str,
                                          options: GetSearchIndexedDocumentsCountOptions | None = None,
                                          **kwargs: Any) -> int:
        async with ObservableRequestHandler.create(Op.SearchIndexGetDocumentsCount,
                                                   self.observability_instruments) as obs_handler:
            return await self.send_get_indexed_documents_count(
                self.request_builder().build_get_indexed_documents_count_request(index_name,
                                                                                 obs_handler,
                                                                                 options,
                                                                                 **kwargs))

    async def pause_ingest(self,
                           index_name: str,
                           options: PauseIngestSearchIndexOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexPauseIngest,
                                                   self.observability_instruments) as obs_handler:
            await self.send_pause_ingest(
                self.request_builder().build_pause_ingest_request(index_name, obs_handler, options, **kwargs))

    async def resume_ingest(self,
                            index_name: str,
                            options: ResumeIngestSearchIndexOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexResumeIngest,
                                                   self.observability_instruments) as obs_handler:
            await self.send_resume_ingest(
                self.request_builder().build_resume_ingest_request(index_name, obs_handler, options, **kwargs))

    async def allow_querying(self,
                             index_name: str,
                             options: AllowQueryingSearchIndexOptions | None = None,
                             **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexAllowQuerying,
                                                   self.observability_instruments) as obs_handler:
            await self.send_allow_querying(
                self.request_builder().build_allow_querying_request(index_name, obs_handler, options, **kwargs))

    async def disallow_querying(self,
                                index_name: str,
                                options: DisallowQueryingSearchIndexOptions | None = None,
                                **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexDisallowQuerying,
                                                   self.observability_instruments) as obs_handler:
            await self.send_disallow_querying(
                self.request_builder().build_disallow_querying_request(index_name, obs_handler, options, **kwargs))

    async def freeze_plan(self,
                          index_name: str,
                          options: FreezePlanSearchIndexOptions | None = None,
                          **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexFreezePlan,
                                                   self.observability_instruments) as obs_handler:
            await self.send_freeze_plan(
                self.request_builder().build_freeze_plan_request(index_name, obs_handler, options, **kwargs))

    async def unfreeze_plan(self,
                            index_name: str,
                            options: UnfreezePlanSearchIndexOptions | None = None,
                            **kwargs: Any) -> None:
        async with ObservableRequestHandler.create(Op.SearchIndexUnfreezePlan,
                                                   self.observability_instruments) as obs_handler:
            await self.send_unfreeze_plan(
                self.request_builder().build_unfreeze_plan_request(index_name, obs_handler, options, **kwargs))

    async def analyze_document(self,
                               index_name: str,
                               document: Any,
                               options: AnalyzeDocumentSearchIndexOptions | None = None,
                               **kwargs: Any) -> dict[str, Any]:
        async with ObservableRequestHandler.create(Op.SearchIndexAnalyzeDocument,
                                                   self.observability_instruments) as obs_handler:
            return await self.send_analyze_document(
                self.request_builder().build_analyze_document_request(index_name,
                                                                      document,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs))

    async def get_index_stats(self,
                              index_name: str,
                              options: GetSearchIndexStatsOptions | None = None,
                              **kwargs: Any) -> dict[str, Any]:
        raise unsupported('get_index_stats')

    async def get_all_index_stats(self,
                                  options: GetAllSearchIndexStatsOptions | None = None,
                                  **kwargs: Any) -> dict[str, Any]:
        raise unsupported('get_all_index_stats')

    async def send_upsert_index(
        self,
        req: SearchIndexMgmtRequest[search_pb2.CreateIndexRequest | search_pb2.UpdateIndexRequest]
    ) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        if isinstance(req.message, search_pb2.UpdateIndexRequest):
            await self._send(Op.SearchIndexUpsert,
                             stub.UpdateIndex,
                             req.message,
                             req.timeout,
                             req.obs_handler)
        else:
            await self._send(Op.SearchIndexUpsert,
                             stub.CreateIndex,
                             req.message,
                             req.timeout,
                             req.obs_handler)

    async def send_drop_index(self, req: SearchIndexMgmtRequest[search_pb2.DeleteIndexRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        await self._send_req(Op.SearchIndexDrop, stub.DeleteIndex, req)

    async def send_get_index(self, req: SearchIndexMgmtRequest[search_pb2.GetIndexRequest]) -> SearchIndex:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        return found_index(req.message.name,
                           await self._send_req(Op.SearchIndexGet, stub.GetIndex, req))

    async def send_get_all_indexes(self,
                                   req: SearchIndexMgmtRequest[search_pb2.ListIndexesRequest]) -> list[SearchIndex]:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        response = await self._send_req(Op.SearchIndexGetAll, stub.ListIndexes, req)
        return [search_index(index) for index in response.indexes]

    async def send_get_indexed_documents_count(
        self,
        req: SearchIndexMgmtRequest[search_pb2.GetIndexedDocumentsCountRequest]
    ) -> int:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        response = await self._send_req(Op.SearchIndexGetDocumentsCount,
                                        stub.GetIndexedDocumentsCount,
                                        req)
        return response.count

    async def send_pause_ingest(self, req: SearchIndexMgmtRequest[search_pb2.PauseIndexIngestRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        await self._send_req(Op.SearchIndexPauseIngest, stub.PauseIndexIngest, req)

    async def send_resume_ingest(self, req: SearchIndexMgmtRequest[search_pb2.ResumeIndexIngestRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        await self._send_req(Op.SearchIndexResumeIngest, stub.ResumeIndexIngest, req)

    async def send_allow_querying(self, req: SearchIndexMgmtRequest[search_pb2.AllowIndexQueryingRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        await self._send_req(Op.SearchIndexAllowQuerying, stub.AllowIndexQuerying, req)

    async def send_disallow_querying(self,
                                     req: SearchIndexMgmtRequest[search_pb2.DisallowIndexQueryingRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        await self._send_req(Op.SearchIndexDisallowQuerying, stub.DisallowIndexQuerying, req)

    async def send_freeze_plan(self, req: SearchIndexMgmtRequest[search_pb2.FreezeIndexPlanRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        await self._send_req(Op.SearchIndexFreezePlan, stub.FreezeIndexPlan, req)

    async def send_unfreeze_plan(self, req: SearchIndexMgmtRequest[search_pb2.UnfreezeIndexPlanRequest]) -> None:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        await self._send_req(Op.SearchIndexUnfreezePlan, stub.UnfreezeIndexPlan, req)

    async def send_analyze_document(self,
                                    req: SearchIndexMgmtRequest[search_pb2.AnalyzeDocumentRequest]) -> dict[str, Any]:
        """**INTERNAL**"""
        stub = await self._cluster_impl.search_admin()
        return analysis(await self._send_req(Op.SearchIndexAnalyzeDocument, stub.AnalyzeDocument, req))

    async def _send_req(self,
                        op: Op,
                        rpc: grpc.aio.UnaryUnaryMultiCallable[Req, Resp],
                        req: SearchIndexMgmtRequest[Req]) -> Resp:
        return await self._send(op, rpc, req.message, req.timeout, req.obs_handler)

    async def _send(self,
                    op: Op,
                    rpc: grpc.aio.UnaryUnaryMultiCallable[Req, Resp],
                    message: Req,
                    timeout: float,
                    obs_handler: ObservableRequestHandler | None) -> Resp:
        core = self._cluster_impl.core
        ctx = core.context(op, timeout, obs_handler=obs_handler)
        return await call_unary_async(rpc, message, ctx, core.metadata)
