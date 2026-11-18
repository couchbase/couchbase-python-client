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

"""A search index manager's requests to the gateway, shared by the sync, asyncio and Twisted impls.

A scope's manager addresses every request to its bucket and scope; the cluster's names neither, and
the gateway reads that as an index of the cluster's own.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Protocol,
                    TypeVar)

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_args import merge
from couchbase.common.management._search_index_mgmt_args import (encoded_document,
                                                                 validated_index,
                                                                 validated_index_name)
from couchbase.protocol.couchbase2._core import timeout_seconds
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2
from couchbase.protocol.couchbase2._management._search_index_mgmt_types import (SearchIndexMgmtOptionsBlock,
                                                                                SearchIndexMgmtRequest,
                                                                                set_index_fields)

if TYPE_CHECKING:
    from couchbase.common._option_diagnostics import OptionsBlock
    from couchbase.common.management.search import SearchIndex
    from couchbase.protocol.couchbase2._core import ClusterCore


class _Scoped(Protocol):
    bucket_name: str
    scope_name: str


class _Named(_Scoped, Protocol):
    name: str


S = TypeVar('S', bound=_Scoped)
N = TypeVar('N', bound=_Named)


class SearchIndexMgmtRequestBuilder:

    def __init__(self, core: ClusterCore, scope_context: tuple[str, str] | None = None) -> None:
        self._core = core
        self._scope_context = scope_context

    def _final_args(self,
                    block: OptionsBlock,
                    options: Any,
                    kwargs: dict[str, Any],
                    obs_handler: ObservableRequestHandler | None) -> dict[str, Any]:
        final_args = merge(kwargs, options)
        self._core.validate_options(block, final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            bucket_name, scope_name = self._scope_context or (None, None)
            obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return final_args

    def _request(self,
                 message: S,
                 final_args: dict[str, Any],
                 obs_handler: ObservableRequestHandler | None) -> SearchIndexMgmtRequest[S]:
        if self._scope_context is not None:
            message.bucket_name, message.scope_name = self._scope_context
        return SearchIndexMgmtRequest(message,
                                      timeout_seconds(final_args, self._core.management_timeout),
                                      obs_handler=obs_handler)

    def _named(self,
               block: OptionsBlock,
               message: N,
               index_name: str,
               obs_handler: ObservableRequestHandler | None,
               options: Any,
               kwargs: dict[str, Any]) -> SearchIndexMgmtRequest[N]:
        final_args = self._final_args(block, options, kwargs, obs_handler)
        message.name = validated_index_name(index_name)
        return self._request(message, final_args, obs_handler)

    def build_upsert_index_request(
        self,
        index: SearchIndex,
        obs_handler: ObservableRequestHandler | None,
        options: Any = None,
        **kwargs: Any
    ) -> SearchIndexMgmtRequest[search_pb2.CreateIndexRequest | search_pb2.UpdateIndexRequest]:
        """A create, or an update when the index has a uuid.

        The uuid names a definition that already exists, and the server holds the update to it, so
        one written since is not overwritten.  The gateway refuses an update without one, and a
        create of a name that exists.
        """
        final_args = self._final_args(SearchIndexMgmtOptionsBlock.UpsertSearchIndex, options, kwargs, obs_handler)
        index = validated_index(index)
        message: search_pb2.CreateIndexRequest | search_pb2.UpdateIndexRequest
        if index.uuid:
            message = search_pb2.UpdateIndexRequest()
            set_index_fields(index, message.index)
            message.index.uuid = index.uuid
        else:
            message = search_pb2.CreateIndexRequest()
            set_index_fields(index, message)
        return self._request(message, final_args, obs_handler)

    def build_drop_index_request(self,
                                 index_name: str,
                                 obs_handler: ObservableRequestHandler | None,
                                 options: Any = None,
                                 **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.DeleteIndexRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.DropSearchIndex,
                           search_pb2.DeleteIndexRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_get_index_request(self,
                                index_name: str,
                                obs_handler: ObservableRequestHandler | None,
                                options: Any = None,
                                **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.GetIndexRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.GetSearchIndex,
                           search_pb2.GetIndexRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_get_all_indexes_request(self,
                                      obs_handler: ObservableRequestHandler | None,
                                      options: Any = None,
                                      **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.ListIndexesRequest]:
        final_args = self._final_args(SearchIndexMgmtOptionsBlock.GetAllSearchIndexes, options, kwargs, obs_handler)
        return self._request(search_pb2.ListIndexesRequest(), final_args, obs_handler)

    def build_get_indexed_documents_count_request(
        self,
        index_name: str,
        obs_handler: ObservableRequestHandler | None,
        options: Any = None,
        **kwargs: Any
    ) -> SearchIndexMgmtRequest[search_pb2.GetIndexedDocumentsCountRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.GetSearchIndexedDocumentsCount,
                           search_pb2.GetIndexedDocumentsCountRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_pause_ingest_request(self,
                                   index_name: str,
                                   obs_handler: ObservableRequestHandler | None,
                                   options: Any = None,
                                   **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.PauseIndexIngestRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.PauseIngestSearchIndex,
                           search_pb2.PauseIndexIngestRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_resume_ingest_request(self,
                                    index_name: str,
                                    obs_handler: ObservableRequestHandler | None,
                                    options: Any = None,
                                    **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.ResumeIndexIngestRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.ResumeIngestSearchIndex,
                           search_pb2.ResumeIndexIngestRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_allow_querying_request(self,
                                     index_name: str,
                                     obs_handler: ObservableRequestHandler | None,
                                     options: Any = None,
                                     **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.AllowIndexQueryingRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.AllowQueryingSearchIndex,
                           search_pb2.AllowIndexQueryingRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_disallow_querying_request(
        self,
        index_name: str,
        obs_handler: ObservableRequestHandler | None,
        options: Any = None,
        **kwargs: Any
    ) -> SearchIndexMgmtRequest[search_pb2.DisallowIndexQueryingRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.DisallowQueryingSearchIndex,
                           search_pb2.DisallowIndexQueryingRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_freeze_plan_request(self,
                                  index_name: str,
                                  obs_handler: ObservableRequestHandler | None,
                                  options: Any = None,
                                  **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.FreezeIndexPlanRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.FreezePlanSearchIndex,
                           search_pb2.FreezeIndexPlanRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_unfreeze_plan_request(self,
                                    index_name: str,
                                    obs_handler: ObservableRequestHandler | None,
                                    options: Any = None,
                                    **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.UnfreezeIndexPlanRequest]:
        return self._named(SearchIndexMgmtOptionsBlock.UnfreezePlanSearchIndex,
                           search_pb2.UnfreezeIndexPlanRequest(),
                           index_name,
                           obs_handler,
                           options,
                           kwargs)

    def build_analyze_document_request(self,
                                       index_name: str,
                                       document: Any,
                                       obs_handler: ObservableRequestHandler | None,
                                       options: Any = None,
                                       **kwargs: Any) -> SearchIndexMgmtRequest[search_pb2.AnalyzeDocumentRequest]:
        message = search_pb2.AnalyzeDocumentRequest(doc=encoded_document(document).encode())
        return self._named(SearchIndexMgmtOptionsBlock.AnalyzeDocumentSearchIndex,
                           message,
                           index_name,
                           obs_handler,
                           options,
                           kwargs)
