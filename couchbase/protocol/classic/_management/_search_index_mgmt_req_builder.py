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

import json
from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._utils import is_null_or_empty
from couchbase.common.exceptions import FeatureUnavailableException, InvalidArgumentException
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
from couchbase.common.option_enums import OptionValidationMode
from couchbase.protocol.classic._management._search_index_mgmt_types import (SEARCH_INDEX_MGMT_ERROR_MAP,
                                                                             AllowQueryingRequest,
                                                                             AnalyzeDocumentRequest,
                                                                             DisallowQueryingRequest,
                                                                             DropIndexRequest,
                                                                             FreezePlanRequest,
                                                                             GetAllIndexesRequest,
                                                                             GetAllIndexStatsRequest,
                                                                             GetIndexedDocumentsCountRequest,
                                                                             GetIndexRequest,
                                                                             GetIndexStatsRequest,
                                                                             PauseIngestRequest,
                                                                             ResumeIngestRequest,
                                                                             SearchIndexMgmtOptionsBlock,
                                                                             UnfreezePlanRequest,
                                                                             UpsertIndexRequest)
from couchbase.protocol.classic._wire import forward_args


class SearchIndexMgmtRequestBuilder:

    def __init__(self, option_validation: OptionValidationMode | None = None) -> None:
        self._error_map = SEARCH_INDEX_MGMT_ERROR_MAP
        self._option_validation = option_validation

    def _get_scope_context(self,
                           scope_context: tuple[str, str] | None = None) -> tuple[str | None, str | None]:
        if scope_context is not None:
            return scope_context[0], scope_context[1]

        return None, None

    def _get_valid_document(self, document: Any) -> str:
        if not document:
            raise InvalidArgumentException('Expected a non-empty document to analyze.')
        json_doc = None
        try:
            json_doc = json.dumps(document)
        except Exception as ex:
            raise InvalidArgumentException('Cannot convert doc to json to analyze') from ex
        return json_doc

    def _validate_index(self, index: object) -> SearchIndex:
        if not index:
            raise InvalidArgumentException('Expected index to not be None')
        if not isinstance(index, SearchIndex):
            raise InvalidArgumentException('Expected index to be a SearchIndex')
        if not index.is_valid():
            raise InvalidArgumentException('Index must have name, source set')
        return index

    def _validate_index_name(self, index_name: object) -> str:
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided.')

        if is_null_or_empty(index_name):
            raise InvalidArgumentException('Expected non-empty index_name')

        return index_name

    def build_allow_querying_request(self,
                                     index_name: str,
                                     scope_context: tuple[str, str] | None,
                                     obs_handler: ObservableRequestHandler,
                                     options: AllowQueryingSearchIndexOptions | None = None,
                                     **kwargs: object) -> AllowQueryingRequest:
        final_args = forward_args(kwargs, options, block=SearchIndexMgmtOptionsBlock.AllowQueryingSearchIndex,
                                  mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return AllowQueryingRequest(self._error_map,
                                    index_name=self._validate_index_name(index_name),
                                    allow=True,
                                    bucket_name=bucket_name,
                                    scope_name=scope_name,
                                    timeout=final_args.pop('timeout', None))

    def build_analyze_document_request(self,
                                       index_name: str,
                                       document: Any,
                                       scope_context: tuple[str, str] | None,
                                       obs_handler: ObservableRequestHandler,
                                       options: AnalyzeDocumentSearchIndexOptions | None = None,
                                       **kwargs: object) -> AnalyzeDocumentRequest:
        final_args = forward_args(kwargs, options, block=SearchIndexMgmtOptionsBlock.AnalyzeDocumentSearchIndex,
                                  mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        valid_index_name = self._validate_index_name(index_name)
        return AnalyzeDocumentRequest(self._error_map,
                                      index_name=valid_index_name,
                                      encoded_document=self._get_valid_document(document),
                                      bucket_name=bucket_name,
                                      scope_name=scope_name,
                                      timeout=final_args.pop('timeout', None))

    def build_drop_index_request(self,
                                 index_name: str,
                                 scope_context: tuple[str, str] | None,
                                 obs_handler: ObservableRequestHandler,
                                 options: DropSearchIndexOptions | None = None,
                                 **kwargs: object) -> DropIndexRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.DropSearchIndex, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return DropIndexRequest(self._error_map,
                                index_name=self._validate_index_name(index_name),
                                bucket_name=bucket_name,
                                scope_name=scope_name,
                                timeout=final_args.pop('timeout', None))

    def build_disallow_querying_request(self,
                                        index_name: str,
                                        scope_context: tuple[str, str] | None,
                                        obs_handler: ObservableRequestHandler,
                                        options: DisallowQueryingSearchIndexOptions | None = None,
                                        **kwargs: object) -> DisallowQueryingRequest:
        final_args = forward_args(kwargs, options, block=SearchIndexMgmtOptionsBlock.DisallowQueryingSearchIndex,
                                  mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return DisallowQueryingRequest(self._error_map,
                                       index_name=self._validate_index_name(index_name),
                                       allow=False,
                                       bucket_name=bucket_name,
                                       scope_name=scope_name,
                                       timeout=final_args.pop('timeout', None))

    def build_freeze_plan_request(self,
                                  index_name: str,
                                  scope_context: tuple[str, str] | None,
                                  obs_handler: ObservableRequestHandler,
                                  options: FreezePlanSearchIndexOptions | None = None,
                                  **kwargs: object) -> FreezePlanRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.FreezePlanSearchIndex, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return FreezePlanRequest(self._error_map,
                                 index_name=self._validate_index_name(index_name),
                                 freeze=True,
                                 bucket_name=bucket_name,
                                 scope_name=scope_name,
                                 timeout=final_args.pop('timeout', None))

    def build_get_all_indexes_request(self,
                                      scope_context: tuple[str, str] | None,
                                      obs_handler: ObservableRequestHandler,
                                      options: GetAllSearchIndexesOptions | None = None,
                                      **kwargs: object) -> GetAllIndexesRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.GetAllSearchIndexes, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return GetAllIndexesRequest(self._error_map,
                                    bucket_name=bucket_name,
                                    scope_name=scope_name,
                                    timeout=final_args.pop('timeout', None))

    def build_get_all_index_stats_request(self,
                                          obs_handler: ObservableRequestHandler,
                                          options: GetAllSearchIndexStatsOptions | None = None,
                                          **kwargs: object) -> GetAllIndexStatsRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.GetAllSearchIndexStats, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=None, scope_name=None)
        return GetAllIndexStatsRequest(self._error_map, timeout=final_args.pop('timeout', None))

    def build_get_indexed_documents_count_request(self,
                                                  index_name: str,
                                                  scope_context: tuple[str, str] | None,
                                                  obs_handler: ObservableRequestHandler,
                                                  options: GetSearchIndexedDocumentsCountOptions | None = None,
                                                  **kwargs: object) -> GetIndexedDocumentsCountRequest:
        final_args = forward_args(kwargs, options, block=SearchIndexMgmtOptionsBlock.GetSearchIndexedDocumentsCount,
                                  mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return GetIndexedDocumentsCountRequest(self._error_map,
                                               index_name=self._validate_index_name(index_name),
                                               bucket_name=bucket_name,
                                               scope_name=scope_name,
                                               timeout=final_args.pop('timeout', None))

    def build_get_index_request(self,
                                index_name: str,
                                scope_context: tuple[str, str] | None,
                                obs_handler: ObservableRequestHandler,
                                options: GetSearchIndexOptions | None = None,
                                **kwargs: object) -> GetIndexRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.GetSearchIndex, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return GetIndexRequest(self._error_map,
                               index_name=self._validate_index_name(index_name),
                               bucket_name=bucket_name,
                               scope_name=scope_name,
                               timeout=final_args.pop('timeout', None))

    def build_get_index_stats_request(self,
                                      index_name: str,
                                      scope_context: tuple[str, str] | None,
                                      obs_handler: ObservableRequestHandler,
                                      options: GetSearchIndexStatsOptions | None = None,
                                      **kwargs: object) -> GetIndexStatsRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.GetSearchIndexStats, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        if bucket_name is not None and scope_name is not None:
            raise FeatureUnavailableException(('get_index_stats unavailable at scope level. '
                                               'Use cluster.searchIndexes().get_index_stats(...) instead.'))

        return GetIndexStatsRequest(self._error_map,
                                    index_name=self._validate_index_name(index_name),
                                    timeout=final_args.pop('timeout', None))

    def build_pause_ingest_request(self,
                                   index_name: str,
                                   scope_context: tuple[str, str] | None,
                                   obs_handler: ObservableRequestHandler,
                                   options: PauseIngestSearchIndexOptions | None = None,
                                   **kwargs: object) -> PauseIngestRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.PauseIngestSearchIndex, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return PauseIngestRequest(self._error_map,
                                  index_name=self._validate_index_name(index_name),
                                  pause=True,
                                  bucket_name=bucket_name,
                                  scope_name=scope_name,
                                  timeout=final_args.pop('timeout', None))

    def build_resume_ingest_request(self,
                                    index_name: str,
                                    scope_context: tuple[str, str] | None,
                                    obs_handler: ObservableRequestHandler,
                                    options: ResumeIngestSearchIndexOptions | None = None,
                                    **kwargs: object) -> ResumeIngestRequest:
        final_args = forward_args(kwargs, options, block=SearchIndexMgmtOptionsBlock.ResumeIngestSearchIndex,
                                  mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return ResumeIngestRequest(self._error_map,
                                   index_name=self._validate_index_name(index_name),
                                   pause=False,
                                   bucket_name=bucket_name,
                                   scope_name=scope_name,
                                   timeout=final_args.pop('timeout', None))

    def build_unfreeze_plan_request(self,
                                    index_name: str,
                                    scope_context: tuple[str, str] | None,
                                    obs_handler: ObservableRequestHandler,
                                    options: UnfreezePlanSearchIndexOptions | None = None,
                                    **kwargs: object) -> UnfreezePlanRequest:
        final_args = forward_args(kwargs, options, block=SearchIndexMgmtOptionsBlock.UnfreezePlanSearchIndex,
                                  mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return UnfreezePlanRequest(self._error_map,
                                   index_name=self._validate_index_name(index_name),
                                   freeze=False,
                                   bucket_name=bucket_name,
                                   scope_name=scope_name,
                                   timeout=final_args.pop('timeout', None))

    def build_upsert_index_request(self,
                                   index: SearchIndex,
                                   scope_context: tuple[str, str] | None,
                                   obs_handler: ObservableRequestHandler,
                                   options: UpsertSearchIndexOptions | None = None,
                                   **kwargs: object) -> UpsertIndexRequest:
        final_args = forward_args(
            kwargs, options, block=SearchIndexMgmtOptionsBlock.UpsertSearchIndex, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return UpsertIndexRequest(self._error_map,
                                  index=self._validate_index(index).as_dict(),
                                  bucket_name=bucket_name,
                                  scope_name=scope_name,
                                  timeout=final_args.pop('timeout', None))
