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

import json
from typing import (TYPE_CHECKING,
                    Any,
                    Optional,
                    Tuple)

from couchbase._utils import is_null_or_empty
from couchbase.exceptions import FeatureUnavailableException, InvalidArgumentException
from couchbase.management.logic.search_index_mgmt_types import (SEARCH_INDEX_MGMT_ERROR_MAP,
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
                                                                UnfreezePlanRequest,
                                                                UpsertIndexRequest)
from couchbase.options import forward_args
from couchbase.pycbc_core import mgmt_operations, search_index_mgmt_operations

if TYPE_CHECKING:
    from couchbase.management.logic.search_index_mgmt_types import SearchIndex


class SearchIndexMgmtRequestBuilder:

    def __init__(self) -> None:
        self._error_map = SEARCH_INDEX_MGMT_ERROR_MAP

    def _get_scope_context(self,
                           scope_context: Optional[Tuple[str, str]] = None) -> Tuple[Optional[str], Optional[str]]:
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

    def _validate_index(self, index: SearchIndex) -> None:
        if not index:
            raise InvalidArgumentException('Expected index to not be None')
        else:
            if not index.is_valid():
                raise InvalidArgumentException('Index must have name, source set')

    def _validate_index_name(self, index_name: str) -> None:
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided.')

        if is_null_or_empty(index_name):
            raise InvalidArgumentException('Expected non-empty index_name')

    def build_allow_querying_request(self,
                                     index_name: str,
                                     scope_context: Optional[Tuple[str, str]] = None,
                                     *options: object,
                                     **kwargs: object) -> AllowQueryingRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = AllowQueryingRequest(self._error_map,
                                   mgmt_operations.SEARCH_INDEX.value,
                                   search_index_mgmt_operations.CONTROL_QUERY.value,
                                   index_name=index_name,
                                   allow=True,
                                   bucket_name=bucket_name,
                                   scope_name=scope_name,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_analyze_document_request(self,
                                       index_name: str,
                                       document: Any,
                                       scope_context: Optional[Tuple[str, str]] = None,
                                       *options: object,
                                       **kwargs: object) -> AnalyzeDocumentRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        encoded_document = self._get_valid_document(document)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = AnalyzeDocumentRequest(self._error_map,
                                     mgmt_operations.SEARCH_INDEX.value,
                                     search_index_mgmt_operations.ANALYZE_DOCUMENT.value,
                                     index_name=index_name,
                                     encoded_document=encoded_document,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_index_request(self,
                                 index_name: str,
                                 scope_context: Optional[Tuple[str, str]] = None,
                                 *options: object,
                                 **kwargs: object) -> DropIndexRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = DropIndexRequest(self._error_map,
                               mgmt_operations.SEARCH_INDEX.value,
                               search_index_mgmt_operations.DROP_INDEX.value,
                               index_name=index_name,
                               bucket_name=bucket_name,
                               scope_name=scope_name,
                               **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_disallow_querying_request(self,
                                        index_name: str,
                                        scope_context: Optional[Tuple[str, str]] = None,
                                        *options: object,
                                        **kwargs: object) -> DisallowQueryingRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = DisallowQueryingRequest(self._error_map,
                                      mgmt_operations.SEARCH_INDEX.value,
                                      search_index_mgmt_operations.CONTROL_QUERY.value,
                                      index_name=index_name,
                                      allow=False,
                                      bucket_name=bucket_name,
                                      scope_name=scope_name,
                                      **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_freeze_plan_request(self,
                                  index_name: str,
                                  scope_context: Optional[Tuple[str, str]] = None,
                                  *options: object,
                                  **kwargs: object) -> FreezePlanRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = FreezePlanRequest(self._error_map,
                                mgmt_operations.SEARCH_INDEX.value,
                                search_index_mgmt_operations.FREEZE_PLAN.value,
                                index_name=index_name,
                                freeze=True,
                                bucket_name=bucket_name,
                                scope_name=scope_name,
                                **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_indexes_request(self,
                                      scope_context: Optional[Tuple[str, str]] = None,
                                      *options: object,
                                      **kwargs: object) -> GetAllIndexesRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = GetAllIndexesRequest(self._error_map,
                                   mgmt_operations.SEARCH_INDEX.value,
                                   search_index_mgmt_operations.GET_ALL_INDEXES.value,
                                   bucket_name=bucket_name,
                                   scope_name=scope_name,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_index_stats_request(self, *options: object, **kwargs: object) -> GetAllIndexStatsRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetAllIndexStatsRequest(self._error_map,
                                      mgmt_operations.SEARCH_INDEX.value,
                                      search_index_mgmt_operations.GET_ALL_STATS.value,
                                      **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_indexed_documents_count_request(self,
                                                  index_name: str,
                                                  scope_context: Optional[Tuple[str, str]] = None,
                                                  *options: object,
                                                  **kwargs: object) -> GetIndexedDocumentsCountRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = GetIndexedDocumentsCountRequest(self._error_map,
                                              mgmt_operations.SEARCH_INDEX.value,
                                              search_index_mgmt_operations.GET_INDEX_DOCUMENT_COUNT.value,
                                              index_name=index_name,
                                              bucket_name=bucket_name,
                                              scope_name=scope_name,
                                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_index_request(self,
                                index_name: str,
                                scope_context: Optional[Tuple[str, str]] = None,
                                *options: object,
                                **kwargs: object) -> GetIndexRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = GetIndexRequest(self._error_map,
                              mgmt_operations.SEARCH_INDEX.value,
                              search_index_mgmt_operations.GET_INDEX.value,
                              index_name=index_name,
                              bucket_name=bucket_name,
                              scope_name=scope_name,
                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_index_stats_request(self,
                                      index_name: str,
                                      scope_context: Optional[Tuple[str, str]] = None,
                                      *options: object,
                                      **kwargs: object) -> GetIndexStatsRequest:
        bucket_name, scope_name = self._get_scope_context(scope_context)
        if bucket_name is not None and scope_name is not None:
            raise FeatureUnavailableException(('get_index_stats unavailable at scope level. '
                                               'Use cluster.searchIndexes().get_index_stats(...) instead.'))
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        req = GetIndexStatsRequest(self._error_map,
                                   mgmt_operations.SEARCH_INDEX.value,
                                   search_index_mgmt_operations.GET_INDEX_STATS.value,
                                   index_name=index_name,
                                   bucket_name=bucket_name,
                                   scope_name=scope_name,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_pause_ingest_request(self,
                                   index_name: str,
                                   scope_context: Optional[Tuple[str, str]] = None,
                                   *options: object,
                                   **kwargs: object) -> PauseIngestRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = PauseIngestRequest(self._error_map,
                                 mgmt_operations.SEARCH_INDEX.value,
                                 search_index_mgmt_operations.CONTROL_INGEST.value,
                                 index_name=index_name,
                                 pause=True,
                                 bucket_name=bucket_name,
                                 scope_name=scope_name,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_resume_ingest_request(self,
                                    index_name: str,
                                    scope_context: Optional[Tuple[str, str]] = None,
                                    *options: object,
                                    **kwargs: object) -> ResumeIngestRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = ResumeIngestRequest(self._error_map,
                                  mgmt_operations.SEARCH_INDEX.value,
                                  search_index_mgmt_operations.CONTROL_INGEST.value,
                                  index_name=index_name,
                                  pause=False,
                                  bucket_name=bucket_name,
                                  scope_name=scope_name,
                                  **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_unfreeze_plan_request(self,
                                    index_name: str,
                                    scope_context: Optional[Tuple[str, str]] = None,
                                    *options: object,
                                    **kwargs: object) -> UnfreezePlanRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index_name(index_name)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = UnfreezePlanRequest(self._error_map,
                                  mgmt_operations.SEARCH_INDEX.value,
                                  search_index_mgmt_operations.FREEZE_PLAN.value,
                                  index_name=index_name,
                                  freeze=False,
                                  bucket_name=bucket_name,
                                  scope_name=scope_name,
                                  **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_upsert_index_request(self,
                                   index: SearchIndex,
                                   scope_context: Optional[Tuple[str, str]] = None,
                                   *options: object,
                                   **kwargs: object) -> UpsertIndexRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        self._validate_index(index)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = UpsertIndexRequest(self._error_map,
                                 mgmt_operations.SEARCH_INDEX.value,
                                 search_index_mgmt_operations.UPSERT_INDEX.value,
                                 index=index.as_dict(),
                                 bucket_name=bucket_name,
                                 scope_name=scope_name,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req
