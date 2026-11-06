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

from collections.abc import Callable
from dataclasses import dataclass, fields
from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.exceptions import (CouchbaseException,
                                         QuotaLimitedException,
                                         SearchIndexNotFoundException)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._bindings import SearchIndexMgmtBinding
from couchbase.protocol.classic._management._mgmt_req import MgmtRequest


class SearchIndexMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    AllowQueryingSearchIndex = 'AllowQueryingSearchIndexOptions'
    AnalyzeDocumentSearchIndex = 'AnalyzeDocumentSearchIndexOptions'
    DisallowQueryingSearchIndex = 'DisallowQueryingSearchIndexOptions'
    DropSearchIndex = 'DropSearchIndexOptions'
    FreezePlanSearchIndex = 'FreezePlanSearchIndexOptions'
    GetAllSearchIndexStats = 'GetAllSearchIndexStatsOptions'
    GetAllSearchIndexes = 'GetAllSearchIndexesOptions'
    GetSearchIndex = 'GetSearchIndexOptions'
    GetSearchIndexStats = 'GetSearchIndexStatsOptions'
    GetSearchIndexedDocumentsCount = 'GetSearchIndexedDocumentsCountOptions'
    PauseIngestSearchIndex = 'PauseIngestSearchIndexOptions'
    ResumeIngestSearchIndex = 'ResumeIngestSearchIndexOptions'
    UnfreezePlanSearchIndex = 'UnfreezePlanSearchIndexOptions'
    UpsertSearchIndex = 'UpsertSearchIndexOptions'


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class SearchIndexMgmtRequest(MgmtRequest):

    def req_to_dict(self,
                    obs_handler: ObservableRequestHandler | None = None,
                    callback: Callable[..., None] | None = None,
                    errback: Callable[..., None] | None = None) -> dict[str, Any]:
        cls = type(self)
        cached_fields = _FIELDS_CACHE.get(cls)
        if cached_fields is None:
            cached_fields = [f for f in fields(cls) if f.name not in _OPARG_SKIP_SET]
            _FIELDS_CACHE[cls] = cached_fields

        mgmt_kwargs = {
            f.name: getattr(self, f.name)
            for f in cached_fields
            if getattr(self, f.name) is not None
        }

        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if obs_handler:
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if obs_handler.is_legacy_tracer:
                legacy_request_span = obs_handler.legacy_request_span
                if legacy_request_span:
                    mgmt_kwargs['parent_span'] = legacy_request_span
            else:
                mgmt_kwargs['wrapper_span_name'] = obs_handler.wrapper_span_name

        return mgmt_kwargs


@dataclass
class AllowQueryingRequest(SearchIndexMgmtRequest):
    index_name: str
    allow: bool
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexControlQuery.value


@dataclass
class AnalyzeDocumentRequest(SearchIndexMgmtRequest):
    index_name: str
    encoded_document: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexAnalyzeDocument.value


@dataclass
class DisallowQueryingRequest(SearchIndexMgmtRequest):
    index_name: str
    allow: bool
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexControlQuery.value


@dataclass
class DropIndexRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexDrop.value


@dataclass
class FreezePlanRequest(SearchIndexMgmtRequest):
    index_name: str
    freeze: bool
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexControlPlanFreeze.value


@dataclass
class GetAllIndexesRequest(SearchIndexMgmtRequest):
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexGetAll.value


@dataclass
class GetAllIndexStatsRequest(SearchIndexMgmtRequest):
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchGetStats.value


@dataclass
class GetIndexedDocumentsCountRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexGetDocumentsCount.value


@dataclass
class GetIndexRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexGet.value


@dataclass
class GetIndexStatsRequest(SearchIndexMgmtRequest):
    index_name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexGetStats.value


@dataclass
class PauseIngestRequest(SearchIndexMgmtRequest):
    index_name: str
    pause: bool
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexControlIngest.value


@dataclass
class ResumeIngestRequest(SearchIndexMgmtRequest):
    index_name: str
    pause: bool
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexControlIngest.value


@dataclass
class UnfreezePlanRequest(SearchIndexMgmtRequest):
    index_name: str
    freeze: bool
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexControlPlanFreeze.value


@dataclass
class UpsertIndexRequest(SearchIndexMgmtRequest):
    index: dict[str, Any]
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return SearchIndexMgmtBinding.SearchIndexUpsert.value


SEARCH_INDEX_MGMT_ERROR_MAP: dict[str, type[CouchbaseException]] = {
    r'.*[iI]ndex not found.*': SearchIndexNotFoundException,
    r'.*[Cc]reate[Ii]ndex, [Pp]repare failed, err: [Ee]xceeds indexes limit': QuotaLimitedException
}
