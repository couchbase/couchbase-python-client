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
                                         DesignDocumentNotFoundException,
                                         RateLimitedException)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._bindings import ViewIndexMgmtBinding
from couchbase.protocol.classic._management._mgmt_req import MgmtRequest


class ViewIndexMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    DropDesignDocument = 'DropDesignDocumentOptions'
    GetAllDesignDocuments = 'GetAllDesignDocumentsOptions'
    GetDesignDocument = 'GetDesignDocumentOptions'
    PublishDesignDocument = 'PublishDesignDocumentOptions'
    UpsertDesignDocument = 'UpsertDesignDocumentOptions'


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class ViewIndexMgmtRequest(MgmtRequest):

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
class DropDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document_name: str
    ns: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtBinding.ViewIndexDrop.value


@dataclass
class GetAllDesignDocumentsRequest(ViewIndexMgmtRequest):
    bucket_name: str
    ns: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtBinding.ViewIndexGetAll.value


@dataclass
class GetDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document_name: str
    ns: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtBinding.ViewIndexGet.value


@dataclass
class UpsertDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtBinding.ViewIndexUpsert.value


@dataclass
class PublishDesignDocumentRequest:
    """Request for publish_design_document.

    This is a Python-only composite operation (get + upsert) that doesn't
    have a corresponding C++ core request. This class holds the necessary
    info for the operation to keep the API consistent with other operations.
    """
    bucket_name: str
    design_doc_name: str
    # Applied to each of the get and the upsert.
    timeout: int | None = None


VIEW_INDEX_MGMT_ERROR_MAP: dict[str, type[CouchbaseException]] = {
    r'not_found': DesignDocumentNotFoundException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
