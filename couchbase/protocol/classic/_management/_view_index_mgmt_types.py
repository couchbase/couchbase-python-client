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

from dataclasses import dataclass, fields
from typing import (Any,
                    Callable,
                    Dict,
                    Optional)

from couchbase.exceptions import DesignDocumentNotFoundException, RateLimitedException
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import MgmtOperationType, ViewIndexMgmtOperationType
from couchbase.protocol.classic._management._mgmt_req import MgmtRequest

# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: Dict[type, list] = {}


@dataclass
class ViewIndexMgmtRequest(MgmtRequest):

    def req_to_dict(self,
                    obs_handler: Optional[ObservableRequestHandler] = None,
                    callback: Optional[Callable[..., None]] = None,
                    errback: Optional[Callable[..., None]] = None) -> Dict[str, Any]:
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
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexDrop.value


@dataclass
class GetAllDesignDocumentsRequest(ViewIndexMgmtRequest):
    bucket_name: str
    ns: str
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexGetAll.value


@dataclass
class GetDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document_name: str
    ns: str
    timeout: Optional[int] = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexGet.value


@dataclass
class UpsertDesignDocumentRequest(ViewIndexMgmtRequest):
    bucket_name: str
    document: Dict[str, Any]
    client_context_id: Optional[str] = None
    timeout: Optional[int] = None

    @property
    def binding_method(self) -> str:
        return ViewIndexMgmtOperationType.ViewIndexUpsert.value


@dataclass
class PublishDesignDocumentRequest:
    """Request for publish_design_document.

    This is a Python-only composite operation (get + upsert) that doesn't
    have a corresponding C++ core request. This class holds the necessary
    info for the operation to keep the API consistent with other operations.
    """
    bucket_name: str
    design_doc_name: str

    @property
    def binding_method(self) -> str:
        return MgmtOperationType.ViewIndexPublish.value


VIEW_INDEX_MGMT_ERROR_MAP: Dict[str, Exception] = {
    r'not_found': DesignDocumentNotFoundException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
