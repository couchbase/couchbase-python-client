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

from collections.abc import Callable
from dataclasses import dataclass, fields
from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.exceptions import (CollectionAlreadyExistsException,
                                         CollectionNotFoundException,
                                         CouchbaseException,
                                         QuotaLimitedException,
                                         RateLimitedException,
                                         ScopeAlreadyExistsException,
                                         ScopeNotFoundException)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._bindings import CollectionMgmtBinding
from couchbase.protocol.classic._management._mgmt_req import MgmtRequest


class CollectionMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    CreateCollection = 'CreateCollectionOptions'
    CreateScope = 'CreateScopeOptions'
    DropCollection = 'DropCollectionOptions'
    DropScope = 'DropScopeOptions'
    GetAllScopes = 'GetAllScopesOptions'
    UpdateCollection = 'UpdateCollectionOptions'


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class CollectionMgmtRequest(MgmtRequest):

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
class CreateCollectionRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    collection_name: str
    max_expiry: int | None = None
    history: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return CollectionMgmtBinding.CollectionCreate.value


@dataclass
class CreateScopeRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return CollectionMgmtBinding.ScopeCreate.value


@dataclass
class DropCollectionRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    collection_name: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return CollectionMgmtBinding.CollectionDrop.value


@dataclass
class DropScopeRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return CollectionMgmtBinding.ScopeDrop.value


@dataclass
class GetAllScopesRequest(CollectionMgmtRequest):
    bucket_name: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return CollectionMgmtBinding.ScopeGetAll.value


@dataclass
class UpdateCollectionRequest(CollectionMgmtRequest):
    bucket_name: str
    scope_name: str
    collection_name: str
    max_expiry: int | None = None
    history: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return CollectionMgmtBinding.CollectionUpdate.value


COLLECTION_MGMT_ERROR_MAP: dict[str, type[CouchbaseException]] = {
    r'.*Scope with.*name.*already exists': ScopeAlreadyExistsException,
    r'.*Scope with.*name.*not found': ScopeNotFoundException,
    r'.*Collection with.*name.*not found': CollectionNotFoundException,
    r'.*Collection with.*name.*already exists': CollectionAlreadyExistsException,
    r'.*collection_not_found.*': CollectionNotFoundException,
    r'.*scope_not_found.*': ScopeNotFoundException,
    r'.*Maximum number of collections has been reached for scope.*': QuotaLimitedException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
