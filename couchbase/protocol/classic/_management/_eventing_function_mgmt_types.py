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
from couchbase.common.exceptions import (CouchbaseException,
                                         EventingFunctionAlreadyDeployedException,
                                         EventingFunctionCollectionNotFoundException,
                                         EventingFunctionCompilationFailureException,
                                         EventingFunctionIdenticalKeyspaceException,
                                         EventingFunctionNotBootstrappedException,
                                         EventingFunctionNotDeployedException,
                                         EventingFunctionNotFoundException,
                                         EventingFunctionNotUnDeployedException)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._bindings import EventingFunctionMgmtBinding
from couchbase.protocol.classic._management._mgmt_req import MgmtRequest


class EventingFunctionMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    DeployFunction = 'DeployFunctionOptions'
    DropFunction = 'DropFunctionOptions'
    FunctionsStatus = 'FunctionsStatusOptions'
    GetAllFunction = 'GetAllFunctionOptions'
    GetFunction = 'GetFunctionOptions'
    PauseFunction = 'PauseFunctionOptions'
    ResumeFunction = 'ResumeFunctionOptions'
    UndeployFunction = 'UndeployFunctionOptions'
    UpsertFunction = 'UpsertFunctionOptions'


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class EventingFunctionMgmtRequest(MgmtRequest):

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
class DeployFunctionRequest(EventingFunctionMgmtRequest):
    name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingDeployFunction.value


@dataclass
class DropFunctionRequest(EventingFunctionMgmtRequest):
    name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingDropFunction.value


@dataclass
class GetAllFunctionsRequest(EventingFunctionMgmtRequest):
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingGetAllFunctions.value


@dataclass
class GetFunctionRequest(EventingFunctionMgmtRequest):
    name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingGetFunction.value


@dataclass
class GetFunctionsStatusRequest(EventingFunctionMgmtRequest):
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingGetStatus.value


@dataclass
class PauseFunctionRequest(EventingFunctionMgmtRequest):
    name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingPauseFunction.value


@dataclass
class ResumeFunctionRequest(EventingFunctionMgmtRequest):
    name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingResumeFunction.value


@dataclass
class UndeployFunctionRequest(EventingFunctionMgmtRequest):
    name: str
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingUndeployFunction.value


@dataclass
class UpsertFunctionRequest(EventingFunctionMgmtRequest):
    function: dict[str, Any]
    bucket_name: str | None = None
    scope_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return EventingFunctionMgmtBinding.EventingUpsertFunction.value


EVENTING_FUNCTION_MGMT_ERROR_MAP: dict[str, type[CouchbaseException]] = {
    r'.*ERR_APP_NOT_FOUND_TS': EventingFunctionNotFoundException,
    r'.*ERR_HANDLER_COMPILATION': EventingFunctionCompilationFailureException,
    r'.*ERR_APP_NOT_BOOTSTRAPPED': EventingFunctionNotBootstrappedException,
    r'.*ERR_APP_NOT_DEPLOYED': EventingFunctionNotDeployedException,
    r'.*ERR_APP_NOT_UNDEPLOYED': EventingFunctionNotUnDeployedException,
    r'.*ERR_COLLECTION_MISSING': EventingFunctionCollectionNotFoundException,
    r'.*ERR_APP_ALREADY_DEPLOYED': EventingFunctionAlreadyDeployedException,
    r'.*ERR_SRC_MB_SAME': EventingFunctionIdenticalKeyspaceException
}
