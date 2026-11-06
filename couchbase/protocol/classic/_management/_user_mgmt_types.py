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
                                         FeatureUnavailableException,
                                         GroupNotFoundException,
                                         RateLimitedException,
                                         UserNotFoundException)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._bindings import UserMgmtBinding
from couchbase.protocol.classic._management._mgmt_req import MgmtRequest


class UserMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    ChangePassword = 'ChangePasswordOptions'
    DropGroup = 'DropGroupOptions'
    DropUser = 'DropUserOptions'
    GetAllGroups = 'GetAllGroupsOptions'
    GetAllUsers = 'GetAllUsersOptions'
    GetGroup = 'GetGroupOptions'
    GetRoles = 'GetRolesOptions'
    GetUser = 'GetUserOptions'
    UpsertGroup = 'UpsertGroupOptions'
    UpsertUser = 'UpsertUserOptions'


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class UserMgmtRequest(MgmtRequest):

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
class ChangePasswordRequest(UserMgmtRequest):
    password: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.ChangePassword.value

    def req_to_dict(self,
                    obs_handler: ObservableRequestHandler | None = None,
                    callback: Callable[..., None] | None = None,
                    errback: Callable[..., None] | None = None) -> dict[str, Any]:
        op_kwargs = super().req_to_dict(obs_handler=obs_handler, callback=callback, errback=errback)
        pw = op_kwargs.pop('password')
        op_kwargs['newPassword'] = pw
        return op_kwargs


@dataclass
class DropGroupRequest(UserMgmtRequest):
    name: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.GroupDrop.value


@dataclass
class DropUserRequest(UserMgmtRequest):
    username: str
    domain: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.UserDrop.value


@dataclass
class GetAllGroupsRequest(UserMgmtRequest):
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.GroupGetAll.value


@dataclass
class GetAllUsersRequest(UserMgmtRequest):
    domain: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.UserGetAll.value


@dataclass
class GetGroupRequest(UserMgmtRequest):
    name: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.GroupGet.value


@dataclass
class GetRolesRequest(UserMgmtRequest):
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.RoleGetAll.value


@dataclass
class GetUserRequest(UserMgmtRequest):
    username: str
    domain: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.UserGet.value


@dataclass
class UpsertGroupRequest(UserMgmtRequest):
    group: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.GroupUpsert.value


@dataclass
class UpsertUserRequest(UserMgmtRequest):
    user: dict[str, Any]
    domain: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return UserMgmtBinding.UserUpsert.value


USER_MGMT_ERROR_MAP: dict[str, type[CouchbaseException]] = {
    r'Unknown group.*': GroupNotFoundException,
    r'Unknown user.*': UserNotFoundException,
    # The drop endpoints word it differently from the get endpoints, and the patterns are
    # applied with re.match, so the get wording above cannot reach them.
    r'Group was not found.*': GroupNotFoundException,
    r'User was not found.*': UserNotFoundException,
    r'Not found.*': FeatureUnavailableException,
    r'Method Not Allowed.*': FeatureUnavailableException,
    r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException
}
