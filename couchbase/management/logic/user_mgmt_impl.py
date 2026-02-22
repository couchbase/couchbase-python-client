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

from typing import TYPE_CHECKING, Iterable

from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.management.logic.user_mgmt_req_builder import UserMgmtRequestBuilder
from couchbase.management.logic.user_mgmt_types import (Group,
                                                        RoleAndDescription,
                                                        UserAndMetadata)

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter
    from couchbase.management.logic.user_mgmt_types import (ChangePasswordRequest,
                                                            DropGroupRequest,
                                                            DropUserRequest,
                                                            GetAllGroupsRequest,
                                                            GetAllUsersRequest,
                                                            GetGroupRequest,
                                                            GetRolesRequest,
                                                            GetUserRequest,
                                                            UpsertGroupRequest,
                                                            UpsertUserRequest)


class UserMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = UserMgmtRequestBuilder()
        self._observability_instruments = observability_instruments

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._observability_instruments

    @property
    def request_builder(self) -> UserMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    def change_password(self, req: ChangePasswordRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_group(self, req: DropGroupRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_user(self, req: DropUserRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_groups(self, req: GetAllGroupsRequest, obs_handler: ObservableRequestHandler) -> Iterable[Group]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        groups = []
        raw_groups = ret.raw_result['groups']
        for g in raw_groups:
            group = Group.create_group(g)
            groups.append(group)

        return groups

    def get_all_users(
        self,
        req: GetAllUsersRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Iterable[UserAndMetadata]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        users = []
        raw_users = ret.raw_result['users']
        for u in raw_users:
            user = UserAndMetadata.create_user_and_metadata(u)
            users.append(user)

        return users

    def get_group(self, req: GetGroupRequest, obs_handler: ObservableRequestHandler) -> Group:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_group = ret.raw_result['group']
        return Group.create_group(raw_group)

    def get_roles(self, req: GetRolesRequest, obs_handler: ObservableRequestHandler) -> Iterable[RoleAndDescription]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        roles = []
        raw_roles = ret.raw_result['roles']
        for r in raw_roles:
            role = RoleAndDescription.create_role_and_description(r)
            roles.append(role)

        return roles

    def get_user(self, req: GetUserRequest, obs_handler: ObservableRequestHandler) -> UserAndMetadata:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_user = ret.raw_result['user']
        return UserAndMetadata.create_user_and_metadata(raw_user)

    def upsert_group(self, req: UpsertGroupRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def upsert_user(self, req: UpsertUserRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
