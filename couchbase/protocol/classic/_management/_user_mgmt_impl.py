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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.users import (Group,
                                               RoleAndDescription,
                                               UserAndMetadata)
from couchbase.protocol.classic._management._user_mgmt_req_builder import UserMgmtRequestBuilder

if TYPE_CHECKING:
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.options import (ChangePasswordOptions,
                                                     DropGroupOptions,
                                                     DropUserOptions,
                                                     GetAllGroupsOptions,
                                                     GetAllUsersOptions,
                                                     GetGroupOptions,
                                                     GetRolesOptions,
                                                     GetUserOptions,
                                                     UpsertGroupOptions,
                                                     UpsertUserOptions)
    from couchbase.common.management.users import User
    from couchbase.protocol.classic._client_adapter import ClientAdapter


class UserMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = UserMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    def get_user(self,
                 username: str,
                 options: GetUserOptions | None = None,
                 **kwargs: Any) -> UserAndMetadata:
        with ObservableRequestHandler(Op.UserGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_user_request(username, obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_user = ret.raw_result['user']
            return UserAndMetadata.create_user_and_metadata(raw_user)

    def get_all_users(self, options: GetAllUsersOptions | None = None, **kwargs: Any) -> Iterable[UserAndMetadata]:
        with ObservableRequestHandler(Op.UserGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_users_request(obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            users = []
            raw_users = ret.raw_result['users']
            for u in raw_users:
                user = UserAndMetadata.create_user_and_metadata(u)
                users.append(user)

            return users

    def upsert_user(self,
                    user: User,
                    options: UpsertUserOptions | None = None,
                    **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.UserUpsert, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_user_request(user, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_user(self,
                  username: str,
                  options: DropUserOptions | None = None,
                  **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.UserDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_user_request(username, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def change_password(self,
                        new_password: str,
                        options: ChangePasswordOptions | None = None,
                        **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.ChangePassword, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_change_password_request(new_password, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_roles(self, options: GetRolesOptions | None = None, **kwargs: Any) -> Iterable[RoleAndDescription]:
        with ObservableRequestHandler(Op.RoleGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_roles_request(obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            roles = []
            raw_roles = ret.raw_result['roles']
            for r in raw_roles:
                role = RoleAndDescription.create_role_and_description(r)
                roles.append(role)

            return roles

    def get_group(self,
                  group_name: str,
                  options: GetGroupOptions | None = None,
                  **kwargs: Any) -> Group:
        with ObservableRequestHandler(Op.GroupGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_group_request(group_name, obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_group = ret.raw_result['group']
            return Group.create_group(raw_group)

    def get_all_groups(self, options: GetAllGroupsOptions | None = None, **kwargs: Any) -> Iterable[Group]:
        with ObservableRequestHandler(Op.GroupGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_groups_request(obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            groups = []
            raw_groups = ret.raw_result['groups']
            for g in raw_groups:
                group = Group.create_group(g)
                groups.append(group)

            return groups

    def upsert_group(self,
                     group: Group,
                     options: UpsertGroupOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.GroupUpsert, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_group_request(group, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_group(self,
                   group_name: str,
                   options: DropGroupOptions | None = None,
                   **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.GroupDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_group_request(group_name, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
