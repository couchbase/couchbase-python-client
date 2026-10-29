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

from asyncio import AbstractEventLoop
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.users import (Group,
                                               RoleAndDescription,
                                               UserAndMetadata)
from couchbase.protocol.classic._management._user_mgmt_req_builder import UserMgmtRequestBuilder

if TYPE_CHECKING:
    from acouchbase.protocol.classic._client_adapter import AsyncClientAdapter
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
    from couchbase.protocol.classic._management._user_mgmt_types import (ChangePasswordRequest,
                                                                         DropGroupRequest,
                                                                         DropUserRequest,
                                                                         GetAllGroupsRequest,
                                                                         GetAllUsersRequest,
                                                                         GetGroupRequest,
                                                                         GetRolesRequest,
                                                                         GetUserRequest,
                                                                         UpsertGroupRequest,
                                                                         UpsertUserRequest)


class AsyncUserMgmtImpl:
    def __init__(self, client_adapter: AsyncClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = UserMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL** The loop the txcouchbase impl runs these coroutines on."""
        return self._client_adapter.loop

    async def get_user(self,
                       username: str,
                       options: GetUserOptions | None = None,
                       **kwargs: Any) -> UserAndMetadata:
        async with ObservableRequestHandler(Op.UserGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_user_request(username, obs_handler, options, **kwargs)
            return await self._get_user(req, obs_handler)

    async def get_all_users(self,
                            options: GetAllUsersOptions | None = None,
                            **kwargs: Any) -> Iterable[UserAndMetadata]:
        async with ObservableRequestHandler(Op.UserGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_users_request(obs_handler, options, **kwargs)
            return await self._get_all_users(req, obs_handler)

    async def upsert_user(self,
                          user: User,
                          options: UpsertUserOptions | None = None,
                          **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.UserUpsert, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_user_request(user, obs_handler, options, **kwargs)
            await self._upsert_user(req, obs_handler)

    async def drop_user(self,
                        username: str,
                        options: DropUserOptions | None = None,
                        **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.UserDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_user_request(username, obs_handler, options, **kwargs)
            await self._drop_user(req, obs_handler)

    async def change_password(self,
                              new_password: str,
                              options: ChangePasswordOptions | None = None,
                              **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.ChangePassword, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_change_password_request(new_password, obs_handler, options, **kwargs)
            await self._change_password(req, obs_handler)

    async def get_roles(self, options: GetRolesOptions | None = None, **kwargs: Any) -> Iterable[RoleAndDescription]:
        async with ObservableRequestHandler(Op.RoleGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_roles_request(obs_handler, options, **kwargs)
            return await self._get_roles(req, obs_handler)

    async def get_group(self,
                        group_name: str,
                        options: GetGroupOptions | None = None,
                        **kwargs: Any) -> Group:
        async with ObservableRequestHandler(Op.GroupGet, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_group_request(group_name, obs_handler, options, **kwargs)
            return await self._get_group(req, obs_handler)

    async def get_all_groups(self, options: GetAllGroupsOptions | None = None, **kwargs: Any) -> Iterable[Group]:
        async with ObservableRequestHandler(Op.GroupGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_groups_request(obs_handler, options, **kwargs)
            return await self._get_all_groups(req, obs_handler)

    async def upsert_group(self,
                           group: Group,
                           options: UpsertGroupOptions | None = None,
                           **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.GroupUpsert, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_group_request(group, obs_handler, options, **kwargs)
            await self._upsert_group(req, obs_handler)

    async def drop_group(self,
                         group_name: str,
                         options: DropGroupOptions | None = None,
                         **kwargs: Any) -> None:
        async with ObservableRequestHandler(Op.GroupDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_group_request(group_name, obs_handler, options, **kwargs)
            await self._drop_group(req, obs_handler)

    # The execute half of each operation.  The txcouchbase impl builds the request itself, inside
    # the `start()` it hands to `defer()`, and returns one of these as the coroutine to run.
    async def _change_password(self, req: ChangePasswordRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _drop_group(self, req: DropGroupRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _drop_user(self, req: DropUserRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _get_all_groups(self, req: GetAllGroupsRequest, obs_handler: ObservableRequestHandler) -> Iterable[Group]:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        groups = []
        raw_groups = ret.raw_result['groups']
        for g in raw_groups:
            group = Group.create_group(g)
            groups.append(group)

        return groups

    async def _get_all_users(
        self,
        req: GetAllUsersRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Iterable[UserAndMetadata]:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        users = []
        raw_users = ret.raw_result['users']
        for u in raw_users:
            user = UserAndMetadata.create_user_and_metadata(u)
            users.append(user)

        return users

    async def _get_group(
        self,
        req: GetGroupRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Group:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_group = ret.raw_result['group']
        return Group.create_group(raw_group)

    async def _get_roles(
        self,
        req: GetRolesRequest,
        obs_handler: ObservableRequestHandler,
    ) -> Iterable[RoleAndDescription]:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        roles = []
        raw_roles = ret.raw_result['roles']
        for r in raw_roles:
            role = RoleAndDescription.create_role_and_description(r)
            roles.append(role)

        return roles

    async def _get_user(self, req: GetUserRequest, obs_handler: ObservableRequestHandler) -> UserAndMetadata:
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_user = ret.raw_result['user']
        return UserAndMetadata.create_user_and_metadata(raw_user)

    async def _upsert_group(self, req: UpsertGroupRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def _upsert_user(self, req: UpsertUserRequest, obs_handler: ObservableRequestHandler) -> None:
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
