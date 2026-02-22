#  Copyright 2016-2022. Couchbase, Inc.
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

from typing import (TYPE_CHECKING,
                    Any,
                    Iterable)

from acouchbase.management.logic.user_mgmt_impl import AsyncUserMgmtImpl
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import UserMgmtOperationType
from couchbase.management.logic.user_mgmt_types import (Group,
                                                        RoleAndDescription,
                                                        User,
                                                        UserAndMetadata)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.logic.observability import ObservabilityInstruments
    from couchbase.management.options import (ChangePasswordOptions,
                                              DropGroupOptions,
                                              DropUserOptions,
                                              GetAllGroupsOptions,
                                              GetAllUsersOptions,
                                              GetGroupOptions,
                                              GetRolesOptions,
                                              GetUserOptions,
                                              UpsertGroupOptions,
                                              UpsertUserOptions)


class UserManager:

    def __init__(self, client_adapter: AsyncClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._impl = AsyncUserMgmtImpl(client_adapter, observability_instruments)

    async def get_user(self,
                       username,  # type: str
                       *options,  # type: GetUserOptions
                       **kwargs   # type: Any
                       ) -> UserAndMetadata:
        op_type = UserMgmtOperationType.UserGet
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_user_request(username, obs_handler, *options, **kwargs)
            return await self._impl.get_user(req, obs_handler)

    async def get_all_users(self,
                            *options,  # type: GetAllUsersOptions
                            **kwargs  # type: Any
                            ) -> Iterable[UserAndMetadata]:
        op_type = UserMgmtOperationType.UserGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_users_request(obs_handler, *options, **kwargs)
            return await self._impl.get_all_users(req, obs_handler)

    async def upsert_user(self,
                          user,     # type: User
                          *options,  # type: UpsertUserOptions
                          **kwargs  # type: Any
                          ) -> None:
        op_type = UserMgmtOperationType.UserUpsert
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_upsert_user_request(user, obs_handler, *options, **kwargs)
            await self._impl.upsert_user(req, obs_handler)

    async def drop_user(self,
                        username,  # type: str
                        *options,  # type: DropUserOptions
                        **kwargs   # type: Any
                        ) -> None:
        op_type = UserMgmtOperationType.UserDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_user_request(username, obs_handler, *options, **kwargs)
            await self._impl.drop_user(req, obs_handler)

    async def change_password(self,
                              new_password,  # type: str
                              *options,     # type: ChangePasswordOptions
                              **kwargs      # type: Any
                              ) -> None:
        op_type = UserMgmtOperationType.ChangePassword
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_change_password_request(
                new_password, obs_handler, *options, **kwargs)
            await self._impl.change_password(req, obs_handler)

    async def get_roles(self,
                        *options,  # type: GetRolesOptions
                        **kwargs   # type: Any
                        ) -> Iterable[RoleAndDescription]:
        op_type = UserMgmtOperationType.RoleGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_roles_request(obs_handler, *options, **kwargs)
            return await self._impl.get_roles(req, obs_handler)

    async def get_group(self,
                        group_name,   # type: str
                        *options,     # type: GetGroupOptions
                        **kwargs      # type: Any
                        ) -> Group:
        op_type = UserMgmtOperationType.GroupGet
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_group_request(group_name, obs_handler, *options, **kwargs)
            return await self._impl.get_group(req, obs_handler)

    async def get_all_groups(self,
                             *options,    # type: GetAllGroupsOptions
                             **kwargs     # type: Any
                             ) -> Iterable[Group]:
        op_type = UserMgmtOperationType.GroupGetAll
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_groups_request(obs_handler, *options, **kwargs)
            return await self._impl.get_all_groups(req, obs_handler)

    async def upsert_group(self,
                           group,     # type: Group
                           *options,  # type: UpsertGroupOptions
                           **kwargs   # type: Any
                           ) -> None:
        op_type = UserMgmtOperationType.GroupUpsert
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_upsert_group_request(group, obs_handler, *options, **kwargs)
            await self._impl.upsert_group(req, obs_handler)

    async def drop_group(self,
                         group_name,  # type: str
                         *options,    # type: DropGroupOptions
                         **kwargs     # type: Any
                         ) -> None:
        op_type = UserMgmtOperationType.GroupDrop
        async with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_group_request(group_name, obs_handler, *options, **kwargs)
            await self._impl.drop_group(req, obs_handler)
