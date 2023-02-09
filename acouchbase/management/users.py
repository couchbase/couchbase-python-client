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

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Iterable)

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
from couchbase.management.logic import ManagementType
from couchbase.management.logic.users_logic import (Group,
                                                    RoleAndDescription,
                                                    User,
                                                    UserAndMetadata,
                                                    UserManagerLogic)

if TYPE_CHECKING:
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


class UserManager(UserManagerLogic):

    def __init__(self, connection, loop):
        super().__init__(connection)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AsyncMgmtWrapper.inject_callbacks(UserAndMetadata, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def get_user(self,
                 username,  # type: str
                 *options,  # type: GetUserOptions
                 **kwargs   # type: Any
                 ) -> Awaitable[UserAndMetadata]:
        super().get_user(username, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(UserAndMetadata, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def get_all_users(self,
                      *options,  # type: GetAllUsersOptions
                      **kwargs  # type: Any
                      ) -> Awaitable[Iterable[UserAndMetadata]]:
        super().get_all_users(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def upsert_user(self,
                    user,     # type: User
                    *options,  # type: UpsertUserOptions
                    **kwargs  # type: Any
                    ) -> Awaitable[None]:

        super().upsert_user(user, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def drop_user(self,
                  username,  # type: str
                  *options,  # type: DropUserOptions
                  **kwargs   # type: Any
                  ) -> Awaitable[None]:
        super().drop_user(username, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def change_password(self,
                        new_password,  # type: str
                        *options,     # type: ChangePasswordOptions
                        **kwargs      # type: Any
                        ) -> Awaitable[None]:
        super().change_password(new_password, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(RoleAndDescription, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def get_roles(self,
                  *options,  # type: GetRolesOptions
                  **kwargs   # type: Any
                  ) -> Awaitable[Iterable[RoleAndDescription]]:
        super().get_roles(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(Group, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def get_group(self,
                  group_name,   # type: str
                  *options,     # type: GetGroupOptions
                  **kwargs      # type: Any
                  ) -> Awaitable[Group]:
        super().get_group(group_name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(Group, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def get_all_groups(self,
                       *options,    # type: GetAllGroupsOptions
                       **kwargs     # type: Any
                       ) -> Awaitable[Iterable[Group]]:
        super().get_all_groups(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def upsert_group(self,
                     group,     # type: Group
                     *options,  # type: UpsertGroupOptions
                     **kwargs   # type: Any
                     ) -> Awaitable[None]:
        super().upsert_group(group, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.UserMgmt, UserManagerLogic._ERROR_MAPPING)
    def drop_group(self,
                   group_name,  # type: str
                   *options,    # type: DropGroupOptions
                   **kwargs     # type: Any
                   ) -> Awaitable[None]:
        super().drop_group(group_name, *options, **kwargs)
