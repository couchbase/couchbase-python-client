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

"""What a protocol's user management impl must provide to the `UserManager` facades.

The impl is built by its cluster impl.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

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
    from couchbase.common.management.users import (Group,
                                                   RoleAndDescription,
                                                   User,
                                                   UserAndMetadata)


class UserMgmtInterface(Protocol):
    def get_user(self,
                 username: str,
                 options: GetUserOptions | None = None,
                 **kwargs: Any) -> UserAndMetadata: ...

    def get_all_users(self, options: GetAllUsersOptions | None = None, **kwargs: Any) -> Iterable[UserAndMetadata]: ...

    def upsert_user(self,
                    user: User,
                    options: UpsertUserOptions | None = None,
                    **kwargs: Any) -> None: ...

    def drop_user(self,
                  username: str,
                  options: DropUserOptions | None = None,
                  **kwargs: Any) -> None: ...

    def change_password(self,
                        new_password: str,
                        options: ChangePasswordOptions | None = None,
                        **kwargs: Any) -> None: ...

    def get_roles(self, options: GetRolesOptions | None = None, **kwargs: Any) -> Iterable[RoleAndDescription]: ...

    def get_group(self,
                  group_name: str,
                  options: GetGroupOptions | None = None,
                  **kwargs: Any) -> Group: ...

    def get_all_groups(self, options: GetAllGroupsOptions | None = None, **kwargs: Any) -> Iterable[Group]: ...

    def upsert_group(self,
                     group: Group,
                     options: UpsertGroupOptions | None = None,
                     **kwargs: Any) -> None: ...

    def drop_group(self,
                   group_name: str,
                   options: DropGroupOptions | None = None,
                   **kwargs: Any) -> None: ...


class AsyncUserMgmtInterface(Protocol):
    async def get_user(self,
                       username: str,
                       options: GetUserOptions | None = None,
                       **kwargs: Any) -> UserAndMetadata: ...

    async def get_all_users(self,
                            options: GetAllUsersOptions | None = None,
                            **kwargs: Any) -> Iterable[UserAndMetadata]: ...

    async def upsert_user(self,
                          user: User,
                          options: UpsertUserOptions | None = None,
                          **kwargs: Any) -> None: ...

    async def drop_user(self,
                        username: str,
                        options: DropUserOptions | None = None,
                        **kwargs: Any) -> None: ...

    async def change_password(self,
                              new_password: str,
                              options: ChangePasswordOptions | None = None,
                              **kwargs: Any) -> None: ...

    async def get_roles(self, options: GetRolesOptions | None = None, **
                        kwargs: Any) -> Iterable[RoleAndDescription]: ...

    async def get_group(self,
                        group_name: str,
                        options: GetGroupOptions | None = None,
                        **kwargs: Any) -> Group: ...

    async def get_all_groups(self, options: GetAllGroupsOptions | None = None, **kwargs: Any) -> Iterable[Group]: ...

    async def upsert_group(self,
                           group: Group,
                           options: UpsertGroupOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def drop_group(self,
                         group_name: str,
                         options: DropGroupOptions | None = None,
                         **kwargs: Any) -> None: ...


class TxUserMgmtInterface(Protocol):
    def get_user_deferred(self,
                          username: str,
                          options: GetUserOptions | None = None,
                          **kwargs: Any) -> Deferred[UserAndMetadata]: ...

    def get_all_users_deferred(self,
                               options: GetAllUsersOptions | None = None,
                               **kwargs: Any) -> Deferred[Iterable[UserAndMetadata]]: ...

    def upsert_user_deferred(self,
                             user: User,
                             options: UpsertUserOptions | None = None,
                             **kwargs: Any) -> Deferred[None]: ...

    def drop_user_deferred(self,
                           username: str,
                           options: DropUserOptions | None = None,
                           **kwargs: Any) -> Deferred[None]: ...

    def change_password_deferred(self,
                                 new_password: str,
                                 options: ChangePasswordOptions | None = None,
                                 **kwargs: Any) -> Deferred[None]: ...

    def get_roles_deferred(self,
                           options: GetRolesOptions | None = None,
                           **kwargs: Any) -> Deferred[Iterable[RoleAndDescription]]: ...

    def get_group_deferred(self,
                           group_name: str,
                           options: GetGroupOptions | None = None,
                           **kwargs: Any) -> Deferred[Group]: ...

    def get_all_groups_deferred(self,
                                options: GetAllGroupsOptions | None = None,
                                **kwargs: Any) -> Deferred[Iterable[Group]]: ...

    def upsert_group_deferred(self,
                              group: Group,
                              options: UpsertGroupOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def drop_group_deferred(self,
                            group_name: str,
                            options: DropGroupOptions | None = None,
                            **kwargs: Any) -> Deferred[None]: ...
