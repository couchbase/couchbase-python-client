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

__all__ = [
    'Group',
    'Origin',
    'Role',
    'RoleAndDescription',
    'RoleAndOrigins',
    'User',
    'UserAndMetadata',
    'UserManager',
]

from collections.abc import Iterable
from typing import TYPE_CHECKING

from couchbase.common.management.users import (Group,
                                               Origin,
                                               Role,
                                               RoleAndDescription,
                                               RoleAndOrigins,
                                               User,
                                               UserAndMetadata)

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._user_mgmt import AsyncUserMgmtInterface
    from couchbase.common.management.options import (ChangePasswordOptionsKwargs,
                                                     DropGroupOptionsKwargs,
                                                     DropUserOptionsKwargs,
                                                     GetAllGroupsOptionsKwargs,
                                                     GetAllUsersOptionsKwargs,
                                                     GetGroupOptionsKwargs,
                                                     GetRolesOptionsKwargs,
                                                     GetUserOptionsKwargs,
                                                     UpsertGroupOptionsKwargs,
                                                     UpsertUserOptionsKwargs)
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
    """Performs management operations on users, groups and roles."""

    def __init__(self, impl: AsyncUserMgmtInterface) -> None:
        self._impl = impl

    async def get_user(self,
                       username: str,
                       options: GetUserOptions | None = None,
                       **kwargs: Unpack[GetUserOptionsKwargs]) -> UserAndMetadata:
        """Returns a user by its username.

        Args:
            username (str): The name of the user to retrieve.
            options (:class:`~couchbase.management.options.GetUserOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.management.users.UserAndMetadata`: The user, with what the server records about it.

        Raises:
            :class:`~couchbase.exceptions.UserNotFoundException`: If the user does not exist.
        """
        return await self._impl.get_user(username, options, **kwargs)

    async def get_all_users(self,
                            options: GetAllUsersOptions | None = None,
                            **kwargs: Unpack[GetAllUsersOptionsKwargs]) -> Iterable[UserAndMetadata]:
        """Returns a list of all existing users.

        Args:
            options (:class:`~couchbase.management.options.GetAllUsersOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`~couchbase.management.users.UserAndMetadata`]: A list of existing users.
        """
        return await self._impl.get_all_users(options, **kwargs)

    async def upsert_user(self,
                          user: User,
                          options: UpsertUserOptions | None = None,
                          **kwargs: Unpack[UpsertUserOptionsKwargs]) -> None:
        """Creates a new user or updates an existing user.

        Args:
            user (:class:`.User`): The user to create or update.
            options (:class:`~couchbase.management.options.UpsertUserOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided user argument contains an
                invalid value or type.
        """
        await self._impl.upsert_user(user, options, **kwargs)

    async def drop_user(self,
                        username: str,
                        options: DropUserOptions | None = None,
                        **kwargs: Unpack[DropUserOptionsKwargs]) -> None:
        """Drops an existing user.

        Args:
            username (str): The name of the user to drop.
            options (:class:`~couchbase.management.options.DropUserOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.UserNotFoundException`: If the user does not exist.
        """
        await self._impl.drop_user(username, options, **kwargs)

    async def change_password(self,
                              new_password: str,
                              options: ChangePasswordOptions | None = None,
                              **kwargs: Unpack[ChangePasswordOptionsKwargs]) -> None:
        """Changes the password of the currently authenticated user.

        The previous credentials stop being valid, so the SDK must be restarted and a new connection established
        with the new password.

        Args:
            new_password (str): The new password for the user.
            options (:class:`~couchbase.management.options.ChangePasswordOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the new password is not a valid value or type.
        """
        await self._impl.change_password(new_password, options, **kwargs)

    async def get_roles(self,
                        options: GetRolesOptions | None = None,
                        **kwargs: Unpack[GetRolesOptionsKwargs]) -> Iterable[RoleAndDescription]:
        """Returns a list of roles available on the server.

        Args:
            options (:class:`~couchbase.management.options.GetRolesOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`~couchbase.management.users.RoleAndDescription`]: A list of roles available on the server.
        """
        return await self._impl.get_roles(options, **kwargs)

    async def get_group(self,
                        group_name: str,
                        options: GetGroupOptions | None = None,
                        **kwargs: Unpack[GetGroupOptionsKwargs]) -> Group:
        """Returns a group by it's name.

        Args:
            group_name (str): The name of the group to retrieve.
            options (:class:`~couchbase.management.options.GetGroupOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`~couchbase.management.users.Group`: A :class:`~couchbase.management.users.Group` instance.

        Raises:
            :class:`~couchbase.exceptions.GroupNotFoundException`: If the group does not exist.
        """
        return await self._impl.get_group(group_name, options, **kwargs)

    async def get_all_groups(self,
                             options: GetAllGroupsOptions | None = None,
                             **kwargs: Unpack[GetAllGroupsOptionsKwargs]) -> Iterable[Group]:
        """Returns a list of all existing groups.

        Args:
            options (:class:`~couchbase.management.options.GetAllGroupsOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`~couchbase.management.users.Group`]: A list of existing groups.
        """
        return await self._impl.get_all_groups(options, **kwargs)

    async def upsert_group(self,
                           group: Group,
                           options: UpsertGroupOptions | None = None,
                           **kwargs: Unpack[UpsertGroupOptionsKwargs]) -> None:
        """Creates a new group or updates an existing group.

        Args:
            group (:class:`.Group`): The group to create or update.
            options (:class:`~couchbase.management.options.UpsertGroupOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided group argument contains an
                invalid value or type.
        """
        await self._impl.upsert_group(group, options, **kwargs)

    async def drop_group(self,
                         group_name: str,
                         options: DropGroupOptions | None = None,
                         **kwargs: Unpack[DropGroupOptionsKwargs]) -> None:
        """Drops an existing group.

        Args:
            group_name (str): The name of the group to drop.
            options (:class:`~couchbase.management.options.DropGroupOptions`): Optional parameters for this
                operation.
            **kwargs: keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.GroupNotFoundException`: If the group does not exist.
        """
        await self._impl.drop_group(group_name, options, **kwargs)
