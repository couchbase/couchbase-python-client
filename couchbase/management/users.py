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
from typing import TYPE_CHECKING, Any

from couchbase.common.management.users import Origin  # noqa: F401
from couchbase.common.management.users import Role  # noqa: F401
from couchbase.common.management.users import RoleAndOrigins  # noqa: F401
from couchbase.common.management.users import (Group,
                                               RoleAndDescription,
                                               User,
                                               UserAndMetadata)

# @TODO:  lets deprecate import of options from couchbase.management.users
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

if TYPE_CHECKING:
    from couchbase.common._interfaces._user_mgmt import UserMgmtInterface


class UserManager:

    def __init__(self, impl: UserMgmtInterface) -> None:
        self._impl = impl

    def get_user(self,
                 username: str,
                 options: GetUserOptions | None = None,
                 **kwargs: Any) -> UserAndMetadata:
        """Returns a user by its username.

        Args:
            username (str): The name of the user to retrieve.
            options (:class:`~couchbase.management.options.GetUserOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`UserAndMetadata`: A :class:`UserAndMetadata` instance.

        Raises:
            :class:`~couchbase.exceptions.UserNotFoundException`: If the user does not exist.
        """
        return self._impl.get_user(username, options, **kwargs)

    def get_all_users(self, options: GetAllUsersOptions | None = None, **kwargs: Any) -> Iterable[UserAndMetadata]:
        """Returns a list of all existing users.

        Args:
            options (:class:`~couchbase.management.options.GetAllUsersOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`UserAndMetadata`]: A list of existing users.
        """
        return self._impl.get_all_users(options, **kwargs)

    def upsert_user(self,
                    user: User,
                    options: UpsertUserOptions | None = None,
                    **kwargs: Any) -> None:
        """Creates a new user or updates an existing user.

        Args:
            user (:class:`.User`): The user to create or update.
            options (:class:`~couchbase.management.options.UpsertUserOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided user argument contains an
                invalid value or type.
        """
        self._impl.upsert_user(user, options, **kwargs)

    def drop_user(self,
                  username: str,
                  options: DropUserOptions | None = None,
                  **kwargs: Any) -> None:
        """Drops an existing user.

        Args:
            username (str): The name of the user to drop.
            options (:class:`~couchbase.management.options.DropUserOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.UserNotFoundException`: If the user does not exist.
        """
        self._impl.drop_user(username, options, **kwargs)

    def change_password(self,
                        new_password: str,
                        options: ChangePasswordOptions | None = None,
                        **kwargs: Any) -> None:
        """Changes the password of the currently authenticated user. SDK must be re-started and a new connection
         established after running, as the previous credentials will no longer be valid.

         Args:
             new_password (str): The new password for the user
             options (:class:`~couchbase.management.options.ChangePasswordOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided group argument contains an
                invalid value or type.

        """
        self._impl.change_password(new_password, options, **kwargs)

    def get_roles(self, options: GetRolesOptions | None = None, **kwargs: Any) -> Iterable[RoleAndDescription]:
        """Returns a list of roles available on the server.

        Args:
            options (:class:`~couchbase.management.options.GetRolesOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`RoleAndDescription`]: A list of roles available on the server.
        """
        return self._impl.get_roles(options, **kwargs)

    def get_group(self,
                  group_name: str,
                  options: GetGroupOptions | None = None,
                  **kwargs: Any) -> Group:
        """Returns a group by it's name.

        Args:
            group_name (str): The name of the group to retrieve.
            options (:class:`~couchbase.management.options.GetGroupOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`Group`: A :class:`Group` instance.

        Raises:
            :class:`~couchbase.exceptions.GroupNotFoundException`: If the group does not exist.
        """
        return self._impl.get_group(group_name, options, **kwargs)

    def get_all_groups(self, options: GetAllGroupsOptions | None = None, **kwargs: Any) -> Iterable[Group]:
        """Returns a list of all existing groups.

        Args:
            options (:class:`~couchbase.management.options.GetAllGroupsOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`Group`]: A list of existing groups.
        """
        return self._impl.get_all_groups(options, **kwargs)

    def upsert_group(self,
                     group: Group,
                     options: UpsertGroupOptions | None = None,
                     **kwargs: Any) -> None:
        """Creates a new group or updates an existing group.

        Args:
            group (:class:`.Group`): The group to create or update.
            options (:class:`~couchbase.management.options.UpsertGroupOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided group argument contains an
                invalid value or type.
        """
        self._impl.upsert_group(group, options, **kwargs)

    def drop_group(self,
                   group_name: str,
                   options: DropGroupOptions | None = None,
                   **kwargs: Any) -> None:
        """Drops an existing group.

        Args:
            group_name (str): The name of the group to drop.
            options (:class:`~couchbase.management.options.DropGroupOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.GroupNotFoundException`: If the group does not exist.
        """
        self._impl.drop_group(group_name, options, **kwargs)
