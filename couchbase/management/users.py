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

from couchbase.management.logic.user_mgmt_impl import UserMgmtImpl
from couchbase.management.logic.user_mgmt_types import Origin  # noqa: F401
from couchbase.management.logic.user_mgmt_types import Role  # noqa: F401
from couchbase.management.logic.user_mgmt_types import RoleAndOrigins  # noqa: F401
from couchbase.management.logic.user_mgmt_types import (Group,
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
    from couchbase.logic.client_adapter import ClientAdapter


class UserManager:

    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._impl = UserMgmtImpl(client_adapter)

    def get_user(self,
                 username,  # type: str
                 *options,  # type: GetUserOptions
                 **kwargs   # type: Any
                 ) -> UserAndMetadata:
        """Returns a user by its username.

        Args:
            username (str): The name of the user to retrieve.
            options (:class:`~couchbase.management.options.GetUserOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`UserAndMetadata`: A :class:`UserAndMetadata` instance.

        Raises:
            :class:`~couchbase.exceptions.UserNotFoundException`: If the user does not exist.
        """
        req = self._impl.request_builder.build_get_user_request(username, *options, **kwargs)
        return self._impl.get_user(req)

    def get_all_users(self,
                      *options,  # type: GetAllUsersOptions
                      **kwargs  # type: Any
                      ) -> Iterable[UserAndMetadata]:
        """Returns a list of all existing users.

        Args:
            options (:class:`~couchbase.management.options.GetAllUsersOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`UserAndMetadata`]: A list of existing users.
        """
        req = self._impl.request_builder.build_get_all_users_request(*options, **kwargs)
        return self._impl.get_all_users(req)

    def upsert_user(self,
                    user,     # type: User
                    *options,  # type: UpsertUserOptions
                    **kwargs  # type: Any
                    ) -> None:
        """Creates a new user or updates an existing user.

        Args:
            user (:class:`.User`): The user to create or update.
            options (:class:`~couchbase.management.options.UpsertUserOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided user argument contains an
                invalid value or type.
        """
        req = self._impl.request_builder.build_upsert_user_request(user, *options, **kwargs)
        self._impl.upsert_user(req)

    def drop_user(self,
                  username,  # type: str
                  *options,  # type: DropUserOptions
                  **kwargs   # type: Any
                  ) -> None:
        """Drops an existing user.

        Args:
            username (str): The name of the user to drop.
            options (:class:`~couchbase.management.options.DropUserOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.UserNotFoundException`: If the user does not exist.
        """
        req = self._impl.request_builder.build_drop_user_request(username, *options, **kwargs)
        self._impl.drop_user(req)

    def change_password(self,
                        new_password,  # type: str
                        *options,     # type: ChangePasswordOptions
                        **kwargs      # type: Any
                        ) -> None:
        """Changes the password of the currently authenticated user. SDK must be re-started and a new connection
         established after running, as the previous credentials will no longer be valid.

         Args:
             new_password (str): The new password for the user
             options (:class:`~couchbase.management.options.ChangePasswordOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided group argument contains an
                invalid value or type.

        """
        req = self._impl.request_builder.build_change_password_request(new_password, *options, **kwargs)
        self._impl.change_password(req)

    def get_roles(self,
                  *options,  # type: GetRolesOptions
                  **kwargs   # type: Any
                  ) -> Iterable[RoleAndDescription]:
        """Returns a list of roles available on the server.

        Args:
            options (:class:`~couchbase.management.options.GetRolesOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`RoleAndDescription`]: A list of roles available on the server.
        """
        req = self._impl.request_builder.build_get_roles_request(*options, **kwargs)
        return self._impl.get_roles(req)

    def get_group(self,
                  group_name,   # type: str
                  *options,     # type: GetGroupOptions
                  **kwargs      # type: Any
                  ) -> Group:
        """Returns a group by it's name.

        Args:
            group_name (str): The name of the group to retrieve.
            options (:class:`~couchbase.management.options.GetGroupOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`Group`: A :class:`Group` instance.

        Raises:
            :class:`~couchbase.exceptions.GroupNotFoundException`: If the group does not exist.
        """
        req = self._impl.request_builder.build_get_group_request(group_name, *options, **kwargs)
        return self._impl.get_group(req)

    def get_all_groups(self,
                       *options,    # type: GetAllGroupsOptions
                       **kwargs     # type: Any
                       ) -> Iterable[Group]:
        """Returns a list of all existing groups.

        Args:
            options (:class:`~couchbase.management.options.GetAllGroupsOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            Iterable[:class:`Group`]: A list of existing groups.
        """
        req = self._impl.request_builder.build_get_all_groups_request(*options, **kwargs)
        return self._impl.get_all_groups(req)

    def upsert_group(self,
                     group,     # type: Group
                     *options,  # type: UpsertGroupOptions
                     **kwargs   # type: Any
                     ) -> None:
        """Creates a new group or updates an existing group.

        Args:
            group (:class:`.Group`): The group to create or update.
            options (:class:`~couchbase.management.options.UpsertGroupOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided group argument contains an
                invalid value or type.
        """
        req = self._impl.request_builder.build_upsert_group_request(group, *options, **kwargs)
        self._impl.upsert_group(req)

    def drop_group(self,
                   group_name,  # type: str
                   *options,    # type: DropGroupOptions
                   **kwargs     # type: Any
                   ) -> None:
        """Drops an existing group.

        Args:
            group_name (str): The name of the group to drop.
            options (:class:`~couchbase.management.options.DropGroupOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.GroupNotFoundException`: If the group does not exist.
        """
        req = self._impl.request_builder.build_drop_group_request(group_name, *options, **kwargs)
        self._impl.drop_group(req)
