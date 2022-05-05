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
                    Iterable)

from twisted.internet.defer import Deferred

from couchbase.management.logic.users_logic import (Group,
                                                    RoleAndDescription,
                                                    User,
                                                    UserAndMetadata,
                                                    UserManagerLogic)

if TYPE_CHECKING:
    from couchbase.management.options import (DropGroupOptions,
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

    def get_user(self,
                 username,  # type: str
                 *options,  # type: GetUserOptions
                 **kwargs   # type: Any
                 ) -> Deferred[UserAndMetadata]:
        return Deferred.fromFuture(super().get_user(username, *options, **kwargs))

    def get_all_users(self,
                      *options,  # type: GetAllUsersOptions
                      **kwargs  # type: Any
                      ) -> Deferred[Iterable[UserAndMetadata]]:
        return Deferred.fromFuture(super().get_all_users(*options, **kwargs))

    def upsert_user(self,
                    user,     # type: User
                    *options,  # type: UpsertUserOptions
                    **kwargs  # type: Any
                    ) -> Deferred[None]:

        return Deferred.fromFuture(super().upsert_user(user, *options, **kwargs))

    def drop_user(self,
                  username,  # type: str
                  *options,  # type: DropUserOptions
                  **kwargs   # type: Any
                  ) -> Deferred[None]:
        return Deferred.fromFuture(super().drop_user(username, *options, **kwargs))

    def get_roles(self,
                  *options,  # type: GetRolesOptions
                  **kwargs   # type: Any
                  ) -> Deferred[Iterable[RoleAndDescription]]:
        return Deferred.fromFuture(super().get_roles(*options, **kwargs))

    def get_group(self,
                  group_name,   # type: str
                  *options,     # type: GetGroupOptions
                  **kwargs      # type: Any
                  ) -> Deferred[Group]:
        return Deferred.fromFuture(super().get_group(group_name, *options, **kwargs))

    def get_all_groups(self,
                       *options,    # type: GetAllGroupsOptions
                       **kwargs     # type: Any
                       ) -> Deferred[Iterable[Group]]:
        return Deferred.fromFuture(super().get_all_groups(*options, **kwargs))

    def upsert_group(self,
                     group,     # type: Group
                     *options,  # type: UpsertGroupOptions
                     **kwargs   # type: Any
                     ) -> Deferred[None]:
        return Deferred.fromFuture(super().upsert_group(group, *options, **kwargs))

    def drop_group(self,
                   group_name,  # type: str
                   *options,    # type: DropGroupOptions
                   **kwargs     # type: Any
                   ) -> Deferred[None]:
        return Deferred.fromFuture(super().drop_group(group_name, *options, **kwargs))
