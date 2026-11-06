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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._user_mgmt_impl import AsyncUserMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

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


class TxUserMgmtImpl(AsyncUserMgmtImpl):

    def get_user_deferred(self,
                          username: str,
                          options: GetUserOptions | None = None,
                          **kwargs: Any) -> Deferred[UserAndMetadata]:
        obs_handler = ObservableRequestHandler(Op.UserGet, self._observability_instruments)

        def start() -> Coroutine[Any, Any, UserAndMetadata]:
            req = self._request_builder.build_get_user_request(username, obs_handler, options, **kwargs)
            return self._get_user(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_users_deferred(self,
                               options: GetAllUsersOptions | None = None,
                               **kwargs: Any) -> Deferred[Iterable[UserAndMetadata]]:
        obs_handler = ObservableRequestHandler(Op.UserGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[UserAndMetadata]]:
            req = self._request_builder.build_get_all_users_request(obs_handler, options, **kwargs)
            return self._get_all_users(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def upsert_user_deferred(self,
                             user: User,
                             options: UpsertUserOptions | None = None,
                             **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.UserUpsert, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_upsert_user_request(user, obs_handler, options, **kwargs)
            return self._upsert_user(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_user_deferred(self,
                           username: str,
                           options: DropUserOptions | None = None,
                           **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.UserDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_user_request(username, obs_handler, options, **kwargs)
            return self._drop_user(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def change_password_deferred(self,
                                 new_password: str,
                                 options: ChangePasswordOptions | None = None,
                                 **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.ChangePassword, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_change_password_request(new_password, obs_handler, options, **kwargs)
            return self._change_password(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_roles_deferred(self,
                           options: GetRolesOptions | None = None,
                           **kwargs: Any) -> Deferred[Iterable[RoleAndDescription]]:
        obs_handler = ObservableRequestHandler(Op.RoleGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[RoleAndDescription]]:
            req = self._request_builder.build_get_roles_request(obs_handler, options, **kwargs)
            return self._get_roles(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_group_deferred(self,
                           group_name: str,
                           options: GetGroupOptions | None = None,
                           **kwargs: Any) -> Deferred[Group]:
        obs_handler = ObservableRequestHandler(Op.GroupGet, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Group]:
            req = self._request_builder.build_get_group_request(group_name, obs_handler, options, **kwargs)
            return self._get_group(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_groups_deferred(self,
                                options: GetAllGroupsOptions | None = None,
                                **kwargs: Any) -> Deferred[Iterable[Group]]:
        obs_handler = ObservableRequestHandler(Op.GroupGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[Group]]:
            req = self._request_builder.build_get_all_groups_request(obs_handler, options, **kwargs)
            return self._get_all_groups(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def upsert_group_deferred(self,
                              group: Group,
                              options: UpsertGroupOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.GroupUpsert, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_upsert_group_request(group, obs_handler, options, **kwargs)
            return self._upsert_group(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_group_deferred(self,
                            group_name: str,
                            options: DropGroupOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.GroupDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_group_request(group_name, obs_handler, options, **kwargs)
            return self._drop_group(req, obs_handler)

        return defer(self.loop, obs_handler, start)
