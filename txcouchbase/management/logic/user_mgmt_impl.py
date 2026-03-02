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

import asyncio
from typing import TYPE_CHECKING, Iterable

from twisted.internet.defer import Deferred

from acouchbase.management.logic.user_mgmt_impl import AsyncUserMgmtImpl
from couchbase.management.logic.user_mgmt_types import (Group,
                                                        RoleAndDescription,
                                                        UserAndMetadata)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.logic.user_mgmt_types import (ChangePasswordRequest,
                                                            DropGroupRequest,
                                                            DropUserRequest,
                                                            GetAllGroupsRequest,
                                                            GetAllUsersRequest,
                                                            GetGroupRequest,
                                                            GetRolesRequest,
                                                            GetUserRequest,
                                                            UpsertGroupRequest,
                                                            UpsertUserRequest)


class TxUserMgmtImpl(AsyncUserMgmtImpl):
    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        super().__init__(client_adapter)

    def change_password_deferred(self, req: ChangePasswordRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().change_password(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_group_deferred(self, req: DropGroupRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_group(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_user_deferred(self, req: DropUserRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_user(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_groups_deferred(self, req: GetAllGroupsRequest) -> Deferred[Iterable[Group]]:
        """**INTERNAL**"""
        coro = super().get_all_groups(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_users_deferred(self, req: GetAllUsersRequest) -> Deferred[Iterable[UserAndMetadata]]:
        """**INTERNAL**"""
        coro = super().get_all_users(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_group_deferred(self, req: GetGroupRequest) -> Deferred[Group]:
        """**INTERNAL**"""
        coro = super().get_group(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_roles_deferred(self, req: GetRolesRequest) -> Deferred[Iterable[RoleAndDescription]]:
        """**INTERNAL**"""
        coro = super().get_roles(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_user_deferred(self, req: GetUserRequest) -> Deferred[UserAndMetadata]:
        """**INTERNAL**"""
        coro = super().get_user(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_group_deferred(self, req: UpsertGroupRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_group(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_user_deferred(self, req: UpsertUserRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_user(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
