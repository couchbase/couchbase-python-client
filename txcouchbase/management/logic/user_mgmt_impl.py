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
from twisted.python.failure import Failure

from acouchbase.management.logic.user_mgmt_impl import AsyncUserMgmtImpl
from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
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
    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 observability_instruments: ObservabilityInstruments) -> None:
        super().__init__(client_adapter, observability_instruments)

    def _finish_span(self, result, obs_handler: ObservableRequestHandler):
        """Callback to properly end the span on success or failure."""
        if isinstance(result, Failure):
            exc = result.value
            obs_handler.__exit__(type(exc), exc, exc.__traceback__)
            return result
        else:
            obs_handler.__exit__(None, None, None)
            return result

    def change_password_deferred(self,
                                 req: ChangePasswordRequest,
                                 obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().change_password(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_group_deferred(self,
                            req: DropGroupRequest,
                            obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_group(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_user_deferred(self,
                           req: DropUserRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_user(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_groups_deferred(self,
                                req: GetAllGroupsRequest,
                                obs_handler: ObservableRequestHandler) -> Deferred[Iterable[Group]]:
        """**INTERNAL**"""
        coro = super().get_all_groups(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_users_deferred(self,
                               req: GetAllUsersRequest,
                               obs_handler: ObservableRequestHandler) -> Deferred[Iterable[UserAndMetadata]]:
        """**INTERNAL**"""
        coro = super().get_all_users(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_group_deferred(self,
                           req: GetGroupRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[Group]:
        """**INTERNAL**"""
        coro = super().get_group(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_roles_deferred(self,
                           req: GetRolesRequest,
                           obs_handler: ObservableRequestHandler) -> Deferred[Iterable[RoleAndDescription]]:
        """**INTERNAL**"""
        coro = super().get_roles(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_user_deferred(self,
                          req: GetUserRequest,
                          obs_handler: ObservableRequestHandler) -> Deferred[UserAndMetadata]:
        """**INTERNAL**"""
        coro = super().get_user(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_group_deferred(self,
                              req: UpsertGroupRequest,
                              obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_group(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_user_deferred(self,
                             req: UpsertUserRequest,
                             obs_handler: ObservableRequestHandler) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_user(req, obs_handler)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
