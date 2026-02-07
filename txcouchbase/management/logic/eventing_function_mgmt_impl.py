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
from typing import TYPE_CHECKING, List

from twisted.internet.defer import Deferred

from acouchbase.management.logic.eventing_function_mgmt_impl import AsyncEventingFunctionMgmtImpl
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunction, EventingFunctionsStatus

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.logic.eventing_function_mgmt_types import (DeployFunctionRequest,
                                                                         DropFunctionRequest,
                                                                         GetAllFunctionsRequest,
                                                                         GetFunctionRequest,
                                                                         GetFunctionsStatusRequest,
                                                                         PauseFunctionRequest,
                                                                         ResumeFunctionRequest,
                                                                         UndeployFunctionRequest,
                                                                         UpsertFunctionRequest)


class TxEventingFunctionMgmtImpl(AsyncEventingFunctionMgmtImpl):
    def __init___(self, client_adapter: AsyncClientAdapter) -> None:
        super().__init__(client_adapter)

    def deploy_function_deferred(self, req: DeployFunctionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().deploy_function(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def drop_function_deferred(self, req: DropFunctionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().drop_function(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_all_functions_deferred(self, req: GetAllFunctionsRequest) -> Deferred[List[EventingFunction]]:
        """**INTERNAL**"""
        coro = super().get_all_functions(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_function_deferred(self, req: GetFunctionRequest) -> Deferred[EventingFunction]:
        """**INTERNAL**"""
        coro = super().deploy_function(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def get_function_status_deferred(self, req: GetFunctionsStatusRequest) -> Deferred[EventingFunctionsStatus]:
        """**INTERNAL**"""
        coro = super().get_function_status(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def pause_function_deferred(self, req: PauseFunctionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().pause_function(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def resume_function_deferred(self, req: ResumeFunctionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().resume_function(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def undeploy_function_deferred(self, req: UndeployFunctionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().undeploy_function(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d

    def upsert_function_deferred(self, req: UpsertFunctionRequest) -> Deferred[None]:
        """**INTERNAL**"""
        coro = super().upsert_function(req)
        future = asyncio.ensure_future(coro, loop=self.loop)
        d = Deferred.fromFuture(future)
        return d
