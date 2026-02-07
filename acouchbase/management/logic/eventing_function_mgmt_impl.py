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

from asyncio import AbstractEventLoop
from typing import TYPE_CHECKING, List

from couchbase.management.logic.eventing_function_mgmt_req_builder import EventingFunctionMgmtRequestBuilder
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


class AsyncEventingFunctionMgmtImpl:
    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        self._client_adapter = client_adapter
        self._request_builder = EventingFunctionMgmtRequestBuilder()

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL**"""
        return self._client_adapter.loop

    @property
    def request_builder(self) -> EventingFunctionMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    async def deploy_function(self, req: DeployFunctionRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def drop_function(self, req: DropFunctionRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def get_all_functions(self, req: GetAllFunctionsRequest) -> List[EventingFunction]:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        functions = []
        raw_functions = ret.raw_result.get('functions', None)
        if raw_functions:
            functions = [EventingFunction.from_server(f) for f in raw_functions]

        return functions

    async def get_function(self, req: GetFunctionRequest) -> EventingFunction:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        raw_func = ret.raw_result.get('function', None)
        func = None
        if raw_func:
            func = EventingFunction.from_server(raw_func)
        return func

    async def get_functions_status(self, req: GetFunctionsStatusRequest) -> EventingFunctionsStatus:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req)
        raw_status = ret.raw_result.get('status', None)
        status = None
        if raw_status:
            status = EventingFunctionsStatus.from_server(raw_status)

        return status

    async def pause_function(self, req: PauseFunctionRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def resume_function(self, req: ResumeFunctionRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def undeploy_function(self, req: UndeployFunctionRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)

    async def upsert_function(self, req: UpsertFunctionRequest) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req)
