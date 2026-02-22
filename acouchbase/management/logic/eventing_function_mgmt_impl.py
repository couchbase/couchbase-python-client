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
    from couchbase.logic.observability import ObservabilityInstruments, ObservableRequestHandler
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
    def __init__(self, client_adapter: AsyncClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = EventingFunctionMgmtRequestBuilder()
        self._observability_instruments = observability_instruments

    @property
    def loop(self) -> AbstractEventLoop:
        """**INTERNAL**"""
        return self._client_adapter.loop

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._observability_instruments

    @property
    def request_builder(self) -> EventingFunctionMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    async def deploy_function(self, req: DeployFunctionRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def drop_function(self, req: DropFunctionRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def get_all_functions(
        self,
        req: GetAllFunctionsRequest,
        obs_handler: ObservableRequestHandler,
    ) -> List[EventingFunction]:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_functions = ret.raw_result['functions']
        return [EventingFunction.from_server(f) for f in raw_functions]

    async def get_function(
        self,
        req: GetFunctionRequest,
        obs_handler: ObservableRequestHandler,
    ) -> EventingFunction:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_func = ret.raw_result['function']
        return EventingFunction.from_server(raw_func)

    async def get_functions_status(
        self,
        req: GetFunctionsStatusRequest,
        obs_handler: ObservableRequestHandler,
    ) -> EventingFunctionsStatus:
        """**INTERNAL**"""
        ret = await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_status = ret.raw_result['status']
        return EventingFunctionsStatus.from_server(raw_status)

    async def pause_function(self, req: PauseFunctionRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def resume_function(self, req: ResumeFunctionRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def undeploy_function(self, req: UndeployFunctionRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    async def upsert_function(self, req: UpsertFunctionRequest, obs_handler: ObservableRequestHandler) -> None:
        """**INTERNAL**"""
        await self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
