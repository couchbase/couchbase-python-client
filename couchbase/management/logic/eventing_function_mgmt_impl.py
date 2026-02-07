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

from typing import TYPE_CHECKING, List

from couchbase.management.logic.eventing_function_mgmt_req_builder import EventingFunctionMgmtRequestBuilder
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunction, EventingFunctionsStatus

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter
    from couchbase.management.logic.eventing_function_mgmt_types import (DeployFunctionRequest,
                                                                         DropFunctionRequest,
                                                                         GetAllFunctionsRequest,
                                                                         GetFunctionRequest,
                                                                         GetFunctionsStatusRequest,
                                                                         PauseFunctionRequest,
                                                                         ResumeFunctionRequest,
                                                                         UndeployFunctionRequest,
                                                                         UpsertFunctionRequest)


class EventingFunctionMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._client_adapter = client_adapter
        self._request_builder = EventingFunctionMgmtRequestBuilder()

    @property
    def request_builder(self) -> EventingFunctionMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    def deploy_function(self, req: DeployFunctionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_function(self, req: DropFunctionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def get_all_functions(self, req: GetAllFunctionsRequest) -> List[EventingFunction]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req)
        raw_functions = ret.raw_result['functions']
        return [EventingFunction.from_server(f) for f in raw_functions]

    def get_function(self, req: GetFunctionRequest) -> EventingFunction:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req)
        raw_func = ret.raw_result['function']
        return EventingFunction.from_server(raw_func)

    def get_functions_status(self, req: GetFunctionsStatusRequest) -> EventingFunctionsStatus:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req)
        raw_status = ret.raw_result['status']
        return EventingFunctionsStatus.from_server(raw_status)

    def pause_function(self, req: PauseFunctionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def resume_function(self, req: ResumeFunctionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def undeploy_function(self, req: UndeployFunctionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def upsert_function(self, req: UpsertFunctionRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)
