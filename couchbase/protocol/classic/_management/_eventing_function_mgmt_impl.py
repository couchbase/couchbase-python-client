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

from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.eventing import EventingFunction, EventingFunctionsStatus
from couchbase.protocol.classic._management._eventing_function_mgmt_req_builder import (
    EventingFunctionMgmtRequestBuilder)

if TYPE_CHECKING:
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.options import (DeployFunctionOptions,
                                                     DropFunctionOptions,
                                                     FunctionsStatusOptions,
                                                     GetAllFunctionOptions,
                                                     GetFunctionOptions,
                                                     PauseFunctionOptions,
                                                     ResumeFunctionOptions,
                                                     UndeployFunctionOptions,
                                                     UpsertFunctionOptions)
    from couchbase.protocol.classic._client_adapter import ClientAdapter


class EventingFunctionMgmtImpl:
    def __init__(self,
                 client_adapter: ClientAdapter,
                 observability_instruments: ObservabilityInstruments,
                 scope_context: tuple[str, str] | None = None) -> None:
        self._client_adapter = client_adapter
        self._request_builder = EventingFunctionMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments
        self._scope_context = scope_context

    def upsert_function(self,
                        function: EventingFunction,
                        options: UpsertFunctionOptions | None = None,
                        **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.EventingUpsertFunction, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_upsert_function_request(function,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_function(self,
                      name: str,
                      options: DropFunctionOptions | None = None,
                      **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.EventingDropFunction, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_function_request(name,
                                                                    self._scope_context,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def deploy_function(self,
                        name: str,
                        options: DeployFunctionOptions | None = None,
                        **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.EventingDeployFunction, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_deploy_function_request(name,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_functions(self, options: GetAllFunctionOptions | None = None, **kwargs: Any) -> list[EventingFunction]:
        with ObservableRequestHandler(Op.EventingGetAllFunctions, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_functions_request(self._scope_context,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_functions = ret.raw_result['functions']
            return [EventingFunction.from_server(f) for f in raw_functions]

    def get_function(self,
                     name: str,
                     options: GetFunctionOptions | None = None,
                     **kwargs: Any) -> EventingFunction:
        with ObservableRequestHandler(Op.EventingGetFunction, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_function_request(name,
                                                                   self._scope_context,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_func = ret.raw_result['function']
            return EventingFunction.from_server(raw_func)

    def pause_function(self,
                       name: str,
                       options: PauseFunctionOptions | None = None,
                       **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.EventingPauseFunction, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_pause_function_request(name,
                                                                     self._scope_context,
                                                                     obs_handler,
                                                                     options,
                                                                     **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def resume_function(self,
                        name: str,
                        options: ResumeFunctionOptions | None = None,
                        **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.EventingResumeFunction, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_resume_function_request(name,
                                                                      self._scope_context,
                                                                      obs_handler,
                                                                      options,
                                                                      **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def undeploy_function(self,
                          name: str,
                          options: UndeployFunctionOptions | None = None,
                          **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.EventingUndeployFunction, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_undeploy_function_request(name,
                                                                        self._scope_context,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def functions_status(self, options: FunctionsStatusOptions | None = None, **kwargs: Any) -> EventingFunctionsStatus:
        with ObservableRequestHandler(Op.EventingGetStatus, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_functions_status_request(self._scope_context,
                                                                           obs_handler,
                                                                           options,
                                                                           **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_status = ret.raw_result['status']
            return EventingFunctionsStatus.from_server(raw_status)
