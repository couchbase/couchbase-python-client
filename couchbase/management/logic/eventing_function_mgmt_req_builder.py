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

from typing import (TYPE_CHECKING,
                    Optional,
                    Tuple)

from couchbase.management.logic.eventing_function_mgmt_types import (EVENTING_FUNCTION_MGMT_ERROR_MAP,
                                                                     DeployFunctionRequest,
                                                                     DropFunctionRequest,
                                                                     GetAllFunctionsRequest,
                                                                     GetFunctionRequest,
                                                                     GetFunctionsStatusRequest,
                                                                     PauseFunctionRequest,
                                                                     ResumeFunctionRequest,
                                                                     UndeployFunctionRequest,
                                                                     UpsertFunctionRequest)
from couchbase.options import forward_args
from couchbase.pycbc_core import eventing_function_mgmt_operations, mgmt_operations

if TYPE_CHECKING:
    from couchbase.management.logic.eventing_function_mgmt_types import EventingFunction


class EventingFunctionMgmtRequestBuilder:

    def __init__(self) -> None:
        self._error_map = EVENTING_FUNCTION_MGMT_ERROR_MAP

    def _get_scope_context(self,
                           scope_context: Optional[Tuple[str, str]] = None) -> Tuple[Optional[str], Optional[str]]:
        if scope_context is not None:
            return scope_context[0], scope_context[1]

        return None, None

    def build_deploy_function_request(self,
                                      name: str,
                                      scope_context: Optional[Tuple[str, str]] = None,
                                      *options: object,
                                      **kwargs: object) -> DeployFunctionRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = DeployFunctionRequest(self._error_map,
                                    mgmt_operations.EVENTING_FUNCTION.value,
                                    eventing_function_mgmt_operations.DEPLOY_FUNCTION.value,
                                    name=name,
                                    bucket_name=bucket_name,
                                    scope_name=scope_name,
                                    **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_function_request(self,
                                    name: str,
                                    scope_context: Optional[Tuple[str, str]] = None,
                                    *options: object,
                                    **kwargs: object) -> DropFunctionRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = DropFunctionRequest(self._error_map,
                                  mgmt_operations.EVENTING_FUNCTION.value,
                                  eventing_function_mgmt_operations.DROP_FUNCTION.value,
                                  name=name,
                                  bucket_name=bucket_name,
                                  scope_name=scope_name,
                                  **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_functions_request(self,
                                        scope_context: Optional[Tuple[str, str]] = None,
                                        *options: object,
                                        **kwargs: object) -> GetAllFunctionsRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = GetAllFunctionsRequest(self._error_map,
                                     mgmt_operations.EVENTING_FUNCTION.value,
                                     eventing_function_mgmt_operations.GET_ALL_FUNCTIONS.value,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_function_request(self,
                                   name: str,
                                   scope_context: Optional[Tuple[str, str]] = None,
                                   *options: object,
                                   **kwargs: object) -> GetFunctionRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = GetFunctionRequest(self._error_map,
                                 mgmt_operations.EVENTING_FUNCTION.value,
                                 eventing_function_mgmt_operations.GET_FUNCTION.value,
                                 name=name,
                                 bucket_name=bucket_name,
                                 scope_name=scope_name,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_functions_status_request(self,
                                           scope_context: Optional[Tuple[str, str]] = None,
                                           *options: object,
                                           **kwargs: object) -> GetFunctionsStatusRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = GetFunctionsStatusRequest(self._error_map,
                                        mgmt_operations.EVENTING_FUNCTION.value,
                                        eventing_function_mgmt_operations.GET_STATUS.value,
                                        bucket_name=bucket_name,
                                        scope_name=scope_name,
                                        **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_pause_function_request(self,
                                     name: str,
                                     scope_context: Optional[Tuple[str, str]] = None,
                                     *options: object,
                                     **kwargs: object) -> PauseFunctionRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = PauseFunctionRequest(self._error_map,
                                   mgmt_operations.EVENTING_FUNCTION.value,
                                   eventing_function_mgmt_operations.PAUSE_FUNCTION.value,
                                   name=name,
                                   bucket_name=bucket_name,
                                   scope_name=scope_name,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_resume_function_request(self,
                                      name: str,
                                      scope_context: Optional[Tuple[str, str]] = None,
                                      *options: object,
                                      **kwargs: object) -> ResumeFunctionRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = ResumeFunctionRequest(self._error_map,
                                    mgmt_operations.EVENTING_FUNCTION.value,
                                    eventing_function_mgmt_operations.RESUME_FUNCTION.value,
                                    name=name,
                                    bucket_name=bucket_name,
                                    scope_name=scope_name,
                                    **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_undeploy_function_request(self,
                                        name: str,
                                        scope_context: Optional[Tuple[str, str]] = None,
                                        *options: object,
                                        **kwargs: object) -> UndeployFunctionRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = UndeployFunctionRequest(self._error_map,
                                      mgmt_operations.EVENTING_FUNCTION.value,
                                      eventing_function_mgmt_operations.UNDEPLOY_FUNCTION.value,
                                      name=name,
                                      bucket_name=bucket_name,
                                      scope_name=scope_name,
                                      **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_upsert_function_request(self,
                                      function: EventingFunction,
                                      scope_context: Optional[Tuple[str, str]] = None,
                                      *options: object,
                                      **kwargs: object) -> UpsertFunctionRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        req = UpsertFunctionRequest(self._error_map,
                                    mgmt_operations.EVENTING_FUNCTION.value,
                                    eventing_function_mgmt_operations.UPSERT_FUNCTION.value,
                                    eventing_function=function.as_dict(),
                                    bucket_name=bucket_name,
                                    scope_name=scope_name,
                                    **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req
