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

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management.eventing import (EventingFunction,
                                                  EventingFunctionBucketAccess,
                                                  EventingFunctionDcpBoundary,
                                                  EventingFunctionDeploymentStatus,
                                                  EventingFunctionLanguageCompatibility,
                                                  EventingFunctionLogLevel,
                                                  EventingFunctionProcessingStatus)
from couchbase.common.management.options import (DeployFunctionOptions,
                                                 DropFunctionOptions,
                                                 FunctionsStatusOptions,
                                                 GetAllFunctionOptions,
                                                 GetFunctionOptions,
                                                 PauseFunctionOptions,
                                                 ResumeFunctionOptions,
                                                 UndeployFunctionOptions,
                                                 UpsertFunctionOptions)
from couchbase.common.option_enums import OptionValidationMode
from couchbase.protocol.classic._management._eventing_function_mgmt_types import (EVENTING_FUNCTION_MGMT_ERROR_MAP,
                                                                                  DeployFunctionRequest,
                                                                                  DropFunctionRequest,
                                                                                  EventingFunctionMgmtOptionsBlock,
                                                                                  GetAllFunctionsRequest,
                                                                                  GetFunctionRequest,
                                                                                  GetFunctionsStatusRequest,
                                                                                  PauseFunctionRequest,
                                                                                  ResumeFunctionRequest,
                                                                                  UndeployFunctionRequest,
                                                                                  UpsertFunctionRequest)
from couchbase.protocol.classic._wire import forward_args

# The settings a server read can leave as Unknown.  Such a value names something the server
# reported and the SDK cannot, and the binding would send its own default in its place.
_UNKNOWN_SETTINGS = {
    'dcp_stream_boundary': EventingFunctionDcpBoundary.Unknown,
    'deployment_status': EventingFunctionDeploymentStatus.Unknown,
    'processing_status': EventingFunctionProcessingStatus.Unknown,
    'language_compatibility': EventingFunctionLanguageCompatibility.Unknown,
    'log_level': EventingFunctionLogLevel.Unknown,
}


class EventingFunctionMgmtRequestBuilder:

    def __init__(self, option_validation: OptionValidationMode | None = None) -> None:
        self._error_map = EVENTING_FUNCTION_MGMT_ERROR_MAP
        self._option_validation = option_validation

    def _get_scope_context(self,
                           scope_context: tuple[str, str] | None = None) -> tuple[str | None, str | None]:
        if scope_context is not None:
            return scope_context[0], scope_context[1]

        return None, None

    def build_deploy_function_request(self,
                                      name: str,
                                      scope_context: tuple[str, str] | None,
                                      obs_handler: ObservableRequestHandler,
                                      options: DeployFunctionOptions | None = None,
                                      **kwargs: object) -> DeployFunctionRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.DeployFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return DeployFunctionRequest(self._error_map,
                                     name=name,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     timeout=final_args.pop('timeout', None))

    def build_drop_function_request(self,
                                    name: str,
                                    scope_context: tuple[str, str] | None,
                                    obs_handler: ObservableRequestHandler,
                                    options: DropFunctionOptions | None = None,
                                    **kwargs: object) -> DropFunctionRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.DropFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return DropFunctionRequest(self._error_map,
                                   name=name,
                                   bucket_name=bucket_name,
                                   scope_name=scope_name,
                                   timeout=final_args.pop('timeout', None))

    def build_get_all_functions_request(self,
                                        scope_context: tuple[str, str] | None,
                                        obs_handler: ObservableRequestHandler,
                                        options: GetAllFunctionOptions | None = None,
                                        **kwargs: object) -> GetAllFunctionsRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.GetAllFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return GetAllFunctionsRequest(self._error_map,
                                      bucket_name=bucket_name,
                                      scope_name=scope_name,
                                      timeout=final_args.pop('timeout', None))

    def build_get_function_request(self,
                                   name: str,
                                   scope_context: tuple[str, str] | None,
                                   obs_handler: ObservableRequestHandler,
                                   options: GetFunctionOptions | None = None,
                                   **kwargs: object) -> GetFunctionRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.GetFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return GetFunctionRequest(self._error_map,
                                  name=name,
                                  bucket_name=bucket_name,
                                  scope_name=scope_name,
                                  timeout=final_args.pop('timeout', None))

    def build_get_functions_status_request(self,
                                           scope_context: tuple[str, str] | None,
                                           obs_handler: ObservableRequestHandler,
                                           options: FunctionsStatusOptions | None = None,
                                           **kwargs: object) -> GetFunctionsStatusRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.FunctionsStatus, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return GetFunctionsStatusRequest(self._error_map,
                                         bucket_name=bucket_name,
                                         scope_name=scope_name,
                                         timeout=final_args.pop('timeout', None))

    def build_pause_function_request(self,
                                     name: str,
                                     scope_context: tuple[str, str] | None,
                                     obs_handler: ObservableRequestHandler,
                                     options: PauseFunctionOptions | None = None,
                                     **kwargs: object) -> PauseFunctionRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.PauseFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return PauseFunctionRequest(self._error_map,
                                    name=name,
                                    bucket_name=bucket_name,
                                    scope_name=scope_name,
                                    timeout=final_args.pop('timeout', None))

    def build_resume_function_request(self,
                                      name: str,
                                      scope_context: tuple[str, str] | None,
                                      obs_handler: ObservableRequestHandler,
                                      options: ResumeFunctionOptions | None = None,
                                      **kwargs: object) -> ResumeFunctionRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.ResumeFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return ResumeFunctionRequest(self._error_map,
                                     name=name,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     timeout=final_args.pop('timeout', None))

    def build_undeploy_function_request(self,
                                        name: str,
                                        scope_context: tuple[str, str] | None,
                                        obs_handler: ObservableRequestHandler,
                                        options: UndeployFunctionOptions | None = None,
                                        **kwargs: object) -> UndeployFunctionRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.UndeployFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        return UndeployFunctionRequest(self._error_map,
                                       name=name,
                                       bucket_name=bucket_name,
                                       scope_name=scope_name,
                                       timeout=final_args.pop('timeout', None))

    @staticmethod
    def _validate_function(function: EventingFunction) -> None:
        if not isinstance(function, EventingFunction):
            raise InvalidArgumentException('An EventingFunction must be provided when upserting an eventing function.')
        if not function.name:
            raise InvalidArgumentException('The EventingFunction must have a name.')
        if not function.code:
            raise InvalidArgumentException('The EventingFunction must have code.')
        unknown = [f'settings.{name}' for name, value in _UNKNOWN_SETTINGS.items()
                   if function.settings is not None and getattr(function.settings, name) is value]
        unknown += [f'bucket_bindings[{i}].access' for i, binding in enumerate(function.bucket_bindings or [])
                    if binding.access is EventingFunctionBucketAccess.Unknown]
        if unknown:
            raise InvalidArgumentException(
                f'The EventingFunction has a value the SDK cannot send: {", ".join(unknown)}.')

    def build_upsert_function_request(self,
                                      function: EventingFunction,
                                      scope_context: tuple[str, str] | None,
                                      obs_handler: ObservableRequestHandler,
                                      options: UpsertFunctionOptions | None = None,
                                      **kwargs: object) -> UpsertFunctionRequest:
        final_args = forward_args(
            kwargs, options, block=EventingFunctionMgmtOptionsBlock.UpsertFunction, mode=self._option_validation)
        bucket_name, scope_name = self._get_scope_context(scope_context)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name, scope_name=scope_name)
        self._validate_function(function)
        return UpsertFunctionRequest(self._error_map,
                                     function=function.as_dict(),
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     timeout=final_args.pop('timeout', None))
