#  Copyright 2016-2022. Couchbase, Inc.
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
                    Any,
                    Dict,
                    List)

from couchbase.management.logic.eventing_function_mgmt_impl import EventingFunctionMgmtImpl
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionBucketAccess  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionBucketBinding  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionConstantBinding  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionDcpBoundary  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionDeploymentStatus  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionKeyspace  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionLanguageCompatibility  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionLogLevel  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionProcessingStatus  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionSettings  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionState  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionUrlAuthBasic  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionUrlAuthBearer  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionUrlAuthDigest  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionUrlBinding  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import EventingFunctionUrlNoAuth  # noqa: F401
from couchbase.management.logic.eventing_function_mgmt_types import (EventingFunction,
                                                                     EventingFunctionsStatus,
                                                                     EventingFunctionStatus)

# @TODO:  lets deprecate import of options from couchbase.management.eventing
from couchbase.management.options import (DeployFunctionOptions,
                                          DropFunctionOptions,
                                          FunctionsStatusOptions,
                                          GetAllFunctionOptions,
                                          GetFunctionOptions,
                                          PauseFunctionOptions,
                                          ResumeFunctionOptions,
                                          UndeployFunctionOptions,
                                          UpsertFunctionOptions)

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter


class EventingFunctionManager:

    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._impl = EventingFunctionMgmtImpl(client_adapter)
        self._scope_context = None

    def upsert_function(self,
                        function,  # type: EventingFunction
                        *options,  # type: UpsertFunctionOptions
                        **kwargs  # type: Any
                        ) -> None:
        req = self._impl.request_builder.build_upsert_function_request(function,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        self._impl.upsert_function(req)

    def drop_function(self,
                      name,  # type: str
                      *options,  # type: DropFunctionOptions
                      **kwargs  # type: Any
                      ) -> None:
        req = self._impl.request_builder.build_drop_function_request(name, self._scope_context, *options, **kwargs)
        self._impl.drop_function(req)

    def deploy_function(self,
                        name,  # type: str
                        *options,  # type: DeployFunctionOptions
                        **kwargs  # type: Any
                        ) -> None:
        req = self._impl.request_builder.build_deploy_function_request(name, self._scope_context, *options, **kwargs)
        self._impl.deploy_function(req)

    def get_all_functions(self,
                          *options,  # type: GetAllFunctionOptions
                          **kwargs  # type: Any
                          ) -> List[EventingFunction]:
        req = self._impl.request_builder.build_get_all_functions_request(self._scope_context, *options, **kwargs)
        return self._impl.get_all_functions(req)

    def get_function(self,
                     name,  # type: str
                     *options,  # type: GetFunctionOptions
                     **kwargs  # type: Any
                     ) -> EventingFunction:
        req = self._impl.request_builder.build_get_function_request(name, self._scope_context, *options, **kwargs)
        return self._impl.get_function(req)

    def pause_function(self,
                       name,  # type: str
                       *options,  # type: PauseFunctionOptions
                       **kwargs  # type: Any
                       ) -> None:
        req = self._impl.request_builder.build_pause_function_request(name, self._scope_context, *options, **kwargs)
        self._impl.pause_function(req)

    def resume_function(self,
                        name,  # type: str
                        *options,  # type: ResumeFunctionOptions
                        **kwargs  # type: Any
                        ) -> None:
        req = self._impl.request_builder.build_resume_function_request(name, self._scope_context, *options, **kwargs)
        self._impl.resume_function(req)

    def undeploy_function(self,
                          name,  # type: str
                          *options,  # type: UndeployFunctionOptions
                          **kwargs  # type: Any
                          ) -> None:
        req = self._impl.request_builder.build_undeploy_function_request(name, self._scope_context, *options, **kwargs)
        self._impl.undeploy_function(req)

    def functions_status(self,
                         *options,  # type: FunctionsStatusOptions
                         **kwargs  # type: Any
                         ) -> EventingFunctionsStatus:
        req = self._impl.request_builder.build_get_functions_status_request(self._scope_context, *options, **kwargs)
        return self._impl.get_functions_status(req)

    def _get_status(self, name: str) -> EventingFunctionStatus:
        statuses = self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None


class ScopeEventingFunctionManager:

    def __init__(self, client_adapter: ClientAdapter, bucket_name: str, scope_name: str) -> None:
        self._impl = EventingFunctionMgmtImpl(client_adapter)
        self._scope_context = bucket_name, scope_name

    def upsert_function(self,
                        function,  # type: EventingFunction
                        *options,  # type: UpsertFunctionOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> None:
        req = self._impl.request_builder.build_upsert_function_request(function,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        self._impl.upsert_function(req)

    def drop_function(self,
                      name,  # type: str
                      *options,  # type: DropFunctionOptions
                      **kwargs  # type: Any
                      ) -> None:
        req = self._impl.request_builder.build_drop_function_request(name,
                                                                     self._scope_context,
                                                                     *options,
                                                                     **kwargs)
        self._impl.drop_function(req)

    def deploy_function(self,
                        name,  # type: str
                        *options,  # type: DeployFunctionOptions
                        **kwargs  # type: Any
                        ) -> None:
        req = self._impl.request_builder.build_deploy_function_request(name,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        self._impl.deploy_function(req)

    def get_all_functions(self,
                          *options,  # type: GetAllFunctionOptions
                          **kwargs  # type: Any
                          ) -> List[EventingFunction]:
        req = self._impl.request_builder.build_get_all_functions_request(self._scope_context,
                                                                         *options,
                                                                         **kwargs)
        return self._impl.get_all_functions(req)

    def get_function(self,
                     name,  # type: str
                     *options,  # type: GetFunctionOptions
                     **kwargs  # type: Any
                     ) -> EventingFunction:
        req = self._impl.request_builder.build_get_function_request(name,
                                                                    self._scope_context,
                                                                    *options,
                                                                    **kwargs)
        return self._impl.get_function(req)

    def pause_function(self,
                       name,  # type: str
                       *options,  # type: PauseFunctionOptions
                       **kwargs  # type: Any
                       ) -> None:
        req = self._impl.request_builder.build_pause_function_request(name,
                                                                      self._scope_context,
                                                                      *options,
                                                                      **kwargs)
        self._impl.pause_function(req)

    def resume_function(self,
                        name,  # type: str
                        *options,  # type: ResumeFunctionOptions
                        **kwargs  # type: Any
                        ) -> None:
        req = self._impl.request_builder.build_resume_function_request(name,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        self._impl.resume_function(req)

    def undeploy_function(self,
                          name,  # type: str
                          *options,  # type: UndeployFunctionOptions
                          **kwargs  # type: Any
                          ) -> None:
        req = self._impl.request_builder.build_undeploy_function_request(name,
                                                                         self._scope_context,
                                                                         *options,
                                                                         **kwargs)
        self._impl.undeploy_function(req)

    def functions_status(self,
                         *options,  # type: FunctionsStatusOptions
                         **kwargs  # type: Any
                         ) -> EventingFunctionsStatus:
        req = self._impl.request_builder.build_get_functions_status_request(self._scope_context,
                                                                            *options,
                                                                            **kwargs)
        return self._impl.get_functions_status(req)

    def _get_status(self, name: str) -> EventingFunctionStatus:
        statuses = self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None
