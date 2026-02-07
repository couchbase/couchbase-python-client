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
                    List)

from acouchbase.management.logic.eventing_function_mgmt_impl import AsyncEventingFunctionMgmtImpl
from couchbase.management.logic.eventing_function_mgmt_types import (EventingFunction,
                                                                     EventingFunctionsStatus,
                                                                     EventingFunctionStatus)

if TYPE_CHECKING:
    from acouchbase.logic.client_adapter import AsyncClientAdapter
    from couchbase.management.options import (DeployFunctionOptions,
                                              DropFunctionOptions,
                                              FunctionsStatusOptions,
                                              GetAllFunctionOptions,
                                              GetFunctionOptions,
                                              PauseFunctionOptions,
                                              ResumeFunctionOptions,
                                              UndeployFunctionOptions,
                                              UpsertFunctionOptions)


class EventingFunctionManager:

    def __init__(self, client_adapter: AsyncClientAdapter) -> None:
        self._impl = AsyncEventingFunctionMgmtImpl(client_adapter)
        self._scope_context = None

    async def upsert_function(self,
                              function,  # type: EventingFunction
                              *options,  # type: UpsertFunctionOptions
                              **kwargs  # type: Any
                              ) -> None:
        req = self._impl.request_builder.build_upsert_function_request(function,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        await self._impl.upsert_function(req)

    async def drop_function(self,
                            name,  # type: str
                            *options,  # type: DropFunctionOptions
                            **kwargs  # type: Any
                            ) -> None:
        req = self._impl.request_builder.build_drop_function_request(name, self._scope_context, *options, **kwargs)
        await self._impl.drop_function(req)

    async def deploy_function(self,
                              name,  # type: str
                              *options,  # type: DeployFunctionOptions
                              **kwargs  # type: Any
                              ) -> None:
        req = self._impl.request_builder.build_deploy_function_request(name, self._scope_context, *options, **kwargs)
        await self._impl.deploy_function(req)

    async def get_all_functions(self,
                                *options,  # type: GetAllFunctionOptions
                                **kwargs  # type: Any
                                ) -> List[EventingFunction]:
        req = self._impl.request_builder.build_get_all_functions_request(self._scope_context, *options, **kwargs)
        return await self._impl.get_all_functions(req)

    async def get_function(self,
                           name,  # type: str
                           *options,  # type: GetFunctionOptions
                           **kwargs  # type: Any
                           ) -> EventingFunction:
        req = self._impl.request_builder.build_get_function_request(name, self._scope_context, *options, **kwargs)
        return await self._impl.get_function(req)

    async def pause_function(self,
                             name,  # type: str
                             *options,  # type: PauseFunctionOptions
                             **kwargs  # type: Any
                             ) -> None:
        req = self._impl.request_builder.build_pause_function_request(name, self._scope_context, *options, **kwargs)
        await self._impl.pause_function(req)

    async def resume_function(self,
                              name,  # type: str
                              *options,  # type: ResumeFunctionOptions
                              **kwargs  # type: Any
                              ) -> None:
        req = self._impl.request_builder.build_pause_function_request(name, self._scope_context, *options, **kwargs)
        await self._impl.pause_function(req)

    async def undeploy_function(self,
                                name,  # type: str
                                *options,  # type: UndeployFunctionOptions
                                **kwargs  # type: Any
                                ) -> None:
        req = self._impl.request_builder.build_undeploy_function_request(name, self._scope_context, *options, **kwargs)
        await self._impl.undeploy_function(req)

    async def functions_status(self,
                               *options,  # type: FunctionsStatusOptions
                               **kwargs  # type: Any
                               ) -> EventingFunctionsStatus:
        req = self._impl.request_builder.build_get_functions_status_request(self._scope_context, *options, **kwargs)
        return await self._impl.get_functions_status(req)

    async def _get_status(self, name: str) -> EventingFunctionStatus:
        statuses = await self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None


class ScopeEventingFunctionManager:

    def __init__(self, client_adapter: AsyncClientAdapter, bucket_name: str, scope_name: str) -> None:
        self._impl = AsyncEventingFunctionMgmtImpl(client_adapter)
        self._scope_context = bucket_name, scope_name

    async def upsert_function(self,
                              function,  # type: EventingFunction
                              *options,  # type: UpsertFunctionOptions
                              **kwargs  # type:  Any
                              ) -> None:
        req = self._impl.request_builder.build_upsert_function_request(function,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        await self._impl.upsert_function(req)

    async def drop_function(self,
                            name,  # type: str
                            *options,  # type: DropFunctionOptions
                            **kwargs  # type: Any
                            ) -> None:
        req = self._impl.request_builder.build_drop_function_request(name,
                                                                     self._scope_context,
                                                                     *options,
                                                                     **kwargs)
        await self._impl.drop_function(req)

    async def deploy_function(self,
                              name,  # type: str
                              *options,  # type: DeployFunctionOptions
                              **kwargs  # type: Any
                              ) -> None:
        req = self._impl.request_builder.build_deploy_function_request(name,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        await self._impl.deploy_function(req)

    async def get_all_functions(self,
                                *options,  # type: GetAllFunctionOptions
                                **kwargs  # type: Any
                                ) -> List[EventingFunction]:
        req = self._impl.request_builder.build_get_all_functions_request(self._scope_context,
                                                                         *options,
                                                                         **kwargs)
        return await self._impl.get_all_functions(req)

    async def get_function(self,
                           name,  # type: str
                           *options,  # type: GetFunctionOptions
                           **kwargs  # type: Any
                           ) -> EventingFunction:
        req = self._impl.request_builder.build_get_function_request(name,
                                                                    self._scope_context,
                                                                    *options,
                                                                    **kwargs)
        return await self._impl.get_function(req)

    async def pause_function(self,
                             name,  # type: str
                             *options,  # type: PauseFunctionOptions
                             **kwargs  # type: Any
                             ) -> None:
        req = self._impl.request_builder.build_pause_function_request(name,
                                                                      self._scope_context,
                                                                      *options,
                                                                      **kwargs)
        await self._impl.pause_function(req)

    async def resume_function(self,
                              name,  # type: str
                              *options,  # type: ResumeFunctionOptions
                              **kwargs  # type: Any
                              ) -> None:
        req = self._impl.request_builder.build_resume_function_request(name,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        await self._impl.resume_function(req)

    async def undeploy_function(self,
                                name,  # type: str
                                *options,  # type: UndeployFunctionOptions
                                **kwargs  # type: Any
                                ) -> None:
        req = self._impl.request_builder.build_undeploy_function_request(name,
                                                                         self._scope_context,
                                                                         *options,
                                                                         **kwargs)
        await self._impl.undeploy_function(req)

    async def functions_status(self,
                               *options,  # type: FunctionsStatusOptions
                               **kwargs  # type: Any
                               ) -> EventingFunctionsStatus:
        req = self._impl.request_builder.build_get_functions_status_request(self._scope_context,
                                                                            *options,
                                                                            **kwargs)
        return await self._impl.get_functions_status(req)

    async def _get_status(self, name: str) -> EventingFunctionStatus:
        statuses = await self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None
