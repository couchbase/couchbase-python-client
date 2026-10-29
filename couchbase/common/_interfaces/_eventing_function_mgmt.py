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

"""What a protocol's eventing function management impl must provide to the eventing function manager facades.

One impl serves both facades, `EventingFunctionManager` and `ScopeEventingFunctionManager`: the cluster impl builds it
with no scope context and the scope impl with its bucket and scope names.  txcouchbase offers no eventing function
manager, so there is no Twisted interface.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from couchbase.common.management.eventing import EventingFunction, EventingFunctionsStatus
    from couchbase.common.management.options import (DeployFunctionOptions,
                                                     DropFunctionOptions,
                                                     FunctionsStatusOptions,
                                                     GetAllFunctionOptions,
                                                     GetFunctionOptions,
                                                     PauseFunctionOptions,
                                                     ResumeFunctionOptions,
                                                     UndeployFunctionOptions,
                                                     UpsertFunctionOptions)


class EventingFunctionMgmtInterface(Protocol):
    def upsert_function(self,
                        function: EventingFunction,
                        options: UpsertFunctionOptions | None = None,
                        **kwargs: Any) -> None: ...

    def drop_function(self,
                      name: str,
                      options: DropFunctionOptions | None = None,
                      **kwargs: Any) -> None: ...

    def deploy_function(self,
                        name: str,
                        options: DeployFunctionOptions | None = None,
                        **kwargs: Any) -> None: ...

    def get_all_functions(self,
                          options: GetAllFunctionOptions | None = None,
                          **kwargs: Any) -> list[EventingFunction]: ...

    def get_function(self,
                     name: str,
                     options: GetFunctionOptions | None = None,
                     **kwargs: Any) -> EventingFunction: ...

    def pause_function(self,
                       name: str,
                       options: PauseFunctionOptions | None = None,
                       **kwargs: Any) -> None: ...

    def resume_function(self,
                        name: str,
                        options: ResumeFunctionOptions | None = None,
                        **kwargs: Any) -> None: ...

    def undeploy_function(self,
                          name: str,
                          options: UndeployFunctionOptions | None = None,
                          **kwargs: Any) -> None: ...

    def functions_status(self,
                         options: FunctionsStatusOptions | None = None,
                         **kwargs: Any) -> EventingFunctionsStatus: ...


class AsyncEventingFunctionMgmtInterface(Protocol):
    async def upsert_function(self,
                              function: EventingFunction,
                              options: UpsertFunctionOptions | None = None,
                              **kwargs: Any) -> None: ...

    async def drop_function(self,
                            name: str,
                            options: DropFunctionOptions | None = None,
                            **kwargs: Any) -> None: ...

    async def deploy_function(self,
                              name: str,
                              options: DeployFunctionOptions | None = None,
                              **kwargs: Any) -> None: ...

    async def get_all_functions(self,
                                options: GetAllFunctionOptions | None = None,
                                **kwargs: Any) -> list[EventingFunction]: ...

    async def get_function(self,
                           name: str,
                           options: GetFunctionOptions | None = None,
                           **kwargs: Any) -> EventingFunction: ...

    async def pause_function(self,
                             name: str,
                             options: PauseFunctionOptions | None = None,
                             **kwargs: Any) -> None: ...

    async def resume_function(self,
                              name: str,
                              options: ResumeFunctionOptions | None = None,
                              **kwargs: Any) -> None: ...

    async def undeploy_function(self,
                                name: str,
                                options: UndeployFunctionOptions | None = None,
                                **kwargs: Any) -> None: ...

    async def functions_status(self,
                               options: FunctionsStatusOptions | None = None,
                               **kwargs: Any) -> EventingFunctionsStatus: ...
