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

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
from couchbase.management.logic import ManagementType
from couchbase.management.logic.eventing_logic import (EventingFunction,
                                                       EventingFunctionManagerLogic,
                                                       EventingFunctionsStatus,
                                                       EventingFunctionStatus)

if TYPE_CHECKING:
    from couchbase.management.options import (DeployFunctionOptions,
                                              DropFunctionOptions,
                                              FunctionsStatusOptions,
                                              GetAllFunctionOptions,
                                              GetFunctionOptions,
                                              PauseFunctionOptions,
                                              ResumeFunctionOptions,
                                              UndeployFunctionOptions,
                                              UpsertFunctionOptions)


class EventingFunctionManager(EventingFunctionManagerLogic):

    def __init__(self, connection, loop):
        super().__init__(connection)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def upsert_function(
        self,
        function,  # type: EventingFunction
        *options,  # type: UpsertFunctionOptions
        **kwargs  # type: Dict[str, Any]
    ) -> None:
        super().upsert_function(function, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def drop_function(
        self,
        name,  # type: str
        *options,  # type: DropFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().drop_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def deploy_function(
        self,
        name,  # type: str
        *options,  # type: DeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().deploy_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(EventingFunction, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_all_functions(
        self,
        *options,  # type: GetAllFunctionOptions
        **kwargs  # type: Any
    ) -> List[EventingFunction]:
        super().get_all_functions(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(EventingFunction, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_function(
        self,
        name,  # type: str
        *options,  # type: GetFunctionOptions
        **kwargs  # type: Any
    ) -> EventingFunction:
        super().get_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def pause_function(
        self,
        name,  # type: str
        *options,  # type: PauseFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().pause_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def resume_function(
        self,
        name,  # type: str
        *options,  # type: ResumeFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().resume_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def undeploy_function(
        self,
        name,  # type: str
        *options,  # type: UndeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().undeploy_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(EventingFunctionsStatus, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def functions_status(
        self,
        *options,  # type: FunctionsStatusOptions
        **kwargs  # type: Any
    ) -> EventingFunctionsStatus:
        super().functions_status(*options, **kwargs)

    async def _get_status(
        self,
        name,  # type: str
    ) -> EventingFunctionStatus:

        statuses = await self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None


class ScopeEventingFunctionManager(EventingFunctionManagerLogic):

    def __init__(self,
                 connection,
                 loop,
                 bucket_name,  # type: str
                 scope_name,  # type: str
                 ):
        super().__init__(connection, bucket_name=bucket_name, scope_name=scope_name)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def upsert_function(
        self,
        function,  # type: EventingFunction
        *options,  # type: UpsertFunctionOptions
        **kwargs  # type: Dict[str, Any]
    ) -> None:
        super().upsert_function(function, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def drop_function(
        self,
        name,  # type: str
        *options,  # type: DropFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().drop_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def deploy_function(
        self,
        name,  # type: str
        *options,  # type: DeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().deploy_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(EventingFunction, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_all_functions(
        self,
        *options,  # type: GetAllFunctionOptions
        **kwargs  # type: Any
    ) -> List[EventingFunction]:
        super().get_all_functions(*options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(EventingFunction, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_function(
        self,
        name,  # type: str
        *options,  # type: GetFunctionOptions
        **kwargs  # type: Any
    ) -> EventingFunction:
        super().get_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def pause_function(
        self,
        name,  # type: str
        *options,  # type: PauseFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().pause_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def resume_function(
        self,
        name,  # type: str
        *options,  # type: ResumeFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().resume_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def undeploy_function(
        self,
        name,  # type: str
        *options,  # type: UndeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().undeploy_function(name, *options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(EventingFunctionsStatus, ManagementType.EventingFunctionMgmt,
                                       EventingFunctionManagerLogic._ERROR_MAPPING)
    def functions_status(
        self,
        *options,  # type: FunctionsStatusOptions
        **kwargs  # type: Any
    ) -> EventingFunctionsStatus:
        super().functions_status(*options, **kwargs)

    async def _get_status(
        self,
        name,  # type: str
    ) -> EventingFunctionStatus:

        statuses = await self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None
