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

from typing import (Any,
                    Dict,
                    List)

from couchbase.management.logic.eventing_logic import EventingFunctionBucketAccess  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionBucketBinding  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionConstantBinding  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionDcpBoundary  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionDeploymentStatus  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionKeyspace  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionLanguageCompatibility  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionLogLevel  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionProcessingStatus  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionSettings  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionState  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionStatus  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionUrlAuthBasic  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionUrlAuthBearer  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionUrlAuthDigest  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionUrlBinding  # noqa: F401
from couchbase.management.logic.eventing_logic import EventingFunctionUrlNoAuth  # noqa: F401
from couchbase.management.logic.eventing_logic import (EventingFunction,
                                                       EventingFunctionManagerLogic,
                                                       EventingFunctionsStatus)
from couchbase.management.logic.wrappers import BlockingMgmtWrapper, ManagementType

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


class EventingFunctionManager(EventingFunctionManagerLogic):
    def __init__(self, connection):
        super().__init__(connection)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def upsert_function(
        self,
        function,  # type: EventingFunction
        *options,  # type: UpsertFunctionOptions
        **kwargs  # type: Dict[str, Any]
    ) -> None:
        return super().upsert_function(function, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def drop_function(
        self,
        name,  # type: str
        *options,  # type: DropFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().drop_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def deploy_function(
        self,
        name,  # type: str
        *options,  # type: DeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().deploy_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(EventingFunction, ManagementType.EventingFunctionMgmt,
                               EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_all_functions(
        self,
        *options,  # type: GetAllFunctionOptions
        **kwargs  # type: Any
    ) -> List[EventingFunction]:
        return super().get_all_functions(*options, **kwargs)

    @BlockingMgmtWrapper.block(EventingFunction, ManagementType.EventingFunctionMgmt,
                               EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_function(
        self,
        name,  # type: str
        *options,  # type: GetFunctionOptions
        **kwargs  # type: Any
    ) -> EventingFunction:
        return super().get_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def pause_function(
        self,
        name,  # type: str
        *options,  # type: PauseFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().pause_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def resume_function(
        self,
        name,  # type: str
        *options,  # type: ResumeFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().resume_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def undeploy_function(
        self,
        name,  # type: str
        *options,  # type: UndeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().undeploy_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(EventingFunctionsStatus, ManagementType.EventingFunctionMgmt,
                               EventingFunctionManagerLogic._ERROR_MAPPING)
    def functions_status(
        self,
        *options,  # type: FunctionsStatusOptions
        **kwargs  # type: Any
    ) -> EventingFunctionsStatus:
        return super().functions_status(*options, **kwargs)

    def _get_status(
        self,
        name,  # type: str
    ) -> EventingFunctionStatus:

        statuses = self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None


class ScopeEventingFunctionManager(EventingFunctionManagerLogic):
    def __init__(self,
                 connection,
                 bucket_name,  # type: str
                 scope_name,  # type: str
                 ):
        super().__init__(connection, bucket_name=bucket_name, scope_name=scope_name)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def upsert_function(
        self,
        function,  # type: EventingFunction
        *options,  # type: UpsertFunctionOptions
        **kwargs  # type: Dict[str, Any]
    ) -> None:
        return super().upsert_function(function, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def drop_function(
        self,
        name,  # type: str
        *options,  # type: DropFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().drop_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def deploy_function(
        self,
        name,  # type: str
        *options,  # type: DeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().deploy_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(EventingFunction, ManagementType.EventingFunctionMgmt,
                               EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_all_functions(
        self,
        *options,  # type: GetAllFunctionOptions
        **kwargs  # type: Any
    ) -> List[EventingFunction]:
        return super().get_all_functions(*options, **kwargs)

    @BlockingMgmtWrapper.block(EventingFunction, ManagementType.EventingFunctionMgmt,
                               EventingFunctionManagerLogic._ERROR_MAPPING)
    def get_function(
        self,
        name,  # type: str
        *options,  # type: GetFunctionOptions
        **kwargs  # type: Any
    ) -> EventingFunction:
        return super().get_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def pause_function(
        self,
        name,  # type: str
        *options,  # type: PauseFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().pause_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def resume_function(
        self,
        name,  # type: str
        *options,  # type: ResumeFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().resume_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.EventingFunctionMgmt, EventingFunctionManagerLogic._ERROR_MAPPING)
    def undeploy_function(
        self,
        name,  # type: str
        *options,  # type: UndeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        return super().undeploy_function(name, *options, **kwargs)

    @BlockingMgmtWrapper.block(EventingFunctionsStatus, ManagementType.EventingFunctionMgmt,
                               EventingFunctionManagerLogic._ERROR_MAPPING)
    def functions_status(
        self,
        *options,  # type: FunctionsStatusOptions
        **kwargs  # type: Any
    ) -> EventingFunctionsStatus:
        return super().functions_status(*options, **kwargs)

    def _get_status(
        self,
        name,  # type: str
    ) -> EventingFunctionStatus:

        statuses = self.functions_status()

        if statuses.functions:
            return next((f for f in statuses.functions if f.name == name), None)

        return None
