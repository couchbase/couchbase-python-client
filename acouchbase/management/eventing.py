from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List)

from acouchbase.management.logic.wrappers import EventingFunctionMgmtWrapper
from couchbase.management.logic.eventing_logic import (EventingFunction,
                                                       EventingFunctionManagerLogic,
                                                       EventingFunctionsStatus)

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

    EventingFunctionMgmtWrapper.inject_callbacks(None, EventingFunctionManagerLogic._ERROR_MAPPING)

    def upsert_function(
        self,
        function,  # type: EventingFunction
        *options,  # type: UpsertFunctionOptions
        **kwargs  # type: Dict[str, Any]
    ) -> None:
        super().upsert_function(function, *options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(None, EventingFunctionManagerLogic._ERROR_MAPPING)

    def drop_function(
        self,
        name,  # type: str
        *options,  # type: DropFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().drop_function(name, *options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(None, EventingFunctionManagerLogic._ERROR_MAPPING)

    def deploy_function(
        self,
        name,  # type: str
        *options,  # type: DeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().deploy_function(name, *options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(EventingFunction, EventingFunctionManagerLogic._ERROR_MAPPING)

    def get_all_functions(
        self,
        *options,  # type: GetAllFunctionOptions
        **kwargs  # type: Any
    ) -> List[EventingFunction]:
        super().get_all_functions(*options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(EventingFunction, EventingFunctionManagerLogic._ERROR_MAPPING)

    def get_function(
        self,
        name,  # type: str
        *options,  # type: GetFunctionOptions
        **kwargs  # type: Any
    ) -> EventingFunction:
        super().get_function(name, *options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(None, EventingFunctionManagerLogic._ERROR_MAPPING)

    def pause_function(
        self,
        name,  # type: str
        *options,  # type: PauseFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().pause_function(name, *options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(None, EventingFunctionManagerLogic._ERROR_MAPPING)

    def resume_function(
        self,
        name,  # type: str
        *options,  # type: ResumeFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().resume_function(name, *options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(None, EventingFunctionManagerLogic._ERROR_MAPPING)

    def undeploy_function(
        self,
        name,  # type: str
        *options,  # type: UndeployFunctionOptions
        **kwargs  # type: Any
    ) -> None:
        super().undeploy_function(name, *options, **kwargs)

    EventingFunctionMgmtWrapper.inject_callbacks(EventingFunctionsStatus, EventingFunctionManagerLogic._ERROR_MAPPING)

    def functions_status(
        self,
        *options,  # type: FunctionsStatusOptions
        **kwargs  # type: Any
    ) -> EventingFunctionsStatus:
        super().functions_status(*options, **kwargs)
