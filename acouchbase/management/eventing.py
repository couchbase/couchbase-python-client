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

__all__ = [
    'EventingFunction',
    'EventingFunctionBucketAccess',
    'EventingFunctionBucketBinding',
    'EventingFunctionConstantBinding',
    'EventingFunctionDcpBoundary',
    'EventingFunctionDeploymentStatus',
    'EventingFunctionKeyspace',
    'EventingFunctionLanguageCompatibility',
    'EventingFunctionLogLevel',
    'EventingFunctionManager',
    'EventingFunctionProcessingStatus',
    'EventingFunctionSettings',
    'EventingFunctionState',
    'EventingFunctionStatus',
    'EventingFunctionUrlAuthBasic',
    'EventingFunctionUrlAuthBearer',
    'EventingFunctionUrlAuthDigest',
    'EventingFunctionUrlBinding',
    'EventingFunctionUrlNoAuth',
    'EventingFunctionsStatus',
    'ScopeEventingFunctionManager',
]

from typing import TYPE_CHECKING, Any

from couchbase.common.management.eventing import (EventingFunction,
                                                  EventingFunctionBucketAccess,
                                                  EventingFunctionBucketBinding,
                                                  EventingFunctionConstantBinding,
                                                  EventingFunctionDcpBoundary,
                                                  EventingFunctionDeploymentStatus,
                                                  EventingFunctionKeyspace,
                                                  EventingFunctionLanguageCompatibility,
                                                  EventingFunctionLogLevel,
                                                  EventingFunctionProcessingStatus,
                                                  EventingFunctionSettings,
                                                  EventingFunctionsStatus,
                                                  EventingFunctionState,
                                                  EventingFunctionStatus,
                                                  EventingFunctionUrlAuthBasic,
                                                  EventingFunctionUrlAuthBearer,
                                                  EventingFunctionUrlAuthDigest,
                                                  EventingFunctionUrlBinding,
                                                  EventingFunctionUrlNoAuth)

if TYPE_CHECKING:
    from couchbase.common._interfaces._eventing_function_mgmt import AsyncEventingFunctionMgmtInterface
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
    """Performs management operations on eventing functions.

    For managing the eventing functions of a scope, :class:`.ScopeEventingFunctionManager` should be used.
    """

    def __init__(self, impl: AsyncEventingFunctionMgmtInterface) -> None:
        self._impl = impl

    async def upsert_function(self,
                              function: EventingFunction,
                              options: UpsertFunctionOptions | None = None,
                              **kwargs: Any) -> None:
        """Creates an eventing function, or replaces one of the same name.

        Args:
            function (:class:`.EventingFunction`): The function to create or replace.
            options (:class:`~couchbase.management.options.UpsertFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the function is not an
                :class:`.EventingFunction`, has no name or no code, or carries a value the SDK cannot send.
            :class:`~couchbase.exceptions.EventingFunctionCompilationFailureException`: If the function's code does not
                compile.
            :class:`~couchbase.exceptions.EventingFunctionCollectionNotFoundException`: If a keyspace the function names
                does not exist.
            :class:`~couchbase.exceptions.EventingFunctionIdenticalKeyspaceException`: If the source and metadata
                keyspaces are the same.
        """
        await self._impl.upsert_function(function, options, **kwargs)

    async def drop_function(self,
                            name: str,
                            options: DropFunctionOptions | None = None,
                            **kwargs: Any) -> None:
        """Drops an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.DropFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotUnDeployedException`: If the function is deployed.
        """
        await self._impl.drop_function(name, options, **kwargs)

    async def deploy_function(self,
                              name: str,
                              options: DeployFunctionOptions | None = None,
                              **kwargs: Any) -> None:
        """Deploys an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.DeployFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionAlreadyDeployedException`: If the function is already
                deployed.
        """
        await self._impl.deploy_function(name, options, **kwargs)

    async def get_all_functions(self,
                                options: GetAllFunctionOptions | None = None,
                                **kwargs: Any) -> list[EventingFunction]:
        """Returns every eventing function.

        Args:
            options (:class:`~couchbase.management.options.GetAllFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Awaitable[list[:class:`.EventingFunction`]]: The functions.
        """
        return await self._impl.get_all_functions(options, **kwargs)

    async def get_function(self,
                           name: str,
                           options: GetFunctionOptions | None = None,
                           **kwargs: Any) -> EventingFunction:
        """Returns an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.GetFunctionOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Awaitable[:class:`.EventingFunction`]: The function.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
        """
        return await self._impl.get_function(name, options, **kwargs)

    async def pause_function(self,
                             name: str,
                             options: PauseFunctionOptions | None = None,
                             **kwargs: Any) -> None:
        """Pauses a deployed eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.PauseFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotDeployedException`: If the function is not deployed.
        """
        await self._impl.pause_function(name, options, **kwargs)

    async def resume_function(self,
                              name: str,
                              options: ResumeFunctionOptions | None = None,
                              **kwargs: Any) -> None:
        """Resumes a paused eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.ResumeFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotDeployedException`: If the function is not deployed.
        """
        await self._impl.resume_function(name, options, **kwargs)

    async def undeploy_function(self,
                                name: str,
                                options: UndeployFunctionOptions | None = None,
                                **kwargs: Any) -> None:
        """Undeploys an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.UndeployFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotDeployedException`: If the function is not deployed.
        """
        await self._impl.undeploy_function(name, options, **kwargs)

    async def functions_status(self,
                               options: FunctionsStatusOptions | None = None,
                               **kwargs: Any) -> EventingFunctionsStatus:
        """Returns the status of every eventing function.

        Args:
            options (:class:`~couchbase.management.options.FunctionsStatusOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Awaitable[:class:`.EventingFunctionsStatus`]: The status of each function.
        """
        return await self._impl.functions_status(options, **kwargs)


class ScopeEventingFunctionManager:
    """Performs management operations on the eventing functions of a scope."""

    def __init__(self, impl: AsyncEventingFunctionMgmtInterface) -> None:
        self._impl = impl

    async def upsert_function(self,
                              function: EventingFunction,
                              options: UpsertFunctionOptions | None = None,
                              **kwargs: Any) -> None:
        """Creates an eventing function, or replaces one of the same name.

        Args:
            function (:class:`.EventingFunction`): The function to create or replace.
            options (:class:`~couchbase.management.options.UpsertFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the function is not an
                :class:`.EventingFunction`, has no name or no code, or carries a value the SDK cannot send.
            :class:`~couchbase.exceptions.EventingFunctionCompilationFailureException`: If the function's code does not
                compile.
            :class:`~couchbase.exceptions.EventingFunctionCollectionNotFoundException`: If a keyspace the function names
                does not exist.
            :class:`~couchbase.exceptions.EventingFunctionIdenticalKeyspaceException`: If the source and metadata
                keyspaces are the same.
        """
        await self._impl.upsert_function(function, options, **kwargs)

    async def drop_function(self,
                            name: str,
                            options: DropFunctionOptions | None = None,
                            **kwargs: Any) -> None:
        """Drops an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.DropFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotUnDeployedException`: If the function is deployed.
        """
        await self._impl.drop_function(name, options, **kwargs)

    async def deploy_function(self,
                              name: str,
                              options: DeployFunctionOptions | None = None,
                              **kwargs: Any) -> None:
        """Deploys an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.DeployFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionAlreadyDeployedException`: If the function is already
                deployed.
        """
        await self._impl.deploy_function(name, options, **kwargs)

    async def get_all_functions(self,
                                options: GetAllFunctionOptions | None = None,
                                **kwargs: Any) -> list[EventingFunction]:
        """Returns every eventing function.

        Args:
            options (:class:`~couchbase.management.options.GetAllFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Awaitable[list[:class:`.EventingFunction`]]: The functions.
        """
        return await self._impl.get_all_functions(options, **kwargs)

    async def get_function(self,
                           name: str,
                           options: GetFunctionOptions | None = None,
                           **kwargs: Any) -> EventingFunction:
        """Returns an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.GetFunctionOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Awaitable[:class:`.EventingFunction`]: The function.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
        """
        return await self._impl.get_function(name, options, **kwargs)

    async def pause_function(self,
                             name: str,
                             options: PauseFunctionOptions | None = None,
                             **kwargs: Any) -> None:
        """Pauses a deployed eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.PauseFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotDeployedException`: If the function is not deployed.
        """
        await self._impl.pause_function(name, options, **kwargs)

    async def resume_function(self,
                              name: str,
                              options: ResumeFunctionOptions | None = None,
                              **kwargs: Any) -> None:
        """Resumes a paused eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.ResumeFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotDeployedException`: If the function is not deployed.
        """
        await self._impl.resume_function(name, options, **kwargs)

    async def undeploy_function(self,
                                name: str,
                                options: UndeployFunctionOptions | None = None,
                                **kwargs: Any) -> None:
        """Undeploys an eventing function.

        Args:
            name (str): The name of the eventing function.
            options (:class:`~couchbase.management.options.UndeployFunctionOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.EventingFunctionNotFoundException`: If the function does not exist.
            :class:`~couchbase.exceptions.EventingFunctionNotDeployedException`: If the function is not deployed.
        """
        await self._impl.undeploy_function(name, options, **kwargs)

    async def functions_status(self,
                               options: FunctionsStatusOptions | None = None,
                               **kwargs: Any) -> EventingFunctionsStatus:
        """Returns the status of every eventing function.

        Args:
            options (:class:`~couchbase.management.options.FunctionsStatusOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Awaitable[:class:`.EventingFunctionsStatus`]: The status of each function.
        """
        return await self._impl.functions_status(options, **kwargs)
