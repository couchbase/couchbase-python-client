#  Copyright 2016-2024. Couchbase, Inc.
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

"""Eventing function test-infrastructure managers, cluster-level and scope-level.

  TestEnvironmentEventingFunctionManager            - sync, cluster.eventing_functions()
  TestEnvironmentScopeEventingFunctionManager       - sync, scope.eventing_functions()
  AsyncTestEnvironmentEventingFunctionManager       - asyncio, cluster-level
  AsyncTestEnvironmentScopeEventingFunctionManager  - asyncio, scope-level

**Two api styles, not three.**  txcouchbase does not support eventing function
management: there is no `txcouchbase/management/eventing.py`, and neither `TxCluster` nor
`TxScope` has an `eventing_functions()` accessor.  So the suites generate for acouchbase only
and these four classes are the whole set.

All four mirror the SDK managers' nine public methods in the SDK's own declaration order,
each forwarding `*options` and `**kwargs`, because the suites drive both the options-object
and the keyword form.

**`get_function_status` is the tenth method and is not a mirror.**  Every wait loop needs
the state of one named function, and the SDK offers that only as `_get_status`, a private
name.  This is a five-line wrapper over the public `functions_status()`: fetch every status,
return the one whose name matches, return None if there is none.  It is the same answer, and
no suite reaches a private attribute to get it.

**The scope managers can be re-pointed, and the cluster managers cannot need to be.**  A
cluster-level manager hangs off the cluster, which exists before any suite runs.  A
scope-level one hangs off a scope, and the eventing suites create the bucket their scope
lives in, so that scope does not exist when the registry is built.  `use_scope()` re-points it
once it does, rather than a second registry entry: the registry is keyed by manager class, so
a second scope would otherwise need a second class whose only difference is which scope
setup() read.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tests_v2.environments.managers.base import TestEnvironmentManager
from tests_v2.test_features import ServerFeatures

if TYPE_CHECKING:
    from couchbase.management.eventing import (EventingFunction,
                                               EventingFunctionsStatus,
                                               EventingFunctionStatus)
    from tests_v2.environments.test_environment import TestEnvironment


# ---------------------------------------------------------------------------
# Sync (couchbase/)
# ---------------------------------------------------------------------------

class TestEnvironmentEventingFunctionManager(TestEnvironmentManager):
    """Mirrors couchbase.cluster.Cluster.eventing_functions(), cluster-level."""
    required_feature = ServerFeatures.EventingFunctionManagement

    def setup(self, env: TestEnvironment) -> None:
        self._efm = env.cluster.eventing_functions()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def upsert_function(self, function: EventingFunction, *options: Any, **kwargs: Any) -> None:
        self._efm.upsert_function(function, *options, **kwargs)

    def drop_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.drop_function(name, *options, **kwargs)

    def deploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.deploy_function(name, *options, **kwargs)

    def get_all_functions(self, *options: Any, **kwargs: Any) -> list[EventingFunction]:
        return self._efm.get_all_functions(*options, **kwargs)

    def get_function(self, name: str, *options: Any, **kwargs: Any) -> EventingFunction:
        return self._efm.get_function(name, *options, **kwargs)

    def pause_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.pause_function(name, *options, **kwargs)

    def resume_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.resume_function(name, *options, **kwargs)

    def undeploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.undeploy_function(name, *options, **kwargs)

    def functions_status(self, *options: Any, **kwargs: Any) -> EventingFunctionsStatus:
        return self._efm.functions_status(*options, **kwargs)

    def get_function_status(self, name: str) -> EventingFunctionStatus | None:
        """The status of one function, or None when this manager cannot see it.

        Not an SDK mirror; see the module docstring.  None covers both a manager holding no
        functions at all and one holding functions among which *name* is absent, which is what
        the wait loops need: a function that has just been dropped and one that was never
        created are the same answer to "what state is it in".
        """
        statuses = self.functions_status()
        if not statuses.functions:
            return None
        return next((f for f in statuses.functions if f.name == name), None)


class TestEnvironmentScopeEventingFunctionManager(TestEnvironmentManager):
    """Mirrors scope.eventing_functions(), scope-level (server >= 7.1)."""
    required_feature = ServerFeatures.ScopeEventingFunctionManagement

    def setup(self, env: TestEnvironment) -> None:
        self._env = env
        self._scope: Any = None

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def use_scope(self, scope: Any) -> None:
        """Manage functions through `scope` instead of the environment's current one.

        Called by the eventing environment after it creates the bucket the scope lives in; see
        the module docstring.  Untyped by the same rule the base class states for the wrapped
        manager itself: this takes whichever Scope class its api style has, and pinning it to
        one would be a lie for the other.
        """
        self._scope = scope

    @property
    def _efm(self) -> Any:
        # Resolved per call rather than captured in setup(), which is what lets use_scope()
        # above take effect on a manager the registry already built.  Any rather than
        # ScopeEventingFunctionManager: self._env is the unparameterized TestEnvironment, so
        # .scope is Any and so is the manager it yields.
        scope = self._scope or self._env.scope or self._env.default_scope
        return scope.eventing_functions()

    def upsert_function(self, function: EventingFunction, *options: Any, **kwargs: Any) -> None:
        self._efm.upsert_function(function, *options, **kwargs)

    def drop_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.drop_function(name, *options, **kwargs)

    def deploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.deploy_function(name, *options, **kwargs)

    def get_all_functions(self, *options: Any, **kwargs: Any) -> list[EventingFunction]:
        return self._efm.get_all_functions(*options, **kwargs)

    def get_function(self, name: str, *options: Any, **kwargs: Any) -> EventingFunction:
        return self._efm.get_function(name, *options, **kwargs)

    def pause_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.pause_function(name, *options, **kwargs)

    def resume_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.resume_function(name, *options, **kwargs)

    def undeploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        self._efm.undeploy_function(name, *options, **kwargs)

    def functions_status(self, *options: Any, **kwargs: Any) -> EventingFunctionsStatus:
        return self._efm.functions_status(*options, **kwargs)

    def get_function_status(self, name: str) -> EventingFunctionStatus | None:
        """The status of one function within this scope, or None; see the cluster-level twin."""
        statuses = self.functions_status()
        if not statuses.functions:
            return None
        return next((f for f in statuses.functions if f.name == name), None)


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentEventingFunctionManager(TestEnvironmentManager):
    """Mirrors acouchbase.cluster.AsyncCluster.eventing_functions(), cluster-level.

    setup() stays synchronous, matching the base class and every other manager in the package:
    TestEnvironment._setup_managers() calls it through an unawaited loop for both api styles,
    and cluster.eventing_functions() performs no I/O.  Only the forwarded calls are coroutines.
    """
    required_feature = ServerFeatures.EventingFunctionManagement

    def setup(self, env: TestEnvironment) -> None:
        self._efm = env.cluster.eventing_functions()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def upsert_function(self, function: EventingFunction, *options: Any, **kwargs: Any) -> None:
        await self._efm.upsert_function(function, *options, **kwargs)

    async def drop_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.drop_function(name, *options, **kwargs)

    async def deploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.deploy_function(name, *options, **kwargs)

    async def get_all_functions(self, *options: Any, **kwargs: Any) -> list[EventingFunction]:
        return await self._efm.get_all_functions(*options, **kwargs)

    async def get_function(self, name: str, *options: Any, **kwargs: Any) -> EventingFunction:
        return await self._efm.get_function(name, *options, **kwargs)

    async def pause_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.pause_function(name, *options, **kwargs)

    async def resume_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.resume_function(name, *options, **kwargs)

    async def undeploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.undeploy_function(name, *options, **kwargs)

    async def functions_status(self, *options: Any, **kwargs: Any) -> EventingFunctionsStatus:
        return await self._efm.functions_status(*options, **kwargs)

    async def get_function_status(self, name: str) -> EventingFunctionStatus | None:
        """The status of one function, or None; see the sync twin."""
        statuses = await self.functions_status()
        if not statuses.functions:
            return None
        return next((f for f in statuses.functions if f.name == name), None)


class AsyncTestEnvironmentScopeEventingFunctionManager(TestEnvironmentManager):
    """Mirrors acouchbase.scope.AsyncScope.eventing_functions(), scope-level (server >= 7.1)."""
    required_feature = ServerFeatures.ScopeEventingFunctionManagement

    def setup(self, env: TestEnvironment) -> None:
        self._env = env
        self._scope: Any = None

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def use_scope(self, scope: Any) -> None:
        """Manage functions through `scope`; see the sync twin."""
        self._scope = scope

    @property
    def _efm(self) -> Any:
        scope = self._scope or self._env.scope or self._env.default_scope
        return scope.eventing_functions()

    async def upsert_function(self, function: EventingFunction, *options: Any, **kwargs: Any) -> None:
        await self._efm.upsert_function(function, *options, **kwargs)

    async def drop_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.drop_function(name, *options, **kwargs)

    async def deploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.deploy_function(name, *options, **kwargs)

    async def get_all_functions(self, *options: Any, **kwargs: Any) -> list[EventingFunction]:
        return await self._efm.get_all_functions(*options, **kwargs)

    async def get_function(self, name: str, *options: Any, **kwargs: Any) -> EventingFunction:
        return await self._efm.get_function(name, *options, **kwargs)

    async def pause_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.pause_function(name, *options, **kwargs)

    async def resume_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.resume_function(name, *options, **kwargs)

    async def undeploy_function(self, name: str, *options: Any, **kwargs: Any) -> None:
        await self._efm.undeploy_function(name, *options, **kwargs)

    async def functions_status(self, *options: Any, **kwargs: Any) -> EventingFunctionsStatus:
        return await self._efm.functions_status(*options, **kwargs)

    async def get_function_status(self, name: str) -> EventingFunctionStatus | None:
        """The status of one function within this scope, or None; see the sync twin."""
        statuses = await self.functions_status()
        if not statuses.functions:
            return None
        return next((f for f in statuses.functions if f.name == name), None)
