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

"""Query index test-infrastructure managers (cluster-level and collection-level).

Both wrappers mirror the SDK's `*options, **kwargs` tail exactly rather than naming
the optional parameters themselves.  Two reasons, both of which querymgmt_t depends on:

  * The suites exercise the options-object form and the keyword form of the same call
    as separate tests.  A wrapper that accepted only `**kwargs` would make the
    positional-options tests unwritable, and one that renamed a parameter (`fields`
    for the SDK's `keys`) would make the keyword tests fail for the wrong reason.
  * `test_create_index_no_fields` asserts that omitting the index keys raises
    TypeError.  Naming `keys` as a required parameter here would satisfy that from
    the wrapper's own signature, and the test would pass without ever reaching the
    SDK.  So `keys` is deliberately left in the `*args` tail.

Required positional parameters stop at the ones the SDK also requires and that this
wrapper genuinely needs to see.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tests_v2.environments.managers.base import TestEnvironmentManager
from tests_v2.test_features import ServerFeatures

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.queries import QueryIndex
    from tests_v2.environments.test_environment import TestEnvironment


class TestEnvironmentQueryIndexManager(TestEnvironmentManager):
    """Wraps couchbase.cluster.Cluster.query_indexes() (cluster-level)."""
    required_feature = ServerFeatures.QueryIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._qixm = env.cluster.query_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_all_indexes(self, bucket_name: str, *options: Any, **kwargs: Any) -> list[QueryIndex]:
        # list(), not the SDK's Iterable: the suites index and len() this result.
        return list(self._qixm.get_all_indexes(bucket_name, *options, **kwargs))

    def create_index(self, bucket_name: str, index_name: str, *args: Any, **kwargs: Any) -> None:
        self._qixm.create_index(bucket_name, index_name, *args, **kwargs)

    def create_primary_index(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._qixm.create_primary_index(bucket_name, *options, **kwargs)

    def drop_index(self, bucket_name: str, index_name: str, *options: Any, **kwargs: Any) -> None:
        self._qixm.drop_index(bucket_name, index_name, *options, **kwargs)

    def drop_primary_index(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._qixm.drop_primary_index(bucket_name, *options, **kwargs)

    def build_deferred_indexes(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._qixm.build_deferred_indexes(bucket_name, *options, **kwargs)

    def watch_indexes(self, bucket_name: str, index_names: Iterable[str], *options: Any, **kwargs: Any) -> None:
        self._qixm.watch_indexes(bucket_name, index_names, *options, **kwargs)


class TestEnvironmentCollectionQueryIndexManager(TestEnvironmentManager):
    """Wraps couchbase.collection.Collection.query_indexes() (collection-level)."""
    required_feature = ServerFeatures.QueryIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        # The environment is stored and the SDK manager resolved per call, rather than
        # env.collection.query_indexes() being captured once here.  Two reasons:
        # _setup_managers() runs before setup() provisions a named collection, so a
        # captured manager under CollectionStrategy.NAMED would address the default
        # collection for the whole run; and CollectionPool's current pointer can move
        # again mid-suite, which a collection-level manager has to follow.
        self._env = env

    def teardown(self, env: TestEnvironment) -> None:
        pass

    @property
    def _qixm(self) -> Any:
        # Any, not CollectionQueryIndexManager: self._env is the unparameterized
        # TestEnvironment, so .collection is Any and so is the manager it yields.
        # See TestEnvironmentManager's docstring on why that is deliberate.
        return self._env.collection.query_indexes()

    def get_all_indexes(self, *options: Any, **kwargs: Any) -> list[QueryIndex]:
        # list(), not the SDK's Iterable: the suites index and len() this result.
        return list(self._qixm.get_all_indexes(*options, **kwargs))

    def create_index(self, index_name: str, *args: Any, **kwargs: Any) -> None:
        self._qixm.create_index(index_name, *args, **kwargs)

    def create_primary_index(self, *options: Any, **kwargs: Any) -> None:
        self._qixm.create_primary_index(*options, **kwargs)

    def drop_index(self, index_name: str, *options: Any, **kwargs: Any) -> None:
        self._qixm.drop_index(index_name, *options, **kwargs)

    def drop_primary_index(self, *options: Any, **kwargs: Any) -> None:
        self._qixm.drop_primary_index(*options, **kwargs)

    def build_deferred_indexes(self, *options: Any, **kwargs: Any) -> None:
        self._qixm.build_deferred_indexes(*options, **kwargs)

    def watch_indexes(self, index_names: Iterable[str], *options: Any, **kwargs: Any) -> None:
        self._qixm.watch_indexes(index_names, *options, **kwargs)


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentQueryIndexManager(TestEnvironmentManager):
    """Wraps acouchbase.cluster.AsyncCluster.query_indexes() (cluster-level).

    Mirrors the sync wrapper's signature discipline exactly, including keeping `keys` in
    the `*args` tail so test_create_index_no_fields still reaches the SDK.  See the module
    docstring: those choices are load-bearing for querymgmt_t and they are not sync-specific.

    Two differences from the sync class, and both are forced rather than stylistic:

      * every method returns the delegate's awaitable.  The sync wrapper can drop the
        return value of create_index() because there is none; the async delegate returns a
        coroutine, and dropping it means the index is never created while the test carries
        on to assert that it exists.
      * get_all_indexes() awaits before list().  `list(coroutine)` is a TypeError, not a
        list of indexes, so this is the one method whose body genuinely cannot be shared.
    """
    required_feature = ServerFeatures.QueryIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._qixm = env.cluster.query_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def get_all_indexes(self, bucket_name: str, *options: Any, **kwargs: Any) -> list[QueryIndex]:
        # list(), not the SDK's Iterable: the suites index and len() this result.
        return list(await self._qixm.get_all_indexes(bucket_name, *options, **kwargs))

    async def create_index(self, bucket_name: str, index_name: str, *args: Any, **kwargs: Any) -> None:
        await self._qixm.create_index(bucket_name, index_name, *args, **kwargs)

    async def create_primary_index(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        await self._qixm.create_primary_index(bucket_name, *options, **kwargs)

    async def drop_index(self, bucket_name: str, index_name: str, *options: Any, **kwargs: Any) -> None:
        await self._qixm.drop_index(bucket_name, index_name, *options, **kwargs)

    async def drop_primary_index(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        await self._qixm.drop_primary_index(bucket_name, *options, **kwargs)

    async def build_deferred_indexes(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        await self._qixm.build_deferred_indexes(bucket_name, *options, **kwargs)

    async def watch_indexes(self, bucket_name: str, index_names: Iterable[str],
                            *options: Any, **kwargs: Any) -> None:
        await self._qixm.watch_indexes(bucket_name, index_names, *options, **kwargs)


class AsyncTestEnvironmentCollectionQueryIndexManager(TestEnvironmentManager):
    """Wraps acouchbase.collection.AsyncCollection.query_indexes() (collection-level)."""
    required_feature = ServerFeatures.QueryIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        # Resolved per call rather than captured here, for the same two reasons the sync
        # class gives: _setup_managers() runs before a named collection exists, and
        # CollectionPool's current pointer can move mid-suite.
        self._env = env

    def teardown(self, env: TestEnvironment) -> None:
        pass

    @property
    def _qixm(self) -> Any:
        return self._env.collection.query_indexes()

    async def get_all_indexes(self, *options: Any, **kwargs: Any) -> list[QueryIndex]:
        return list(await self._qixm.get_all_indexes(*options, **kwargs))

    async def create_index(self, index_name: str, *args: Any, **kwargs: Any) -> None:
        await self._qixm.create_index(index_name, *args, **kwargs)

    async def create_primary_index(self, *options: Any, **kwargs: Any) -> None:
        await self._qixm.create_primary_index(*options, **kwargs)

    async def drop_index(self, index_name: str, *options: Any, **kwargs: Any) -> None:
        await self._qixm.drop_index(index_name, *options, **kwargs)

    async def drop_primary_index(self, *options: Any, **kwargs: Any) -> None:
        await self._qixm.drop_primary_index(*options, **kwargs)

    async def build_deferred_indexes(self, *options: Any, **kwargs: Any) -> None:
        await self._qixm.build_deferred_indexes(*options, **kwargs)

    async def watch_indexes(self, index_names: Iterable[str], *options: Any, **kwargs: Any) -> None:
        await self._qixm.watch_indexes(index_names, *options, **kwargs)


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------

class TwistedTestEnvironmentQueryIndexManager(TestEnvironmentManager):
    """Wraps txcouchbase.cluster.Cluster.query_indexes(), dispatched via the reactor."""
    required_feature = ServerFeatures.QueryIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        from tests_v2.environments.twisted_environment import _get_run_in_reactor_thread
        self._qixm = env.cluster.query_indexes()
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_all_indexes(self, bucket_name: str, *options: Any, **kwargs: Any) -> list[QueryIndex]:
        return list(self._rit(self._qixm.get_all_indexes, bucket_name, *options, **kwargs))

    def create_index(self, bucket_name: str, index_name: str, *args: Any, **kwargs: Any) -> None:
        self._rit(self._qixm.create_index, bucket_name, index_name, *args, **kwargs)

    def create_primary_index(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._qixm.create_primary_index, bucket_name, *options, **kwargs)

    def drop_index(self, bucket_name: str, index_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._qixm.drop_index, bucket_name, index_name, *options, **kwargs)

    def drop_primary_index(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._qixm.drop_primary_index, bucket_name, *options, **kwargs)

    def build_deferred_indexes(self, bucket_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._qixm.build_deferred_indexes, bucket_name, *options, **kwargs)

    def watch_indexes(self, bucket_name: str, index_names: Iterable[str],
                      *options: Any, **kwargs: Any) -> None:
        self._rit(self._qixm.watch_indexes, bucket_name, index_names, *options, **kwargs)


# There is deliberately no TwistedTestEnvironmentCollectionQueryIndexManager.  The
# collection-level manager does not exist under Twisted: `couchbase/collection.py` and
# `acouchbase/collection.py` both define `query_indexes()` returning a
# CollectionQueryIndexManager, and `txcouchbase/collection.py` defines no such method.
# txcouchbase's only query_indexes() is the cluster-level one on Cluster, which the wrapper
# above drives.
#
# A wrapper for it would type-check, import, and generate a suite that passes on the mock,
# because GoCAVES has no query service and skips every collection-level test.  On a real
# cluster each of them dies at `self._env.collection.query_indexes()` with AttributeError.
# So there is no such wrapper here, not even an unreachable one: keeping one means the next
# retarget gets that AttributeError instead of a generator report.
#
# TXCB_NAME_MAP in transform.py still maps the sync name to this deleted one, on purpose.  A
# suite retargeted at txcouchbase without excluding its collection-level class rewrites the
# name, the resolver fails to find the class, and it is reported under MISSING API-SPECIFIC
# MANAGER CLASSES at --write time.  Removing the map entry instead would leave the sync name
# in place, which resolves, and the failure would move back to the cluster.
