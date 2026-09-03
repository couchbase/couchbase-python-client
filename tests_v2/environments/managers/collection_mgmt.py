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

"""Collection *management* test-infrastructure managers, one per api style.

The second wrapper around ``bucket.collections()``, and the reason there are two is that they
have opposite jobs.  ``managers/collection.py`` provisions: it hands every NAMED suite a scope
and a collection to work in, and exposes nothing that looks like the SDK.  This one is the
mirror, driven by the collection management suite, whose subject *is* that SDK surface.

**They stay apart because this one moves and that one must not.**  The provisioning manager is
registered by every NAMED suite in the tree and is bound to the shared test bucket for the whole
run.  A management suite cannot work there: it creates and drops scopes and collections by the
dozen, and ``get_all_scopes`` has to be able to count what is on the bucket and get an answer
that belongs to this suite alone.  So it creates a bucket of its own and points this manager at
it with :meth:`use_bucket`.  Merging the two classes would put that re-pointing one attribute
away from every NAMED suite in the tree, for no gain beyond one fewer class.

**The create/drop signatures are variadic on purpose**, where every other wrapper in the package
annotates with the sync SDK's parameter types.  ``create_collection`` and ``drop_collection`` are
each three overloads, one of them deprecated, and the suite drives all of them plus a dozen
malformed combinations it expects ``InvalidArgumentException`` for.  A typed wrapper would make
the tests that matter most here unwritable.  ``*args``/``**kwargs`` matches how the SDK itself
spells the dispatcher underneath the overloads, which is the honest thing for a passthrough to
say.  The rest of the surface has one signature and is annotated normally.

**A suite cannot reach the raw SDK manager.**  A local fixture returning
``cb_env.bucket.collections()`` reads like the direct way to test a manager API and cannot work:
acouchbase and txcouchbase suites are generated from the sync source, and the generator resolves
what to await from a class model that holds the framework wrappers and no SDK manager class at
all.  A raw manager in a fixture resolves to nothing, so its calls generate unawaited.  Going
through a wrapper is not a preference here, it is the only thing that works.

**Cross-bucket work names its bucket rather than re-pointing.**  Four tests create a bucket with
a specific storage backend, because history retention is a Magma feature, and one test asserts a
collection did *not* appear on the shared bucket.  Each of those takes a ``..._in_bucket`` method
naming the bucket it means.  The alternative, ``use_bucket`` there and back, leaves the manager
pointing at a dropped bucket whenever a test fails in between, and the test that then fails is
the next one.

The scope and collection name pools live here too.  They are the same idea as the bucket name
pool on TestEnvironment: a suite that creates named things needs names nothing else is using,
including its own earlier rerun.  They sit on the manager rather than the environment because
this suite is their only consumer, where the bucket pool has two.  Each manager *holds* a pool
object and forwards to it, rather than inheriting one alongside TestEnvironmentManager, so
that a manager has one base class and the pools stay something a test can construct and
exercise on its own.
"""

from __future__ import annotations

import random
import uuid
from typing import TYPE_CHECKING, Any

from tests_v2.environments.managers.base import TestEnvironmentManager
from tests_v2.test_features import ServerFeatures

if TYPE_CHECKING:
    from couchbase.management.collections import (CollectionSpec,
                                                  CreateCollectionSettings,
                                                  ScopeSpec,
                                                  UpdateCollectionSettings)
    from tests_v2.environments.test_environment import TestEnvironment


class _CollectionMgmtNamePools:
    """Scope and collection name pools, one per manager.

    Not a manager and not registered as one; it holds no cluster state and does no I/O.  Names
    are drawn from a batch stamped with a fresh uuid per suite run, which is what keeps a rerun
    of a failed test from colliding with the scope the first attempt left behind.
    """

    def __init__(self, num_ids: int = 50) -> None:
        batch_id = str(uuid.uuid4())[:8]
        self._scope_ids = [f'{batch_id}_scope_{i}' for i in range(num_ids)]
        self._collection_ids = [f'{batch_id}_collection_{i}' for i in range(num_ids)]
        self._used_scope_ids: set[str] = set()
        self._dropped_scope_ids: set[str] = set()
        self._used_collection_ids: set[str] = set()

    def get_scope_name(self) -> str:
        available = set(self._scope_ids).difference(self._used_scope_ids)
        name = random.choice(list(available))
        self._used_scope_ids.add(name)
        return name

    def get_collection_name(self) -> str:
        available = set(self._collection_ids).difference(self._used_collection_ids)
        name = random.choice(list(available))
        self._used_collection_ids.add(name)
        return name

    def get_scope_names(self) -> list[str]:
        """The scopes this suite has created and not dropped."""
        return list(self._used_scope_ids.difference(self._dropped_scope_ids))

    def add_dropped_scope(self, scope_name: str) -> None:
        self._dropped_scope_ids.add(scope_name)


class _CollectionMgmtManagerBase(TestEnvironmentManager):
    """What the three api variants share: a name pool, and nothing that reaches a cluster.

    The four forwarding methods are here rather than repeated in each variant.  They are what
    the suite calls, and the pool is reached through them rather than exposed as
    `mgr.names.get_scope_name()`: the generator resolves a call from the class model it built
    for the manager, and a second object hop lands on a class it has no model for, which
    generates the acouchbase call unawaited.
    """
    required_feature = ServerFeatures.Collections

    def __init__(self) -> None:
        self._names = _CollectionMgmtNamePools()

    def get_scope_name(self) -> str:
        return self._names.get_scope_name()

    def get_collection_name(self) -> str:
        return self._names.get_collection_name()

    def get_scope_names(self) -> list[str]:
        """The scopes this suite has created and not dropped."""
        return self._names.get_scope_names()

    def add_dropped_scope(self, scope_name: str) -> None:
        self._names.add_dropped_scope(scope_name)


# ---------------------------------------------------------------------------
# Sync (couchbase/)
# ---------------------------------------------------------------------------

class TestEnvironmentCollectionMgmtManager(_CollectionMgmtManagerBase):
    """Mirrors couchbase.management.collections.CollectionManager."""

    def setup(self, env: TestEnvironment) -> None:
        self._cluster = env.cluster
        self._cm = env.bucket.collections()
        self._bucket_name: str = env.bucket.name
        self._other_cms: dict[str, Any] = {}

    def teardown(self, env: TestEnvironment) -> None:
        pass  # the suite drops the bucket it owns, and everything in it goes with it

    def use_bucket(self, bucket_name: str) -> Any:
        """Manage `bucket_name` from here on, and return the opened Bucket.

        The caller creates the bucket and waits for the cluster to report it before calling
        this, which is what `env.create_bucket()` does and what both call sites go through.
        There is no check for it here because there is nothing this class could add: opening a
        bucket the cluster does not have fails inside the SDK, naming the bucket, on the line
        below.

        The Bucket comes back because the suite needs one: two tests write a document into a
        collection they just created to watch its max_expiry take effect, and that is a KV
        operation on a real Bucket object rather than anything a manager can do.  The suite
        hands it to cb_env.set_collection_mgmt_bucket().
        """
        bucket = self._cluster.bucket(bucket_name)
        self._cm = bucket.collections()
        self._bucket_name = bucket_name
        return bucket

    def _cm_for(self, bucket_name: str) -> Any:
        """A CollectionManager for `bucket_name`, opening the bucket the first time."""
        if bucket_name not in self._other_cms:
            self._other_cms[bucket_name] = self._cluster.bucket(bucket_name).collections()
        return self._other_cms[bucket_name]

    # -- the SDK surface, on the bucket this manager is pointed at ----------

    def create_scope(self, scope_name: str, *options: Any, **kwargs: Any) -> None:
        self._cm.create_scope(scope_name, *options, **kwargs)

    def drop_scope(self, scope_name: str, *options: Any, **kwargs: Any) -> None:
        self._cm.drop_scope(scope_name, *options, **kwargs)

    def get_all_scopes(self, *options: Any, **kwargs: Any) -> list[ScopeSpec]:
        return self._cm.get_all_scopes(*options, **kwargs)

    def create_collection(self, *args: Any, **kwargs: Any) -> None:
        self._cm.create_collection(*args, **kwargs)

    def drop_collection(self, *args: Any, **kwargs: Any) -> None:
        self._cm.drop_collection(*args, **kwargs)

    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          *options: Any, **kwargs: Any) -> None:
        self._cm.update_collection(scope_name, collection_name, settings, *options, **kwargs)

    # -- lookups -----------------------------------------------------------

    def get_scope(self, scope_name: str) -> ScopeSpec | None:
        return next((s for s in self._cm.get_all_scopes() if s.name == scope_name), None)

    def get_collection(self, scope_name: str, collection_name: str) -> CollectionSpec | None:
        scope = self.get_scope(scope_name)
        if scope is None:
            return None
        return next((c for c in scope.collections if c.name == collection_name), None)

    # -- the same three, against a bucket named at the call site -----------

    def get_collection_in_bucket(self,
                                 bucket_name: str,
                                 scope_name: str,
                                 collection_name: str) -> CollectionSpec | None:
        scopes = self._cm_for(bucket_name).get_all_scopes()
        scope = next((s for s in scopes if s.name == scope_name), None)
        if scope is None:
            return None
        return next((c for c in scope.collections if c.name == collection_name), None)

    def create_collection_in_bucket(self,
                                    bucket_name: str,
                                    scope_name: str,
                                    collection_name: str,
                                    settings: CreateCollectionSettings | None = None) -> None:
        self._cm_for(bucket_name).create_collection(scope_name, collection_name, settings)

    def update_collection_in_bucket(self,
                                    bucket_name: str,
                                    scope_name: str,
                                    collection_name: str,
                                    settings: UpdateCollectionSettings) -> None:
        self._cm_for(bucket_name).update_collection(scope_name, collection_name, settings)


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentCollectionMgmtManager(_CollectionMgmtManagerBase):
    """Mirrors acouchbase.management.collections.CollectionManager.

    setup() stays synchronous, matching the base class and every other manager here:
    TestEnvironment._setup_managers() calls it through an unawaited loop for all three api
    styles, and cluster.bucket()/bucket.collections() perform no I/O.  Opening a *second*
    bucket does, because acouchbase needs `await bucket.on_connect()` before the bucket is
    usable, which is why use_bucket() and _cm_for() are coroutines here and are not above.
    """

    def setup(self, env: TestEnvironment) -> None:
        self._cluster = env.cluster
        self._cm = env.bucket.collections()
        self._bucket_name: str = env.bucket.name
        self._other_cms: dict[str, Any] = {}

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def use_bucket(self, bucket_name: str) -> Any:
        bucket = self._cluster.bucket(bucket_name)
        await bucket.on_connect()
        self._cm = bucket.collections()
        self._bucket_name = bucket_name
        return bucket

    async def _cm_for(self, bucket_name: str) -> Any:
        if bucket_name not in self._other_cms:
            bucket = self._cluster.bucket(bucket_name)
            await bucket.on_connect()
            self._other_cms[bucket_name] = bucket.collections()
        return self._other_cms[bucket_name]

    async def create_scope(self, scope_name: str, *options: Any, **kwargs: Any) -> None:
        await self._cm.create_scope(scope_name, *options, **kwargs)

    async def drop_scope(self, scope_name: str, *options: Any, **kwargs: Any) -> None:
        await self._cm.drop_scope(scope_name, *options, **kwargs)

    async def get_all_scopes(self, *options: Any, **kwargs: Any) -> list[ScopeSpec]:
        return await self._cm.get_all_scopes(*options, **kwargs)

    async def create_collection(self, *args: Any, **kwargs: Any) -> None:
        await self._cm.create_collection(*args, **kwargs)

    async def drop_collection(self, *args: Any, **kwargs: Any) -> None:
        await self._cm.drop_collection(*args, **kwargs)

    async def update_collection(self,
                                scope_name: str,
                                collection_name: str,
                                settings: UpdateCollectionSettings,
                                *options: Any, **kwargs: Any) -> None:
        await self._cm.update_collection(scope_name, collection_name, settings, *options, **kwargs)

    async def get_scope(self, scope_name: str) -> ScopeSpec | None:
        scopes = await self._cm.get_all_scopes()
        return next((s for s in scopes if s.name == scope_name), None)

    async def get_collection(self, scope_name: str, collection_name: str) -> CollectionSpec | None:
        scope = await self.get_scope(scope_name)
        if scope is None:
            return None
        return next((c for c in scope.collections if c.name == collection_name), None)

    async def get_collection_in_bucket(self,
                                       bucket_name: str,
                                       scope_name: str,
                                       collection_name: str) -> CollectionSpec | None:
        cm = await self._cm_for(bucket_name)
        scopes = await cm.get_all_scopes()
        scope = next((s for s in scopes if s.name == scope_name), None)
        if scope is None:
            return None
        return next((c for c in scope.collections if c.name == collection_name), None)

    async def create_collection_in_bucket(self,
                                          bucket_name: str,
                                          scope_name: str,
                                          collection_name: str,
                                          settings: CreateCollectionSettings | None = None) -> None:
        cm = await self._cm_for(bucket_name)
        await cm.create_collection(scope_name, collection_name, settings)

    async def update_collection_in_bucket(self,
                                          bucket_name: str,
                                          scope_name: str,
                                          collection_name: str,
                                          settings: UpdateCollectionSettings) -> None:
        cm = await self._cm_for(bucket_name)
        await cm.update_collection(scope_name, collection_name, settings)


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------

class TwistedTestEnvironmentCollectionMgmtManager(_CollectionMgmtManagerBase):
    """Mirrors txcouchbase.management.collections.CollectionManager, via the reactor thread.

    Every txcouchbase manager call returns a Deferred, so each one goes through
    run_in_reactor_thread(), which blocks the pytest thread until it resolves.  That is what
    lets these keep the sync signatures while the async variant above does not.
    """

    def setup(self, env: TestEnvironment) -> None:
        from tests_v2.environments.twisted_environment import _get_run_in_reactor_thread
        self._cluster = env.cluster
        self._cm = env.bucket.collections()
        self._bucket_name: str = env.bucket.name
        self._other_cms: dict[str, Any] = {}
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def use_bucket(self, bucket_name: str) -> Any:
        bucket = self._cluster.bucket(bucket_name)
        self._rit(bucket.on_connect)
        self._cm = bucket.collections()
        self._bucket_name = bucket_name
        return bucket

    def _cm_for(self, bucket_name: str) -> Any:
        if bucket_name not in self._other_cms:
            bucket = self._cluster.bucket(bucket_name)
            self._rit(bucket.on_connect)
            self._other_cms[bucket_name] = bucket.collections()
        return self._other_cms[bucket_name]

    def create_scope(self, scope_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._cm.create_scope, scope_name, *options, **kwargs)

    def drop_scope(self, scope_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._cm.drop_scope, scope_name, *options, **kwargs)

    def get_all_scopes(self, *options: Any, **kwargs: Any) -> list[ScopeSpec]:
        return self._rit(self._cm.get_all_scopes, *options, **kwargs)

    def create_collection(self, *args: Any, **kwargs: Any) -> None:
        self._rit(self._cm.create_collection, *args, **kwargs)

    def drop_collection(self, *args: Any, **kwargs: Any) -> None:
        self._rit(self._cm.drop_collection, *args, **kwargs)

    def update_collection(self,
                          scope_name: str,
                          collection_name: str,
                          settings: UpdateCollectionSettings,
                          *options: Any, **kwargs: Any) -> None:
        self._rit(self._cm.update_collection, scope_name, collection_name, settings, *options, **kwargs)

    def get_scope(self, scope_name: str) -> ScopeSpec | None:
        scopes = self._rit(self._cm.get_all_scopes)
        return next((s for s in scopes if s.name == scope_name), None)

    def get_collection(self, scope_name: str, collection_name: str) -> CollectionSpec | None:
        scope = self.get_scope(scope_name)
        if scope is None:
            return None
        return next((c for c in scope.collections if c.name == collection_name), None)

    def get_collection_in_bucket(self,
                                 bucket_name: str,
                                 scope_name: str,
                                 collection_name: str) -> CollectionSpec | None:
        scopes = self._rit(self._cm_for(bucket_name).get_all_scopes)
        scope = next((s for s in scopes if s.name == scope_name), None)
        if scope is None:
            return None
        return next((c for c in scope.collections if c.name == collection_name), None)

    def create_collection_in_bucket(self,
                                    bucket_name: str,
                                    scope_name: str,
                                    collection_name: str,
                                    settings: CreateCollectionSettings | None = None) -> None:
        self._rit(self._cm_for(bucket_name).create_collection, scope_name, collection_name, settings)

    def update_collection_in_bucket(self,
                                    bucket_name: str,
                                    scope_name: str,
                                    collection_name: str,
                                    settings: UpdateCollectionSettings) -> None:
        self._rit(self._cm_for(bucket_name).update_collection, scope_name, collection_name, settings)
