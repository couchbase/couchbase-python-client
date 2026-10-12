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

"""Collection provisioning test-infrastructure managers.

Three transport variants live here so that all collection-lifecycle logic is
in one place:

  TestEnvironmentCollectionManager        - sync (couchbase/)
  AsyncTestEnvironmentCollectionManager   - asyncio (acouchbase/)
  TwistedTestEnvironmentCollectionManager - Twisted (txcouchbase/)

None of these are mirrors of the SDK CollectionManager surface.  They expose
only what the test framework needs to provision and de-provision named scopes
and collections, on the shared test bucket, for the whole run.

The mirror is a separate class in ``managers/collection_mgmt.py``, which the
collection management suite drives.  It is separate because it moves: that
suite creates a bucket of its own and re-points its manager at it, and this
one, which every NAMED suite in the tree holds, must stay where it is.

Provisioning here uses the current ``create_collection`` / ``drop_collection``
signatures, which take ``scope_name, collection_name``.  The ``CollectionSpec``
overloads are deprecated as of v4.1.9, and a deprecated call in framework code is
not covering the deprecation, it is riding it: nothing asserts on it and every
NAMED suite in the tree inherits the warning.  Coverage of the deprecated form
belongs in the collection management suite, which tests it deliberately.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from tests.environments import CouchbaseTestEnvironmentException
from tests.environments.managers.base import TestEnvironmentManager
from tests.test_features import ServerFeatures

if TYPE_CHECKING:
    from tests.consistency import ConsistencyChecker
    from tests.environments.test_environment import CollectionHandle, TestEnvironment


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

class TestEnvironmentCollectionManager(TestEnvironmentManager):
    """Sync collection-provisioning manager for couchbase/ test suites."""
    required_feature = ServerFeatures.Collections

    def setup(self, env: TestEnvironment) -> None:
        self._cm = env.bucket.collections()
        self._consistency: ConsistencyChecker = env.consistency
        self._bucket_name: str = env.bucket.name

    def teardown(self, env: TestEnvironment) -> None:
        pass  # per-suite cleanup is handled by teardown_named_collection()

    def setup_named_collection(self, scope_name: str, coll_name: str) -> CollectionHandle:
        """Create scope + collection, wait for consistency, return a CollectionHandle.

        If the scope already exists it is dropped and recreated so the collection
        starts from a clean state.  Raises CouchbaseTestEnvironmentException when
        the collection cannot be verified after creation.
        """
        from tests.environments.test_environment import CollectionHandle, CollectionStrategy

        try:
            self._cm.create_scope(scope_name)
        except ScopeAlreadyExistsException:
            self._cm.drop_scope(scope_name)
            self._cm.create_scope(scope_name)

        self._consistency.wait_until_scope_present(self._bucket_name, scope_name)

        try:
            self._cm.create_collection(scope_name, coll_name)
        except CollectionAlreadyExistsException:
            self._cm.drop_collection(scope_name, coll_name)
            self._cm.create_collection(scope_name, coll_name)

        self._consistency.wait_until_collection_present(
            self._bucket_name, scope_name, coll_name
        )

        scopes = self._cm.get_all_scopes()
        scope_spec = next((s for s in scopes if s.name == scope_name), None)
        if scope_spec is None or not any(
            c.name == coll_name for c in scope_spec.collections
        ):
            raise CouchbaseTestEnvironmentException(
                f"Unable to create collection '{scope_name}.{coll_name}' for testing."
            )

        return CollectionHandle(
            scope_name=scope_name,
            collection_name=coll_name,
            collection=None,  # caller wires the live collection object after this returns
            strategy=CollectionStrategy.NAMED,
        )

    def add_collection(self, scope_name: str, coll_name: str) -> None:
        """Create one more collection inside a scope that already exists.

        setup_named_collection() cannot do this.  It drops and recreates the scope, which
        is right when it provisions the suite's scope from nothing and destructive when a
        second collection is wanted beside the first: the drop takes the collection the
        suite is already using with it.  The search collection suite is the first caller,
        needing an `other-collection` alongside the seeded one so a scope-level query has
        something to filter against.

        The scope is dropped whole at teardown, so nothing here has a counterpart in
        teardown_named_collection().
        """
        try:
            self._cm.create_collection(scope_name, coll_name)
        except CollectionAlreadyExistsException:
            self._cm.drop_collection(scope_name, coll_name)
            self._cm.create_collection(scope_name, coll_name)

        self._consistency.wait_until_collection_present(
            self._bucket_name, scope_name, coll_name
        )

    def drop_stale_scopes(self, prefix: str) -> None:
        """Drop every scope whose name starts with `prefix`.

        A class whose setup fails after its scope exists, or whose teardown never runs, leaves
        that scope behind under a name nothing will reuse.  Each NAMED class clears its
        worker's leftovers before provisioning, so a worker holds at most one at a time.
        """
        for spec in self._cm.get_all_scopes():
            if spec.name.startswith(prefix):
                try:
                    self._cm.drop_scope(spec.name)
                except ScopeNotFoundException:
                    pass

    def teardown_named_collection(self, scope_name: str) -> None:
        """Drop scope and wait until it is gone."""
        from tests.environments.test_environment import TestEnvironment

        self._cm.drop_scope(scope_name)
        TestEnvironment.try_n_times_till_exception(
            10, 1, self._cm.drop_scope, scope_name,
            expected_exceptions=(ScopeNotFoundException,),
        )


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentCollectionManager(TestEnvironmentManager):
    """Async collection-provisioning manager for acouchbase/ test suites.

    setup()/teardown() stay synchronous (matching the base class) because
    AsyncTestEnvironment._setup_managers()/_teardown_managers() call them
    through the same unawaited loop as the sync manager. Only the named-
    collection helpers below are coroutines.
    """
    required_feature = ServerFeatures.Collections

    def setup(self, env: TestEnvironment) -> None:
        self._cm = env.bucket.collections()
        self._consistency: ConsistencyChecker = env.consistency
        self._bucket_name: str = env.bucket.name

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def setup_named_collection(
        self, scope_name: str, coll_name: str
    ) -> CollectionHandle:
        """Create scope + collection asynchronously; return a CollectionHandle."""
        from tests.environments.test_environment import CollectionHandle, CollectionStrategy

        try:
            await self._cm.create_scope(scope_name)
        except Exception:  # ScopeAlreadyExistsException, no async stubs
            await self._cm.drop_scope(scope_name)
            await self._cm.create_scope(scope_name)

        try:
            await self._cm.create_collection(scope_name, coll_name)
        except Exception:  # CollectionAlreadyExistsException, no async stubs
            await self._cm.drop_collection(scope_name, coll_name)
            await self._cm.create_collection(scope_name, coll_name)

        scopes = await self._cm.get_all_scopes()
        scope_spec = next((s for s in scopes if s.name == scope_name), None)
        if scope_spec is None or not any(
            c.name == coll_name for c in scope_spec.collections
        ):
            raise CouchbaseTestEnvironmentException(
                f"Unable to create collection '{scope_name}.{coll_name}' for testing."
            )

        return CollectionHandle(
            scope_name=scope_name,
            collection_name=coll_name,
            collection=None,  # caller wires the live collection object after this returns
            strategy=CollectionStrategy.NAMED,
        )

    async def add_collection(self, scope_name: str, coll_name: str) -> None:
        """Create one more collection inside a scope that already exists.

        Async counterpart of the sync wrapper; see that docstring for why
        setup_named_collection() cannot be used for a second collection.  The consistency
        checker is synchronous in all three api styles, so the wait is not awaited here.
        """
        try:
            await self._cm.create_collection(scope_name, coll_name)
        except Exception:  # CollectionAlreadyExistsException, no async stubs
            await self._cm.drop_collection(scope_name, coll_name)
            await self._cm.create_collection(scope_name, coll_name)

        self._consistency.wait_until_collection_present(
            self._bucket_name, scope_name, coll_name
        )

    async def drop_stale_scopes(self, prefix: str) -> None:
        """Async counterpart of the sync wrapper; see that docstring."""
        for spec in await self._cm.get_all_scopes():
            if spec.name.startswith(prefix):
                try:
                    await self._cm.drop_scope(spec.name)
                except ScopeNotFoundException:
                    pass

    async def teardown_named_collection(self, scope_name: str) -> None:
        """Drop scope and wait until it is gone."""
        from tests.environments.async_environment import AsyncTestEnvironment

        await self._cm.drop_scope(scope_name)
        await AsyncTestEnvironment.try_n_times_till_exception(
            10, 1,
            self._cm.drop_scope, scope_name,
            expected_exceptions=(ScopeNotFoundException,),
        )


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------

class TwistedTestEnvironmentCollectionManager(TestEnvironmentManager):
    """Twisted collection-provisioning manager for txcouchbase/ test suites.

    setup()/teardown() stay synchronous (matching the base class); the
    reactor-bound work (deferred sleep, via self._env) lives in the named-
    collection helpers below, called directly by TwistedTestEnvironment.
    """
    required_feature = ServerFeatures.Collections

    def setup(self, env: TestEnvironment) -> None:
        from tests.environments.twisted_environment import _get_run_in_reactor_thread
        self._cm = env.bucket.collections()
        self._consistency: ConsistencyChecker = env.consistency
        self._bucket_name: str = env.bucket.name
        self._rit = _get_run_in_reactor_thread()
        self._env = env

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def setup_named_collection(self, scope_name: str, coll_name: str) -> CollectionHandle:
        """Create scope + collection via the reactor thread; return a CollectionHandle."""
        from tests.environments.test_environment import CollectionHandle, CollectionStrategy

        rit = self._rit
        try:
            rit(self._cm.create_scope, scope_name)
        except ScopeAlreadyExistsException:
            rit(self._cm.drop_scope, scope_name)
            rit(self._cm.create_scope, scope_name)

        try:
            rit(self._cm.create_collection, scope_name, coll_name)
        except CollectionAlreadyExistsException:
            rit(self._cm.drop_collection, scope_name, coll_name)
            rit(self._cm.create_collection, scope_name, coll_name)

        c = self._env.get_collection(scope_name, coll_name, bucket_name=self._bucket_name)
        if c is None:
            raise CouchbaseTestEnvironmentException(
                f"Unable to create collection '{scope_name}.{coll_name}' for testing."
            )

        return CollectionHandle(
            scope_name=scope_name,
            collection_name=coll_name,
            collection=None,  # caller wires the live collection object after this returns
            strategy=CollectionStrategy.NAMED,
        )

    def add_collection(self, scope_name: str, coll_name: str) -> None:
        """Create one more collection inside a scope that already exists.

        Twisted counterpart of the sync wrapper; see that docstring for why
        setup_named_collection() cannot be used for a second collection.
        """
        try:
            self._rit(self._cm.create_collection, scope_name, coll_name)
        except CollectionAlreadyExistsException:
            self._rit(self._cm.drop_collection, scope_name, coll_name)
            self._rit(self._cm.create_collection, scope_name, coll_name)

        self._consistency.wait_until_collection_present(
            self._bucket_name, scope_name, coll_name
        )

    def drop_stale_scopes(self, prefix: str) -> None:
        """Twisted counterpart of the sync wrapper; see that docstring."""
        for spec in self._rit(self._cm.get_all_scopes):
            if spec.name.startswith(prefix):
                try:
                    self._rit(self._cm.drop_scope, spec.name)
                except ScopeNotFoundException:
                    pass

    def teardown_named_collection(self, scope_name: str) -> None:
        """Drop scope via the reactor thread and wait until it is gone."""
        rit = self._rit
        rit(self._cm.drop_scope, scope_name)
        self._env.try_n_times_till_exception(
            10, 1, self._cm.drop_scope, scope_name,
            expected_exceptions=(ScopeNotFoundException,),
        )
