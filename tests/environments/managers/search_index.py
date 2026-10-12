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

"""Search index test-infrastructure managers, cluster-level and scope-level, one pair per api.

All three api styles ship the whole manager, cluster and scope, with identical method
sets: fourteen methods on each, checked rather than assumed.  So the three pairs here differ
only in how a call is driven.

All six wrap all fourteen because there is a caller for all fourteen at both levels.  The
count is a fact about the suites rather than about the SDK, which offers the whole surface at
both levels regardless: a wrapper exists to be called, and wrapping ahead of a caller adds
surface nothing verifies.

One asymmetry survives into the wrappers because it is real in the SDK.  Scope-level
`get_index_stats` raises FeatureUnavailableException out of the request builder instead of
issuing a call, and it is the only one of the fourteen that does.  It is wrapped anyway:
asserting that raise is what the scope-level suite does with it.

The three scope-level classes resolve the SDK manager per call rather than capturing it in
setup(), which the three cluster-level ones do.  _setup_managers() runs before setup()
provisions a named scope, so a captured scope.search_indexes() would address the default
scope for the whole run, and an FTS index created there is invisible to a query issued
through the named scope.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tests.environments.managers.base import TestEnvironmentManager
from tests.test_features import ServerFeatures

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.search import SearchIndex
    from tests.environments.test_environment import TestEnvironment


class TestEnvironmentSearchIndexManager(TestEnvironmentManager):
    """Wraps couchbase.cluster.Cluster.search_indexes(), cluster-level."""
    required_feature = ServerFeatures.SearchIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._sixm = env.cluster.search_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_index(self, index_name: str) -> SearchIndex:
        return self._sixm.get_index(index_name)

    def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[SearchIndex]:
        return self._sixm.get_all_indexes(*options, **kwargs)

    def upsert_index(self, index: SearchIndex) -> None:
        self._sixm.upsert_index(index)

    def drop_index(self, index_name: str) -> None:
        self._sixm.drop_index(index_name)

    def pause_ingest(self, index_name: str) -> None:
        self._sixm.pause_ingest(index_name)

    def resume_ingest(self, index_name: str) -> None:
        self._sixm.resume_ingest(index_name)

    def allow_querying(self, index_name: str) -> None:
        self._sixm.allow_querying(index_name)

    def disallow_querying(self, index_name: str) -> None:
        self._sixm.disallow_querying(index_name)

    def freeze_plan(self, index_name: str) -> None:
        self._sixm.freeze_plan(index_name)

    def unfreeze_plan(self, index_name: str) -> None:
        self._sixm.unfreeze_plan(index_name)

    def analyze_document(self, index_name: str, document: Any) -> dict[str, Any]:
        return self._sixm.analyze_document(index_name, document)

    def get_indexed_documents_count(self, index_name: str) -> int:
        return self._sixm.get_indexed_documents_count(index_name)

    def get_index_stats(self, index_name: str) -> dict[str, Any]:
        return self._sixm.get_index_stats(index_name)

    def get_all_index_stats(self) -> dict[str, Any]:
        return self._sixm.get_all_index_stats()


class TestEnvironmentScopeSearchIndexManager(TestEnvironmentManager):
    """Wraps scope.search_indexes(), scope-level (server >= 7.6)."""
    required_feature = ServerFeatures.ScopeSearchIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._env = env

    def teardown(self, env: TestEnvironment) -> None:
        pass

    @property
    def _sixm(self) -> Any:
        # Resolved per call, not captured in setup(); the module docstring has the reason.
        # Any rather than ScopeSearchIndexManager: self._env is the unparameterized
        # TestEnvironment, so .scope is Any and so is the manager it yields.
        scope = self._env.scope or self._env.default_scope
        return scope.search_indexes()

    def get_index(self, index_name: str) -> SearchIndex:
        return self._sixm.get_index(index_name)

    def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[SearchIndex]:
        return self._sixm.get_all_indexes(*options, **kwargs)

    def upsert_index(self, index: SearchIndex) -> None:
        self._sixm.upsert_index(index)

    def drop_index(self, index_name: str) -> None:
        self._sixm.drop_index(index_name)

    def get_indexed_documents_count(self, index_name: str) -> int:
        return self._sixm.get_indexed_documents_count(index_name)

    def pause_ingest(self, index_name: str) -> None:
        self._sixm.pause_ingest(index_name)

    def resume_ingest(self, index_name: str) -> None:
        self._sixm.resume_ingest(index_name)

    def allow_querying(self, index_name: str) -> None:
        self._sixm.allow_querying(index_name)

    def disallow_querying(self, index_name: str) -> None:
        self._sixm.disallow_querying(index_name)

    def freeze_plan(self, index_name: str) -> None:
        self._sixm.freeze_plan(index_name)

    def unfreeze_plan(self, index_name: str) -> None:
        self._sixm.unfreeze_plan(index_name)

    def analyze_document(self, index_name: str, document: Any) -> dict[str, Any]:
        return self._sixm.analyze_document(index_name, document)

    def get_index_stats(self, index_name: str) -> dict[str, Any]:
        return self._sixm.get_index_stats(index_name)

    def get_all_index_stats(self) -> dict[str, Any]:
        return self._sixm.get_all_index_stats()


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------


class AsyncTestEnvironmentSearchIndexManager(TestEnvironmentManager):
    """Wraps acouchbase.cluster.AsyncCluster.search_indexes(), cluster-level."""
    required_feature = ServerFeatures.SearchIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._sixm = env.cluster.search_indexes()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def get_index(self, index_name: str) -> SearchIndex:
        return await self._sixm.get_index(index_name)

    async def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[SearchIndex]:
        return await self._sixm.get_all_indexes(*options, **kwargs)

    async def upsert_index(self, index: SearchIndex) -> None:
        await self._sixm.upsert_index(index)

    async def drop_index(self, index_name: str) -> None:
        await self._sixm.drop_index(index_name)

    async def pause_ingest(self, index_name: str) -> None:
        await self._sixm.pause_ingest(index_name)

    async def resume_ingest(self, index_name: str) -> None:
        await self._sixm.resume_ingest(index_name)

    async def allow_querying(self, index_name: str) -> None:
        await self._sixm.allow_querying(index_name)

    async def disallow_querying(self, index_name: str) -> None:
        await self._sixm.disallow_querying(index_name)

    async def freeze_plan(self, index_name: str) -> None:
        await self._sixm.freeze_plan(index_name)

    async def unfreeze_plan(self, index_name: str) -> None:
        await self._sixm.unfreeze_plan(index_name)

    async def analyze_document(self, index_name: str, document: Any) -> dict[str, Any]:
        return await self._sixm.analyze_document(index_name, document)

    async def get_indexed_documents_count(self, index_name: str) -> int:
        return await self._sixm.get_indexed_documents_count(index_name)

    async def get_index_stats(self, index_name: str) -> dict[str, Any]:
        return await self._sixm.get_index_stats(index_name)

    async def get_all_index_stats(self) -> dict[str, Any]:
        return await self._sixm.get_all_index_stats()


class AsyncTestEnvironmentScopeSearchIndexManager(TestEnvironmentManager):
    """Wraps acouchbase scope.search_indexes(), scope-level (server >= 7.6)."""
    required_feature = ServerFeatures.ScopeSearchIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        self._env = env

    def teardown(self, env: TestEnvironment) -> None:
        pass

    @property
    def _sixm(self) -> Any:
        scope = self._env.scope or self._env.default_scope
        return scope.search_indexes()

    async def get_index(self, index_name: str) -> SearchIndex:
        return await self._sixm.get_index(index_name)

    async def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[SearchIndex]:
        return await self._sixm.get_all_indexes(*options, **kwargs)

    async def upsert_index(self, index: SearchIndex) -> None:
        await self._sixm.upsert_index(index)

    async def drop_index(self, index_name: str) -> None:
        await self._sixm.drop_index(index_name)

    async def get_indexed_documents_count(self, index_name: str) -> int:
        return await self._sixm.get_indexed_documents_count(index_name)

    async def pause_ingest(self, index_name: str) -> None:
        await self._sixm.pause_ingest(index_name)

    async def resume_ingest(self, index_name: str) -> None:
        await self._sixm.resume_ingest(index_name)

    async def allow_querying(self, index_name: str) -> None:
        await self._sixm.allow_querying(index_name)

    async def disallow_querying(self, index_name: str) -> None:
        await self._sixm.disallow_querying(index_name)

    async def freeze_plan(self, index_name: str) -> None:
        await self._sixm.freeze_plan(index_name)

    async def unfreeze_plan(self, index_name: str) -> None:
        await self._sixm.unfreeze_plan(index_name)

    async def analyze_document(self, index_name: str, document: Any) -> dict[str, Any]:
        return await self._sixm.analyze_document(index_name, document)

    async def get_index_stats(self, index_name: str) -> dict[str, Any]:
        return await self._sixm.get_index_stats(index_name)

    async def get_all_index_stats(self) -> dict[str, Any]:
        return await self._sixm.get_all_index_stats()


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------


class TwistedTestEnvironmentSearchIndexManager(TestEnvironmentManager):
    """Wraps txcouchbase.cluster.Cluster.search_indexes(), dispatched via the reactor."""
    required_feature = ServerFeatures.SearchIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        from tests.environments.twisted_environment import _get_run_in_reactor_thread
        self._sixm = env.cluster.search_indexes()
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_index(self, index_name: str) -> SearchIndex:
        return self._rit(self._sixm.get_index, index_name)

    def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[SearchIndex]:
        return self._rit(self._sixm.get_all_indexes, *options, **kwargs)

    def upsert_index(self, index: SearchIndex) -> None:
        self._rit(self._sixm.upsert_index, index)

    def drop_index(self, index_name: str) -> None:
        self._rit(self._sixm.drop_index, index_name)

    def pause_ingest(self, index_name: str) -> None:
        self._rit(self._sixm.pause_ingest, index_name)

    def resume_ingest(self, index_name: str) -> None:
        self._rit(self._sixm.resume_ingest, index_name)

    def allow_querying(self, index_name: str) -> None:
        self._rit(self._sixm.allow_querying, index_name)

    def disallow_querying(self, index_name: str) -> None:
        self._rit(self._sixm.disallow_querying, index_name)

    def freeze_plan(self, index_name: str) -> None:
        self._rit(self._sixm.freeze_plan, index_name)

    def unfreeze_plan(self, index_name: str) -> None:
        self._rit(self._sixm.unfreeze_plan, index_name)

    def analyze_document(self, index_name: str, document: Any) -> dict[str, Any]:
        return self._rit(self._sixm.analyze_document, index_name, document)

    def get_indexed_documents_count(self, index_name: str) -> int:
        return self._rit(self._sixm.get_indexed_documents_count, index_name)

    def get_index_stats(self, index_name: str) -> dict[str, Any]:
        return self._rit(self._sixm.get_index_stats, index_name)

    def get_all_index_stats(self) -> dict[str, Any]:
        return self._rit(self._sixm.get_all_index_stats)


class TwistedTestEnvironmentScopeSearchIndexManager(TestEnvironmentManager):
    """Wraps txcouchbase scope.search_indexes(), scope-level (server >= 7.6)."""
    required_feature = ServerFeatures.ScopeSearchIndexManagement

    def setup(self, env: TestEnvironment) -> None:
        from tests.environments.twisted_environment import _get_run_in_reactor_thread
        self._env = env
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    @property
    def _sixm(self) -> Any:
        scope = self._env.scope or self._env.default_scope
        return scope.search_indexes()

    def get_index(self, index_name: str) -> SearchIndex:
        return self._rit(self._sixm.get_index, index_name)

    def get_all_indexes(self, *options: Any, **kwargs: Any) -> Iterable[SearchIndex]:
        return self._rit(self._sixm.get_all_indexes, *options, **kwargs)

    def upsert_index(self, index: SearchIndex) -> None:
        self._rit(self._sixm.upsert_index, index)

    def drop_index(self, index_name: str) -> None:
        self._rit(self._sixm.drop_index, index_name)

    def get_indexed_documents_count(self, index_name: str) -> int:
        return self._rit(self._sixm.get_indexed_documents_count, index_name)

    def pause_ingest(self, index_name: str) -> None:
        self._rit(self._sixm.pause_ingest, index_name)

    def resume_ingest(self, index_name: str) -> None:
        self._rit(self._sixm.resume_ingest, index_name)

    def allow_querying(self, index_name: str) -> None:
        self._rit(self._sixm.allow_querying, index_name)

    def disallow_querying(self, index_name: str) -> None:
        self._rit(self._sixm.disallow_querying, index_name)

    def freeze_plan(self, index_name: str) -> None:
        self._rit(self._sixm.freeze_plan, index_name)

    def unfreeze_plan(self, index_name: str) -> None:
        self._rit(self._sixm.unfreeze_plan, index_name)

    def analyze_document(self, index_name: str, document: Any) -> dict[str, Any]:
        return self._rit(self._sixm.analyze_document, index_name, document)

    def get_index_stats(self, index_name: str) -> dict[str, Any]:
        return self._rit(self._sixm.get_index_stats, index_name)

    def get_all_index_stats(self) -> dict[str, Any]:
        return self._rit(self._sixm.get_all_index_stats)
