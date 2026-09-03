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

"""AsyncTestEnvironment and acouchbase session-scoped fixtures.

Split from tests_v2/environments/test_environment.py (sync) so that
acouchbase/tests_v2/conftest.py can register only this module, avoiding
the import of pytest_asyncio and the asyncio event-loop fixture in suites
that never need them.
"""

from __future__ import annotations

import asyncio
import os
from asyncio import AbstractEventLoop
from collections.abc import Callable, Generator
from typing import Any

import pytest
import pytest_asyncio

from acouchbase.bucket import AsyncBucket
from acouchbase.cluster import AsyncCluster, get_event_loop
from acouchbase.collection import AsyncCollection
from acouchbase.scope import AsyncScope
from couchbase.diagnostics import ServiceType
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CouchbaseException,
                                  UnAmbiguousTimeoutException)
from couchbase.options import TransactionConfig
from tests_v2.couchbase_config import CouchbaseConfig
from tests_v2.environments import CouchbaseTestEnvironmentException
from tests_v2.environments.managers import AsyncTestEnvironmentCollectionManager
from tests_v2.environments.test_environment import (CollectionHandle,
                                                    CollectionStrategy,
                                                    ConnectionStrategy,
                                                    DocType,
                                                    SuiteRequirements,
                                                    TestEnvironment,
                                                    TopologyFacts)
from tests_v2.skips import env_degraded, server_label
from tests_v2.test_utils import RetryHelper


class AsyncTestEnvironment(TestEnvironment[AsyncCluster,
                                           AsyncBucket,
                                           AsyncScope,
                                           AsyncCollection]):
    """Asyncio variant of TestEnvironment.

    All I/O methods are async.  Manager setup() / teardown() remain synchronous
    (they create SDK objects without performing I/O).  If a future manager needs
    async I/O during setup, it should define async_setup() and override
    _setup_managers() below to call it.
    """

    # ------------------------------------------------------------------
    # Overrides of inherited sync methods that perform I/O
    # ------------------------------------------------------------------
    #
    # Every method in this block does I/O and is inherited from TestEnvironment, where none
    # of them is a coroutine, so `inspect.iscoroutinefunction` answers False for all of
    # them.  An async suite calling the inherited version gets a silent wrong answer rather
    # than an error, which is why the generator refuses to resolve an inherited method at all
    # and reports it instead.  The pure ones (get_manager, get_new_doc, get_batch_id,
    # get_bucket_name, get_bucket_names, verify_mutation_tokens) are deliberately NOT here:
    # they touch only in-memory state and the inherited versions are correct.

    async def _count_rows(self, result: Any) -> int:  # type: ignore[override]
        """Async counterpart of TestEnvironment._count_rows().

        The base version iterates `result.rows()`, which under acouchbase is an async
        iterator.  A plain `for` over it does not yield rows, so the count stays 0 and every
        assertion built on it is answering a question about an empty iteration rather than
        about the result.  A nonzero expected count fails loudly on that, but an at-least-zero
        one passes, so the wrong answer is silent exactly where nothing else catches it.
        """
        count = 0
        async for row in result.rows():
            assert row is not None, f'{type(result).__name__} yielded a None row at index {count}'
            count += 1
        return count

    async def assert_rows_exact(self, result: Any, expected_count: int) -> None:  # type: ignore[override]
        """Async counterpart of TestEnvironment.assert_rows_exact()."""
        count = await self._count_rows(result)
        assert count == expected_count, f'expected exactly {expected_count} rows, got {count}'

    async def assert_rows_at_least(self, result: Any, expected_count: int) -> None:  # type: ignore[override]
        """Async counterpart of TestEnvironment.assert_rows_at_least()."""
        count = await self._count_rows(result)
        assert count >= expected_count, f'expected at least {expected_count} rows, got {count}'

    async def drain_rows(self, result: Any) -> None:  # type: ignore[override]
        """Async counterpart of TestEnvironment.drain_rows()."""
        await self._count_rows(result)

    # No [override] ignore needed here, unlike the three above: the base declares `-> Any`,
    # and a coroutine returning Any satisfies Any.  warn_unused_ignores is on, so an
    # ignore copied from the method above would itself be an error.
    async def get_existing_doc(self, key_only: bool = False) -> Any:
        """Async counterpart of TestEnvironment.get_existing_doc().

        Only the cold-cache branch differs, and that is exactly what makes the inherited
        version dangerous: it is pure whenever documents are already loaded, and falls
        through to self.load_data() when they are not.  load_data IS overridden as a
        coroutine here, so the inherited version discards it on the one path that matters
        and then picks a key from an empty set.
        """
        import random
        if not self._loaded_docs:
            await self.load_data()
        available = set(self._loaded_docs.keys()).difference(self._used_docs)
        key = random.choice(list(available))
        self._used_docs.add(key)
        if key_only:
            return key
        return key, self._loaded_docs[key]

    async def create_bucket(self,  # type: ignore[override]
                            bucket_name: str,
                            storage_backend: Any = None) -> None:
        from couchbase.management.buckets import (BucketType,
                                                  CreateBucketSettings,
                                                  StorageBackend)
        from tests_v2.environments.managers import AsyncTestEnvironmentBucketManager
        bm = self.get_manager(AsyncTestEnvironmentBucketManager)
        settings_kwargs: dict[str, Any] = {
            'name': bucket_name,
            'bucket_type': BucketType.COUCHBASE,
            'ram_quota_mb': 100,
        }
        if storage_backend is not None:
            settings_kwargs['storage_backend'] = storage_backend
            if storage_backend is StorageBackend.MAGMA:
                settings_kwargs['ram_quota_mb'] = 1024
        try:
            await bm.create_bucket(CreateBucketSettings(**settings_kwargs))
        except Exception as ex:                      # BucketAlreadyExistsException
            if type(ex).__name__ != 'BucketAlreadyExistsException':
                raise
        self.consistency.wait_until_bucket_healthy(bucket_name)

    async def drop_bucket(self) -> None:  # type: ignore[override]
        """Drop one previously-issued, not-yet-dropped bucket name."""
        from couchbase.exceptions import BucketDoesNotExistException
        from tests_v2.environments.managers import AsyncTestEnvironmentBucketManager
        available = self._used_bucket_ids.difference(self._dropped_bucket_ids,
                                                     self._reserved_bucket_ids)
        # @FINDING(PYCBC-1867): carried over from the sync version deliberately.  The name
        # is recorded as dropped before the drop is attempted, so a failed drop still marks
        # it gone.  Fixing it here and not there would make the two environments disagree
        # about a defect that has its own ticket; it moves in one change or not at all.
        bucket_name = available.pop()
        self._dropped_bucket_ids.add(bucket_name)
        bm = self.get_manager(AsyncTestEnvironmentBucketManager)
        try:
            await bm.drop_bucket(bucket_name)
        except BucketDoesNotExistException:
            pass
        # ConsistencyChecker is api-agnostic: it polls the management REST endpoints with
        # `requests` and never touches the SDK, so there is no async variant to call.  It
        # blocks the event loop while it polls, which is acceptable here because nothing
        # else is scheduled on it during suite teardown.
        self.consistency.wait_until_bucket_dropped(bucket_name)

    async def teardown_bucket_pool(self) -> None:  # type: ignore[override]
        """Drop any bucket names issued but not yet cleaned up by individual tests."""
        from couchbase.exceptions import BucketDoesNotExistException
        from tests_v2.environments.managers import AsyncTestEnvironmentBucketManager
        bm = self.get_manager(AsyncTestEnvironmentBucketManager)
        for b in self._used_bucket_ids:
            await AsyncTestEnvironment.try_n_times_till_exception(
                10, 3, bm.drop_bucket, b, expected_exceptions=(BucketDoesNotExistException,)
            )
        self._used_bucket_ids.clear()

    async def get_collection(
        self,
        scope_name: str,
        coll_name: str,
        bucket_name: str | None = None,
    ) -> Any:
        scope = await self.get_scope(scope_name, bucket_name=bucket_name)
        if scope:
            return next((c for c in scope.collections if c.name == coll_name), None)
        return None

    async def get_scope(
        self,
        scope_name: str,
        bucket_name: str | None = None,
    ) -> Any:
        if bucket_name is None:
            raise CouchbaseTestEnvironmentException('Must provide a bucket name.')
        scopes = await self._cluster.bucket(bucket_name).collections().get_all_scopes()
        return next((s for s in scopes if s.name == scope_name), None)

    async def _collect_topology(self) -> TopologyFacts:  # type: ignore[override]
        """Async counterpart of TestEnvironment._collect_topology().

        The one place the topology facts differ by api style: the bucket manager class and the
        two awaits.  Everything that consumes these facts is pure and shared; see
        tests_v2/suite_fixtures.py.
        """
        from tests_v2.environments.managers import AsyncTestEnvironmentBucketManager

        replicas: int | None = None
        if self.has_manager(AsyncTestEnvironmentBucketManager):
            bm = self.get_manager(AsyncTestEnvironmentBucketManager)
            settings = await AsyncTestEnvironment.try_n_times(10, 1, bm.get_bucket, self._bucket.name)
            replicas = settings.get('num_replicas')

        ping_res = await self._bucket.ping()
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)

        return TopologyFacts(
            num_nodes=self._node_count(),
            test_bucket_replicas=replicas,
            test_bucket_kv_endpoints=0 if kv_endpoints is None else len(kv_endpoints),
        )

    async def load_data(self, num_docs: int = 50) -> None:  # type: ignore[override]
        """Seed num_docs of every DocType in the suite's DATA_SET; see the sync version.

        _seed_docs and _seed_upsert_kwargs are pure and inherited: only the upsert differs.
        """
        for doc_type in self._data_set:
            kwargs = self._seed_upsert_kwargs(doc_type)
            for key, value in self._seed_docs(doc_type, num_docs).items():
                loaded = False
                for attempt in range(3):
                    try:
                        await self.collection.upsert(key, value, **kwargs)
                        self._record_seeded(doc_type, key, value)
                        loaded = True
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        if attempt < 2:
                            await asyncio.sleep(3)
                    except Exception:
                        raise

                # Setup owns its postconditions; see the sync environment's load_data.
                if not loaded:
                    self._seed_never_written(doc_type, key)

        self._doc_types = [t.value for t in self._data_set]

    async def purge_data(self) -> None:  # type: ignore[override]
        for k in list(self._loaded_docs.keys()):
            try:
                await self.collection.remove(k)
            except CouchbaseException:
                pass
        for k in list(self._used_extras):
            try:
                await self.collection.remove(k)
            except CouchbaseException:
                pass
        self._loaded_docs.clear()
        self._loaded_by_type.clear()
        self._used_docs.clear()
        self._used_extras.clear()

    async def setup(  # type: ignore[override]
        self,
        test_suite: str,
        collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT,
        num_docs: int = 50,
        data_set: list[DocType] | None = None,
        collect_topology: bool = False,
    ) -> None:
        # Stored rather than threaded through load_data; see the sync setup().
        if data_set is not None:
            self._data_set = data_set

        # Auto-register the async collection manager when needed
        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            if AsyncTestEnvironmentCollectionManager not in self._manager_registry:
                self._manager_registry[AsyncTestEnvironmentCollectionManager] = (
                    AsyncTestEnvironmentCollectionManager()
                )

        self._setup_managers()

        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            if AsyncTestEnvironmentCollectionManager not in self._manager_registry:
                self._no_collection_manager(collection_strategy, test_suite)

            worker_id = os.environ.get('PYTEST_XDIST_WORKER', '')
            scope_name = f'{self.TEST_SCOPE}-{worker_id}' if worker_id else self.TEST_SCOPE
            coll_name = (
                f'{self.TEST_COLLECTION}-{worker_id}' if worker_id else self.TEST_COLLECTION
            )

            coll_mgr = self.get_manager(AsyncTestEnvironmentCollectionManager)
            await AsyncTestEnvironment.try_n_times(
                5, 3, coll_mgr.setup_named_collection, scope_name, coll_name
            )
            named_scope = self._bucket.scope(scope_name)
            named_coll = named_scope.collection(coll_name)
            self._pool.add(
                CollectionHandle(
                    scope_name=scope_name,
                    collection_name=coll_name,
                    collection=named_coll,
                    strategy=collection_strategy,
                    is_clean=(collection_strategy == CollectionStrategy.NAMED_EMPTY),
                )
            )
            self._pool.use(coll_name)

        # Before data loading; see the sync setup().
        if collect_topology:
            self._topology = await self._collect_topology()

        if collection_strategy != CollectionStrategy.NAMED_EMPTY:
            await AsyncTestEnvironment.try_n_times(
                5, 3, self.load_data, num_docs=num_docs
            )

    async def teardown(  # type: ignore[override]
        self,
        collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT,
    ) -> None:
        await AsyncTestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            handle = self._pool.current
            if AsyncTestEnvironmentCollectionManager in self._manager_registry:
                coll_mgr = self.get_manager(AsyncTestEnvironmentCollectionManager)
                await AsyncTestEnvironment.try_n_times(
                    5, 3, coll_mgr.teardown_named_collection, handle.scope_name
                )
            self._pool._handles = [self._pool._handles[0]]
            self._pool._current_index = 0

        self._teardown_managers()

    @classmethod
    async def get_environment(cls, **kwargs: Any) -> AsyncTestEnvironment:  # type: ignore[override]
        config = kwargs.get('couchbase_config')
        if not isinstance(config, CouchbaseConfig):
            raise CouchbaseTestEnvironmentException('No test config provided.')

        conn_string = config.get_connection_string()
        # Inherited from TestEnvironment rather than copied.  Option building is identical
        # across the API styles: it produces a ClusterOptions from the config and kwargs and
        # touches no cluster.  A copy here and one in the sync class could drift without
        # anything noticing.
        opts = cls._build_cluster_options(config, kwargs)

        env_args: dict[str, Any] = {}
        okay = False
        for _ in range(3):
            try:
                cluster = await AsyncCluster.connect(conn_string, opts)
                env_args['cluster'] = cluster
                bucket = cluster.bucket(f'{config.bucket_name}')
                await bucket.on_connect()
                env_args['bucket'] = bucket
                await cluster.cluster_info()
                env_args['default_collection'] = bucket.default_collection()
                okay = True
                break
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException):
                continue

        if not okay:
            # See the sync environment's get_environment for why both branches collapse into
            # one env-degraded raise.
            env_degraded(stage='connect',
                         server=server_label(config.is_mock_server),
                         attempts=3,
                         note=('CAVES did not become reachable' if config.is_mock_server
                               else 'check the environment (e.g. VPN)'))

        env_args.update(**kwargs)
        return cls(**env_args)

    @staticmethod
    async def sleep(num_seconds: float) -> None:  # type: ignore[override]
        await RetryHelper.sleep_async(num_seconds)

    @staticmethod
    async def try_n_times(
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return await RetryHelper.try_n_times_async(num_times, seconds_between, func, *args, **kwargs)

    @staticmethod
    async def try_n_times_till_exception(  # type: ignore[override]
        num_times: int,
        seconds_between: int | float,
        func: Callable[..., Any],
        *args: Any,
        expected_exceptions: tuple[type[Exception], ...] = (Exception,),
        raise_exception: bool = False,
        **kwargs: Any,
    ) -> None:
        await RetryHelper.try_n_times_till_exception_async(
            num_times, seconds_between, func, *args,
            expected_exceptions=expected_exceptions,
            raise_exception=raise_exception,
            **kwargs,
        )

    @staticmethod
    async def wait_until(  # type: ignore[override]
        predicate: Callable[[], Any],
        *,
        condition: str,
        timeout: float = 30,
        interval: float = 1,
        note: str | None = None,
    ) -> None:
        """Async variant of TestEnvironment.wait_until; *predicate* may return an awaitable."""
        await RetryHelper.wait_until_async(predicate,
                                           condition=condition,
                                           timeout=timeout,
                                           interval=interval,
                                           note=note)


# ---------------------------------------------------------------------------
# build_environment / teardown_environment: async variants
# ---------------------------------------------------------------------------
#
# Separate functions rather than an api check inside the sync pair: these are coroutines, so the
# generated acouchbase fixture becomes `async def ... yield` and awaits them.  The generator
# rewrites the call by name, which is why the counterpart has to exist under a name of its own.

async def build_async_environment(
    base_env: TestEnvironment,
    suite_cls: type[SuiteRequirements],
    collection_strategy: CollectionStrategy | None = None,
    **kwargs: Any,
) -> AsyncTestEnvironment:
    """Async counterpart of build_environment() for acouchbase test suites."""
    strategy = (
        collection_strategy if collection_strategy is not None
        else suite_cls.COLLECTION_STRATEGY
    )
    conn_strategy = suite_cls.CONNECTION_STRATEGY
    num_docs = suite_cls.NUM_DOCS
    data_set = suite_cls.DATA_SET
    managers = [cls() for cls in suite_cls.MANAGERS]

    # Derived from the mixin, not declared; see build_environment().
    from tests_v2.suite_fixtures import TopologySuite
    collect_topology = issubclass(suite_cls, TopologySuite)

    if conn_strategy == ConnectionStrategy.NEW_CONNECTION:
        env = await AsyncTestEnvironment.get_environment(
            couchbase_config=base_env.config,
            data_provider=base_env.data_provider,
            managers=managers,
            **kwargs,
        )
    else:
        env = AsyncTestEnvironment(
            **TestEnvironment._core_args(base_env),
            managers=managers,
            **kwargs,
        )

    await env.setup(test_suite=suite_cls.__name__,
                    collection_strategy=strategy,
                    num_docs=num_docs,
                    data_set=data_set,
                    collect_topology=collect_topology)
    return env


async def teardown_async_environment(
    env: AsyncTestEnvironment,
    suite_cls: type[SuiteRequirements],
    collection_strategy: CollectionStrategy | None = None,
) -> None:
    strategy = (
        collection_strategy if collection_strategy is not None
        else suite_cls.COLLECTION_STRATEGY
    )
    await env.teardown(collection_strategy=strategy)
    if suite_cls.CONNECTION_STRATEGY == ConnectionStrategy.NEW_CONNECTION:
        await env.cluster.close()


# ---------------------------------------------------------------------------
# Session-scoped acouchbase fixtures
# ---------------------------------------------------------------------------

# pytest.fixture, not pytest_asyncio.fixture: this is a plain sync generator, and
# pytest_asyncio.fixture is typed to accept only coroutines and async generators.
# The two are equivalent at runtime here.  pytest_asyncio.fixture's only extra effect
# is setting _force_asyncio_fixture on the function, and the plugin's fixture wrapper
# bails out on anything that is not a coroutine or async generator before it ever
# reads that flag.
@pytest.fixture(scope='session')
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    loop = get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope='session', name='acb_base_env')
async def acouchbase_base_environment(test_env_v2: Any) -> AsyncTestEnvironment:
    couchbase_config, data_provider = test_env_v2
    return await AsyncTestEnvironment.get_environment(
        couchbase_config=couchbase_config,
        data_provider=data_provider,
    )


@pytest_asyncio.fixture(scope='session', name='acb_base_txn_env')
async def acouchbase_base_txn_environment(test_env_v2: Any) -> AsyncTestEnvironment:
    couchbase_config, data_provider = test_env_v2
    transaction_config = TransactionConfig(
        durability=ServerDurability(DurabilityLevel.NONE)
    )
    return await AsyncTestEnvironment.get_environment(
        couchbase_config=couchbase_config,
        data_provider=data_provider,
        transaction_config=transaction_config,
    )
