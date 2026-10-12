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

"""
TwistedTestEnvironment: Twisted variant of TestEnvironment.

All I/O is bridged through run_in_reactor_thread() so that synchronous pytest
test functions can call blocking Twisted Deferreds.  The Twisted reactor singleton
lives in tests/environments/twisted_reactor.py; this module only imports the
run_in_reactor_thread helper.

Design notes:
  - Plain subclass of TestEnvironment (no multiple inheritance).
  - try_n_times / try_n_times_till_exception are statics, matching the sync base
    and AsyncTestEnvironment, because the suites call them through the class.
  - Manager setup() / teardown() remain synchronous (they create SDK objects
    without performing I/O).
"""

from __future__ import annotations

from collections.abc import Callable, Generator
from typing import (TYPE_CHECKING,
                    Any,
                    ClassVar,
                    ParamSpec,
                    TypeVar)

import pytest

from couchbase.diagnostics import ServiceType
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CouchbaseException,
                                  DocumentNotFoundException,
                                  InternalServerFailureException,
                                  UnAmbiguousTimeoutException)
from tests.couchbase_config import CouchbaseConfig
from tests.data_provider import DataProvider
from tests.environments import CouchbaseTestEnvironmentException
from tests.environments.managers import TwistedTestEnvironmentCollectionManager
from tests.environments.test_environment import (CollectionHandle,
                                                 CollectionStrategy,
                                                 ConnectionStrategy,
                                                 DocType,
                                                 SuiteRequirements,
                                                 TestEnvironment,
                                                 TopologyFacts,
                                                 check_suite_features)
from tests.skips import env_degraded, server_label
from tests.test_utils import RetryHelper
from txcouchbase.bucket import Bucket as TwistedBucket
from txcouchbase.cluster import Cluster as TwistedCluster
from txcouchbase.collection import Collection as TwistedCollection
from txcouchbase.scope import Scope as TwistedScope

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

P = ParamSpec('P')
T = TypeVar('T')


def _get_run_in_reactor_thread() -> Callable[..., Any]:
    """Late-import run_in_reactor_thread to avoid importing Twisted at module load time
    when tests are running against couchbase/ or acouchbase/.

    Only tests.environments.twisted_reactor is acceptable here.  That module holds the
    TwistedObjects init_reactor() populates, and the shared tree is reached by one module
    name in every layout; a same-named helper from anywhere else reads a different
    TwistedObjects, dispatching into a reactor this run never started.
    """
    from tests.environments.twisted_reactor import run_in_reactor_thread
    return run_in_reactor_thread


def run_in_reactor_thread(func: Callable[P, Deferred[T]],
                          *args: P.args,
                          **kwargs: P.kwargs) -> T:
    """Call `func` on the reactor thread and block until its Deferred resolves.

    A thin re-export, and it exists so that generated txcouchbase suites have one import to
    make rather than two.  Every such suite already imports TwistedTestEnvironment /
    build_twisted_environment from this module, and the alternative, importing the real
    helper from tests.environments.twisted_reactor, duplicates the late-import rule that
    _get_run_in_reactor_thread() exists to keep in one place.

    Late-bound on purpose: importing Twisted at module scope would pull the reactor into
    every couchbase/ and acouchbase/ run, since this module is a registered pytest plugin.
    `Deferred` is therefore imported under TYPE_CHECKING only, which costs nothing because
    this module has `from __future__ import annotations`.

    **`Deferred[T] -> T` is the load-bearing part of this signature, not decoration.**  The
    generated txcouchbase suites route every SDK call through here, so whatever this
    function returns is the type the whole suite sees.  Annotated `-> Any`, the entire tree
    silently loses the SDK return types the generated suites are type-checked against: with
    `ping_res = run_in_reactor_thread(cb_env.bucket.ping)` typed Any, `ping_res.endpoints`
    is Any, `.get(ServiceType.KeyValue)` is Any, and the keyed suppression for PYCBC-1877
    on that very line reports as an *unused* ignore.  Each such report marks a check that has
    stopped happening while still reading as one that passed.  Binding the TypeVar keeps
    them.
    """
    return _get_run_in_reactor_thread()(func, *args, **kwargs)


class TwistedTestEnvironment(TestEnvironment[TwistedCluster,
                                             TwistedBucket,
                                             TwistedScope,
                                             TwistedCollection]):
    """Twisted-aware TestEnvironment.

    All blocking I/O calls are wrapped with run_in_reactor_thread() so that
    synchronous pytest test functions can drive the Twisted reactor without
    having to manage Deferreds manually.
    """

    # Lazy accessor, cached after first call to avoid importing Twisted at
    # module import time when running couchbase/ or acouchbase/ tests.
    _run_in_reactor_thread: ClassVar[Callable[..., Any] | None] = None

    @classmethod
    def _rit(cls) -> Callable[..., Any]:
        if cls._run_in_reactor_thread is None:
            cls._run_in_reactor_thread = _get_run_in_reactor_thread()
        return cls._run_in_reactor_thread

    # ------------------------------------------------------------------
    # Overridden data methods
    # ------------------------------------------------------------------

    def _collect_topology(self) -> TopologyFacts:
        """Twisted counterpart of TestEnvironment._collect_topology().

        Keeps the sync signature: TwistedTestEnvironmentBucketManager.get_bucket already
        blocks the calling thread through run_in_reactor_thread, and ping() goes through the
        same _rit().  Only the manager class and that dispatch differ from the sync body.
        """
        from tests.environments.managers import TwistedTestEnvironmentBucketManager

        rit = self._rit()

        replicas: int | None = None
        if self.has_manager(TwistedTestEnvironmentBucketManager):
            bm = self.get_manager(TwistedTestEnvironmentBucketManager)
            settings = self.try_n_times(10, 1, bm.get_bucket, self._bucket.name, is_deferred=False)
            replicas = settings.get('num_replicas')

        ping_res = rit(self._bucket.ping)
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)

        return TopologyFacts(
            num_nodes=self._node_count(),
            test_bucket_replicas=replicas,
            test_bucket_kv_endpoints=0 if kv_endpoints is None else len(kv_endpoints),
        )

    def _kv_knows_current_collection(self) -> None:
        """See the sync version."""
        try:
            self._rit()(self.collection.get, TestEnvironment.NOT_A_KEY)
        except DocumentNotFoundException:
            pass

    def load_data(self, num_docs: int = 50) -> None:
        """Seed num_docs of every DocType in the suite's DATA_SET; see the sync version.

        A suite that needs an unseeded collection declares CollectionStrategy.NAMED_EMPTY,
        which skips this entirely.
        """
        rit = self._rit()

        for doc_type in self._data_set:
            kwargs = self._seed_upsert_kwargs(doc_type)
            for key, value in self._seed_docs(doc_type, num_docs).items():
                stable = False
                for _ in range(3):
                    try:
                        rit(self.collection.upsert, key, value, **kwargs)
                        self._record_seeded(doc_type, key, value)
                        stable = True
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        rit(TwistedTestEnvironment._deferred_sleep, 3)
                        continue
                    except Exception:
                        raise

                # Through env_degraded rather than a bare raise, so all three api styles
                # report a never-written document in the same parseable grammar.
                if not stable:
                    self._seed_never_written(doc_type, key)

        self._doc_types = [t.value for t in self._data_set]

    def purge_data(self) -> None:
        rit = self._rit()
        for k in list(self._loaded_docs.keys()):
            try:
                rit(self.collection.remove, k)
            except CouchbaseException:
                pass
        for k in list(self._used_extras):
            try:
                rit(self.collection.remove, k)
            except CouchbaseException:
                pass
        self._loaded_docs.clear()
        self._loaded_by_type.clear()
        self._used_docs.clear()
        self._used_extras.clear()

    # ------------------------------------------------------------------
    # Overridden scope/collection helpers
    # ------------------------------------------------------------------

    def get_scope(self, scope_name: str, bucket_name: str | None = None) -> Any:
        if bucket_name is None:
            raise CouchbaseTestEnvironmentException('Must provide a bucket name.')
        rit = self._rit()
        scopes = rit(self._cluster.bucket(bucket_name).collections().get_all_scopes)
        return next((s for s in scopes if s.name == scope_name), None)

    def get_collection(self, scope_name: str, coll_name: str, bucket_name: str | None = None) -> Any:
        scope = self.get_scope(scope_name, bucket_name=bucket_name)
        if scope:
            return next((c for c in scope.collections if c.name == coll_name), None)
        return None

    # ------------------------------------------------------------------
    # setup() override: auto-register TwistedTestEnvironmentCollectionManager
    # ------------------------------------------------------------------

    def setup(self,
              test_suite: str,
              collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT,
              num_docs: int = 50,
              data_set: list[DocType] | None = None,
              collect_topology: bool = False) -> None:
        """Full suite setup: managers, named collections, data loading.

        The sync setup() with two substitutions: the twisted collection manager, and
        `is_deferred=False` on both try_n_times calls, since neither wrapped callable
        returns a Deferred.
        """
        # Stored rather than threaded through load_data; see the sync setup().
        if data_set is not None:
            self._data_set = data_set

        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            if TwistedTestEnvironmentCollectionManager not in self._manager_registry:
                self._manager_registry[TwistedTestEnvironmentCollectionManager] = (
                    TwistedTestEnvironmentCollectionManager()
                )

        # _setup_managers() itself is generic; only the registration above is twisted-specific.
        self._setup_managers()

        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            if TwistedTestEnvironmentCollectionManager not in self._manager_registry:
                self._no_collection_manager(collection_strategy, test_suite)

            coll_mgr = self.get_manager(TwistedTestEnvironmentCollectionManager)
            self.try_n_times(
                5, 3, coll_mgr.drop_stale_scopes, self.named_collection_prefix(),
                is_deferred=False,
            )

            def provision() -> tuple[str, str]:
                # Fresh names per attempt; see the sync setup().
                names = self.new_named_collection_names()
                coll_mgr.setup_named_collection(*names)
                return names

            scope_name, coll_name = self.try_n_times(5, 3, provision, is_deferred=False)
            named_scope = self._bucket.scope(scope_name)
            named_coll = named_scope.collection(coll_name)
            self._pool.add(CollectionHandle(
                scope_name=scope_name,
                collection_name=coll_name,
                collection=named_coll,
                strategy=collection_strategy,
                is_clean=(collection_strategy == CollectionStrategy.NAMED_EMPTY),
            ))
            self._pool.use(coll_name)
            self.try_n_times(10, 1, self._kv_knows_current_collection, is_deferred=False)

        # Before data loading; see the sync setup().
        if collect_topology:
            self._topology = self._collect_topology()

        if collection_strategy != CollectionStrategy.NAMED_EMPTY:
            self.try_n_times(5, 3, self.load_data, num_docs=num_docs, is_deferred=False)

    def teardown(self,
                 collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT) -> None:
        """Purge data, drop named collections via reactor thread, teardown managers."""
        try:
            self.try_n_times(5, 3, self.purge_data, is_deferred=False)
        finally:
            # Dropped even when the purge fails; see the sync teardown().
            if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
                handle = self._pool.current
                if TwistedTestEnvironmentCollectionManager in self._manager_registry:
                    coll_mgr = self.get_manager(TwistedTestEnvironmentCollectionManager)
                    self.try_n_times(
                        5, 3, coll_mgr.teardown_named_collection, handle.scope_name,
                        is_deferred=False,
                    )
                self._pool._handles = [self._pool._handles[0]]
                self._pool._current_index = 0

        self._teardown_managers()

    # ------------------------------------------------------------------
    # Overridden retry helpers (use deferred_sleep)
    # ------------------------------------------------------------------

    # Static, like the sync base and the async subclass, and that is not a style choice.
    # The suites call these through the class, `TestEnvironment.try_n_times(10, 1, ...)`,
    # throughout collection_t and bucketmgmt_t.  The generator rewrites the class
    # name and nothing else, so an instance method here binds `self` to the first
    # positional argument: num_times becomes self, seconds_between becomes num_times, and
    # the retry loop runs a wrong number of times against a wrong delay without ever
    # raising.  Reaching _rit() through the class rather than through self is what makes
    # the static form possible, and it costs nothing because _rit is already a classmethod.
    @staticmethod
    def try_n_times(num_times: int,
                    seconds_between: int | float,
                    func: Callable[..., Any],
                    *args: Any,
                    is_deferred: bool = True,
                    **kwargs: Any) -> Any:
        return RetryHelper.try_n_times_twisted(
            TwistedTestEnvironment._rit(), num_times, seconds_between, func, *args,
            is_deferred=is_deferred, **kwargs,
        )

    @staticmethod
    def try_n_times_till_exception(num_times: int,
                                   seconds_between: int | float,
                                   func: Callable[..., Any],
                                   *args: Any,
                                   is_deferred: bool = True,
                                   expected_exceptions: tuple[type[Exception], ...] = (Exception,),
                                   raise_exception: bool = False,
                                   **kwargs: Any) -> None:
        RetryHelper.try_n_times_till_exception_twisted(
            TwistedTestEnvironment._rit(), num_times, seconds_between, func, *args,
            is_deferred=is_deferred,
            expected_exceptions=expected_exceptions,
            raise_exception=raise_exception,
            **kwargs,
        )

    @staticmethod
    def wait_until(predicate: Callable[[], Any],
                   *,
                   condition: str,
                   timeout: float = 30,
                   interval: float = 1,
                   is_deferred: bool = True,
                   note: str | None = None) -> None:
        """Twisted variant of TestEnvironment.wait_until.

        Static for the same reason the other two are; see the note above try_n_times.
        """
        RetryHelper.wait_until_twisted(
            TwistedTestEnvironment._rit(), predicate,
            condition=condition,
            timeout=timeout,
            interval=interval,
            is_deferred=is_deferred,
            note=note,
        )

    # ------------------------------------------------------------------
    # Bucket-pool overrides
    # ------------------------------------------------------------------
    #
    # Only these three.  The sync versions name TestEnvironmentBucketManager explicitly and
    # would resolve the wrong manager here; everything else on the base class that touches
    # buckets (init_bucket_pool, get_bucket_name, get_bucket_names) is pure in-memory set
    # arithmetic and is correctly inherited.
    #
    # Note what is NOT overridden and why, because it is the more interesting half:
    #
    #   _count_rows      the shared QueryResult dispatches on request type, and under
    #   assert_rows_*    txcouchbase self._request is a TxN1QLRequest, so rows() returns a
    #                    plain __iter__().  The inherited synchronous body is correct.  The
    #                    Deferred lives on cluster.query() itself, not on the iteration,
    #                    which is the opposite of acouchbase: there query() returns
    #                    immediately and the *rows* are async.  The two APIs put the
    #                    asynchrony in different places, so neither transform is a
    #                    relabelling of the other.
    #   get_existing_doc load_data() is already overridden above, synchronously, so the
    #                    inherited cold-cache fallthrough calls the right thing.  Contrast
    #                    AsyncTestEnvironment, where load_data is a coroutine and the
    #                    inherited version silently drops it.

    def create_bucket(self, bucket_name: str, storage_backend: Any = None) -> None:
        from couchbase.management.buckets import (BucketType,
                                                  CreateBucketSettings,
                                                  StorageBackend)
        from tests.environments.managers import TwistedTestEnvironmentBucketManager
        bm = self.get_manager(TwistedTestEnvironmentBucketManager)
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
            bm.create_bucket(CreateBucketSettings(**settings_kwargs))
        except Exception as ex:                      # BucketAlreadyExistsException
            if type(ex).__name__ != 'BucketAlreadyExistsException':
                raise
        self.consistency.wait_until_bucket_healthy(bucket_name)

    def drop_bucket(self) -> None:
        from couchbase.exceptions import BucketDoesNotExistException
        from tests.environments.managers import TwistedTestEnvironmentBucketManager
        available = self._used_bucket_ids.difference(self._dropped_bucket_ids,
                                                     self._reserved_bucket_ids)
        # @FINDING(PYCBC-1867): carried over from the sync version deliberately; see the
        # matching note on AsyncTestEnvironment.drop_bucket.
        bucket_name = available.pop()
        self._dropped_bucket_ids.add(bucket_name)
        bm = self.get_manager(TwistedTestEnvironmentBucketManager)
        try:
            bm.drop_bucket(bucket_name)
        except BucketDoesNotExistException:
            pass
        self.consistency.wait_until_bucket_dropped(bucket_name)

    def teardown_bucket_pool(self) -> None:
        from couchbase.exceptions import BucketDoesNotExistException
        from tests.environments.managers import TwistedTestEnvironmentBucketManager
        bm = self.get_manager(TwistedTestEnvironmentBucketManager)
        for b in self._used_bucket_ids:
            self.try_n_times_till_exception(
                10, 3, bm.drop_bucket, b, is_deferred=False,
                expected_exceptions=(BucketDoesNotExistException,),
            )
        self._used_bucket_ids.clear()

    # ------------------------------------------------------------------
    # Twisted-specific sleep helper (delegated from RetryHelper)
    # ------------------------------------------------------------------

    @staticmethod
    def _deferred_sleep(sleep_seconds: float) -> Any:
        """Return a Deferred that resolves after sleep_seconds.
        Called via run_in_reactor_thread so the blocking side waits.
        Delegates to RetryHelper._deferred_sleep.
        """
        return RetryHelper._deferred_sleep(sleep_seconds)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def get_environment(cls, **kwargs: Any) -> TwistedTestEnvironment:
        rit = _get_run_in_reactor_thread()
        config = kwargs.get('couchbase_config')
        if not isinstance(config, CouchbaseConfig):
            raise CouchbaseTestEnvironmentException('No test config provided.')

        conn_string = config.get_connection_string()
        # Inherited from TestEnvironment rather than copied, for the reason the async
        # environment's identical call already gives: option building is the same across the
        # API styles, and a per-style copy drifts a key at a time with nothing to report it.
        # Only some of those keys have a suite that would notice.
        opts = cls._build_cluster_options(config, kwargs)

        env_args: dict[str, Any] = {}
        okay = False
        cluster = bucket = None
        for _ in range(3):
            try:
                cluster = TwistedCluster(conn_string, opts)
                rit(cluster.on_connect)
                bucket = cluster.bucket(f'{config.bucket_name}')
                rit(bucket.on_connect)
                rit(cluster.cluster_info)
                env_args['cluster'] = cluster
                env_args['bucket'] = bucket
                env_args['default_collection'] = bucket.default_collection()
                okay = True
                break
            # See the sync environment's get_environment for what is retried.
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException, InternalServerFailureException):
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


# ---------------------------------------------------------------------------
# build_environment / teardown_environment: twisted variants
# ---------------------------------------------------------------------------
#
# Unlike the async pair these are not coroutines: env.setup() and env.teardown() are
# synchronous here and do their reactor dispatching internally via run_in_reactor_thread.
# So the generated txcouchbase fixture stays a plain `def ... yield`, and only the *name*
# changes.  That asymmetry with build_async_environment is deliberate and is the reason the
# transformer resolves these by introspection rather than by assuming the api's shape.

def build_twisted_environment(
    base_env: TestEnvironment,
    suite_cls: type[SuiteRequirements],
    collection_strategy: CollectionStrategy | None = None,
    **kwargs: Any,
) -> TwistedTestEnvironment:
    """Twisted counterpart of build_environment() for txcouchbase test suites."""
    check_suite_features(base_env, suite_cls)
    strategy = (
        collection_strategy if collection_strategy is not None
        else suite_cls.COLLECTION_STRATEGY
    )
    num_docs = suite_cls.NUM_DOCS
    data_set = suite_cls.DATA_SET

    # Derived from the mixin, not declared; see build_environment().
    from tests.suite_fixtures import TopologySuite
    collect_topology = issubclass(suite_cls, TopologySuite)
    managers = [cls() for cls in suite_cls.MANAGERS]

    if suite_cls.CONNECTION_STRATEGY == ConnectionStrategy.NEW_CONNECTION:
        env = TwistedTestEnvironment.get_environment(
            couchbase_config=base_env.config,
            data_provider=base_env.data_provider,
            managers=managers,
            **kwargs,
        )
    else:
        env = TwistedTestEnvironment(
            **TestEnvironment._core_args(base_env),
            managers=managers,
            **kwargs,
        )

    env.setup(test_suite=suite_cls.__name__,
              collection_strategy=strategy,
              num_docs=num_docs,
              data_set=data_set,
              collect_topology=collect_topology)
    return env


def teardown_twisted_environment(
    env: TwistedTestEnvironment,
    suite_cls: type[SuiteRequirements],
    collection_strategy: CollectionStrategy | None = None,
) -> None:
    strategy = (
        collection_strategy if collection_strategy is not None
        else suite_cls.COLLECTION_STRATEGY
    )
    env.teardown(collection_strategy=strategy)
    if suite_cls.CONNECTION_STRATEGY == ConnectionStrategy.NEW_CONNECTION:
        _get_run_in_reactor_thread()(env.cluster.close)


# ---------------------------------------------------------------------------
# Session-scoped base fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope='session', name='txcb_base_env')
def txcouchbase_base_environment(
    couchbase_test_config: CouchbaseConfig,
) -> Generator[TwistedTestEnvironment, None, None]:
    """Session-scoped TwistedTestEnvironment.  Requires reactor to be running
    (init_reactor() must be called in conftest.py before this fixture runs).
    """
    # DataProvider builds docs eagerly in __init__; no build_docs() call needed.
    data_provider = DataProvider(100)
    env = TwistedTestEnvironment.get_environment(
        couchbase_config=couchbase_test_config,
        data_provider=data_provider,
    )
    yield env
    rit = _get_run_in_reactor_thread()
    rit(env.cluster.close)
