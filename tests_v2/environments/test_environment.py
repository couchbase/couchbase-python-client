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
TestEnvironment hierarchy for tests_v2/.

TestEnvironment owns *only* cluster/bucket/collection connections, data loading,
named-collection lifecycle, and feature checks.  It does NOT own management objects: those
are delegated to TestEnvironmentManager subclasses registered in _manager_registry (see
the environments/managers package) and reached through get_manager(cls), a TypeVar-generic
typed accessor.

CollectionStrategy drives collection setup, with server < 7.0 falling back transparently.
CollectionPool and CollectionHandle carry the named-collection lifecycle.

AsyncTestEnvironment lives in async_environment.py, split out for import hygiene.
"""

from __future__ import annotations

import os
import random
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    ClassVar,
                    Generic,
                    NoReturn,
                    TypeVar,
                    cast)

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.collection import Collection
from couchbase.diagnostics import ServiceType
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  CouchbaseException,
                                  UnAmbiguousTimeoutException)
from couchbase.management.buckets import (BucketType,
                                          CreateBucketSettings,
                                          StorageBackend)
from couchbase.options import ClusterOptions, TransactionConfig
from couchbase.result import (AnalyticsResult,
                              MutationResult,
                              QueryResult,
                              SearchResult)
from couchbase.scope import Scope
from couchbase.transcoder import (JSONTranscoder,
                                  RawBinaryTranscoder,
                                  RawStringTranscoder)
from tests_v2.assert_helpers import cb_assert_eq, cb_assert_isinstance
from tests_v2.consistency import ConsistencyChecker
from tests_v2.couchbase_config import CouchbaseConfig
from tests_v2.data_provider import DataProvider
from tests_v2.environments import CouchbaseTestEnvironmentException
from tests_v2.skips import (env_degraded,
                            server_label,
                            skip_feature_unsupported,
                            skip_protocol_mismatch)
from tests_v2.test_features import (EnvironmentFeatures,
                                    ServerContext,
                                    ServerFeatures,
                                    TestProtocol)
from tests_v2.test_utils import RetryHelper

if TYPE_CHECKING:
    from tests_v2.environments.managers import TestEnvironmentManager
    from tests_v2.mock_server import MockServerType

M = TypeVar('M', bound='TestEnvironmentManager')

# The four SDK handle types a test body reaches through cb_env.  They are type
# parameters rather than concrete annotations because the sync, acouchbase and
# txcouchbase classes share no base class: each derives straight from object, so
# a subclass cannot narrow an inherited property to its own style's type.
ClusterT = TypeVar('ClusterT')
BucketT = TypeVar('BucketT')
ScopeT = TypeVar('ScopeT')
CollectionT = TypeVar('CollectionT')


# ---------------------------------------------------------------------------
# CollectionStrategy / ConnectionStrategy
# ---------------------------------------------------------------------------

class CollectionStrategy(Enum):
    """How a test suite accesses Couchbase collections.

    Two independent things are encoded here, and it is worth keeping them apart:

    * *Which keyspace the request addresses.*  `_default`.`_default` is a different
      encoding on the wire from a named scope/collection, so both are worth covering.
    * *Whether the suite can enumerate the collection and know everything it will find.*

    The fourth combination, a shared `_default` whose contents are known, does not exist
    and cannot be built: `_default` cannot be dropped or recreated, flushing is
    bucket-wide, and under xdist other workers are writing to it concurrently.  Suites
    needing that guarantee must take a private collection, which is what NAMED_EMPTY is
    for.  See `is_clean_collection` for what a suite should branch on.
    """
    DEFAULT = 1      # _default._default; shared, isolate by UUID-prefixed keys
    NAMED = 2        # Private per-worker test-scope/test-collection, seeded by load_data()
    NAMED_EMPTY = 3  # Same, but load_data() is skipped, so it holds only what the suite writes


class ConnectionStrategy(Enum):
    """Whether a suite shares the session-scoped cluster or needs its own."""
    SHARED = 1          # Reuse the session-scoped base environment (default)
    NEW_CONNECTION = 2  # Create a fresh Cluster (tracing, metrics, transcoder)


class DocType(Enum):
    """A kind of document load_data() can seed, named by the key infix it writes.

    A suite declares DATA_SET and gets exactly those types.  Deriving the corpus from
    anything else, the suite's module name in particular, makes it a property of where the
    tests happen to live: renaming the file silently changes the corpus, and a second suite
    wanting the same one has no way to ask for it.

    The value is the infix `get_*_by_type` matches on, so it is also what `_doc_types`
    reports.  COUNTER_EMPTY is deliberately absent: nothing seeds it, since its whole
    point is a key that does *not* exist yet.  Use get_new_counter_doc_keys() for those.
    """
    VEHICLE = 'vehicle'          # dealership/vehicle JSON, the default corpus
    SIMPLE = 'simple'            # minimal {batch, id, type} docs, for the multi-op suites
    BYTES = 'bytes'              # non-empty bytes, RawBinaryTranscoder
    BYTES_EMPTY = 'bytes_empty'  # b'', the append/prepend target
    COUNTER = 'counter'          # int, plain transcoder
    UTF8 = 'utf8'                # non-empty str, RawStringTranscoder
    UTF8_EMPTY = 'utf8_empty'    # '', the append/prepend target

    # The three subdoc corpora.  Named for the shape of the *document*, not for the suite
    # that wants it, and the first two are deliberately not called ARRAY and COUNT: a bare
    # COUNT sitting next to COUNTER above reads as a synonym for it, and they are nothing
    # alike.  COUNTER is a bare int written for the binary increment/decrement surface;
    # COUNT_FIELD is an ordinary JSON document that happens to carry a numeric field, which
    # is what subdoc increment/decrement addresses by path.  The `_FIELD` suffix says that
    # difference out loud.
    ARRAY_FIELD = 'array'        # JSON doc with an 'array' field holding [1, 2, 3, 4, 5]
    COUNT_FIELD = 'count'        # JSON doc with a 'count' field holding 100
    LIST_ONLY = 'list_only'      # the document *is* a bare JSON array, initially []


# ---------------------------------------------------------------------------
# Declarative suite contract
# ---------------------------------------------------------------------------

class SuiteRequirements:
    """The declarative contract a test suite class states about its environment.

    A base class rather than a Protocol because every member is optional at the suite, while
    Protocol members are required for structural compatibility.  Inheriting the defaults states
    each one once, and keeps every build_environment/teardown_environment variant from carrying
    a `getattr(suite_cls, NAME, <default>)` fallback per member.

    REQUIRED_FEATURES and UNSUPPORTED_PROTOCOLS are declared here for completeness, but
    check_required_features and check_unsupported_protocols must keep reading them through
    getattr: those fixtures are autouse for every class in the tree, including cluster-free unit
    classes that have no reason to inherit this.
    """

    # Manager classes the suite needs instantiated on its environment.  Read but never
    # mutated, so the shared empty-list default is safe.
    MANAGERS: ClassVar[list[type[TestEnvironmentManager]]] = []
    COLLECTION_STRATEGY: ClassVar[CollectionStrategy] = CollectionStrategy.DEFAULT
    CONNECTION_STRATEGY: ClassVar[ConnectionStrategy] = ConnectionStrategy.SHARED
    NUM_DOCS: ClassVar[int] = 50

    # Which document types load_data() seeds, NUM_DOCS of each.  Read at setup() and
    # stored on the environment, so the lazy load in get_existing_doc() seeds the same
    # corpus rather than silently falling back to vehicles.  Never mutated, so the
    # shared default list is safe.
    DATA_SET: ClassVar[list[DocType]] = [DocType.VEHICLE]

    REQUIRED_FEATURES: ClassVar[list[ServerFeatures]] = []

    # A deny-list, not an allow-list of supported protocols.  A suite naming no protocol
    # runs under all of them, so a protocol added later is included unless someone states
    # otherwise.  An allow-list excludes it from every suite that had only ever needed to
    # name the one protocol in existence when it was written, and nothing reports that.
    UNSUPPORTED_PROTOCOLS: ClassVar[list[TestProtocol]] = []

    # The parity manifest: every test method the suite is expected to define.
    # Declared here rather than in a Protocol because `type[SomeProtocol]` does not
    # accept a class that merely assigns the member, which is why this contract is a
    # base class throughout.
    TEST_MANIFEST: ClassVar[list[str]] = []


# ---------------------------------------------------------------------------
# CollectionHandle / CollectionPool
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TopologyFacts:
    """Cluster and shared-test-bucket facts, read once during setup().

    Each field is named for *which* bucket it describes, and that is the whole point.  These
    describe `cb_env.bucket`, the shared test bucket, and do not change when a test creates a
    bucket of its own.  A suite that creates or mutates a bucket must read that bucket back
    through its own manager: `bucketmgmt_t` is the suite that has to care, and it already does
    the right thing, calling `bm.get_bucket(bucket_name)` on the name it just created.  A field
    called plain `num_replicas` would invite exactly the wrong reach.

    Granularity is unchanged from reading these in the fixtures.  Those were class-scoped and
    `cb_env` is a class-scoped parametrized fixture, so both forms capture once per
    (suite class, collection strategy).  `num_nodes` is no more of a snapshot either: the SDK
    populates `cluster_info` at connect, so neither form ever observed a node joining mid-run.

    test_bucket_replicas is None when the suite declares no bucket manager, since there is then
    nothing that can read it.  The fixture exposing it reports that as a broken environment
    rather than substituting a number.
    """
    num_nodes: int
    test_bucket_replicas: int | None
    test_bucket_kv_endpoints: int


@dataclass
class CollectionHandle:
    """A live collection reference together with its strategy metadata."""
    scope_name: str
    collection_name: str
    collection: Any            # couchbase.collection.Collection
    strategy: CollectionStrategy
    is_clean: bool = False     # True when NAMED_EMPTY, i.e. no seed corpus was loaded


class CollectionPool:
    """Ordered pool of CollectionHandles with a moveable 'current' pointer."""

    def __init__(self) -> None:
        self._handles: list[CollectionHandle] = []
        self._current_index: int = 0

    def add(self, handle: CollectionHandle) -> None:
        self._handles.append(handle)

    @property
    def current(self) -> CollectionHandle:
        if not self._handles:
            raise CouchbaseTestEnvironmentException('CollectionPool is empty.')
        return self._handles[self._current_index]

    def use(self, label: str | int) -> CollectionHandle:
        """Move the current pointer to the handle whose collection_name (or index) matches label."""
        if isinstance(label, int):
            self._current_index = label
            return self.current
        for i, h in enumerate(self._handles):
            if h.collection_name == label:
                self._current_index = i
                return self.current
        raise CouchbaseTestEnvironmentException(f'No CollectionHandle with label {label!r} in pool.')

    def all(self) -> list[CollectionHandle]:
        return list(self._handles)


# ---------------------------------------------------------------------------
# TestEnvironment
# ---------------------------------------------------------------------------

class TestEnvironment(Generic[ClusterT, BucketT, ScopeT, CollectionT]):
    NOT_A_KEY = 'not-a-key'
    TEST_SCOPE = 'test-scope'
    TEST_COLLECTION = 'test-collection'

    def __init__(self, **kwargs: Any) -> None:
        # The four SDK handles stay Any internally.  Only the public properties below are
        # typed, via the class's type parameters; this class's own methods cannot use those
        # types, since an unbound TypeVar has no attributes.  See _current_collection.
        self._cluster: Any = kwargs.pop('cluster', None)
        self._bucket: Any = kwargs.pop('bucket', None)
        self._config: CouchbaseConfig = kwargs.pop('couchbase_config', None)
        self._data_provider: DataProvider = kwargs.pop('data_provider', None)
        self._default_collection: Any = kwargs.pop('default_collection', None)
        self._default_scope: Any = (
            self._default_collection._impl._scope
            if self._default_collection is not None else None
        )

        # Collection pool: always has at least one handle (the default collection).
        self._pool = CollectionPool()
        self._pool.add(CollectionHandle(
            scope_name='_default',
            collection_name='_default',
            collection=self._default_collection,
            strategy=CollectionStrategy.DEFAULT,
        ))

        # Manager registry, filled at setup() time.
        # Keys are concrete TestEnvironmentManager subclasses.
        self._manager_registry: dict[type[TestEnvironmentManager], TestEnvironmentManager] = {}
        for mgr in kwargs.pop('managers', None) or []:
            self._manager_registry[type(mgr)] = mgr

        # Document tracking
        self._extra_docs: dict[str, Any] = {}
        self._loaded_docs: dict[str, Any] = {}
        self._used_docs: set = set()
        self._used_extras: set = set()
        self._doc_types = [DocType.VEHICLE.value]
        self._batch_id: str | None = None

        # Which DocTypes to seed, set by setup() from the suite's DATA_SET, and which keys
        # each type actually produced.  _loaded_by_type is what the by-type draws filter on,
        # so a draw never has to recognise a type from the shape of its key.  Matching an
        # infix like '::bytes::' against every loaded key holds only until a key of another
        # type contains it, which is the fragility DocType's docstring describes, one layer
        # down.
        self._data_set: list[DocType] = [DocType.VEHICLE]
        self._loaded_by_type: dict[DocType, list[str]] = {}

        # The bucket a suite created for itself, and its default collection.  Set by the views
        # environment through set_view_bucket(); None everywhere else.  See the properties.
        self._view_bucket: Any = None
        self._view_collection: Any = None

        # The bucket the collection management suites create for themselves.  Same idea as
        # _view_bucket and set for the same reason, that the suite cannot do its work on a
        # bucket other suites share; a separate attribute because the two are unrelated and a
        # single one would let either suite read the other's.  Set through set_collection_mgmt_bucket().
        self._collection_mgmt_bucket: Any = None

        # Bucket name pool, used by bucket management suites to avoid name
        # collisions across reruns of the same test.
        self._bucket_batch_id: str | None = None
        self._used_bucket_ids: set[str] = set()
        self._reserved_bucket_ids: set[str] = set()
        self._dropped_bucket_ids: set[str] = set()
        self._bucket_ids: list[str] = []

        # Populated by setup() only for suites that inherit TopologySuite.  None means
        # nothing asked for it, which is different from "asked and could not read it".
        self._topology: TopologyFacts | None = None

        # ConsistencyChecker (skipped for mock)
        self._consistency = ConsistencyChecker.from_test_environment(self)

        # Cluster-labels support (not meaningful without a management API)
        if self._config.has_management_api:
            cluster_name = self._consistency.fetch_cluster_name()
            self._supports_cluster_labels = bool(cluster_name and not cluster_name.isspace())
        else:
            self._supports_cluster_labels = False

        # Which services the cluster runs.  Probed once here rather than per gate: it is a
        # property of the deployment, so it cannot change under a run, and one HTTP call at
        # connect time is cheaper than one per suite.  None means *unknown*, which is not
        # the same as empty and must never be read as one; see the property below.
        self._available_services: frozenset[ServiceType] | None = (
            self._consistency.fetch_services() if self._config.has_management_api else None
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def available_services(self) -> frozenset[ServiceType] | None:
        """Services this cluster runs, or None when the question does not apply.

        None wherever CouchbaseConfig.has_management_api is False, and the two targets that
        makes it are None for different reasons.  The mock's answer would be a lie in the
        permissive direction (GoCAVES advertises ports for services it does not implement),
        and it is also unnecessary: FEATURES_NOT_IN_MOCK already answers for every feature
        the service gate would cover, and answers first.  Nothing plays that part for
        couchbase2, so under it the service gate waves every service through.

        None therefore means "not asked", never "asked and got nothing back".  A real server
        that cannot be probed raises out of __init__ instead, because a cluster whose service
        list cannot be read is a broken environment, and treating it as an empty list would
        skip every non-kv suite in the run and leave the report looking clean.
        """
        return self._available_services

    @property
    def bucket(self) -> BucketT:
        return self._bucket

    @property
    def view_bucket(self) -> BucketT:
        """The bucket the views suites create, seed and query, rather than the shared one.

        A view is a property of a bucket, so a suite that upserts a design document into the
        shared test bucket makes it visible to every other suite in the run, and drops it from
        underneath them at teardown.  The views suites therefore own a bucket outright.

        Here rather than on a subclass because there is one environment class per api style and
        no per-suite subclasses, so there is nowhere else for the two to live.  It also has to
        be an attribute: `cb_env.<attr>` is the shape the generator's resolver knows how to map
        to an api-specific class.

        Raises rather than returning None when nothing set it, because every reader is a views
        suite whose environment establishes this in setup; None would surface as an
        AttributeError on a later line naming the SDK rather than the environment.
        """
        if self._view_bucket is None:
            raise CouchbaseTestEnvironmentException(
                'No view bucket on this environment.  A suite reaching cb_env.view_bucket must '
                'go through the views environment, whose setup_views() creates it.'
            )
        return self._view_bucket

    @property
    def view_collection(self) -> CollectionT:
        """The view bucket's default collection, which is where the view corpus is seeded."""
        if self._view_collection is None:
            raise CouchbaseTestEnvironmentException(
                'No view collection on this environment; see view_bucket.'
            )
        return self._view_collection

    @property
    def collection_mgmt_bucket(self) -> BucketT:
        """The bucket a collection management suite creates and manages, not the shared one.

        Its scopes and collections are created and dropped by the dozen and counted by
        get_all_scopes, none of which a bucket shared with the rest of the run can survive.  The
        suite reaches it for the two things a manager cannot do: name the bucket it is asserting
        about to the consistency checker, and open a collection to write a document into.
        """
        if self._collection_mgmt_bucket is None:
            raise CouchbaseTestEnvironmentException(
                'No management bucket on this environment.  A suite reaching cb_env.collection_mgmt_bucket '
                'must have created one and registered it through set_collection_mgmt_bucket().'
            )
        return self._collection_mgmt_bucket

    def set_collection_mgmt_bucket(self, bucket: Any) -> None:
        """Record the bucket a collection management suite created.

        Opened by the manager rather than here: acouchbase needs an `await bucket.on_connect()`
        before the bucket is usable, and the manager already has one class per api style to put
        that in.  It hands the connected bucket back and the suite passes it through to this.
        """
        self._collection_mgmt_bucket = bucket

    def set_view_bucket(self, bucket: Any, collection: Any) -> None:
        """Record the bucket a views suite created, and its default collection.

        Both are passed in rather than the collection being derived here: acouchbase needs an
        `await bucket.on_connect()` between opening the bucket and taking its default
        collection, so the sequence belongs in the api-specific environment that knows it.
        """
        self._view_bucket = bucket
        self._view_collection = collection

    @property
    def cluster(self) -> ClusterT:
        return self._cluster

    @property
    def collection(self) -> CollectionT:
        """Always returns the *current* collection from the pool."""
        return self._current_collection

    @property
    def _current_collection(self) -> Any:
        """Untyped view of `collection`, for this class's own implementation.

        `collection` is typed as CollectionT, which is what a test body wants and what
        makes `cb_env.collection.<typo>` an error.  It is unusable *inside* this class,
        though: CollectionT is unbound here, and an unbound TypeVar has no attributes, so
        `self.collection.upsert(...)` does not check.  Internal call sites go through this
        instead.  Same reasoning for `_current_scope`.
        """
        return self._pool.current.collection

    @property
    def collection_pool(self) -> CollectionPool:
        return self._pool

    @property
    def config(self) -> CouchbaseConfig:
        return self._config

    @property
    def consistency(self) -> ConsistencyChecker:
        return self._consistency

    @property
    def data_provider(self) -> DataProvider:
        return self._data_provider

    @property
    def default_collection(self) -> CollectionT:
        return self._default_collection

    @property
    def default_scope(self) -> ScopeT:
        return self._default_scope

    @property
    def topology(self) -> TopologyFacts:
        """Cluster and shared-test-bucket facts; see TopologyFacts.

        Raises rather than lazily collecting, because collection needs I/O and this property
        cannot await one.  A suite reaches this only by inheriting TopologySuite, and
        build_environment collects the facts for exactly the suites that inherit it, so the
        raise means the mixin was bypassed.
        """
        if self._topology is None:
            raise CouchbaseTestEnvironmentException(
                'Topology facts were never collected.  A suite reaching cb_env.topology must '
                'inherit TopologySuite, which is what build_environment keys the collection '
                'off.'
            )
        return self._topology

    @property
    def loaded_keys(self) -> list[str]:
        """Keys load_data() seeded, in seed order.  Read-only view for setup postconditions."""
        return list(self._loaded_docs.keys())

    @property
    def is_clean_collection(self) -> bool:
        """True when nothing but this suite's own writes can be in the current collection.

        Only NAMED_EMPTY gives that.  Note this is *not* the same as "private": NAMED is
        private too, but load_data() seeds it with vehicle documents, which do not have the
        shape a suite's own assertions expect.  The distinguishing property is the absence
        of a seed corpus, not the absence of other suites.

        Branch on this rather than on the collection strategy directly: the guarantee, not
        the mechanism, is what an assertion actually depends on.

        A suite needs this when it asserts something whose truth depends on the *absence*
        of documents it did not write, i.e. an upper bound or an exact equality over a read
        it did not key-scope.  Lower bounds (`>=`) never need it.  Note that a suite must
        also tolerate foreign documents being of any shape at all, including non-JSON and
        non-dict values, so guard the deserialization too and not only the comparison.
        """
        return self._pool.current.is_clean

    @property
    def is_developer_preview(self) -> bool | None:
        return self._cluster._impl.is_developer_preview

    @property
    def is_mock_server(self) -> bool:
        return self._config.is_mock_server

    @property
    def is_real_server(self) -> bool:
        return not self._config.is_mock_server

    @property
    def mock_server_type(self) -> MockServerType | None:
        if self.is_mock_server and self._config.mock_server is not None:
            return self._config.mock_server.mock_type
        return None

    @property
    def protocol(self) -> TestProtocol:
        """The wire protocol this run targets.

        The axis a test branches on when couchbase2 returns a different value from classic
        rather than no value at all, which is the case UNSUPPORTED_PROTOCOLS cannot express.
        """
        return self._config.protocol

    @property
    def named_collection(self) -> CollectionT | None:
        """Convenience: the first NAMED/NAMED_EMPTY handle in the pool, or None."""
        for h in self._pool.all():
            if h.strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
                return h.collection
        return None

    @property
    def named_scope(self) -> ScopeT | None:
        for h in self._pool.all():
            if h.strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
                return self._bucket.scope(h.scope_name)
        return None

    @property
    def scope(self) -> ScopeT:
        return self._current_scope

    @property
    def _current_scope(self) -> Any:
        """Untyped view of `scope`.  See _current_collection."""
        handle = self._pool.current
        if handle.strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            return self._bucket.scope(handle.scope_name)
        return self._default_scope

    @property
    def server_context(self) -> ServerContext:
        """The four facts the feature gate needs, as one argument.

        Built fresh per access rather than captured in __init__, and the reason is narrow:
        `server_version_short` is read off the live cluster, and snapshotting it would turn
        any window in which it is not yet populated into a permanent env-degraded for the
        rest of the run.  The gate raises on a None version by design, so a stale snapshot
        would be indistinguishable from a broken connection.  The services probe is the one
        field that *is* cached, in __init__, because it costs an HTTP round trip; the other
        three are attribute reads and this runs at class scope, not per operation.
        """
        return ServerContext(version=self.server_version_short,
                             patch=self.server_version_patch,
                             mock_type=self.mock_server_type,
                             services=self._available_services)

    @property
    def server_version(self) -> str | None:
        return self._cluster._impl.server_version

    @property
    def server_version_full(self) -> str | None:
        return self._cluster._impl.server_version_full

    @property
    def server_version_short(self) -> float | None:
        return self._cluster._impl.server_version_short

    @property
    def server_version_patch(self) -> int | None:
        if self.server_version:
            try:
                return int(self.server_version.split('-')[0].split('.')[2])
            except Exception:
                return None
        return None

    @property
    def supports_cluster_labels(self) -> bool:
        return self._supports_cluster_labels

    # ------------------------------------------------------------------
    # Manager registry API
    # ------------------------------------------------------------------

    def get_manager(self, manager_cls: type[M]) -> M:
        """Return the active TestEnvironmentManager of the given type.

        The TypeVar M resolves to the concrete subclass, giving full IDE/type-checker
        completion on the returned object.

        Raises when the manager is absent, either because the suite never registered it or
        because _setup_managers() dropped it as unsupported.  Deliberately an error and not a
        feature-unsupported skip: a suite that reaches for a manager it did not declare has
        an authoring bug, and a suite whose feature really is unsupported should have been
        gated by REQUIRED_FEATURES at class scope.  Skipping here would hide both.
        """
        m = self._manager_registry.get(manager_cls)
        if m is None:
            env_degraded(stage='get-manager',
                         manager=manager_cls.__name__,
                         required_feature=getattr(manager_cls, 'required_feature', None),
                         note=('not registered for this suite; add it to MANAGERS on the '
                               'suite class, or gate the suite on the feature it needs'))
        # The registry is a dict keyed by manager class, so its value type is the common
        # base; only the caller's type argument ties key to value and no annotation can
        # express that.  A cast is the narrow form of that claim: it asserts this one
        # conversion rather than silencing whatever else the line might report.
        return cast(M, m)

    def has_manager(self, manager_cls: type[TestEnvironmentManager]) -> bool:
        """True when the manager is registered, i.e. its required feature is supported here.

        Suite teardown needs this. A class-scoped skip from REQUIRED_FEATURES fires only
        after cb_env has been built, so the environment's finalizer still runs on a server
        that supports none of what the suite needs, and cleanup that reaches for a manager
        has to ask first rather than raise out of teardown.
        """
        return manager_cls in self._manager_registry

    def _setup_managers(self) -> None:
        """Run setup() on each registered manager; drop managers whose feature is unavailable."""
        unsupported: list[type[TestEnvironmentManager]] = []
        for mgr_cls, mgr in self._manager_registry.items():
            required = getattr(mgr_cls, 'required_feature', None)
            if required is not None:
                if not EnvironmentFeatures.is_feature_supported(required, self.server_context):
                    unsupported.append(mgr_cls)
                    continue
            mgr.setup(self)
        for key in unsupported:
            del self._manager_registry[key]

    def _teardown_managers(self) -> None:
        for mgr in self._manager_registry.values():
            mgr.teardown(self)

    # ------------------------------------------------------------------
    # Collection helpers
    # ------------------------------------------------------------------

    def get_collection(self, scope_name: str, coll_name: str, bucket_name: str | None = None) -> Any:
        """Look up a collection spec via get_all_scopes().  Returns None if not found."""
        scope = self.get_scope(scope_name, bucket_name=bucket_name)
        if scope:
            return next((c for c in scope.collections if c.name == coll_name), None)
        return None

    def get_scope(self, scope_name: str, bucket_name: str | None = None) -> Any:
        """Look up a scope spec via get_all_scopes().  Returns None if not found."""
        if bucket_name is None:
            raise CouchbaseTestEnvironmentException('Must provide a bucket name.')
        scopes = self._cluster.bucket(bucket_name).collections().get_all_scopes()
        return next((s for s in scopes if s.name == scope_name), None)

    # ------------------------------------------------------------------
    # Data helpers
    # ------------------------------------------------------------------

    def get_existing_doc(self, key_only: bool = False) -> Any:
        if not self._loaded_docs:
            self.load_data()
        all_keys = set(self._loaded_docs.keys())
        available = all_keys.difference(self._used_docs)
        key = random.choice(list(available))
        self._used_docs.add(key)
        if key_only:
            return key
        return key, self._loaded_docs[key]

    def get_new_doc(self, key_only: bool = False) -> Any:
        doc = self.data_provider.get_new_vehicle()
        self._used_extras.add(doc['id'])
        if key_only:
            return doc['id']
        return doc['id'], doc

    def _draw_keys(self, num_docs: int, doc_type: DocType | None = None) -> list[str]:
        """Draw num_docs *distinct* unused keys, optionally of one DocType, marking them used.

        random.sample, not random.choices.  Sampling with replacement can return the same
        key twice, and the caller's dict(zip(keys, values)) then collapses to fewer entries
        than it asked for: a multi-op test exercises 3 keys where it asked for 4 and still
        reports green, because nothing downstream asserts the count.  Nor is it a rare
        corner.  Draws mark keys used, so a suite well into its own sequence is drawing from
        a pool of a handful, and taking 4 of 8 with replacement collides more often than not.

        Sampling without replacement raises when the pool is too small instead of silently
        returning duplicates, so an over-drawing suite is a setup error naming the
        shortfall.
        """
        if doc_type is None:
            pool = [k for k in self._loaded_docs if k not in self._used_docs]
        else:
            pool = [k for k in self._loaded_by_type.get(doc_type, []) if k not in self._used_docs]
        if len(pool) < num_docs:
            raise CouchbaseTestEnvironmentException(
                f'Cannot draw {num_docs} unused '
                f'{"document" if doc_type is None else f"DocType.{doc_type.name}"} key(s): '
                f'{len(pool)} available.  Raise the suite\'s NUM_DOCS.'
            )
        keys = random.sample(pool, num_docs)
        self._used_docs.update(keys)
        return keys

    def get_existing_doc_by_type(self, doc_type: DocType, key_only: bool = False) -> Any:
        key = self._draw_keys(1, doc_type)[0]
        if key_only:
            return key
        return key, self._loaded_docs[key]

    def get_new_doc_by_type(self, doc_type: DocType, key_only: bool = False) -> Any:
        """One unwritten key->value pair of doc_type, tracked for teardown.

        The by-type counterpart of get_new_doc(), which mints vehicles.  Built on _seed_docs
        so there is exactly one rule per DocType for what a document of that type looks like,
        whether load_data() writes it or a test does.  A second copy of those rules here
        would be free to disagree with the seeded corpus about what a document of a given
        type contains, and nothing compares the two.

        VEHICLE is rejected rather than handled: _seed_docs slices a fixed pre-built list for
        it, so every call would hand back the same document, and that document is already
        seeded.  get_new_doc() is the one that mints a fresh vehicle.
        """
        if doc_type == DocType.VEHICLE:
            raise CouchbaseTestEnvironmentException(
                'get_new_doc_by_type cannot mint a DocType.VEHICLE: the vehicle corpus is a '
                'fixed list and is already seeded.  Use get_new_doc().'
            )
        key, value = next(iter(self._seed_docs(doc_type, 1).items()))
        self._used_extras.add(key)
        if key_only:
            return key
        return key, value

    def get_multiple_existing_docs_by_type(self, doc_type: DocType, num_docs: int) -> list[str]:
        return self._draw_keys(num_docs, doc_type)

    def get_new_counter_doc_keys(self, num_docs: int) -> list[str]:
        """Return num_docs fresh keys for counter documents that do not exist yet.

        Separate from the by-type draws, and not a DocType, because nothing seeds these:
        the whole point of an increment/decrement `initial` is that the document is absent.
        Reaching them through get_multiple_existing_docs_by_type would name them existing in
        the one case where they must not be.  The counter types split in the data provider
        for the same reason.
        """
        keys = [self.data_provider.get_counter_doc_key() for _ in range(num_docs)]
        self._used_extras.update(keys)
        return keys

    def get_docs(self, num_docs: int) -> dict[str, Any]:
        """Draw num_docs distinct seeded documents as a key->value mapping."""
        return {k: self._loaded_docs[k] for k in self._draw_keys(num_docs)}

    def get_new_docs(self, num_docs: int) -> dict[str, Any]:
        """Return num_docs unwritten key->document pairs, tracked for teardown."""
        docs = {}
        for v in self.data_provider.get_simple_docs(num_docs):
            key = f'{v["id"]}'
            self._used_extras.add(key)
            docs[key] = v
        return docs

    def get_batch_id(self) -> str | None:
        """Return doc['batch'] from the first loaded document, or None if nothing is loaded.

        Shared by query/analytics suites, which filter on this to isolate their own
        documents when running against a shared bucket-wide namespace.
        """
        if not self._loaded_docs:
            return None
        if self._batch_id is None:
            self._batch_id = next(iter(self._loaded_docs.values()))['batch']
        return self._batch_id

    @property
    def fqdn(self) -> str:
        """Fully-qualified `bucket`.`scope`.`collection` name for the current collection."""
        return f'`{self._bucket.name}`.`{self._current_scope.name}`.`{self._current_collection.name}`'

    def _count_rows(self, result: QueryResult | AnalyticsResult | SearchResult) -> int:
        """Drain result.rows() and return the count, asserting no row is None.

        The union is not a convenience.  QueryResult and AnalyticsResult share no base
        class in the SDK, so a helper declared on one of them is a type error for the
        other, and the analytics suites are the first callers to need the second.
        SearchResult is here for the same reason, added when rate_limit_t became the first
        caller wanting a search result driven to completion with nothing to assert about its
        rows; the search suites reach SearchEnvironment, which validates every row as it
        drains.  ViewResult is still absent, and stays absent until something needs it.

        The isinstance check is widened rather than dropped.  It is what catches a suite
        handing over the request object, or an un-awaited coroutine, instead of the
        result: both iterate or fail in ways that read as a server problem.
        """
        count = 0
        cb_assert_isinstance(result,
                             (QueryResult, AnalyticsResult, SearchResult),
                             what='result passed to _count_rows')
        for row in result.rows():
            assert row is not None, f'{type(result).__name__} yielded a None row at index {count}'
            count += 1
        return count

    def assert_rows_exact(self, result: QueryResult | AnalyticsResult | SearchResult, expected_count: int) -> None:
        """Assert result yields exactly expected_count non-None rows.

        Use this whenever the suite owns its corpus and therefore knows the answer.  It is
        the right default for views, whose environment creates and drops its own bucket, and
        for any query whose result set the test itself just wrote.

        There is deliberately no `assert_rows(result, n, exact=...)` and no plain
        `assert_rows`.  A default here is a silent policy, and a caller who does not think
        about the question gets whichever meaning the framework picked.  The failure mode
        is specific and it is silent: a single `>=` helper accepts expected_count=0, which
        asserts nothing at all while reading as though it asserts something, and a
        read-your-own-write test that queries the wrong document passes underneath it.
        """
        count = self._count_rows(result)
        assert count == expected_count, f'expected exactly {expected_count} rows, got {count}'

    def assert_rows_at_least(self, result: QueryResult | AnalyticsResult | SearchResult, expected_count: int) -> None:
        """Assert result yields at least expected_count non-None rows.

        Use this only when something outside the test can legitimately add rows: a shared
        cluster where another suite's documents match the same predicate, or a query whose
        result set the test does not fully control.  If the count is knowable, say so with
        assert_rows_exact instead.

        Note that `metadata.total_rows()` is a different quantity from the rows returned and
        keeps its own `>=` comparisons: it counts total matches, while the rows are whatever
        survived a `limit`.  Do not convert those to equality alongside a row count.
        """
        count = self._count_rows(result)
        assert count >= expected_count, f'expected at least {expected_count} rows, got {count}'

    def drain_rows(self, result: QueryResult | AnalyticsResult | SearchResult) -> None:
        """Drive a statement to completion and discard the rows.

        For the statements whose result set is not what is under test: DDL, an UPDATE with
        no RETURNING clause, a SELECT run only to prove the index behind it exists.  What
        those tests assert is that the call returns at all, or that it raises the exception
        the test names.  There is no count to compare against, and the rows still have to be
        drained, because draining is what drives the statement to completion.

        Returns None deliberately.  Handing the count back would invite
        `assert cb_env.drain_rows(result) == 2`, which is assert_rows_exact written the long
        way and out of reach of anything looking for the assertion helpers.  If the count is
        knowable, one of those two is the call to make; this is for when it is not.  It is
        for the same reason not spelled `assert_rows_at_least(result, 0)`: that asserts
        nothing while reading as though it asserts something, which is the failure
        assert_rows_exact's docstring describes.

        This is also what the `.execute()` it replaced did.  `couchbase/n1ql.py` implements
        that as `[r for r in list(self)]` and every caller here discarded the list, but it
        exists only on the sync and acouchbase requests: `txcouchbase.n1ql.N1QLRequest`
        defines `execute_query()` and no `execute()`, so a suite written on `.execute()`
        cannot be generated for txcouchbase at all.
        """
        self._count_rows(result)

    def _seed_docs(self, doc_type: DocType, num_docs: int) -> dict[str, Any]:  # noqa: C901
        """Return the key->value mapping for one DocType, unwritten.

        Pure data, so all three api styles share it: only the upsert differs between them.

        The C901 waiver is on the flat shape being the point.  Every branch is one DocType's
        rule with no nesting, so the measured complexity is just the number of document types
        and rises by one each time the enum grows.  A dispatch table would score better and
        read worse; EnvironmentFeatures.supports_feature waives the same check for the same
        reason.
        """
        dp = self.data_provider
        if doc_type == DocType.VEHICLE:
            return {f'{v["id"]}': v for v in dp.get_vehicles()[:num_docs]}
        if doc_type == DocType.SIMPLE:
            return {f'{v["id"]}': v for v in dp.get_simple_docs(num_docs)}
        if doc_type == DocType.BYTES:
            return cast('dict[str, Any]', dp.get_bytes_docs(num_docs, b'XXXX'))
        if doc_type == DocType.BYTES_EMPTY:
            return cast('dict[str, Any]', dp.get_bytes_docs(num_docs))
        if doc_type == DocType.COUNTER:
            return cast('dict[str, Any]', dp.get_counter_docs(num_docs, 100))
        if doc_type == DocType.UTF8:
            return cast('dict[str, Any]', dp.get_utf8_docs(num_docs, 'XXXX'))
        if doc_type == DocType.UTF8_EMPTY:
            return cast('dict[str, Any]', dp.get_utf8_docs(num_docs))
        if doc_type == DocType.ARRAY_FIELD:
            return {f'{v["id"]}': v for v in dp.get_array_docs(num_docs)}
        if doc_type == DocType.COUNT_FIELD:
            return {f'{v["id"]}': v for v in dp.get_count_docs(num_docs)}
        if doc_type == DocType.LIST_ONLY:
            # get_list_only_docs is already key->value, unlike the two above, which are
            # lists of documents carrying their own 'id'.
            return cast('dict[str, Any]', dp.get_list_only_docs(num_docs))
        raise CouchbaseTestEnvironmentException(f'No seeding rule for DocType.{doc_type.name}.')

    @staticmethod
    def _seed_upsert_kwargs(doc_type: DocType) -> dict[str, Any]:
        """The transcoder to seed one DocType through.  Always explicit, never the default.

        The binary types are not JSON and must be written through the raw transcoders or the
        value comes back wrapped.  The JSON types name JSONTranscoder rather than returning
        `{}` and letting the collection's default apply, because that default is a property of
        the suite, and the suite whose entire subject is changing it is the one it breaks:
        under a cluster-level RawBinaryTranscoder, seeding a DocType.SIMPLE dict raises
        ValueFormatException inside setup, and the failure reads as an environment fault
        rather than as a suite declaring a corpus its transcoder cannot write.

        Seeding is not what any of these suites is testing, so it is stated outright.  Counters
        are plain ints, which JSONTranscoder writes unchanged.
        """
        if doc_type in (DocType.BYTES, DocType.BYTES_EMPTY):
            return {'transcoder': RawBinaryTranscoder()}
        if doc_type in (DocType.UTF8, DocType.UTF8_EMPTY):
            return {'transcoder': RawStringTranscoder()}
        return {'transcoder': JSONTranscoder()}

    def _record_seeded(self, doc_type: DocType, key: str, value: Any) -> None:
        self._loaded_docs[key] = value
        self._loaded_by_type.setdefault(doc_type, []).append(key)

    def _seed_never_written(self, doc_type: DocType, key: str) -> NoReturn:
        # Setup owns its postconditions.  Without this the retry loop could exhaust all
        # three attempts and fall through, leaving the suite to run against a
        # partially-populated collection and report a setup failure as N assertion
        # failures scattered across the tests.
        env_degraded(stage='load-data',
                     doc=key,
                     doc_type=doc_type.value,
                     attempts=3,
                     server=server_label(self.config.is_mock_server),
                     note='document never became writable')

    def load_data(self, num_docs: int = 50) -> None:
        """Seed num_docs of every DocType in the suite's DATA_SET.

        A suite that needs an unseeded collection declares CollectionStrategy.NAMED_EMPTY,
        which skips this entirely.  There is no per-suite exemption here.
        """
        for doc_type in self._data_set:
            kwargs = self._seed_upsert_kwargs(doc_type)
            for key, value in self._seed_docs(doc_type, num_docs).items():
                loaded = False
                for _ in range(3):
                    try:
                        self._current_collection.upsert(key, value, **kwargs)
                        self._record_seeded(doc_type, key, value)
                        loaded = True
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        time.sleep(3)
                        continue
                    except Exception:
                        raise

                if not loaded:
                    self._seed_never_written(doc_type, key)

        self._doc_types = [t.value for t in self._data_set]

    def purge_data(self) -> None:
        try:
            self._current_collection.remove_multi(list(self._loaded_docs.keys()))
        except CouchbaseException:
            pass
        try:
            self._current_collection.remove_multi(list(self._used_extras))
        except CouchbaseException:
            pass
        self._loaded_docs.clear()
        self._loaded_by_type.clear()
        self._used_docs.clear()
        self._used_extras.clear()

    def verify_mutation_tokens(self, bucket_name: str, result: MutationResult) -> None:
        mutation_token = result.mutation_token()
        assert mutation_token is not None, f'{type(result).__name__} carried no mutation token'
        partition_id, partition_uuid, sequence_number, mt_bucket_name = mutation_token.as_tuple()
        # @FINDING(PYCBC-1885): partition_id is the vbucket id, and 0 is a legal vbucket, so it cannot
        # be asserted nonzero.  Keys here are random, so that assertion failed once per ~1024
        # calls.  partition_uuid and sequence_number carry the populated-token signal, and an
        # empty token cannot fake either of them.
        cb_assert_isinstance(partition_id, int, what='mutation token partition_id')
        cb_assert_isinstance(partition_uuid, int, what='mutation token partition_uuid')
        cb_assert_isinstance(sequence_number, int, what='mutation token sequence_number')
        assert partition_uuid != 0, 'mutation token partition_uuid is 0, so the token is empty'
        assert sequence_number != 0, 'mutation token sequence_number is 0, so the token is empty'
        cb_assert_eq(mt_bucket_name, bucket_name, what='mutation token bucket name')

    def verify_mutation_tokens_disabled(self, bucket_name: str, result: MutationResult) -> None:
        mutation_token = result.mutation_token()
        assert mutation_token is not None, f'{type(result).__name__} carried no mutation token'
        partition_id, partition_uuid, sequence_number, mt_bucket_name = mutation_token.as_tuple()
        # @FINDING(PYCBC-1885): same vbucket-id defect as above, and less defensible here: it claimed a
        # token with no sequence still carries a nonzero vbucket.  Absent uuid and sequence are
        # the whole signal.
        cb_assert_isinstance(partition_id, int, what='mutation token partition_id')
        cb_assert_eq(partition_uuid, 0, what='mutation token partition_uuid')
        cb_assert_eq(sequence_number, 0, what='mutation token sequence_number')
        cb_assert_eq(mt_bucket_name, bucket_name, what='mutation token bucket name')

    # ------------------------------------------------------------------
    # Lifecycle: setup / teardown
    # ------------------------------------------------------------------

    def _no_collection_manager(self,
                               collection_strategy: CollectionStrategy,
                               test_suite: str) -> NoReturn:
        """Report a missing collection manager, having first established *why* it is missing.

        Reading absence from the registry and announcing "Server does not support
        Collections" would be an inference from a symptom: absence only ever means "not
        registered", and `_setup_managers()` is not the only thing that can leave it that
        way.  When Collections genuinely is unsupported this is an ordinary expected skip;
        when the gate says the server *does* support Collections and the manager still is not
        there, the environment did not build correctly and saying "unsupported" would retire
        the suite's whole coverage behind a plausible-looking message.

        Shared by all three environments; only the manager class differs and this reports
        rather than looks up.
        """
        supported = EnvironmentFeatures.is_feature_supported(ServerFeatures.Collections,
                                                             self.server_context)
        srv = server_label(self.is_mock_server)
        if supported:
            env_degraded(stage='setup-managers',
                         feature=ServerFeatures.Collections,
                         suite=test_suite,
                         strategy=collection_strategy,
                         server=srv,
                         note=('collection manager absent although the server supports '
                               'Collections; the environment did not build'))
        skip_feature_unsupported(feature=ServerFeatures.Collections,
                                 observed=self.server_version_short,
                                 suite=test_suite,
                                 strategy=collection_strategy,
                                 server=srv,
                                 note=f'CollectionStrategy.{collection_strategy.name} requires Collections')

    def _node_count(self) -> int:
        """Node count from the connect-time cluster info.  Style-agnostic: no I/O, no await."""
        cluster_info = self._cluster._impl.cluster_info
        assert cluster_info is not None, 'Cluster is not connected.'
        assert cluster_info.nodes is not None, 'Cluster info carries no node list.'
        return len(cluster_info.nodes)

    def _collect_topology(self) -> TopologyFacts:
        """Read the topology facts.  Overridden per api style; this is the only I/O involved.

        The whole reason the facts live here rather than in the fixtures that consume them:
        every difference between the three api styles is in these few lines, and setup() is
        already overridden in all three.  That leaves the fixtures pure, hence identical across
        the trees, hence shareable.  See tests_v2/suite_fixtures.py.
        """
        from tests_v2.environments.managers import TestEnvironmentBucketManager

        replicas: int | None = None
        if self.has_manager(TestEnvironmentBucketManager):
            bm = self.get_manager(TestEnvironmentBucketManager)
            settings = TestEnvironment.try_n_times(10, 1, bm.get_bucket, self._bucket.name)
            replicas = settings.get('num_replicas')

        ping_res = self._bucket.ping()
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)

        return TopologyFacts(
            num_nodes=self._node_count(),
            test_bucket_replicas=replicas,
            test_bucket_kv_endpoints=0 if kv_endpoints is None else len(kv_endpoints),
        )

    def setup(self,
              test_suite: str,
              collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT,
              num_docs: int = 50,
              data_set: list[DocType] | None = None,
              collect_topology: bool = False) -> None:
        """Full suite setup: managers, named collections, data loading.

        When collection_strategy is NAMED or NAMED_EMPTY the method auto-ensures
        TestEnvironmentCollectionManager is in the registry, then delegates scope
        and collection creation to it.  If the manager is missing afterwards,
        _no_collection_manager() decides whether that is an expected skip or a
        broken environment; either way setup() never falls back silently.
        """
        from tests_v2.environments.managers import TestEnvironmentCollectionManager

        # Stored rather than threaded through load_data, so the lazy load in
        # get_existing_doc() seeds the suite's own corpus instead of falling back to
        # vehicles behind its back.
        if data_set is not None:
            self._data_set = data_set

        # Auto-register TestEnvironmentCollectionManager when a named collection
        # is needed, even if the suite's MANAGERS list does not mention it.
        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            if TestEnvironmentCollectionManager not in self._manager_registry:
                self._manager_registry[TestEnvironmentCollectionManager] = (
                    TestEnvironmentCollectionManager()
                )

        # Setup managers (feature-checks happen here; unsupported ones are dropped)
        self._setup_managers()

        # Provision named collections when requested
        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            if TestEnvironmentCollectionManager not in self._manager_registry:
                self._no_collection_manager(collection_strategy, test_suite)

            # Determine per-worker scope/collection names for xdist safety
            worker_id = os.environ.get('PYTEST_XDIST_WORKER', '')
            scope_name = f'{self.TEST_SCOPE}-{worker_id}' if worker_id else self.TEST_SCOPE
            coll_name = f'{self.TEST_COLLECTION}-{worker_id}' if worker_id else self.TEST_COLLECTION

            coll_mgr = self.get_manager(TestEnvironmentCollectionManager)
            TestEnvironment.try_n_times(
                5, 3, coll_mgr.setup_named_collection, scope_name, coll_name,
            )
            named_scope = self._bucket.scope(scope_name)
            named_coll = named_scope.collection(coll_name)
            self._pool.add(CollectionHandle(
                scope_name=scope_name,
                collection_name=coll_name,
                collection=named_coll,
                strategy=collection_strategy,
                is_clean=(collection_strategy == CollectionStrategy.NAMED_EMPTY),
            ))
            # Make the named collection the current one
            self._pool.use(coll_name)

        # Topology facts, before data loading: the fixtures that read them can skip a whole
        # class, and there is no reason to seed a collection for a suite that is about to be
        # skipped for having too few nodes.
        if collect_topology:
            self._topology = self._collect_topology()

        # Load data
        if collection_strategy != CollectionStrategy.NAMED_EMPTY:
            TestEnvironment.try_n_times(
                5, 3, self.load_data, num_docs=num_docs
            )

    def teardown(self,
                 collection_strategy: CollectionStrategy = CollectionStrategy.DEFAULT) -> None:
        """Full suite teardown: purge data, drop named collections, teardown managers."""
        from tests_v2.environments.managers import TestEnvironmentCollectionManager

        TestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_strategy in (CollectionStrategy.NAMED, CollectionStrategy.NAMED_EMPTY):
            handle = self._pool.current
            if TestEnvironmentCollectionManager in self._manager_registry:
                coll_mgr = self.get_manager(TestEnvironmentCollectionManager)
                TestEnvironment.try_n_times(
                    5, 3, coll_mgr.teardown_named_collection, handle.scope_name
                )
            # Reset pool to default regardless
            self._pool._handles = [self._pool._handles[0]]
            self._pool._current_index = 0

        self._teardown_managers()

    # ------------------------------------------------------------------
    # Bucket management helpers (used by bucket mgmt suites via mgr fixture)
    # ------------------------------------------------------------------

    def create_bucket(self, bucket_name: str, storage_backend: StorageBackend | None = None) -> None:
        from tests_v2.environments.managers import TestEnvironmentBucketManager
        bm = self.get_manager(TestEnvironmentBucketManager)._bm
        try:
            settings_kwargs: dict[str, Any] = {
                'name': bucket_name,
                'bucket_type': BucketType.COUCHBASE,
                'ram_quota_mb': 100,
            }
            if storage_backend is not None:
                settings_kwargs['storage_backend'] = storage_backend
                if storage_backend is StorageBackend.MAGMA:
                    settings_kwargs['ram_quota_mb'] = 1024
            bm.create_bucket(CreateBucketSettings(**settings_kwargs))
        except BucketAlreadyExistsException:
            pass
        self.consistency.wait_until_bucket_healthy(bucket_name)

    # ------------------------------------------------------------------
    # Bucket name pool (used by bucketmgmt_t.py to avoid name collisions on reruns)
    # ------------------------------------------------------------------

    def init_bucket_pool(self, num_ids: int = 50) -> None:
        """Generate a pool of unique candidate bucket names for this suite run."""
        self._bucket_batch_id = str(uuid.uuid4())[:8]
        self._used_bucket_ids = set()
        self._dropped_bucket_ids = set()
        self._reserved_bucket_ids = set()
        self._bucket_ids = [f'{self._bucket_batch_id}_bucket_{i}' for i in range(num_ids)]

    def get_bucket_name(self) -> str:
        available = set(self._bucket_ids).difference(self._used_bucket_ids)
        name = random.choice(list(available))
        self._used_bucket_ids.add(name)
        return name

    def get_bucket_names(self, num: int = 3) -> list[str]:
        available = set(self._bucket_ids).difference(self._used_bucket_ids)
        names = list(available)[:num]
        self._used_bucket_ids.update(names)
        return names

    def reserve_bucket_name(self) -> str:
        """Issue a pool name that drop_bucket() will never choose.

        drop_bucket() names no bucket: it drops whichever issued name has not been dropped
        yet, which is only correct while every outstanding name is interchangeable.  A suite
        that holds one bucket for its whole class breaks that, and the failure is not the one
        you would guess: the per-test drop takes the suite's bucket, the per-test bucket
        survives, and what fails is the *next* test that needs either of them.  Reserving is
        the smallest thing that keeps both true.  teardown_bucket_pool() still drops these,
        so nothing is left behind.
        """
        name = self.get_bucket_name()
        self._reserved_bucket_ids.add(name)
        return name

    def drop_bucket(self) -> None:
        """Drop one previously-issued, not-yet-dropped, unreserved bucket name."""
        from tests_v2.environments.managers import TestEnvironmentBucketManager
        available = self._used_bucket_ids.difference(self._dropped_bucket_ids,
                                                     self._reserved_bucket_ids)
        # @FINDING(PYCBC-1867): the name is recorded as dropped before the drop is
        # attempted, so a failed drop still marks it gone.  teardown_bucket_pool() only
        # cleans up names not yet marked, so it skips this one and the bucket survives the
        # run; the next test to claim that name fails with BucketAlreadyExistsException,
        # far from the cause.  Record the drop after it succeeds.
        bucket_name = available.pop()
        self._dropped_bucket_ids.add(bucket_name)
        bm = self.get_manager(TestEnvironmentBucketManager)
        try:
            bm.drop_bucket(bucket_name)
        except BucketDoesNotExistException:
            pass
        self.consistency.wait_until_bucket_dropped(bucket_name)

    def teardown_bucket_pool(self) -> None:
        """Drop any bucket names issued but not yet cleaned up by individual tests."""
        from tests_v2.environments.managers import TestEnvironmentBucketManager
        bm = self.get_manager(TestEnvironmentBucketManager)
        for b in self._used_bucket_ids:
            TestEnvironment.try_n_times_till_exception(
                10, 3, bm.drop_bucket, b, expected_exceptions=(BucketDoesNotExistException,)
            )
        self._used_bucket_ids.clear()

    # ------------------------------------------------------------------
    # _core_args: eliminates the repeated 5-line extraction in from_environment()
    # ------------------------------------------------------------------

    @classmethod
    def _core_args(cls, base_env: TestEnvironment) -> dict[str, Any]:
        """Return the minimal kwargs needed to construct a new TestEnvironment from an existing one."""
        return {
            'cluster': base_env._cluster,
            'bucket': base_env._bucket,
            'default_collection': base_env._default_collection,
            'couchbase_config': base_env._config,
            'data_provider': base_env._data_provider,
        }

    # ------------------------------------------------------------------
    # Class-method factory
    # ------------------------------------------------------------------

    @staticmethod
    def _build_cluster_options(config: CouchbaseConfig, kwargs: dict[str, Any]) -> ClusterOptions:
        """Pops the option-building keys it consumes out of kwargs in place."""
        username, pw = config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))

        enable_mutation_tokens = kwargs.pop('enable_mutation_tokens', None)
        if enable_mutation_tokens is not None:
            opts['enable_mutation_tokens'] = enable_mutation_tokens

        meter = kwargs.pop('meter', None)
        if meter:
            opts['meter'] = meter
        else:
            opts['enable_metrics'] = False

        tracer = kwargs.pop('tracer', None)
        if tracer:
            opts['tracer'] = tracer
        else:
            opts['enable_tracing'] = False

        transaction_config = kwargs.pop('transaction_config', None)
        if transaction_config:
            opts['transaction_config'] = transaction_config

        # The cluster-wide default transcoder, which every collection derived from this cluster
        # inherits.  The transcoder suites are what need it, and this is the public way to
        # set it: the alternative reaches `collection._impl._set_default_transcoder()`, which
        # is a private name and sets it on the one collection rather than the cluster.
        transcoder = kwargs.pop('transcoder', None)
        if transcoder:
            opts['transcoder'] = transcoder

        return opts

    @classmethod
    def get_environment(cls, **kwargs: Any) -> TestEnvironment:
        """Create a brand-new TestEnvironment (new Cluster connection)."""
        config = kwargs.get('couchbase_config')
        if not isinstance(config, CouchbaseConfig):
            raise CouchbaseTestEnvironmentException('No test config provided.')

        conn_string = config.get_connection_string()
        opts = cls._build_cluster_options(config, kwargs)

        env_args: dict[str, Any] = {}
        okay = False
        for _ in range(3):
            try:
                cluster = Cluster.connect(conn_string, opts)
                env_args['cluster'] = cluster
                bucket = cluster.bucket(f'{config.bucket_name}')
                env_args['bucket'] = bucket
                cluster.cluster_info()
                env_args['default_collection'] = bucket.default_collection()
                okay = True
                break
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException):
                continue

        if not okay:
            # One raise, carrying which target it was, rather than a skip on the mock and a
            # raise on a real server.  A connection failure is not an accurate representation
            # of the state of the test either way, and skipping means a mock run drops every
            # test in the suite and still reports green.
            env_degraded(stage='connect',
                         server=server_label(config.is_mock_server),
                         attempts=3,
                         note=('CAVES did not become reachable' if config.is_mock_server
                               else 'check the environment (e.g. VPN)'))

        env_args.update(**kwargs)
        return cls(**env_args)

    # ------------------------------------------------------------------
    # Utility statics
    # ------------------------------------------------------------------

    @staticmethod
    def sleep(num_seconds: float) -> None:
        RetryHelper.sleep(num_seconds)

    @staticmethod
    def try_n_times(num_times: int,
                    seconds_between: int | float,
                    func: Callable[..., Any],
                    *args: Any,
                    **kwargs: Any) -> Any:
        return RetryHelper.try_n_times(num_times, seconds_between, func, *args, **kwargs)

    @staticmethod
    def try_n_times_till_exception(num_times: int,
                                   seconds_between: int | float,
                                   func: Callable[..., Any],
                                   *args: Any,
                                   expected_exceptions: tuple[type[Exception], ...] = (Exception,),
                                   raise_exception: bool = False,
                                   **kwargs: Any) -> None:
        RetryHelper.try_n_times_till_exception(
            num_times, seconds_between, func, *args,
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
                   note: str | None = None) -> None:
        """Poll for an observable postcondition; see RetryHelper.wait_until.

        Prefer this over try_n_times whenever the thing being waited for is checkable.  A
        bare sleep() followed by an assertion is a defect.
        """
        RetryHelper.wait_until(predicate,
                               condition=condition,
                               timeout=timeout,
                               interval=interval,
                               note=note)


# ---------------------------------------------------------------------------
# Per-api-style bindings
# ---------------------------------------------------------------------------

# The sync binding of the generic base.  acouchbase and txcouchbase bind theirs by
# subclassing (AsyncTestEnvironment, TwistedTestEnvironment), but the sync tree has no
# subclass to hang the binding on, so it is an alias.  Suites annotate this name, which
# keeps the generator's substitution a single-name rewrite:
#
#     cb_env: SyncTestEnvironment  ->  cb_env: AsyncTestEnvironment
#
# Annotating bare `TestEnvironment` instead would silently mean
# TestEnvironment[Any, Any, Any, Any] and put every test body back on Any, which is the
# condition the annotation exists to remove.
SyncTestEnvironment = TestEnvironment[Cluster, Bucket, Scope, Collection]


def verify_seeded_docs_readable(cb_env: SyncTestEnvironment) -> None:
    """Read back every seeded document, failing setup if one is not yet retrievable.

    Sync-only and opt-in, called from a suite fixture rather than from setup().  Not a
    framework-wide postcondition: it costs one get per seeded document, and only a suite that
    seeds and then immediately reads needs the guarantee, so charging every suite in the tree
    for it is the wrong trade.
    """
    for key in list(cb_env.loaded_keys):
        TestEnvironment.try_n_times(5, 1, cb_env.collection.get, key)


# ---------------------------------------------------------------------------
# build_environment / teardown_environment helpers
# ---------------------------------------------------------------------------
#
# Free functions rather than methods on TestEnvironment, for two reasons.  They are factories:
# each builds a new environment from the session-scoped base one by reading the suite class's
# declarations, rather than acting on an environment that already exists.  And every concrete
# test class calls them by name from its own cb_env fixture, which is what lets the generator
# rewrite the pair to the acouchbase or txcouchbase variant and leave the rest of the fixture
# untouched.

def build_environment(base_env: TestEnvironment,
                      suite_cls: type[SuiteRequirements],
                      collection_strategy: CollectionStrategy | None = None,
                      **kwargs: Any) -> TestEnvironment:
    """Construct and setup a TestEnvironment for a test suite class.

    Reads MANAGERS, COLLECTION_STRATEGY, CONNECTION_STRATEGY, and NUM_DOCS from
    suite_cls, which inherits SuiteRequirements for the defaults. REQUIRED_FEATURES and
    UNSUPPORTED_PROTOCOLS are handled separately, by check_required_features and
    check_unsupported_protocols below.
    """
    strategy = (
        collection_strategy if collection_strategy is not None
        else suite_cls.COLLECTION_STRATEGY
    )
    conn_strategy = suite_cls.CONNECTION_STRATEGY
    num_docs = suite_cls.NUM_DOCS
    data_set = suite_cls.DATA_SET
    managers = [cls() for cls in suite_cls.MANAGERS]

    # Derived, not declared.  A suite that inherits TopologySuite is exactly a suite whose
    # fixtures read cb_env.topology, so deriving the flag from the base class cannot drift the
    # way a declared knob could.  TopologySuite subclasses SuiteRequirements rather than sitting
    # beside it, so this derivation costs no multiple inheritance; see its docstring.  Imported
    # here rather than at module scope because suite_fixtures imports this module.
    from tests_v2.suite_fixtures import TopologySuite
    collect_topology = issubclass(suite_cls, TopologySuite)

    if conn_strategy == ConnectionStrategy.NEW_CONNECTION:
        env = TestEnvironment.get_environment(
            couchbase_config=base_env.config,
            data_provider=base_env.data_provider,
            managers=managers,
            **kwargs,
        )
    else:
        env = TestEnvironment(
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


def teardown_environment(env: TestEnvironment,
                         suite_cls: type[SuiteRequirements],
                         collection_strategy: CollectionStrategy | None = None) -> None:
    strategy = (
        collection_strategy if collection_strategy is not None
        else suite_cls.COLLECTION_STRATEGY
    )
    env.teardown(collection_strategy=strategy)
    if suite_cls.CONNECTION_STRATEGY == ConnectionStrategy.NEW_CONNECTION:
        env.cluster.close()


# ---------------------------------------------------------------------------
# Session-scoped base fixtures (sync only)
# acouchbase fixtures live in async_environment.py
# ---------------------------------------------------------------------------

@pytest.fixture(scope='session', name='test_env_v2')
def base_test_environment_v2(couchbase_test_config: CouchbaseConfig) -> tuple[CouchbaseConfig, DataProvider]:
    # DataProvider builds docs eagerly in __init__; no build_docs() call needed.
    data_provider = DataProvider(100)
    return couchbase_test_config, data_provider


@pytest.fixture(scope='session', name='cb_base_env')
def couchbase_base_environment(test_env_v2: tuple[CouchbaseConfig, DataProvider]) -> SyncTestEnvironment:
    couchbase_config, data_provider = test_env_v2
    return TestEnvironment.get_environment(couchbase_config=couchbase_config, data_provider=data_provider)


@pytest.fixture(scope='session', name='cb_base_txn_env')
def couchbase_base_txn_environment(
    test_env_v2: tuple[CouchbaseConfig, DataProvider],
) -> SyncTestEnvironment:
    couchbase_config, data_provider = test_env_v2
    transaction_config = TransactionConfig(durability=ServerDurability(DurabilityLevel.NONE))
    return TestEnvironment.get_environment(
        couchbase_config=couchbase_config,
        data_provider=data_provider,
        transaction_config=transaction_config,
    )


# ---------------------------------------------------------------------------
# Declarative suite-requirement fixtures
# Registered once here (via pytest_plugins), autouse for every class in tests_v2/.
# ---------------------------------------------------------------------------

@pytest.fixture(scope='class', autouse=True)
def check_required_features(request: pytest.FixtureRequest) -> None:
    """Skip the class unless the server/mock supports every REQUIRED_FEATURES entry.

    cb_env is resolved lazily, and only when the class actually declares
    REQUIRED_FEATURES.  Taking it as a fixture parameter would break every
    cluster-free class in this tree: these fixtures are autouse for the whole
    directory. A class that declares no cb_env of its own, such as a suite that only
    exercises request builders or option validation with no cluster at all, would error
    out with "fixture 'cb_env' not found" before running a single test.
    """
    required: list[ServerFeatures] = getattr(request.cls, 'REQUIRED_FEATURES', [])
    if not required:
        return
    cb_env: TestEnvironment = request.getfixturevalue('cb_env')
    EnvironmentFeatures.check_if_feature_supported(required, cb_env.server_context)


@pytest.fixture(scope='class', autouse=True)
def check_unsupported_protocols(request: pytest.FixtureRequest) -> None:
    """Skip the class when the active protocol is in UNSUPPORTED_PROTOCOLS (empty = all run).

    cb_env is resolved lazily for the same reason as check_required_features above.
    """
    unsupported: list[TestProtocol] = getattr(request.cls, 'UNSUPPORTED_PROTOCOLS', [])
    if not unsupported:
        return
    cb_env: TestEnvironment = request.getfixturevalue('cb_env')
    active = cb_env.config.protocol
    if active in unsupported:
        skip_protocol_mismatch(unsupported=','.join(p.name for p in unsupported),
                               active=active,
                               server=server_label(cb_env.is_mock_server))
