"""Decide, per call site, whether the generated test has to bridge the call.

There is no single heuristic.  There are three tiers, and the third is a table maintained
by hand:

  Tier 1  `inspect.iscoroutinefunction`.  Covers the async SDK's object model, where the
          methods really are `async def`: AsyncCollection, AsyncCluster, AsyncBucket,
          Transactions.

  Tier 2  a `-> Awaitable[...]` return annotation.  Covers the classes that return a
          Future from a plain `def`, which is the whole transactions AttemptContext
          surface (8 of 8 methods).  Tier 1 answers False for every one of them.

  Tier 3  a hand-maintained table, OVERRIDES below.  Covers the streaming result types,
          where a single class serves both APIs and branches on the request type at
          runtime.  Nothing static can distinguish those, and every entry says so.

And one negative rule that matters more than any of the three:

  Tier 0  if the method resolves to something *inherited from the sync test environment*,
          the answer is UNKNOWN, not SYNC.  This is the dangerous direction.
          AsyncTestEnvironment inherits the assert_rows_* helpers, verify_mutation_tokens()
          and the bucket-pool helpers.  All of them do I/O.  iscoroutinefunction answers
          False, confidently and wrongly, and a transformer that trusted it would emit a
          test that calls an async iterator synchronously.  Reporting beats guessing
          precisely here.
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Kind(Enum):
    """What the transformer must do at a call site.

    AWAIT means "this call does not return its result directly, so the call site has to be
    bridged".  What the bridge *is* belongs to the transformer, not here: acouchbase
    prefixes `await `, txcouchbase wraps the call in run_in_reactor_thread().  Keeping the
    resolution api-neutral is what lets one analysis serve both, and it is also the honest
    description, since the two APIs put the asynchrony in different places.
    """
    SYNC = 'sync'            # leave alone
    AWAIT = 'await'          # bridge it: `await ` / run_in_reactor_thread(...)
    ASYNC_ITER = 'aiter'     # the call's result is iterated: `for` becomes `async for`
    UNKNOWN = 'unknown'      # could not resolve; report it, change nothing


@dataclass(frozen=True)
class Resolution:
    kind: Kind
    why: str


SYNC = Resolution(Kind.SYNC, 'not awaitable on the async class')

# ---------------------------------------------------------------------------
# The async class model
# ---------------------------------------------------------------------------
# `cb_env.<attr>` has a known type because TestEnvironment declares it.
ENV_ATTR_TYPES = {
    'cluster': 'AsyncCluster',
    'bucket': 'AsyncBucket',
    # The bucket a views suite creates for itself, and its default collection.  Same classes as
    # `bucket` and `collection`; separate entries because they are separate attributes, and
    # without them `cb_env.view_bucket.view_query(...)` has an untyped receiver, which resolves
    # SYNC by omission rather than being reported.
    'view_bucket': 'AsyncBucket',
    'view_collection': 'AsyncCollection',
    # Same again for the bucket a collection management suite creates for itself.
    'collection_mgmt_bucket': 'AsyncBucket',
    'scope': 'AsyncScope',
    'collection': 'AsyncCollection',
    'default_scope': 'AsyncScope',
    'default_collection': 'AsyncCollection',
    'named_scope': 'AsyncScope',
    'named_collection': 'AsyncCollection',
    'consistency': 'ConsistencyChecker',
    'config': 'CouchbaseConfig',
    'data_provider': 'DataProvider',
    'collection_pool': 'CollectionPool',
}

# Annotations that appear on nested-closure parameters in the suites, mapped to the
# acouchbase class the async variant would see.
PARAM_ANNOTATION_TYPES = {
    'AttemptContext': 'AttemptContext',
    'TestEnvironment': 'AsyncTestEnvironment',
    'SyncTestEnvironment': 'AsyncTestEnvironment',
    # SDK object types.  test_project in collection_t is the case that proves these are
    # not optional: it defines `def cas_matches(cb: Collection, new_cas: int)` and passes
    # it to try_n_times.  Without a binding for `Collection`, `cb.get(key)` stays
    # synchronous, the closure stays `def`, and the generated test retries a coroutine ten
    # times over thirty seconds while the document it is waiting on expires.  The failure
    # surfaces later as DocumentNotFoundException on an unrelated line.
    'Collection': 'AsyncCollection',
    'AsyncCollection': 'AsyncCollection',
    'Scope': 'AsyncScope',
    'AsyncScope': 'AsyncScope',
    'Bucket': 'AsyncBucket',
    'AsyncBucket': 'AsyncBucket',
    'Cluster': 'AsyncCluster',
    'AsyncCluster': 'AsyncCluster',
    # A helper that takes an SDK object has to say so.  kv_range_scan_t's
    # _validate_result(result: ScanResultIterable) is why the annotation is not decoration:
    # without it the transformer has no type for `result`, so `for r in result:` stays
    # synchronous and eighteen call sites go untouched.
    'ScanResultIterable': 'ScanResults',
    'QueryResult': 'QueryResult',
    'SearchResult': 'SearchResult',
    'AnalyticsResult': 'AnalyticsResult',
    'ViewResult': 'ViewResult',
}

# Types whose *object* is iterated directly (`for r in result:`), rather than via a .rows()
# call.  This is the shape the suites should prefer: both APIs' result classes implement
# __iter__ and __aiter__, so the sync source reads naturally and the transform is a bare
# `async ` insertion with no expression rewriting.
#
# CouchbaseMap is the odd entry and it is deliberate.  It implements neither __iter__ nor
# __aiter__ in either tree, and datastructures_t asserts exactly that by iterating it inside
# pytest.raises(TypeError).  Both spellings do raise TypeError, so leaving the entry out
# would still pass; it would just be testing sync iteration of an async-tree object rather
# than the `async for` an acouchbase user would actually write.  So read this set as "a
# `for` over this type has to become `async for`", which is what the transform does with it.
ASYNC_ITERABLE_TYPES = {'ScanResults', 'QueryResult', 'SearchResult', 'AnalyticsResult',
                        'ViewResult', 'CouchbaseList', 'CouchbaseQueue', 'CouchbaseMap'}


def _load_manager_classes() -> dict[str, Any]:
    """The manager wrappers, keyed by the name the transformer rewrites to.

    `qixm = cb_env.get_manager(AsyncTestEnvironmentQueryIndexManager)` binds qixm to the
    pseudo-type `manager:AsyncTestEnvironmentQueryIndexManager`, and method_kind() then
    introspects the real class the same way it does for the SDK.  Before these classes
    existed the same binding produced a blocker naming the missing class; nothing about the
    resolution path changed, only whether the lookup succeeds.
    """
    from tests.environments import managers
    return {n: getattr(managers, n) for n in dir(managers) if n.endswith('Manager')}


# The three modules that define the api environment classes themselves.  Each CLASS_LOADERS
# entry already loads its own under the name that api's transformer emits, so pulling them in
# as helper classes would add TestEnvironment and SyncTestEnvironment to every model under
# names no generated file ever uses.  Excluded by name rather than by omission from a list,
# so that the discovery below stays exhaustive.
CORE_ENV_MODULES = {'tests.environments.test_environment',
                    'tests.environments.async_environment',
                    'tests.environments.twisted_environment'}


def _load_helper_classes() -> dict[str, Any]:
    """The static-method helper classes the suites call by bare name.

    QueryEnvironment, AnalyticsEnvironment, QueryIndexManagementEnvironment and their
    async and twisted counterparts are reached as
    `AsyncQueryEnvironment.setup_query_indexes(env, ...)`: a bare `ast.Name`
    receiver, not an expression with a type.  Outside the model such a call resolves to
    nothing AND goes unreported, because the receiver is neither a tracked local nor a known
    class and so falls through both branches.  It emits unawaited, the feature check inside
    it never runs, and query_t stops skipping on the mock and fails 32 tests against a
    GoCAVES that has no query service: a silent miss that reads like an SDK problem.

    Discovered rather than listed, because a written-out list of the modules in a package
    goes stale the moment one is added and the miss is silent.  Omitting the twisted query
    environment leaves every `QueryEnvironment.setup_query_indexes()` in the txcouchbase
    query suites resolving to nothing; those particular calls are correct unbridged, since
    the Twisted helpers are blocking, so the cost is four bogus lines in the unresolved
    report.  The same omission for querymgmt_t is not survivable: its clear_all_indexes is
    passed *to* try_n_times(), and an unresolved callable there is dispatched into the
    reactor thread, which deadlocks on a blocking function.

    That is the same failure the ENV_MODULE_MAP blocker in transform.py exists to stop, one
    layer down, so it gets the same treatment: ask the package, not a list.
    """
    import importlib
    import pkgutil

    import tests.environments as env_pkg

    out: dict[str, Any] = {}
    for info in pkgutil.iter_modules(env_pkg.__path__):
        mod_name = f'{env_pkg.__name__}.{info.name}'
        if not info.name.endswith('_environment') or mod_name in CORE_ENV_MODULES:
            continue
        try:
            mod = importlib.import_module(mod_name)
        except ImportError:
            continue
        for name in dir(mod):
            obj = getattr(mod, name)
            if isinstance(obj, type) and name.endswith('Environment'):
                out[name] = obj
    return out


def _load_async_classes() -> dict[str, Any]:
    from acouchbase.binary_collection import BinaryCollection
    from acouchbase.bucket import AsyncBucket
    from acouchbase.cluster import AsyncCluster
    from acouchbase.collection import AsyncCollection
    from acouchbase.datastructures import (CouchbaseList,
                                           CouchbaseMap,
                                           CouchbaseQueue,
                                           CouchbaseSet)
    from acouchbase.logic.cluster_impl import AsyncClusterImpl
    from acouchbase.management.analytics import AnalyticsIndexManager
    from acouchbase.management.buckets import BucketManager
    from acouchbase.management.collections import CollectionManager
    from acouchbase.management.eventing import EventingFunctionManager
    from acouchbase.management.queries import QueryIndexManager
    from acouchbase.management.search import SearchIndexManager
    from acouchbase.management.users import UserManager
    from acouchbase.management.views import ViewIndexManager
    from acouchbase.scope import AsyncScope
    from acouchbase.transactions.transactions import AttemptContext, Transactions
    from tests.environments.async_environment import AsyncTestEnvironment
    return {
        'AsyncCluster': AsyncCluster,
        'AsyncClusterImpl': AsyncClusterImpl,
        'AsyncBucket': AsyncBucket,
        'AsyncScope': AsyncScope,
        'AsyncCollection': AsyncCollection,
        # Named BinaryCollection in all three api trees, not AsyncBinaryCollection.  The
        # per-api class name convention the rest of this map follows simply does not hold
        # here; each Resolver is constructed for one api and loads that api's module, so the
        # shared name resolves correctly without disambiguation.
        'BinaryCollection': BinaryCollection,
        # Same shared-name situation as BinaryCollection: acouchbase.datastructures names its
        # classes CouchbaseList/Map/Set/Queue, not AsyncCouchbaseList.  Their methods really
        # are `async def`, so tier 1 answers these correctly once the class is reachable;
        # the lookup is the only thing needed.
        'CouchbaseList': CouchbaseList,
        'CouchbaseMap': CouchbaseMap,
        'CouchbaseSet': CouchbaseSet,
        'CouchbaseQueue': CouchbaseQueue,
        'AsyncTestEnvironment': AsyncTestEnvironment,
        'AttemptContext': AttemptContext,
        'Transactions': Transactions,
        # The SDK management surface.  Every one of these carries the api-neutral name in all
        # three trees, like BinaryCollection above, and each is reachable only through its
        # accessor entry in CALL_RETURN_TYPES.  Registering them is what makes tier 1 work on
        # the managers: their methods really are `async def`, so once the receiver has a type
        # nothing further is needed.
        'AnalyticsIndexManager': AnalyticsIndexManager,
        'BucketManager': BucketManager,
        'CollectionManager': CollectionManager,
        'EventingFunctionManager': EventingFunctionManager,
        'QueryIndexManager': QueryIndexManager,
        'SearchIndexManager': SearchIndexManager,
        'UserManager': UserManager,
        'ViewIndexManager': ViewIndexManager,
    }


# Tier 3.  Keyed by (class name, method name); each entry names why static analysis fails.
OVERRIDES: dict[tuple[str, str], Resolution] = {
    # acouchbase.cluster.QueryResult is one class serving both APIs.  rows() branches on
    # isinstance(self._request, AsyncN1QLRequest) and returns __aiter__() or __iter__();
    # execute() delegates to a request whose execute() returns a Future under async and a
    # list under sync.  Neither is `async def` and neither carries a return annotation, so
    # tiers 1 and 2 both answer no.  This is the streaming surface, and it is the entire
    # reason a hand-maintained table has to exist at all.
    ('QueryResult', 'execute'): Resolution(Kind.AWAIT, 'runtime-dispatching result type'),
    ('QueryResult', 'rows'): Resolution(Kind.ASYNC_ITER, 'runtime-dispatching result type'),
    ('SearchResult', 'rows'): Resolution(Kind.ASYNC_ITER, 'runtime-dispatching result type'),
    ('AnalyticsResult', 'rows'): Resolution(Kind.ASYNC_ITER, 'runtime-dispatching result type'),
    # scan() is a plain method returning ScanResultIterable, which is async under
    # acouchbase.  Same shape as the results above: the annotation names the iterable, not
    # its asyncness.
    ('AsyncCollection', 'scan'): Resolution(Kind.ASYNC_ITER, 'returns an async iterable'),
    # ScanResults is the fourth streaming result type.  RESULT_TYPES below is a catch-all
    # returning SYNC for "any accessor not listed above", so an omission here surfaces as a
    # confident wrong answer rather than as a resolver miss: kv_range_scan_t's generated
    # test_range_scan_cancel iterates an AsyncRangeScanRequest with a plain `for`, and only
    # a real server catches it.  See the assertion under RESULT_TYPES.
    ('ScanResults', 'rows'): Resolution(Kind.ASYNC_ITER, 'runtime-dispatching result type'),
    # The fifth streaming result type, added with the views suites.  Same runtime dispatch as
    # the three above: ViewResult.rows() branches on isinstance(self._request, AsyncViewRequest)
    # and returns __aiter__() or __iter__().  Stated even though the views suites read rows only
    # through their environment, because RESULT_TYPES below answers SYNC for anything not here
    # and a wrong answer is worse than a reported miss.
    ('ViewResult', 'rows'): Resolution(Kind.ASYNC_ITER, 'runtime-dispatching result type'),
}

# Return types for calls whose result is bound to a local and used as a receiver later.
CALL_RETURN_TYPES = {
    ('AsyncCluster', 'query'): 'QueryResult',
    ('AsyncScope', 'query'): 'QueryResult',
    ('AsyncCluster', 'search_query'): 'SearchResult',
    ('AsyncScope', 'search_query'): 'SearchResult',
    ('AsyncCluster', 'analytics_query'): 'AnalyticsResult',
    ('AsyncScope', 'analytics_query'): 'AnalyticsResult',
    ('AsyncBucket', 'view_query'): 'ViewResult',
    ('AsyncCollection', 'scan'): 'ScanResults',
    ('AsyncCluster', 'transactions'): 'Transactions',
    ('AsyncCluster', 'bucket'): 'AsyncBucket',
    ('AsyncBucket', 'default_collection'): 'AsyncCollection',
    # bucket.collection(name) is annotated `-> Collection`, which is an alias for
    # AsyncCollection rather than a name the class model holds, so introspection cannot
    # resolve it and the entry has to be written out.
    ('AsyncBucket', 'collection'): 'AsyncCollection',
    ('AsyncBucket', 'scope'): 'AsyncScope',
    ('AsyncScope', 'collection'): 'AsyncCollection',
    # binary() itself is synchronous; what it returns carries the awaitables.  Without this
    # entry `collection.binary().append(...)` has no receiver type, and an unknown receiver
    # resolves SYNC by omission, so the acouchbase mirror emits the append unawaited while
    # awaiting the `collection.get()` two lines below it.  Same shape as the ScanResults
    # omission recorded under OVERRIDES: not a reported miss, a confident wrong answer.
    ('AsyncCollection', 'binary'): 'BinaryCollection',
    # usermgmt_t's change-password test is the only place a suite reaches an SDK manager
    # directly rather than through a framework wrapper, and it has to: change_password acts on
    # whoever the *connection* is authenticated as, so it is called on a second cluster the
    # test builds for itself.  users() is synchronous in every api and everything awaitable
    # hangs off what it returns, which is the BinaryCollection shape above: without this entry
    # the receiver is untyped, an untyped receiver resolves SYNC by omission, and the generated
    # acouchbase test would call the coroutine and never await it.
    ('AsyncCluster', 'users'): 'UserManager',
    # The rest of the management accessor family, on the same reasoning as users() above and
    # added for the same reason: cluster_t's close suite calls all seven, and every one is a
    # synchronous accessor whose *result* carries the coroutine.  Without an entry the
    # receiver is untyped, an untyped receiver resolves SYNC by omission, and each
    # `pytest.raises(RuntimeError)` would instead see an un-awaited coroutine and no raise.
    #
    # Suites reach these directly only when the manager has to hang off a cluster the test
    # opened itself; every other suite goes through cb_env.get_manager(), which the
    # manager: pseudo-type already handles.
    ('AsyncCluster', 'analytics_indexes'): 'AnalyticsIndexManager',
    ('AsyncCluster', 'buckets'): 'BucketManager',
    ('AsyncCluster', 'eventing_functions'): 'EventingFunctionManager',
    ('AsyncCluster', 'query_indexes'): 'QueryIndexManager',
    ('AsyncCluster', 'search_indexes'): 'SearchIndexManager',
    ('AsyncBucket', 'collections'): 'CollectionManager',
    ('AsyncBucket', 'view_indexes'): 'ViewIndexManager',
    # Same shape as binary() above: the accessor is synchronous in both trees and everything
    # awaitable hangs off what it returns.  Without these four, `cb_list = ...couchbase_list(
    # key)` leaves cb_list untyped, and an untyped receiver resolves SYNC by omission, so
    # every append/prepend/size in datastructures_t would be emitted unawaited.
    ('AsyncCollection', 'couchbase_list'): 'CouchbaseList',
    ('AsyncCollection', 'couchbase_map'): 'CouchbaseMap',
    ('AsyncCollection', 'couchbase_set'): 'CouchbaseSet',
    ('AsyncCollection', 'couchbase_queue'): 'CouchbaseQueue',
}

# Constructor-style calls that name their own type: `Cluster.connect(...)`.  The Name has
# already been rewritten to the acouchbase class by the import substitution, so the sync
# spelling in the source is what has to be matched here.
STATIC_CTOR_TYPES = {
    ('Cluster', 'connect'): 'AsyncCluster',
    ('AsyncCluster', 'connect'): 'AsyncCluster',
}

# Attribute (not call) types.
ATTR_RETURN_TYPES = {
    ('AsyncCluster', 'transactions'): 'Transactions',
    # The one private attribute in this model, and it is here because credentials_t reaches
    # through it twice: `cluster._impl.get_connection_info()` is the only way to see the
    # credentials the core is holding, and the method is marked **INTERNAL** in both impls.
    # Without this entry the base resolves to AsyncCluster, `_impl` answers None, and the call
    # is reported unresolved, which reads INCOMPLETE on a test that is in fact correct: tier 1
    # answers SYNC for get_connection_info once it can see AsyncClusterImpl, because that impl
    # declares it a plain `def` over execute_cluster_request_sync.
    #
    # cluster_t reaches `cluster._impl.connected`, which is an attribute rather than a call and
    # so never reached the resolver at all.  This is the first call on an impl.
    ('AsyncCluster', '_impl'): 'AsyncClusterImpl',
}

# The sync-only test-infrastructure classes.  Reaching one of these from an async suite is
# a blocker, not an await decision, so they are named rather than introspected.
SYNC_ONLY_TYPES = {
    # ConsistencyChecker turned out to need no async variant at all.  It polls the
    # management REST endpoints with `requests` and sleeps with time.sleep; it never
    # touches the SDK, so there is nothing api-specific in it.  It does block the event
    # loop while polling, which is acceptable because nothing else is scheduled on it
    # during suite setup and teardown, the only places it is called.
    'ConsistencyChecker': None,
    'CouchbaseConfig': None,      # pure config reads, genuinely sync
    'DataProvider': None,         # in-memory, genuinely sync
    'CollectionPool': None,       # in-memory, genuinely sync
}

# Pure helper namespaces the suites call as bare names.  None of these touch the SDK or
# the network, so every method on them is synchronous under all three api styles:
#
#   EnvironmentFeatures  version arithmetic over server_version_short / mock_server_type
#   SD                   couchbase.subdocument, builds Spec objects for lookup_in/mutate_in
#   MutationState        assembles tokens already in hand
#   TestEnvironmentManager subclasses are handled separately, via the manager: pseudo-type
#
# Naming them here is what keeps the widened "report an unresolved CapWords receiver" rule
# useful: without it the report carries 33 lines of noise and the one real entry is easy to
# miss.  The rule stays conservative, the exceptions are enumerated and justified.
PURE_HELPER_NAMESPACES = {
    'EnvironmentFeatures',
    'SD',
    'MutationState',
    # A dataclass in couchbase.management.logic, one class for all three apis, with four
    # methods that only read their arguments.  searchmgmt_t calls SearchIndex.from_json()
    # directly, and a bare CapWords receiver with no binding is reported rather than
    # guessed, so without this the four from-json tests read INCOMPLETE under acouchbase
    # and BLOCKED under txcouchbase on the strength of a pure dict-to-dataclass call.
    'SearchIndex',
    # The same situation as SearchIndex one class up: a dataclass in
    # couchbase.management.logic serving all three apis, whose new_settings() reads its
    # keyword arguments and returns an object.  Every function the eventing suites build
    # goes through it, so leaving it out puts fifteen sites in the unresolved report for a
    # call that does not touch the network.
    'EventingFunctionSettings',
    'CollectionStrategy',
    'ConnectionStrategy',
    'ServerFeatures',
    'MockServerType',
}

# Result types: one class serves both APIs, so they are not in the introspected model.
# Everything on them is sync except the entries in OVERRIDES above.
RESULT_TYPES = {'QueryResult', 'SearchResult', 'AnalyticsResult', 'ScanResults',
                'ViewResult'}

# Every streaming result type must say what rows() does.  RESULT_TYPES is consulted *after*
# OVERRIDES and answers SYNC for everything it is asked about, so a type missing from the
# table gets a wrong answer rather than a reported miss.  That is precisely how
# ScanResults.rows() reached a generated file as a synchronous `for` loop.  Fail at import
# instead.
#
# `raise`, not `assert`: `python -O` strips asserts, and a guard whose whole purpose is
# "this must never silently not happen" cannot be one of the things that silently does not
# happen.  bandit flags the assert form (B101) for exactly this reason, so this is the fix
# rather than a suppression.
_missing_rows = {t for t in RESULT_TYPES if (t, 'rows') not in OVERRIDES}
if _missing_rows:
    raise RuntimeError(f'streaming result types with no rows() resolution: {_missing_rows}')

# Tier 0 triage.  Each of these is a method AsyncTestEnvironment inherits unchanged from
# the sync TestEnvironment, reached by at least one generated suite.  The split is by
# *what the body does*, read once, rather than by anything a checker can see.
PURE_ENV_METHODS = {
    'get_manager',        # dict lookup on _manager_registry
    'get_new_doc',        # data_provider.get_new_vehicle(), in memory
    'get_batch_id',       # reads _loaded_docs, in memory
    'get_bucket_name',    # set difference over _bucket_ids, in memory
    'get_bucket_names',   # same
    'reserve_bucket_name',  # get_bucket_name() plus one set add
    # uuid4() plus three assignments and a list comprehension.  It only *names* buckets;
    # create_bucket() is what creates them, and that one is in NEEDS_ASYNC_OVERRIDE.
    'init_bucket_pool',
    'verify_mutation_tokens',       # unpacks a token already in hand
    'verify_mutation_tokens_disabled',
    # The by-type draw helpers.  All three reduce to either _draw_keys (a list comprehension
    # plus random.sample over _loaded_docs) or a data_provider factory, and none touches a
    # collection.  Read together because the distinction that matters is against
    # get_existing_doc, which is in NEEDS_ASYNC_OVERRIDE for one specific reason: it falls
    # through to self.load_data() on an empty cache.  _draw_keys does not.  It raises naming
    # the shortfall, which is the behaviour its docstring argues for, so there is no cold-cache
    # path here to discard a coroutine on.
    'get_existing_doc_by_type',
    'get_multiple_existing_docs_by_type',
    'get_new_doc_by_type',        # _seed_docs, pure data, plus _used_extras.add
    'get_new_counter_doc_keys',   # data_provider.get_counter_doc_key() per key
    # One assignment.  The bucket it records was opened and connected by the collection
    # management manager, which has an api-specific class to do that in; by the time it
    # reaches here there is nothing left to await.  set_view_bucket is the same shape and is
    # absent only because no generated suite calls it: the views environments do.
    'set_collection_mgmt_bucket',
}

NEEDS_ASYNC_OVERRIDE = {
    '_count_rows':
        'iterates result.rows(), which is an async iterator under acouchbase.  Inheriting '
        'it means the loop never yields a row, so every assertion built on the count is '
        'answering a question about an empty iteration.',
    'assert_rows_exact':
        'delegates to _count_rows, which is a coroutine here.  Inheriting the sync version '
        'compares an unawaited coroutine object against an int.',
    'assert_rows_at_least':
        'delegates to _count_rows, which is a coroutine here.  Inheriting the sync version '
        'compares an unawaited coroutine object against an int.',
    'drain_rows':
        'delegates to _count_rows, which is a coroutine here.  Inheriting the sync version '
        'creates the coroutine and drops it, so the statement is never driven to '
        'completion and the test asserting it does not raise never ran it.',
    'get_existing_doc':
        'pure on a warm cache, but falls through to self.load_data() when _loaded_docs is '
        'empty, and load_data IS overridden as a coroutine.  So the inherited version '
        'discards a coroutine exactly on the cold-cache path, which is the run that '
        'matters and the one least likely to be hit while developing.',
    'drop_bucket':
        'calls bm.drop_bucket() and consistency.wait_until_bucket_dropped(), both I/O.',
    'create_bucket':
        'calls bm.create_bucket() and TestEnvironment.try_n_times(), both I/O.',
    'teardown_bucket_pool':
        'calls try_n_times_till_exception over bm.drop_bucket, I/O.',
}


# ---------------------------------------------------------------------------
# The Twisted class model
# ---------------------------------------------------------------------------
#
# Everything below is the txcouchbase counterpart of the eight tables above.  Two
# structural differences are worth stating before the tables, because they are why this is
# a second model rather than a rename of the first:
#
#   1. Tier 1 finds nothing.  Not one txcouchbase method is `async def`; they are plain
#      `def`s returning a Deferred.  Measured across the whole surface the suites reach:
#      TxCollection 16 of 18 public methods carry `-> Deferred[...]`, TxCluster 10 of 16,
#      TxScope 4 of 7, TxBucket 4 of 9, TxBucketManager 7 of 7.  So tier 2 does all the
#      work here, and it does it cleanly, because the annotation is real rather than a
#      `# type:` comment.
#
#   2. The bridging happens in a different place.  Under acouchbase `cluster.query()`
#      returns a QueryResult immediately and the *rows* are async.  Under txcouchbase
#      `cluster.query()` returns `Deferred[QueryResult]` and the rows are an ordinary
#      iterator: QueryResult.rows() branches on `isinstance(self._request,
#      AsyncN1QLRequest)`, and a TxN1QLRequest is not one, so it returns __iter__().
#      Nothing about the async transform can be relabelled into this one.

TX_ENV_ATTR_TYPES = {
    'cluster': 'TxCluster',
    'bucket': 'TxBucket',
    'view_bucket': 'TxBucket',
    'view_collection': 'TxCollection',
    'collection_mgmt_bucket': 'TxBucket',
    'scope': 'TxScope',
    'collection': 'TxCollection',
    'default_scope': 'TxScope',
    'default_collection': 'TxCollection',
    'named_scope': 'TxScope',
    'named_collection': 'TxCollection',
    'consistency': 'ConsistencyChecker',
    'config': 'CouchbaseConfig',
    'data_provider': 'DataProvider',
    'collection_pool': 'CollectionPool',
}

TX_PARAM_ANNOTATION_TYPES = {
    'TestEnvironment': 'TwistedTestEnvironment',
    'SyncTestEnvironment': 'TwistedTestEnvironment',
    'Collection': 'TxCollection',
    'Scope': 'TxScope',
    'Bucket': 'TxBucket',
    'Cluster': 'TxCluster',
    'QueryResult': 'QueryResult',
    'SearchResult': 'SearchResult',
    'AnalyticsResult': 'AnalyticsResult',
    'ViewResult': 'ViewResult',
}

# Empty, and deliberately so.  There is no async iteration anywhere under txcouchbase: the
# Deferred is consumed by run_in_reactor_thread() before the result object is ever touched,
# so by the time a `for` runs it is looking at a fully materialised iterator.  This is the
# one place where txcouchbase is simpler than acouchbase rather than merely different.
TX_ITERABLE_TYPES: set[str] = set()

TX_OVERRIDES: dict[tuple[str, str], Resolution] = {
    # Stated rather than defaulted.  RESULT_TYPES answers SYNC for anything it is asked
    # about, so leaving these out would give the right answer for the wrong reason, and the
    # assertion below could not tell the difference.  That is exactly how ScanResults.rows()
    # reached a generated file broken on the acouchbase side.
    ('QueryResult', 'rows'): Resolution(Kind.SYNC, 'TxN1QLRequest is not an AsyncN1QLRequest, '
                                                   'so rows() returns __iter__()'),
    ('SearchResult', 'rows'): Resolution(Kind.SYNC, 'same runtime dispatch, sync branch'),
    ('AnalyticsResult', 'rows'): Resolution(Kind.SYNC, 'same runtime dispatch, sync branch'),
    ('ViewResult', 'rows'): Resolution(Kind.SYNC, 'same runtime dispatch, sync branch'),
    # ScanResults has no txcouchbase form at all: TxCollection has no scan() and there is no
    # txcouchbase.kv_range_scan module.  Reaching this is a suite-targeting mistake, so say
    # so rather than answering SYNC.
    ('ScanResults', 'rows'): Resolution(Kind.UNKNOWN, 'txcouchbase has no KV range scan'),
    # The shared QueryResult.execute() delegates to the request, and txcouchbase's
    # N1QLRequest has execute_query() and no execute().  A generated call would raise
    # AttributeError at runtime.  This is the fact that keeps query_t and querymgmt_t off
    # the txcouchbase target list; the resolver states it so that a suite added to that list
    # by mistake is reported rather than silently emitted.
    ('QueryResult', 'execute'): Resolution(Kind.UNKNOWN,
                                           'txcouchbase N1QLRequest has execute_query(), not '
                                           'execute(); iterate rows() instead'),
}

TX_CALL_RETURN_TYPES = {
    ('TxCluster', 'query'): 'QueryResult',
    ('TxScope', 'query'): 'QueryResult',
    ('TxCluster', 'search_query'): 'SearchResult',
    ('TxScope', 'search_query'): 'SearchResult',
    ('TxCluster', 'analytics_query'): 'AnalyticsResult',
    ('TxScope', 'analytics_query'): 'AnalyticsResult',
    ('TxBucket', 'view_query'): 'ViewResult',
    ('TxCluster', 'bucket'): 'TxBucket',
    ('TxBucket', 'default_collection'): 'TxCollection',
    ('TxBucket', 'collection'): 'TxCollection',
    ('TxBucket', 'scope'): 'TxScope',
    ('TxScope', 'collection'): 'TxCollection',
    # See the acouchbase map: binary() is sync, its result is not.
    ('TxCollection', 'binary'): 'BinaryCollection',
    # The management accessor family, mirroring the acouchbase entries.  Here tier 2 does the
    # work rather than tier 1: no txcouchbase manager method is `async def`, every one is a
    # plain `def` returning `Deferred[...]`, so the receiver type is again all that is missing.
    #
    # `eventing_functions` is absent and not an oversight: TxCluster has no such accessor and
    # there is no txcouchbase/management/eventing.py, which is why eventingmgmt_t targets
    # acouchbase alone.  A miss here is reported, which is the outcome wanted for a suite
    # aimed at the wrong api.
    ('TxCluster', 'analytics_indexes'): 'AnalyticsIndexManager',
    ('TxCluster', 'buckets'): 'BucketManager',
    ('TxCluster', 'query_indexes'): 'QueryIndexManager',
    ('TxCluster', 'search_indexes'): 'SearchIndexManager',
    ('TxCluster', 'users'): 'UserManager',
    ('TxBucket', 'collections'): 'CollectionManager',
    ('TxBucket', 'view_indexes'): 'ViewIndexManager',
}

# Empty on purpose.  txcouchbase.cluster.Cluster has no connect() classmethod, so the sync
# suites' `Cluster.connect(...)` has no counterpart to name here.  An entry would be a
# guess; with none, the site is reported as an unresolved CapWords receiver, which is the
# correct outcome for a suite that should not have been targeted at txcouchbase.
TX_STATIC_CTOR_TYPES: dict[tuple[str, str], str] = {}

TX_ATTR_RETURN_TYPES: dict[tuple[str, str], str] = {}

# Tier 0, Twisted.  The split lands differently from acouchbase, and in the safe direction:
# the whole TwistedTestEnvironment is blocking, so a method inherited from the sync
# TestEnvironment is usually inherited *correctly*.  Each addition below is that claim made
# once, in writing, rather than left to a default.
TX_PURE_ENV_METHODS = PURE_ENV_METHODS | {
    # These read result.rows(), which under txcouchbase is a plain iterator (see
    # TX_OVERRIDES).  The acouchbase side has to override all four; here the inherited
    # bodies are already right.
    '_count_rows',
    'assert_rows_exact',
    'assert_rows_at_least',
    'drain_rows',
    # Falls through to self.load_data() on a cold cache, and TwistedTestEnvironment
    # overrides load_data() *synchronously*.  So the inherited fallthrough calls the right
    # thing.  Contrast AsyncTestEnvironment, where load_data is a coroutine and the
    # inherited version silently drops it.
    'get_existing_doc',
    # RetryHelper.sleep -> time.sleep, on the pytest thread.  The reactor runs in its own
    # thread and is unaffected, so unlike acouchbase there is nothing to yield to.
    'sleep',
}
TX_NEEDS_OVERRIDE: dict[str, str] = {
    # create_bucket, drop_bucket and teardown_bucket_pool are all overridden on
    # TwistedTestEnvironment, so they never reach the inherited branch.  This table is
    # empty rather than deleted: anything that appears here later gets "not yet triaged",
    # which is the reported outcome we want for a method nobody has read.
}


@dataclass(frozen=True)
class Model:
    """One api's answer to every question the transformer asks the resolver."""
    api: str
    env_class: str
    env_attr_types: dict[str, str]
    param_annotation_types: dict[str, str]
    iterable_types: set[str]
    overrides: dict[tuple[str, str], Resolution]
    call_return_types: dict[tuple[str, str], str]
    static_ctor_types: dict[tuple[str, str], str]
    attr_return_types: dict[tuple[str, str], str]
    pure_env_methods: set[str]
    needs_override: dict[str, str]
    # Return-annotation tokens that mean "this call has to be bridged".  Tier 2.
    bridge_tokens: tuple[str, ...]
    function_modules: tuple[str, ...]


MODELS = {
    'acouchbase': Model(
        api='acouchbase',
        env_class='AsyncTestEnvironment',
        env_attr_types=ENV_ATTR_TYPES,
        param_annotation_types=PARAM_ANNOTATION_TYPES,
        iterable_types=ASYNC_ITERABLE_TYPES,
        overrides=OVERRIDES,
        call_return_types=CALL_RETURN_TYPES,
        static_ctor_types=STATIC_CTOR_TYPES,
        attr_return_types=ATTR_RETURN_TYPES,
        pure_env_methods=PURE_ENV_METHODS,
        needs_override=NEEDS_ASYNC_OVERRIDE,
        bridge_tokens=('Awaitable', 'Coroutine', 'Future'),
        # Module-level helpers the suites call as bare names.  Tier 1 applies to these too,
        # it just needs somewhere to look the name up; a bare `ast.Name` call carries no
        # receiver to resolve from.  Without it the generated collection_t fails at its
        # first mock run: the concrete class's cb_env fixture calls
        # build_async_environment(...) and teardown_async_environment(...), both
        # `async def`, and an unawaited build hands every test a coroutine object in place
        # of cb_env.
        function_modules=('tests.environments.async_environment',
                          'tests.environments.test_environment',
                          'tests.assert_helpers',
                          'tests.manifest'),
    ),
    'txcouchbase': Model(
        api='txcouchbase',
        env_class='TwistedTestEnvironment',
        env_attr_types=TX_ENV_ATTR_TYPES,
        param_annotation_types=TX_PARAM_ANNOTATION_TYPES,
        iterable_types=TX_ITERABLE_TYPES,
        overrides=TX_OVERRIDES,
        call_return_types=TX_CALL_RETURN_TYPES,
        static_ctor_types=TX_STATIC_CTOR_TYPES,
        attr_return_types=TX_ATTR_RETURN_TYPES,
        pure_env_methods=TX_PURE_ENV_METHODS,
        needs_override=TX_NEEDS_OVERRIDE,
        bridge_tokens=('Deferred',),
        function_modules=('tests.environments.twisted_environment',
                          'tests.environments.test_environment',
                          'tests.assert_helpers',
                          'tests.manifest'),
    ),
}

# Same guard as the acouchbase one above, applied per model: every streaming result type
# must have a stated rows() resolution, because RESULT_TYPES is a catch-all that answers
# SYNC and cannot distinguish "sync" from "nobody looked".
# `raise` rather than `assert`, for the reason given at the acouchbase guard above.
for _m in MODELS.values():
    _missing = {t for t in RESULT_TYPES if (t, 'rows') not in _m.overrides}
    if _missing:
        raise RuntimeError(f'{_m.api}: result types with no rows() resolution: {_missing}')


def _load_twisted_classes() -> dict[str, Any]:
    from tests.environments.twisted_environment import TwistedTestEnvironment
    from txcouchbase.binary_collection import BinaryCollection
    from txcouchbase.bucket import TxBucket
    from txcouchbase.cluster import Cluster as TxCluster
    from txcouchbase.collection import TxCollection
    from txcouchbase.management.analytics import AnalyticsIndexManager
    from txcouchbase.management.buckets import BucketManager
    from txcouchbase.management.collections import CollectionManager
    from txcouchbase.management.queries import QueryIndexManager
    from txcouchbase.management.search import SearchIndexManager
    from txcouchbase.management.users import UserManager
    from txcouchbase.management.views import ViewIndexManager
    from txcouchbase.scope import TxScope
    return {
        'TxCluster': TxCluster,
        'TxBucket': TxBucket,
        'TxScope': TxScope,
        'TxCollection': TxCollection,
        # See the acouchbase loader: the class is BinaryCollection in every tree.
        'BinaryCollection': BinaryCollection,
        # And the same for the management surface, minus eventing, which txcouchbase does not
        # have.  See TX_CALL_RETURN_TYPES.
        'AnalyticsIndexManager': AnalyticsIndexManager,
        'BucketManager': BucketManager,
        'CollectionManager': CollectionManager,
        'QueryIndexManager': QueryIndexManager,
        'SearchIndexManager': SearchIndexManager,
        'UserManager': UserManager,
        'ViewIndexManager': ViewIndexManager,
        'TwistedTestEnvironment': TwistedTestEnvironment,
    }


CLASS_LOADERS = {
    'acouchbase': _load_async_classes,
    'txcouchbase': _load_twisted_classes,
}


class Resolver:
    def __init__(self, api: str = 'acouchbase') -> None:
        self.api = api
        self.model = MODELS[api]
        self.classes = CLASS_LOADERS[api]()
        self.classes.update(_load_helper_classes())
        self.managers = _load_manager_classes()
        self.misses: list[str] = []
        self.inherited_sync: set[str] = set()
        self.missing_managers: dict[str, set[str]] = {}

    @property
    def FUNCTION_MODULES(self) -> tuple[str, ...]:
        return self.model.function_modules

    def function_kind(self, name: str) -> Resolution:
        """Decide what happens at a bare `name(...)` call."""
        import importlib
        for mod_name in self.FUNCTION_MODULES:
            try:
                mod = importlib.import_module(mod_name)
            except ImportError:
                continue
            fn = getattr(mod, name, None)
            if fn is None or not callable(fn):
                continue
            if inspect.iscoroutinefunction(fn):
                return Resolution(Kind.AWAIT, f'{mod_name}.{name} is a coroutine function')
            if inspect.isasyncgenfunction(fn):
                return Resolution(Kind.ASYNC_ITER, f'{mod_name}.{name} is an async generator')
            # Tier 2 for a bare-name call.  Nothing in the twisted environment module
            # returns a Deferred today (build_twisted_environment and its teardown do their
            # reactor dispatching internally and return plain values), but a module-level
            # helper that did would otherwise resolve SYNC by omission.
            ret = getattr(fn, '__annotations__', {}).get('return')
            ret_txt = ret if isinstance(ret, str) else getattr(ret, '__name__', str(ret))
            if any(tok in str(ret_txt) for tok in self.model.bridge_tokens):
                return Resolution(Kind.AWAIT, f'{mod_name}.{name} returns {ret_txt}')
            return SYNC
        return SYNC

    def knows_function(self, name: str) -> bool:
        """True if `name` resolves to a callable in one of the FUNCTION_MODULES.

        Asked separately from function_kind(), which answers SYNC for a name it never
        found.  A caller that needs to distinguish "synchronous" from "not known" has to
        ask both, and the two txcouchbase callers of this do.
        """
        import importlib
        for mod_name in self.FUNCTION_MODULES:
            try:
                mod = importlib.import_module(mod_name)
            except ImportError:
                continue
            if callable(getattr(mod, name, None)):
                return True
        return False

    def function_return_type(self, name: str) -> str | None:
        """Return type of a module-level helper, read from its annotation.

        `env = await build_async_environment(...)` binds env, and without this the
        environment is untyped for the rest of the fixture, so `env.teardown_bucket_pool()`
        two lines later emits unawaited.  A bare Name call carries no receiver to resolve
        from, so the only handle on its type is the annotation, and these helpers all have
        one.
        """
        import importlib
        for mod_name in self.FUNCTION_MODULES:
            try:
                mod = importlib.import_module(mod_name)
            except ImportError:
                continue
            fn = getattr(mod, name, None)
            if fn is None or not callable(fn):
                continue
            ret = getattr(fn, '__annotations__', {}).get('return')
            ret_txt = ret if isinstance(ret, str) else getattr(ret, '__name__', None)
            if ret_txt in self.classes:
                return ret_txt
            return None
        return None

    def method_kind(self, cls_name: str, attr: str) -> Resolution:
        """Decide what happens at `<receiver of type cls_name>.<attr>(...)`."""
        # Managers are a pseudo-type: `qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)`.
        if cls_name.startswith('manager:'):
            return self.manager_kind(cls_name.split(':', 1)[1], attr)

        tabled = self.tabled_kind(cls_name, attr)
        if tabled is not None:
            return tabled

        cls = self.classes.get(cls_name)
        if cls is None:
            self.misses.append(f'{cls_name}.{attr}: class not in the {self.api} model')
            return Resolution(Kind.UNKNOWN, f'{cls_name} is not in the {self.api} class model')

        fn: Any = getattr(cls, attr, None)
        if fn is None:
            self.misses.append(f'{cls_name}.{attr}: attribute missing on the {self.api} class')
            return Resolution(Kind.UNKNOWN, f'{attr} does not exist on {cls_name}')
        if isinstance(fn, property):
            return SYNC

        inherited = self.inherited_env_kind(cls_name, attr, fn)
        if inherited is not None:
            return inherited
        return self.introspected_kind(cls_name, attr, fn)

    def manager_kind(self, mgr: str, attr: str) -> Resolution:
        """A manager resolves against the sync class the test named, not against a type."""
        cls = self.managers.get(mgr)
        if cls is None:
            self.missing_managers.setdefault(mgr, set()).add(attr)
            return Resolution(Kind.UNKNOWN, f'no counterpart for {mgr}')

        fn = getattr(cls, attr, None)
        if fn is None:
            return Resolution(Kind.UNKNOWN, f'{attr} does not exist on {mgr}')
        if inspect.iscoroutinefunction(fn):
            return Resolution(Kind.AWAIT, f'{mgr}.{attr} is a coroutine function')

        # The Twisted manager wrappers are deliberately blocking: each one calls self._rit()
        # internally and returns a plain value, which is what lets a synchronous test body
        # drive them and is why they keep the sync signatures.  So SYNC here is the right
        # answer and the call site needs no wrapping.  The token check still runs, for a
        # wrapper that ever hands its Deferred back.
        ret_txt = self.bridged_return(fn)
        if ret_txt is not None:
            return Resolution(Kind.AWAIT, f'{mgr}.{attr} returns {ret_txt}')
        return SYNC

    def tabled_kind(self, cls_name: str, attr: str) -> Resolution | None:
        """The answers that come from a table rather than from introspection."""
        if cls_name in PURE_HELPER_NAMESPACES:
            return SYNC

        if cls_name in SYNC_ONLY_TYPES:
            reason = SYNC_ONLY_TYPES[cls_name]
            if reason is None:
                return SYNC
            return Resolution(Kind.UNKNOWN, f'{cls_name}.{attr}: {reason}')

        override = self.model.overrides.get((cls_name, attr))
        if override is not None:
            return override

        # Result types are one class per API, so introspection has nothing to say about
        # them.  Anything not caught by OVERRIDES above is a plain accessor: metadata(),
        # cancel_scan(), profile().
        if cls_name in RESULT_TYPES:
            return SYNC
        return None

    def inherited_env_kind(self, cls_name: str, attr: str, fn: Any) -> Resolution | None:
        """Tier 0.  A method the api-specific environment merely inherits from the sync one
        is not automatically a SYNC answer.  Triaged once, by reading the body: see
        PURE_ENV_METHODS / NEEDS_ASYNC_OVERRIDE and their TX_ counterparts above.
        """
        env_class = self.model.env_class
        if cls_name != env_class:
            return None
        owner = getattr(fn, '__qualname__', '').split('.')[0]
        if owner == env_class or inspect.iscoroutinefunction(fn):
            return None
        if attr in self.model.pure_env_methods:
            return None

        self.inherited_sync.add(f'{owner}.{attr}')
        why = self.model.needs_override.get(attr, 'not yet triaged')
        return Resolution(Kind.UNKNOWN, f'{env_class} inherits {attr}() from {owner}: {why}')

    def introspected_kind(self, cls_name: str, attr: str, fn: Any) -> Resolution:
        """Tiers 1 and 2, asked of the object itself.

        Tier 1 finds nothing at all under txcouchbase, by construction: the whole surface
        is plain `def` returning a Deferred.  It stays in the shared path anyway, since a
        coroutine appearing there later should be bridged, not silently called.
        """
        if inspect.iscoroutinefunction(fn):
            return Resolution(Kind.AWAIT, f'{cls_name}.{attr} is a coroutine function')
        if inspect.isasyncgenfunction(fn):
            return Resolution(Kind.ASYNC_ITER, f'{cls_name}.{attr} is an async generator')

        ret_txt = self.bridged_return(fn)
        if ret_txt is not None:
            return Resolution(Kind.AWAIT, f'{cls_name}.{attr} returns {ret_txt}')
        return SYNC

    def bridged_return(self, fn: Any) -> str | None:
        """Tier 2.  The rendered return annotation, when it names something to bridge."""
        ret = getattr(fn, '__annotations__', {}).get('return')
        ret_txt = ret if isinstance(ret, str) else getattr(ret, '__name__', str(ret))
        if any(tok in str(ret_txt) for tok in self.model.bridge_tokens):
            return str(ret_txt)
        return None

    def return_type(self, cls_name: str, attr: str) -> str | None:
        return (self.model.call_return_types.get((cls_name, attr))
                or self.model.attr_return_types.get((cls_name, attr)))
