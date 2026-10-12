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

# Rate and quota limits exist only between 7.1 and 7.2, which is why this suite carries a
# maximum server version as well as a minimum.  Budgets are spent in a bounded number of
# operations rather than for a fixed number of seconds, so reaching a deadline can never
# read as success, and every cluster opened here is closed: one of the limits under test is
# the connection count, so a leak is charged to the test that asserts on it.  Limits are
# passed in the server's own vocabulary, `{'kv': {...}}` rather than `{'kv_limits': {...}}`.

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (CouchbaseException,
                                  DocumentNotFoundException,
                                  InternalServerFailureException,
                                  QuotaLimitedException,
                                  RateLimitedException,
                                  ScopeNotFoundException,
                                  UnAmbiguousTimeoutException,
                                  UserNotFoundException)
from couchbase.management.search import SearchIndex
from couchbase.options import (ClusterOptions,
                               GetOptions,
                               SearchOptions)
from couchbase.search import TermQuery
from tests.assert_helpers import cb_assert_eq
from tests.environments.managers import (TestEnvironmentCollectionMgmtManager,
                                         TestEnvironmentQueryIndexManager,
                                         TestEnvironmentSearchIndexManager,
                                         TestEnvironmentUserManager)
from tests.environments.rate_limit_environment import RateLimitEnvironment
from tests.environments.test_environment import (CollectionStrategy,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.search_index_defs import collection_index_params, default_index_params
from tests.skips import CouchbaseTestEnvironmentException
from tests.test_features import EnvironmentFeatures, ServerFeatures


class RateLimitTestSuite(SuiteRequirements):
    MANAGERS = [TestEnvironmentUserManager,
                TestEnvironmentCollectionMgmtManager,
                TestEnvironmentSearchIndexManager,
                TestEnvironmentQueryIndexManager]
    COLLECTION_STRATEGY = CollectionStrategy.DEFAULT
    NUM_DOCS = 0

    # RateLimiting alone takes the class outside the 7.1 to 7.2 window and on GoCAVES.  The
    # per-service features are gated per test, because deployment is what they answer.
    REQUIRED_FEATURES = [ServerFeatures.RateLimiting]

    # Fixed names: enforcement is a cluster-global switch, so this suite cannot share a
    # cluster whatever it names things, and a fixed name is what the cleanup fixture drops.
    KEY = 'ratelimit-key'
    INGRESS_KEY = 'ratelimit-ingress'
    EGRESS_KEY = 'ratelimit-egress'
    DATA_SIZE_KEY = 'ratelimit-datasize'
    FTS_INDEX_NAME = 'rate-limit-idx'
    SCOPED_FTS_INDEX_NAMES = ['rate-limit-scoped-idx-1', 'rate-limit-scoped-idx-2']
    SCOPED_QUERY_INDEX_NAME = 'rate-limit-query-idx'

    # Half a megabyte, so two of them cross a one-megabyte-per-minute limit and three cross
    # it without depending on how the server rounds.
    DOC_SIZE_BYTES = 1024 * 512

    # Operations to attempt against a per-minute budget of ten.  Generous rather than
    # minimal: the budget is a rate, so a few of these are refunded while the loop runs.
    HAMMER_OPS = 100

    # Connections to open against a budget of one.  The SDK opens more than one socket per
    # cluster, so in practice the limit arrives on the first or second.
    MAX_CONN_ATTEMPTS = 5

    TEST_MANIFEST = [
        'test_rate_limits',
        'test_rate_limits_collections_scopes_limits',
        'test_rate_limits_egress',
        'test_rate_limits_fts',
        'test_rate_limits_fts_scopes',
        'test_rate_limits_index_scopes',
        'test_rate_limits_ingress',
        'test_rate_limits_kv_scopes_data_size',
        'test_rate_limits_max_conns',
        'test_rate_limits_query',
    ]

    # --- Per-service gates ---

    @pytest.fixture(scope='class')
    def check_search_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(
            [ServerFeatures.Search, ServerFeatures.SearchIndexManagement], cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_query_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(
            [ServerFeatures.Query, ServerFeatures.QueryIndexManagement], cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_collections_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.Collections, cb_env.server_context)

    # --- Cleanup ---

    @pytest.fixture(autouse=True)
    def cleanup_rate_limit_state(self, cb_env: SyncTestEnvironment) -> Generator[None, None, None]:
        # Autouse and unconditional: the state worth cleaning up is what a failed test left
        # behind.  Dropping the scope takes its collections, documents and indexes with it;
        # the two search indexes are cluster-level and have to be named.
        yield
        umgr = cb_env.get_manager(TestEnvironmentUserManager)
        cmgr = cb_env.get_manager(TestEnvironmentCollectionMgmtManager)
        sixm = cb_env.get_manager(TestEnvironmentSearchIndexManager)

        for index_name in [self.FTS_INDEX_NAME] + self.SCOPED_FTS_INDEX_NAMES:
            TestEnvironment.try_n_times_till_exception(
                3, 1, sixm.drop_index, index_name,
                expected_exceptions=(CouchbaseException,))

        TestEnvironment.try_n_times_till_exception(
            5, 1, cmgr.drop_scope, RateLimitEnvironment.SCOPE_NAME,
            expected_exceptions=(ScopeNotFoundException,))
        cb_env.consistency.wait_until_scope_dropped(cb_env.bucket.name, RateLimitEnvironment.SCOPE_NAME)

        for key in (self.KEY, self.INGRESS_KEY, self.EGRESS_KEY, self.DATA_SIZE_KEY):
            TestEnvironment.try_n_times_till_exception(
                3, 1, cb_env.collection.remove, key,
                expected_exceptions=(DocumentNotFoundException,))

        for username in RateLimitEnvironment.minted_usernames():
            TestEnvironment.try_n_times_till_exception(
                5, 1, umgr.drop_user, username,
                expected_exceptions=(UserNotFoundException, CouchbaseException))
            cb_env.consistency.wait_until_user_dropped(username)
        RateLimitEnvironment.forget_usernames()

    # --- Helpers ---

    def _limited_options(self, username: str) -> ClusterOptions:
        return ClusterOptions(PasswordAuthenticator(username, RateLimitEnvironment.PASSWORD))

    def _search_once(self, cb_env: SyncTestEnvironment, cluster: Cluster) -> None:
        cb_env.drain_rows(cluster.search_query(self.FTS_INDEX_NAME,
                                               TermQuery('auto'),
                                               SearchOptions(limit=1)))

    # --- Rate limits: a user's budget, which refills ---

    def test_rate_limits(self, cb_env: SyncTestEnvironment) -> None:
        user = RateLimitEnvironment.create_rate_limit_user(cb_env, {'kv': {'num_connections': 10,
                                                                           'num_ops_per_min': 10,
                                                                           'ingress_mib_per_min': 1,
                                                                           'egress_mib_per_min': 10}})
        conn_string = cb_env.config.get_connection_string()
        # Annotated because try_n_times returns Any, and the generated asyncio form needs the
        # type to know that the calls below it are coroutines.
        cluster: Cluster = TestEnvironment.try_n_times(10, 1, Cluster.connect,
                                                       conn_string, self._limited_options(user))
        try:
            collection = cluster.bucket(cb_env.bucket.name).default_collection()
            with pytest.raises(RateLimitedException):
                for _ in range(self.HAMMER_OPS):
                    collection.upsert(self.KEY, 'test')
        finally:
            cluster.close()

    def test_rate_limits_ingress(self, cb_env: SyncTestEnvironment) -> None:
        user = RateLimitEnvironment.create_rate_limit_user(cb_env, {'kv': {'num_connections': 10,
                                                                           'num_ops_per_min': 100,
                                                                           'ingress_mib_per_min': 1,
                                                                           'egress_mib_per_min': 10}})
        conn_string = cb_env.config.get_connection_string()
        doc = RateLimitEnvironment.random_doc_by_size(self.DOC_SIZE_BYTES)
        cluster: Cluster = TestEnvironment.try_n_times(10, 1, Cluster.connect,
                                                       conn_string, self._limited_options(user))
        try:
            collection = cluster.bucket(cb_env.bucket.name).default_collection()
            with pytest.raises(RateLimitedException):
                for _ in range(5):
                    collection.upsert(self.INGRESS_KEY, doc)
        finally:
            cluster.close()

    def test_rate_limits_egress(self, cb_env: SyncTestEnvironment) -> None:
        # The write is under this user's ingress limit, so what trips is the reads.  The
        # explicit timeout keeps that honest: a read held back has to fail rather than sit
        # out the default budget.
        user = RateLimitEnvironment.create_rate_limit_user(cb_env, {'kv': {'num_connections': 10,
                                                                           'num_ops_per_min': 100,
                                                                           'ingress_mib_per_min': 10,
                                                                           'egress_mib_per_min': 2}})
        conn_string = cb_env.config.get_connection_string()
        doc = RateLimitEnvironment.random_doc_by_size(self.DOC_SIZE_BYTES)
        cluster: Cluster = TestEnvironment.try_n_times(10, 1, Cluster.connect,
                                                       conn_string, self._limited_options(user))
        try:
            collection = cluster.bucket(cb_env.bucket.name).default_collection()
            collection.upsert(self.EGRESS_KEY, doc)
            with pytest.raises(RateLimitedException):
                for _ in range(10):
                    collection.get(self.EGRESS_KEY, GetOptions(timeout=timedelta(seconds=10)))
        finally:
            cluster.close()

    def test_rate_limits_max_conns(self, cb_env: SyncTestEnvironment) -> None:
        # Two phases, and the split is what makes the assertion mean anything.  A user is not
        # usable the instant it is present: memcached's view of the credential arrives behind
        # `wait_until_user_present`, and every other test here absorbs that in the retry
        # around its connect.  This one cannot, because a refused connection is what it
        # asserts.  So the user is created with a workable budget and connected once, and only
        # then is the budget tightened; both phases share one minted username, which is why
        # the name is passed back in.  A budget of one is exceeded by the first further
        # cluster rather than the second, since a Cluster opens more than one socket.  The
        # close loops reuse the `cluster` and `first` names because those annotations are what
        # tell the generator `close()` is a coroutine; a fresh loop variable is untyped, and
        # an untyped receiver resolves synchronous, so the asyncio form would leak every
        # connection opened here.
        permissive = {'num_connections': 10, 'num_ops_per_min': 100,
                      'ingress_mib_per_min': 10, 'egress_mib_per_min': 10}
        user = RateLimitEnvironment.create_rate_limit_user(cb_env, {'kv': permissive})
        conn_string = cb_env.config.get_connection_string()
        first: Cluster = TestEnvironment.try_n_times(10, 1, Cluster.connect,
                                                     conn_string, self._limited_options(user))
        try:
            first.bucket(cb_env.bucket.name).default_collection().exists(self.KEY)

            RateLimitEnvironment.create_rate_limit_user(cb_env, {'kv': dict(permissive,
                                                                            num_connections=1)},
                                                        user)
            clusters: list[Cluster] = []
            try:
                with pytest.raises(RateLimitedException):
                    for _ in range(self.MAX_CONN_ATTEMPTS):
                        cluster: Cluster = Cluster.connect(conn_string, self._limited_options(user))
                        clusters.append(cluster)
                        # The tightened budget reaches each node on its own schedule.  A
                        # cluster that opens its bucket through a node still on the old
                        # budget succeeds, and its operation then waits on a refused session
                        # to the active node until it times out, so that attempt says
                        # nothing about the limit.
                        try:
                            cluster.bucket(cb_env.bucket.name).default_collection().exists(self.KEY)
                        except UnAmbiguousTimeoutException:
                            continue
            finally:
                for cluster in clusters:
                    cluster.close()
        finally:
            first.close()

    @pytest.mark.usefixtures('check_query_supported')
    def test_rate_limits_query(self, cb_env: SyncTestEnvironment) -> None:
        # The statement selects a constant and names no keyspace, so it needs no index.
        user = RateLimitEnvironment.create_rate_limit_user(cb_env, {'query': {'num_queries_per_min': 1,
                                                                              'num_concurrent_requests': 10,
                                                                              'ingress_mib_per_min': 10,
                                                                              'egress_mib_per_min': 10}})
        conn_string = cb_env.config.get_connection_string()
        cluster: Cluster = TestEnvironment.try_n_times(10, 1, Cluster.connect,
                                                       conn_string, self._limited_options(user))
        try:
            with pytest.raises(RateLimitedException):
                for _ in range(self.HAMMER_OPS):
                    cb_env.drain_rows(cluster.query("SELECT 'Hi there!'"))
        finally:
            cluster.close()

    @pytest.mark.usefixtures('check_search_supported')
    def test_rate_limits_fts(self, cb_env: SyncTestEnvironment) -> None:
        # The index is created on the administrative connection and the queries run on the
        # limited one, so what is counted is the user's queries and not the index build.
        sixm = cb_env.get_manager(TestEnvironmentSearchIndexManager)
        sixm.upsert_index(SearchIndex(name=self.FTS_INDEX_NAME,
                                      source_name=cb_env.bucket.name,
                                      params=default_index_params()))
        # The index exists once the upsert returns, but cannot serve a query until its
        # partitions are built; until then the search service answers `pindex not available`
        # rather than a rate limit.  So one query has to succeed on the administrative
        # connection before the limited user starts counting.
        TestEnvironment.try_n_times(20, 1, self._search_once, cb_env, cb_env.cluster)

        user = RateLimitEnvironment.create_rate_limit_user(cb_env, {'fts': {'num_queries_per_min': 1,
                                                                            'num_concurrent_requests': 10,
                                                                            'ingress_mib_per_min': 10,
                                                                            'egress_mib_per_min': 10}})
        conn_string = cb_env.config.get_connection_string()
        cluster: Cluster = TestEnvironment.try_n_times(10, 1, Cluster.connect,
                                                       conn_string, self._limited_options(user))
        try:
            with pytest.raises(RateLimitedException):
                for _ in range(self.HAMMER_OPS):
                    self._search_once(cb_env, cluster)
        finally:
            cluster.close()

    # --- Quota limits: a scope's ceiling, which does not refill ---

    @pytest.mark.usefixtures('check_collections_supported')
    def test_rate_limits_collections_scopes_limits(self, cb_env: SyncTestEnvironment) -> None:
        RateLimitEnvironment.create_rate_limit_scope(cb_env, {'clusterManager': {'num_collections': 1}})
        cmgr = cb_env.get_manager(TestEnvironmentCollectionMgmtManager)

        cmgr.create_collection(RateLimitEnvironment.SCOPE_NAME, RateLimitEnvironment.COLLECTION_NAME)
        cb_env.consistency.wait_until_collection_present(cb_env.bucket.name,
                                                         RateLimitEnvironment.SCOPE_NAME,
                                                         RateLimitEnvironment.COLLECTION_NAME)

        with pytest.raises(QuotaLimitedException):
            cmgr.create_collection(RateLimitEnvironment.SCOPE_NAME, 'rate-limit-collection-1')

    @pytest.mark.usefixtures('check_collections_supported')
    def test_rate_limits_kv_scopes_data_size(self, cb_env: SyncTestEnvironment) -> None:
        # Each write uses a key of its own.  Five writes under one key is one document
        # overwritten four times, and the scope would never approach the limit it names.
        RateLimitEnvironment.create_rate_limit_scope(cb_env, {'kv': {'data_size': 1024 * 1024}})
        cmgr = cb_env.get_manager(TestEnvironmentCollectionMgmtManager)
        cmgr.create_collection(RateLimitEnvironment.SCOPE_NAME, RateLimitEnvironment.COLLECTION_NAME)
        cb_env.consistency.wait_until_collection_present(cb_env.bucket.name,
                                                         RateLimitEnvironment.SCOPE_NAME,
                                                         RateLimitEnvironment.COLLECTION_NAME)

        scope = cb_env.bucket.scope(RateLimitEnvironment.SCOPE_NAME)
        collection = scope.collection(RateLimitEnvironment.COLLECTION_NAME)
        doc = RateLimitEnvironment.random_doc_by_size(self.DOC_SIZE_BYTES)
        with pytest.raises(QuotaLimitedException):
            for i in range(5):
                collection.upsert(f'{self.DATA_SIZE_KEY}-{i}', doc)

    @pytest.mark.usefixtures('check_collections_supported', 'check_query_supported')
    def test_rate_limits_index_scopes(self, cb_env: SyncTestEnvironment) -> None:
        # The primary index is created through a retry because the query service learns about
        # a new collection a moment after the collection manager reports it present.  It is
        # read back before the second index is attempted, since otherwise a scope holding no
        # index and one holding its single permitted index are indistinguishable from here;
        # outside the raises block, because from inside it a count of zero reports as a
        # missing QuotaLimitedException.
        RateLimitEnvironment.create_rate_limit_scope(cb_env, {'index': {'num_indexes': 1}})
        cmgr = cb_env.get_manager(TestEnvironmentCollectionMgmtManager)
        qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)
        cmgr.create_collection(RateLimitEnvironment.SCOPE_NAME, RateLimitEnvironment.COLLECTION_NAME)
        cb_env.consistency.wait_until_collection_present(cb_env.bucket.name,
                                                         RateLimitEnvironment.SCOPE_NAME,
                                                         RateLimitEnvironment.COLLECTION_NAME)

        TestEnvironment.try_n_times(10, 3, qixm.create_primary_index, cb_env.bucket.name,
                                    scope_name=RateLimitEnvironment.SCOPE_NAME,
                                    collection_name=RateLimitEnvironment.COLLECTION_NAME)

        indexes = qixm.get_all_indexes(cb_env.bucket.name,
                                       scope_name=RateLimitEnvironment.SCOPE_NAME,
                                       collection_name=RateLimitEnvironment.COLLECTION_NAME)
        cb_assert_eq(len(indexes), 1, what='indexes in the limited scope')
        assert indexes[0].is_primary is True
        cb_assert_eq(indexes[0].name, '#primary', what='primary index name')
        cb_assert_eq(indexes[0].collection_name, RateLimitEnvironment.COLLECTION_NAME,
                     what='primary index collection')

        # The one quota here that does not arrive as QuotaLimitedException, and correcting it
        # needs a C++ core change.  `extract_common_query_error_code` maps query error 5000
        # to quota_limited on exactly this message, but it is reached only from the
        # `default:` branch of the switch in `query_index_create.cxx`, which has an explicit
        # `case 5000:` that breaks.  So the error falls through to extract_common_error_code
        # against an HTTP 200 and comes back internal_server_failure.  Asserted as what the
        # SDK raises rather than what it should, so a corrected mapping fails here loudly.
        with pytest.raises(InternalServerFailureException):
            qixm.create_index(cb_env.bucket.name, self.SCOPED_QUERY_INDEX_NAME, ['testField'],
                              scope_name=RateLimitEnvironment.SCOPE_NAME,
                              collection_name=RateLimitEnvironment.COLLECTION_NAME)

    @pytest.mark.usefixtures('check_collections_supported', 'check_search_supported')
    def test_rate_limits_fts_scopes(self, cb_env: SyncTestEnvironment) -> None:
        # Both indexes are collection-aware, which is what attaches them to the scope under
        # test; a bucket-wide index is counted against nothing here.  The first upsert is
        # retried because the search service learns about the collection a moment behind.
        RateLimitEnvironment.create_rate_limit_scope(cb_env, {'fts': {'num_fts_indexes': 1}})
        cmgr = cb_env.get_manager(TestEnvironmentCollectionMgmtManager)
        sixm = cb_env.get_manager(TestEnvironmentSearchIndexManager)
        cmgr.create_collection(RateLimitEnvironment.SCOPE_NAME, RateLimitEnvironment.COLLECTION_NAME)
        cb_env.consistency.wait_until_collection_present(cb_env.bucket.name,
                                                         RateLimitEnvironment.SCOPE_NAME,
                                                         RateLimitEnvironment.COLLECTION_NAME)

        params = collection_index_params(RateLimitEnvironment.SCOPE_NAME,
                                         [RateLimitEnvironment.COLLECTION_NAME])
        first, second = self.SCOPED_FTS_INDEX_NAMES
        TestEnvironment.try_n_times(10, 3, sixm.upsert_index,
                                    SearchIndex(name=first,
                                                source_name=cb_env.bucket.name,
                                                params=params))

        with pytest.raises(QuotaLimitedException):
            sixm.upsert_index(SearchIndex(name=second,
                                          source_name=cb_env.bucket.name,
                                          params=params))


class RateLimitTests(RateLimitTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(RateLimitTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        # The switch is cluster-global and every test here is meaningless without it, so it
        # belongs around the class.  Turning it off again is not tidiness: enforcement left on
        # is state a suite running afterwards would feel.  The feature check is repeated even
        # though REQUIRED_FEATURES declares it, because that is read by a function-scoped
        # fixture pytest resolves after this class-scoped one, so outside the window the POST
        # below would run first and answer `400 Unknown key enforceLimits`, reporting as a
        # broken environment rather than an unsupported feature.
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.RateLimiting,
                                                       cb_base_env.server_context)
        env = build_environment(cb_base_env, RateLimitTestSuite)
        try:
            RateLimitEnvironment.enforce_rate_limits(env, True)
            yield env
        finally:
            # A failure to turn enforcement back off is printed rather than raised, so the
            # exception that reaches the run is the one worth debugging.
            try:
                RateLimitEnvironment.enforce_rate_limits(env, False)
            except CouchbaseTestEnvironmentException as ex:
                print(f'Could not turn rate limit enforcement off: {ex}')
            teardown_environment(env, RateLimitTestSuite)
