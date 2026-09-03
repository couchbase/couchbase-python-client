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

from __future__ import annotations

import threading
from collections.abc import Callable, Generator
from datetime import datetime, timedelta
from typing import Any

import pytest

import couchbase.subdocument as SD
from couchbase.cluster import Cluster
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CouchbaseException,
                                  KeyspaceNotFoundException,
                                  ParsingFailedException,
                                  QueryErrorContext,
                                  ScopeNotFoundException)
from couchbase.mutation_state import MutationState
from couchbase.n1ql import (QueryMetaData,
                            QueryMetrics,
                            QueryProfile,
                            QueryStatus,
                            QueryWarning)
from couchbase.options import (QueryOptions,
                               UnsignedInt64,
                               UpsertOptions)
from couchbase.result import QueryResult
from couchbase.scope import Scope
from tests_v2.environments.managers import TestEnvironmentQueryIndexManager
from tests_v2.environments.query_environment import QueryEnvironment
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SuiteRequirements,
                                                    SyncTestEnvironment,
                                                    TestEnvironment,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.manifest import validate_test_manifest
from tests_v2.skips import (server_label,
                            skip_feature_unsupported,
                            skip_known_issue)
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class QueryCollectionTestSuite(SuiteRequirements):
    MANAGERS = [TestEnvironmentQueryIndexManager]
    COLLECTION_STRATEGY = CollectionStrategy.NAMED

    TEST_MANIFEST = [
        'test_bad_query_context',
        'test_bad_scope_query',
        'test_cluster_query_context',
        'test_query_fully_qualified',
        'test_query_in_thread',
        'test_query_metadata',
        'test_query_metadata_no_metrics',
        'test_query_ryow',
        'test_query_with_metrics',
        'test_scope_query',
        'test_scope_query_with_named_params_in_options',
        'test_scope_query_with_positional_params_in_options',
    ]

    def test_bad_query_context(self, cb_env: SyncTestEnvironment) -> None:
        q_str = f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2"
        # test w/ no context
        with pytest.raises(KeyspaceNotFoundException):
            result = cb_env.cluster.query(q_str)
            cb_env.assert_rows_exact(result, 2)

        # test w/ bad scope
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        with pytest.raises(ScopeNotFoundException):
            result = cb_env.cluster.query(q_str, QueryOptions(query_context=q_context))
            cb_env.assert_rows_exact(result, 2)

    def test_bad_scope_query(self, cb_env: SyncTestEnvironment) -> None:
        q_str = f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2"
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        with pytest.raises(ScopeNotFoundException):
            result = cb_env.scope.query(q_str, QueryOptions(query_context=q_context))
            cb_env.assert_rows_exact(result, 2)

        q_context = f'`fake-bucket`.`{cb_env.scope.name}`'
        with pytest.raises(KeyspaceNotFoundException):
            result = cb_env.scope.query(q_str, query_context=q_context)
            cb_env.assert_rows_exact(result, 2)

    def test_cluster_query_context(self, cb_env: SyncTestEnvironment) -> None:
        q_context = f'{cb_env.bucket.name}.{cb_env.scope.name}'
        # test with QueryOptions
        q_opts = QueryOptions(query_context=q_context, adhoc=True)
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", q_opts)
        cb_env.assert_rows_exact(result, 2)

        # test with kwargs
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", query_context=q_context)
        cb_env.assert_rows_exact(result, 2)

    def test_query_fully_qualified(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")
        cb_env.assert_rows_exact(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    # Driven by a plain threading.Thread, so it has no async analogue: acouchbase and
    # txcouchbase objects are bound to an event loop (a reactor under Twisted), and
    # calling into them from a foreign thread exercises the loop, not the SDK.
    @pytest.mark.testgen_exclude('acouchbase', 'txcouchbase',
                                 reason='drives a threading.Thread; no async analogue')
    def test_query_in_thread(self, cb_env: SyncTestEnvironment) -> None:
        results: list[Any] = [None]

        def run_test(scope: Scope,
                     collection_name: str,
                     assert_fn: Callable[[QueryResult, int], None],
                     results: list[Any]) -> None:
            try:
                result = scope.query(f"SELECT * FROM `{collection_name}` LIMIT 2")
                assert_fn(result, 2)
                assert result.metadata() is not None
            except AssertionError:
                results[0] = False
            except Exception as ex:
                results[0] = ex
            else:
                results[0] = True

        t = threading.Thread(target=run_test,
                             args=(cb_env.scope, cb_env.collection.name,
                                   cb_env.assert_rows_exact, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True

    def test_query_metadata(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        cb_env.assert_rows_exact(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    def test_query_metadata_no_metrics(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                                    QueryOptions(metrics=False))
        cb_env.assert_rows_exact(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg

        metrics = metadata.metrics()
        assert metrics is None

        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    def test_query_ryow(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        result = cb_env.scope.query(f'SELECT * FROM `{cb_env.collection.name}` USE KEYS "{key}"')
        cb_env.assert_rows_exact(result, 0)
        res = cb_env.collection.insert(key, value)
        # add_mutation_token mutates in place and returns None, so it cannot be chained off
        # the constructor: `ms` would be None and the option would fall back to its default,
        # requesting no read-your-own-write consistency at all (PYCBC-1864).
        ms = MutationState()
        ms.add_mutation_token(res.mutation_token())
        result = cb_env.scope.query(f'SELECT * FROM `{cb_env.collection.name}` USE KEYS "{key}"',
                                    QueryOptions(consistent_with=ms))
        cb_env.assert_rows_exact(result, 1)

    def test_query_with_metrics(self, cb_env: SyncTestEnvironment) -> None:
        initial = datetime.now()
        result = cb_env.scope.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 1", QueryOptions(metrics=True))
        cb_env.assert_rows_exact(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        assert isinstance(metadata, QueryMetaData)
        metrics = metadata.metrics()
        assert isinstance(metrics, QueryMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.mutation_count: 0,
                           metrics.result_count: 1,
                           metrics.sort_count: 0,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert UnsignedInt64(expected) == count_result, fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert metrics.error_count() == UnsignedInt64(0)
        assert metadata.profile() is None

    def test_scope_query(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        cb_env.assert_rows_exact(result, 2)
        result = cb_env.scope.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")

    def test_scope_query_with_named_params_in_options(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $batch LIMIT 1",
                                    QueryOptions(named_parameters={'batch': f'{cb_env.get_batch_id()}%'}))
        cb_env.assert_rows_exact(result, 1)

    def test_scope_query_with_positional_params_in_options(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $1 LIMIT 1",
                                    QueryOptions(positional_parameters=[f'{cb_env.get_batch_id()}%']))
        cb_env.assert_rows_exact(result, 1)


class QueryTestSuite(SuiteRequirements):
    MANAGERS = [TestEnvironmentQueryIndexManager]
    # `_default` is shared with every other suite and xdist worker, so a statement that asserts
    # a row count filters on this run's batch.  Unfiltered, a doc another worker deletes between
    # the index scan and the fetch drops out of the result and LIMIT 2 comes back with one row.
    COLLECTION_STRATEGY = CollectionStrategy.DEFAULT

    TEST_MANIFEST = [
        'test_bad_query',
        'test_mixed_named_parameters',
        'test_mixed_positional_parameters',
        'test_preserve_expiry',
        'test_query_error_context',
        'test_query_in_thread',
        'test_query_metadata',
        'test_query_metadata_no_metrics',
        'test_query_raw_options',
        'test_query_ryow',
        'test_query_timeout',
        'test_query_with_metrics',
        'test_query_with_profile',
        'test_simple_query',
        'test_simple_query_explain',
        'test_simple_query_prepared',
        'test_simple_query_with_named_params',
        'test_simple_query_with_named_params_in_options',
        'test_simple_query_with_positional_params',
        'test_simple_query_with_positional_params_in_options',
        'test_simple_query_without_options_with_kwargs_named_params',
        'test_simple_query_without_options_with_kwargs_positional_params',
    ]

    @pytest.fixture(scope='class')
    def check_preserve_expiry_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.PreserveExpiry, cb_env.server_context)

    def test_bad_query(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(ParsingFailedException):
            result = cb_env.cluster.query("I'm not N1QL!")
            cb_env.drain_rows(result)

    def test_mixed_named_parameters(self, cb_env: SyncTestEnvironment) -> None:
        batch_id = cb_env.get_batch_id()
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1',
                                      QueryOptions(named_parameters={'batch': 'xgfflq'}), batch=batch_id)
        cb_env.assert_rows_exact(result, 1)

    def test_mixed_positional_parameters(self, cb_env: SyncTestEnvironment) -> None:
        # we assume that positional overrides one in the Options
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1',
                                      QueryOptions(positional_parameters=['xgfflq']), f'{cb_env.get_batch_id()}')
        cb_env.assert_rows_exact(result, 1)

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_preserve_expiry(self, cb_env: SyncTestEnvironment) -> None:
        key = "imakey"
        content = {"a": "aaa", "b": "bbb"}

        cb_env.collection.upsert(key, content, UpsertOptions(expiry=timedelta(days=1)))

        expiry_path = "$document.exptime"
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        q_str = f"UPDATE `{cb_env.bucket.name}` AS content USE KEYS '{key}' SET content.a = 'aaaa'"
        # No count is asserted, deliberately.  An UPDATE with no RETURNING clause should
        # yield no rows, but this test is PreserveExpiry-gated and so never runs on the mock,
        # so that is unconfirmed here; turning the drain into an exact-zero assertion is a
        # real-server question, not a reading one.
        result = cb_env.cluster.query(q_str, QueryOptions(preserve_expiry=True))
        cb_env.drain_rows(result)

        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2

    def test_query_error_context(self, cb_env: SyncTestEnvironment) -> None:
        try:
            result = cb_env.cluster.query("SELECT * FROM no_such_bucket")
            cb_env.drain_rows(result)
        except CouchbaseException as ex:
            assert isinstance(ex.context, QueryErrorContext)
            assert ex.context.statement is not None
            assert ex.context.first_error_code is not None
            assert ex.context.first_error_message is not None
            assert ex.context.client_context_id is not None
            assert ex.context.response_body is not None

    # Driven by a plain threading.Thread, so it has no async analogue: acouchbase and
    # txcouchbase objects are bound to an event loop (a reactor under Twisted), and
    # calling into them from a foreign thread exercises the loop, not the SDK.
    @pytest.mark.testgen_exclude('acouchbase', 'txcouchbase',
                                 reason='drives a threading.Thread; no async analogue')
    def test_query_in_thread(self, cb_env: SyncTestEnvironment) -> None:
        results: list[Any] = [None]

        def run_test(cluster: Cluster,
                     assert_fn: Callable[[QueryResult, int], None],
                     results: list[Any]) -> None:
            try:
                result = cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` "
                                       f"WHERE batch LIKE '{cb_env.get_batch_id()}%' LIMIT 2")
                assert_fn(result, 2)
                assert result.metadata() is not None
            except AssertionError:
                results[0] = False
            except Exception as ex:
                results[0] = ex
            else:
                results[0] = True

        t = threading.Thread(target=run_test, args=(cb_env.cluster, cb_env.assert_rows_exact, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True

    def test_query_metadata(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` "
                                      f"WHERE batch LIKE '{cb_env.get_batch_id()}%' LIMIT 2")
        cb_env.assert_rows_exact(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    def test_query_metadata_no_metrics(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` "
                                      f"WHERE batch LIKE '{cb_env.get_batch_id()}%' LIMIT 2",
                                      QueryOptions(metrics=False))
        cb_env.assert_rows_exact(result, 2)
        metadata = result.metadata()  # type: QueryMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg

        metrics = metadata.metrics()
        assert metrics is None

        assert metadata.status() == QueryStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), list)

        for warning in metadata.warnings():
            assert isinstance(warning, QueryWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    def test_query_raw_options(self, cb_env: SyncTestEnvironment) -> None:
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        batch_id = cb_env.get_batch_id()
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT $1",
                                      QueryOptions(raw={'$batch': f'{batch_id}%', 'args': [2]}))
        cb_env.assert_rows_exact(result, 2)

        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1",
                                      QueryOptions(raw={'args': [f'{batch_id}%']}))
        cb_env.assert_rows_exact(result, 1)

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    # Excluded from txcouchbase: `txcouchbase.cluster.Cluster` has no `connect` classmethod.
    # It is constructed directly and then bridged through `on_connect()`, so the generated
    # `Cluster.connect(...)` would not resolve.  acouchbase does have it.
    @pytest.mark.testgen_exclude('txcouchbase',
                                 reason='txcouchbase.Cluster has no connect() classmethod')
    def test_query_timeout(self, cb_env: SyncTestEnvironment) -> None:
        assert cb_env.server_version_short is not None
        if cb_env.server_version_short < 7.1:
            skip_feature_unsupported(required=7.1,
                                     observed=cb_env.server_version_short,
                                     server=server_label(cb_env.is_mock_server),
                                     note='the query this test issues needs 7.1')
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import (ClusterOptions,
                                       ClusterTimeoutOptions,
                                       ClusterTracingOptions)
        conn_string = cb_env.config.get_connection_string()
        username, pw = cb_env.config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        # Prior to PYCBC-1521, this test would fail as each request would override the cluster level query_timeout.
        # If a timeout was not provided in the request, the default 75s timeout would be used.
        timeout_opts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=1.5))
        cluster_opts = ClusterOptions(auth,
                                      timeout_options=timeout_opts,
                                      tracing_options=ClusterTracingOptions(enable_tracing=False))
        cluster = Cluster.connect(f'{conn_string}', cluster_opts)
        # don't need to do this except for older server versions
        _ = cluster.bucket(f'{cb_env.bucket.name}')
        slow_query = ' '.join(['SELECT COUNT (1) AS c FROM',
                               'ARRAY_RANGE(0,110) AS d1,'
                               'ARRAY_RANGE(0,110) AS d2,'
                               'ARRAY_RANGE(0,110) AS d3'])
        with pytest.raises(AmbiguousTimeoutException):
            result = cluster.query(slow_query)
            cb_env.drain_rows(result)

        # If we override the timeout w/in the request the query should succeed.
        # NOTE: a timeout of < 10s is most likely acceptable, but for the Jenkins environment we give plenty of room.
        # A loose assertion, deliberately.  The statement is a COUNT aggregate, so it
        # returns exactly one row and `assert_rows_exact(result, 1)` would hold; that is a
        # strengthening, and it wants a real-server run behind it rather than a reading of
        # the N1QL.
        result = cluster.query(slow_query, timeout=timedelta(seconds=30))
        cb_env.assert_rows_at_least(result, 1)

    def test_query_ryow(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        q_str = f'SELECT * FROM `{cb_env.bucket.name}` USE KEYS "{key}"'
        result = cb_env.cluster.query(q_str)
        cb_env.assert_rows_exact(result, 0)
        res = cb_env.collection.insert(key, value)
        # add_mutation_token mutates in place and returns None, so it cannot be chained off
        # the constructor: `ms` would be None and the option would fall back to its default,
        # requesting no read-your-own-write consistency at all (PYCBC-1864).
        ms = MutationState()
        ms.add_mutation_token(res.mutation_token())
        result = cb_env.cluster.query(q_str, QueryOptions(consistent_with=ms))
        cb_env.assert_rows_exact(result, 1)

        # prior to PYCBC-1477 the SDK _could_ crash w/ this this sort of MS creation
        key, value = cb_env.get_new_doc()
        # q_str is rebuilt from the new key.  Without this the round below re-queries the
        # document inserted above: USE KEYS is a KV fetch that bypasses the query index, so
        # that document is visible the moment it is written, and the pre-insert assertion
        # sees 1 row rather than 0.  Neither the write nor the MutationState built from it
        # would then be under test, and the MutationState(res) crash guard would be all
        # that remained.
        q_str = f'SELECT * FROM `{cb_env.bucket.name}` USE KEYS "{key}"'
        result = cb_env.cluster.query(q_str)
        cb_env.assert_rows_exact(result, 0)
        res = cb_env.collection.insert(key, value)
        ms = MutationState(res)
        result = cb_env.cluster.query(q_str, QueryOptions(consistent_with=ms))
        cb_env.assert_rows_exact(result, 1)

    def test_query_with_metrics(self, cb_env: SyncTestEnvironment) -> None:
        initial = datetime.now()
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` "
            f"WHERE batch LIKE '{cb_env.get_batch_id()}%' LIMIT 1", QueryOptions(metrics=True))
        cb_env.assert_rows_exact(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: QueryMetaData
        assert isinstance(metadata, QueryMetaData)
        metrics = metadata.metrics()
        assert isinstance(metrics, QueryMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.mutation_count: 0,
                           metrics.result_count: 1,
                           metrics.sort_count: 0,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert UnsignedInt64(expected) == count_result, fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert metrics.error_count() == UnsignedInt64(0)
        assert metadata.profile() is None

    def test_query_with_profile(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` "
            f"WHERE batch LIKE '{cb_env.get_batch_id()}%' LIMIT 1", QueryOptions(profile=QueryProfile.TIMINGS))
        cb_env.assert_rows_exact(result, 1)
        assert result.metadata().profile() is not None

    def test_simple_query(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` "
                                      f"WHERE batch LIKE '{cb_env.get_batch_id()}%' LIMIT 2")
        cb_env.assert_rows_exact(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    def test_simple_query_explain(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"EXPLAIN SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(metrics=True))
        rows = []
        for r in result.rows():
            rows.append(r)

        assert len(rows) == 1
        assert 'plan' in rows[0]
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None

    def test_simple_query_prepared(self, cb_env: SyncTestEnvironment) -> None:
        assert cb_env.server_version_short is not None
        if cb_env.server_version_short < 6.5:
            # known-issue rather than feature-unsupported: prepared statements exist well
            # before 6.5, so the version is the workaround and CXXCBC-174 is the reason.
            # Filing it under the version would make this look retired when 6.5 support ends.
            skip_known_issue(ticket='CXXCBC-174',
                             observed=cb_env.server_version_short,
                             server=server_label(cb_env.is_mock_server),
                             note='prepared-statement handling below 6.5 pending CXXCBC-174')
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` "
                                      f"WHERE batch LIKE '{cb_env.get_batch_id()}%' LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        cb_env.assert_rows_exact(result, 2)
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None
        assert result._request.params.get('adhoc', None) is False

    def test_simple_query_with_named_params(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 2",
                                      batch=f'{cb_env.get_batch_id()}%')
        cb_env.assert_rows_exact(result, 2)

    def test_simple_query_with_named_params_in_options(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1",
                                      QueryOptions(named_parameters={'batch': f'{cb_env.get_batch_id()}%'}))
        cb_env.assert_rows_exact(result, 1)

    def test_simple_query_with_positional_params(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 2", f'{cb_env.get_batch_id()}%')
        cb_env.assert_rows_exact(result, 2)

    def test_simple_query_with_positional_params_in_options(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 2",
                                      QueryOptions(positional_parameters=[f'{cb_env.get_batch_id()}%']))
        cb_env.assert_rows_exact(result, 2)

    def test_simple_query_without_options_with_kwargs_named_params(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1",
                                      named_parameters={'batch': f'{cb_env.get_batch_id()}%'})
        cb_env.assert_rows_exact(result, 1)

    # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional
    # parameters for the query (once popping off the options if it is in there).  But this seems a bit tricky so for
    # now, kwargs override the corresponding value in the options, only.
    def test_simple_query_without_options_with_kwargs_positional_params(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1",
                                      positional_parameters=[f'{cb_env.get_batch_id()}%'])
        cb_env.assert_rows_exact(result, 1)


class QueryCollectionTests(QueryCollectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(QueryCollectionTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, QueryCollectionTestSuite)
        QueryEnvironment.setup_query_indexes(env, QueryCollectionTestSuite.COLLECTION_STRATEGY)
        yield env
        QueryEnvironment.teardown_query_indexes(env, QueryCollectionTestSuite.COLLECTION_STRATEGY)
        teardown_environment(env, QueryCollectionTestSuite)


@pytest.mark.xdist_group('default_keyspace_indexes')
class QueryTests(QueryTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(QueryTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, QueryTestSuite)
        QueryEnvironment.setup_query_indexes(env, QueryTestSuite.COLLECTION_STRATEGY)
        yield env
        QueryEnvironment.teardown_query_indexes(env, QueryTestSuite.COLLECTION_STRATEGY)
        teardown_environment(env, QueryTestSuite)
