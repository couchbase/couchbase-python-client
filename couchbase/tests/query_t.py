#  Copyright 2016-2022. Couchbase, Inc.
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

import threading
from datetime import datetime, timedelta

import pytest

import couchbase.subdocument as SD
from couchbase.exceptions import (CouchbaseException,
                                  InvalidArgumentException,
                                  KeyspaceNotFoundException,
                                  ParsingFailedException,
                                  QueryErrorContext,
                                  ScopeNotFoundException)
from couchbase.mutation_state import MutationState
from couchbase.n1ql import (N1QLQuery,
                            QueryMetaData,
                            QueryMetrics,
                            QueryProfile,
                            QueryScanConsistency,
                            QueryStatus,
                            QueryWarning)
from couchbase.options import (QueryOptions,
                               UnsignedInt64,
                               UpsertOptions)
from couchbase.result import MutationToken
from tests.environments import CollectionType
from tests.environments.query_environment import QueryTestEnvironment
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class QueryCollectionTestSuite:
    TEST_MANIFEST = [
        'test_bad_query_context',
        'test_bad_scope_query',
        'test_cluster_query_context',
        'test_query_fully_qualified',
        'test_query_in_thread',
        'test_query_metadata',
        'test_query_ryow',
        'test_query_with_metrics',
        'test_scope_query',
        'test_scope_query_with_named_params_in_options',
        'test_scope_query_with_positional_params_in_options',
    ]

    def test_bad_query_context(self, cb_env):
        # test w/ no context
        with pytest.raises(KeyspaceNotFoundException):
            cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2").execute()

        # test w/ bad scope
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        with pytest.raises(ScopeNotFoundException):
            cb_env.cluster.query(
                f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", QueryOptions(query_context=q_context)).execute()

    def test_bad_scope_query(self, cb_env):
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        with pytest.raises(ScopeNotFoundException):
            cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                               QueryOptions(query_context=q_context)).execute()

        q_context = f'`fake-bucket`.`{cb_env.scope.name}`'
        with pytest.raises(KeyspaceNotFoundException):
            cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2",
                               query_context=q_context).execute()

    def test_cluster_query_context(self, cb_env):
        q_context = f'{cb_env.bucket.name}.{cb_env.scope.name}'
        # test with QueryOptions
        q_opts = QueryOptions(query_context=q_context, adhoc=True)
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", q_opts)
        cb_env.assert_rows(result, 2)

        # test with kwargs
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", query_context=q_context)
        cb_env.assert_rows(result, 2)

    def test_query_fully_qualified(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")
        cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    def test_query_in_thread(self, cb_env):
        results = [None]

        def run_test(scope, collection_name, assert_fn, results):
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

        t = threading.Thread(target=run_test, args=(cb_env.scope, cb_env.collection.name, cb_env.assert_rows, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True

    def test_query_metadata(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        cb_env.assert_rows(result, 2)
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

    def test_query_ryow(self, cb_env):
        key, value = cb_env.get_new_doc()
        result = cb_env.scope.query(f'SELECT * FROM `{cb_env.collection.name}` USE KEYS "{key}"')
        cb_env.assert_rows(result, 0)
        res = cb_env.collection.insert(key, value)
        ms = MutationState().add_mutation_token(res.mutation_token())
        result = cb_env.scope.query(f'SELECT * FROM `{cb_env.collection.name}` USE KEYS "{key}"',
                                    QueryOptions(consistent_with=ms))
        cb_env.assert_rows(result, 1)

    def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.scope.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 1", QueryOptions(metrics=True))
        cb_env.assert_rows(result, 1)
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

    def test_scope_query(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        cb_env.assert_rows(result, 2)
        result = cb_env.scope.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")

    def test_scope_query_with_named_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $batch LIMIT 1",
                                    QueryOptions(named_parameters={'batch': f'{cb_env.get_batch_id()}%'}))
        cb_env.assert_rows(result, 1)

    def test_scope_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $1 LIMIT 1",
                                    QueryOptions(positional_parameters=[f'{cb_env.get_batch_id()}%']))
        cb_env.assert_rows(result, 1)


class QueryParamTestSuite:
    TEST_MANIFEST = [
        'test_consistent_with',
        'test_encoded_consistency',
        'test_params_adhoc',
        'test_params_base',
        'test_params_client_context_id',
        'test_params_flex_index',
        'test_params_max_parallelism',
        'test_params_metrics',
        'test_params_pipeline_batch',
        'test_params_pipeline_cap',
        'test_params_preserve_expiry',
        'test_params_profile',
        'test_params_query_context',
        'test_params_readonly',
        'test_params_scan_cap',
        'test_params_scan_consistency',
        'test_params_scan_wait',
        'test_params_serializer',
        'test_params_timeout',
    ]

    @pytest.fixture(scope='class')
    def base_opts(self):
        return {'statement': 'SELECT * FROM default',
                'metrics': False}

    def test_consistent_with(self):

        q_str = 'SELECT * FROM default'
        ms = MutationState()
        mt = MutationToken(token={
            'partition_id': 42,
            'partition_uuid': 3004,
            'sequence_number': 3,
            'bucket_name': 'default'
        })
        ms._add_scanvec(mt)
        q_opts = QueryOptions(consistent_with=ms)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        # couchbase++ will set scan_consistency, so params should be
        # None, but the prop should return AT_PLUS
        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 1
        assert q_mt.pop() == mt

        # Ensure no dups
        ms = MutationState()
        mt1 = MutationToken(token={
            'partition_id': 42,
            'partition_uuid': 3004,
            'sequence_number': 3,
            'bucket_name': 'default'
        })
        ms._add_scanvec(mt)
        ms._add_scanvec(mt1)
        q_opts = QueryOptions(consistent_with=ms)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 1
        assert q_mt.pop() == mt

        # Try with a second bucket
        ms = MutationState()
        mt2 = MutationToken(token={
            'partition_id': 42,
            'partition_uuid': 3004,
            'sequence_number': 3,
            'bucket_name': 'default1'
        })
        ms._add_scanvec(mt)
        ms._add_scanvec(mt2)
        q_opts = QueryOptions(consistent_with=ms)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, list)
        assert len(q_mt) == 2
        assert next((m for m in q_mt if m == mt2), None) is not None

    def test_encoded_consistency(self):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == QueryScanConsistency.REQUEST_PLUS.value
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.NOT_BOUNDED)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == QueryScanConsistency.NOT_BOUNDED.value
        assert query.consistency == QueryScanConsistency.NOT_BOUNDED

        # cannot set scan_consistency to AT_PLUS, need to use consistent_with to do that
        with pytest.raises(InvalidArgumentException):
            q_opts = QueryOptions(scan_consistency=QueryScanConsistency.AT_PLUS)
            query = N1QLQuery.create_query_object(q_str, q_opts)

    def test_params_adhoc(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(adhoc=False)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['adhoc'] = False
        assert query.params == exp_opts

    def test_params_base(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions()
        query = N1QLQuery.create_query_object(q_str, q_opts)
        assert query.params == base_opts

    def test_params_client_context_id(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(client_context_id='test-string-id')
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['client_context_id'] = 'test-string-id'
        assert query.params == exp_opts

    def test_params_flex_index(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(flex_index=True)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['flex_index'] = True
        assert query.params == exp_opts

    def test_params_max_parallelism(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(max_parallelism=5)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['max_parallelism'] = 5
        assert query.params == exp_opts

    def test_params_metrics(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(metrics=True)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['metrics'] = True
        assert query.params == exp_opts

    def test_params_pipeline_batch(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(pipeline_batch=5)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['pipeline_batch'] = 5
        assert query.params == exp_opts

    def test_params_pipeline_cap(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(pipeline_cap=5)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['pipeline_cap'] = 5
        assert query.params == exp_opts

    def test_params_preserve_expiry(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(preserve_expiry=True)
        query = N1QLQuery.create_query_object(q_str, q_opts)
        assert query.params.get('preserve_expiry', None) is True
        assert query.preserve_expiry is True

        q_opts = QueryOptions(preserve_expiry=False)
        query = N1QLQuery.create_query_object(q_str, q_opts)
        assert query.params.get('preserve_expiry', None) is False
        assert query.preserve_expiry is False

        # if not set, the prop will return False, but preserve_expiry should
        # not be in the params
        query = N1QLQuery.create_query_object(q_str)
        assert query.params.get('preserve_expiry', None) is None
        assert query.preserve_expiry is False

    def test_params_profile(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(profile=QueryProfile.PHASES)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['profile_mode'] = QueryProfile.PHASES.value
        assert query.params == exp_opts
        assert query.profile == QueryProfile.PHASES

    def test_params_query_context(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(query_context='bucket.scope')
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['query_context'] = 'bucket.scope'
        assert query.params == exp_opts

    def test_params_readonly(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(read_only=True)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['readonly'] = True
        assert query.params == exp_opts

    def test_params_scan_cap(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_cap=5)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scan_cap'] = 5
        assert query.params == exp_opts

    def test_params_scan_wait(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_wait=timedelta(seconds=30))
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scan_wait'] = 30000000
        assert query.params == exp_opts

    def test_params_scan_consistency(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scan_consistency'] = QueryScanConsistency.REQUEST_PLUS.value
        assert query.params == exp_opts
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

    def test_params_serializer(self, base_opts):
        q_str = 'SELECT * FROM default'
        from couchbase.serializer import DefaultJsonSerializer

        # serializer
        serializer = DefaultJsonSerializer()
        q_opts = QueryOptions(serializer=serializer)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['serializer'] = serializer
        assert query.params == exp_opts

    def test_params_timeout(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(timeout=timedelta(seconds=20))
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        assert query.params == exp_opts

        q_opts = QueryOptions(timeout=20)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        assert query.params == exp_opts

        q_opts = QueryOptions(timeout=25.5)
        query = N1QLQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 25500000
        assert query.params == exp_opts


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_bad_query',
        'test_mixed_named_parameters',
        'test_mixed_positional_parameters',
        'test_preserve_expiry',
        'test_query_error_context',
        'test_query_in_thread',
        'test_query_metadata',
        'test_query_raw_options',
        'test_query_ryow',
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
    def check_preserve_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('preserve_expiry',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    def test_bad_query(self, cb_env):
        with pytest.raises(ParsingFailedException):
            cb_env.cluster.query("I'm not N1QL!").execute()

    def test_mixed_named_parameters(self, cb_env):
        batch_id = cb_env.get_batch_id()
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1',
                                      QueryOptions(named_parameters={'batch': 'xgfflq'}), batch=batch_id)
        cb_env.assert_rows(result, 1)

    def test_mixed_positional_parameters(self, cb_env):
        # we assume that positional overrides one in the Options
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1',
                                      QueryOptions(positional_parameters=['xgfflq']), f'{cb_env.get_batch_id()}')
        cb_env.assert_rows(result, 1)

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_preserve_expiry(self, cb_env):
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

        cb_env.cluster.query(f"UPDATE `{cb_env.bucket.name}` AS content USE KEYS '{key}' SET content.a = 'aaaa'",
                             QueryOptions(preserve_expiry=True)).execute()

        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2

    def test_query_error_context(self, cb_env):
        try:
            cb_env.cluster.query("SELECT * FROM no_such_bucket").execute()
        except CouchbaseException as ex:
            assert isinstance(ex.context, QueryErrorContext)
            assert ex.context.statement is not None
            assert ex.context.first_error_code is not None
            assert ex.context.first_error_message is not None
            assert ex.context.client_context_id is not None
            assert ex.context.response_body is not None
            # @TODO:  these are diff from 3.x -> 4.x
            # self.assertIsNotNone(ex.context.endpoint)
            # self.assertIsNotNone(ex.context.error_response_body)

    def test_query_in_thread(self, cb_env):
        results = [None]

        def run_test(cluster, assert_fn, results):
            try:
                result = cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
                assert_fn(result, 2)
                assert result.metadata() is not None
            except AssertionError:
                results[0] = False
            except Exception as ex:
                results[0] = ex
            else:
                results[0] = True

        t = threading.Thread(target=run_test, args=(cb_env.cluster, cb_env.assert_rows, results))
        t.start()
        t.join()

        assert len(results) == 1
        assert results[0] is True

    def test_query_metadata(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        cb_env.assert_rows(result, 2)
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

    def test_query_raw_options(self, cb_env):
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        batch_id = cb_env.get_batch_id()
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT $1",
                                      QueryOptions(raw={'$batch': f'{batch_id}%', 'args': [2]}))
        cb_env.assert_rows(result, 2)

        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1",
                                      QueryOptions(raw={'args': [f'{batch_id}%']}))
        cb_env.assert_rows(result, 1)

    def test_query_ryow(self, cb_env):
        key, value = cb_env.get_new_doc()
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` USE KEYS "{key}"')
        cb_env.assert_rows(result, 0)
        res = cb_env.collection.insert(key, value)
        ms = MutationState().add_mutation_token(res.mutation_token())
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` USE KEYS "{key}"',
                                      QueryOptions(consistent_with=ms))
        cb_env.assert_rows(result, 1)

    def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(metrics=True))
        cb_env.assert_rows(result, 1)
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

    def test_query_with_profile(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(profile=QueryProfile.TIMINGS))
        cb_env.assert_rows(result, 1)
        assert result.metadata().profile() is not None

    def test_simple_query(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    def test_simple_query_explain(self, cb_env):
        result = cb_env.cluster.query(f"EXPLAIN SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(metrics=True))
        rows = []
        for r in result.rows():
            rows.append(r)

        assert len(rows) == 1
        assert 'plan' in rows[0]
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None

    def test_simple_query_prepared(self, cb_env):
        # @TODO(CXXCBC-174)
        if cb_env.server_version_short < 6.5:
            pytest.skip(f'Skipped on server versions < 6.5 (using {cb_env.server_version_short}). Pending CXXCBC-174')
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None
        assert result._request.params.get('adhoc', None) is False

    def test_simple_query_with_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 2",
                                      batch=f'{cb_env.get_batch_id()}%')
        cb_env.assert_rows(result, 2)

    def test_simple_query_with_named_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1",
                                      QueryOptions(named_parameters={'batch': f'{cb_env.get_batch_id()}%'}))
        cb_env.assert_rows(result, 1)

    def test_simple_query_with_positional_params(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 2", f'{cb_env.get_batch_id()}%')
        cb_env.assert_rows(result, 2)

    def test_simple_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 2",
                                      QueryOptions(positional_parameters=[f'{cb_env.get_batch_id()}%']))
        cb_env.assert_rows(result, 2)

    def test_simple_query_without_options_with_kwargs_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1",
                                      named_parameters={'batch': f'{cb_env.get_batch_id()}%'})
        cb_env.assert_rows(result, 1)

    # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional
    # parameters for the query (once popping off the options if it is in there).  But this seems a bit tricky so for
    # now, kwargs override the corresponding value in the options, only.
    def test_simple_query_without_options_with_kwargs_positional_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1",
                                      positional_parameters=[f'{cb_env.get_batch_id()}%'])
        cb_env.assert_rows(result, 1)


class ClassicQueryCollectionTests(QueryCollectionTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicQueryCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicQueryCollectionTests) if valid_test_method(meth)]
        compare = set(QueryCollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = QueryTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_query_mgmt()
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)


class ClassicQueryParamTests(QueryParamTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicQueryParamTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicQueryParamTests) if valid_test_method(meth)]
        compare = set(QueryParamTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.setup(request.param)
        yield cb_base_env
        cb_base_env.teardown(request.param)


class ClassicQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicQueryTests) if valid_test_method(meth)]
        compare = set(QueryTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = QueryTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_query_mgmt()
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)
