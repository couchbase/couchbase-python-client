#  Copyright 2016-2023. Couchbase, Inc.
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


import asyncio
import time
from datetime import datetime, timedelta

import pytest
import pytest_asyncio

import couchbase.subdocument as SD
from couchbase.exceptions import (CouchbaseException,
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
from tests.environments import CollectionType
from tests.environments.query_environment import AsyncQueryTestEnvironment
from tests.environments.test_environment import AsyncTestEnvironment
from tests.test_features import EnvironmentFeatures


class QueryCollectionTestSuite:
    TEST_MANIFEST = [
        'test_bad_query_context',
        'test_bad_scope_query',
        'test_cluster_query_context',
        'test_query_fully_qualified',
        'test_query_metadata',
        'test_query_ryow',
        'test_query_with_metrics',
        'test_scope_query',
        'test_scope_query_with_named_params_in_options',
        'test_scope_query_with_positional_params_in_options',
    ]

    @pytest.mark.asyncio
    async def test_bad_query_context(self, cb_env):
        q_str = f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2"
        # test w/ no context
        with pytest.raises(KeyspaceNotFoundException):
            await cb_env.cluster.query(q_str).execute()

        # test w/ bad scope
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        with pytest.raises(ScopeNotFoundException):
            await cb_env.cluster.query(q_str, QueryOptions(query_context=q_context)).execute()

    @pytest.mark.asyncio
    async def test_bad_scope_query(self, cb_env):
        q_str = f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2"
        q_context = f'{cb_env.bucket.name}.`fake-scope`'
        with pytest.raises(ScopeNotFoundException):
            await cb_env.scope.query(q_str, QueryOptions(query_context=q_context)).execute()

        q_context = f'`fake-bucket`.`{cb_env.scope.name}`'
        with pytest.raises(KeyspaceNotFoundException):
            await cb_env.scope.query(q_str, query_context=q_context).execute()

    @pytest.mark.asyncio
    async def test_cluster_query_context(self, cb_env):
        q_context = f'{cb_env.bucket.name}.{cb_env.scope.name}'
        # test with QueryOptions
        q_opts = QueryOptions(query_context=q_context, adhoc=True)
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", q_opts)
        await cb_env.assert_rows(result, 2)

        # test with kwargs
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2", query_context=q_context)
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_query_fully_qualified(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")
        await cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    @pytest.mark.asyncio
    async def test_query_metadata(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        await cb_env.assert_rows(result, 2)
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

    @pytest.mark.asyncio
    async def test_query_ryow(self, cb_env):
        key, value = cb_env.get_new_doc()
        result = cb_env.scope.query(f'SELECT * FROM `{cb_env.collection.name}` USE KEYS "{key}"')
        await cb_env.assert_rows(result, 0)
        res = await cb_env.collection.insert(key, value)
        ms = MutationState().add_mutation_token(res.mutation_token())
        result = cb_env.scope.query(f'SELECT * FROM `{cb_env.collection.name}` USE KEYS "{key}"',
                                    QueryOptions(consistent_with=ms))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.scope.query(
            f"SELECT * FROM `{cb_env.collection.name}` LIMIT 1", QueryOptions(metrics=True))
        await cb_env.assert_rows(result, 1)
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

    @pytest.mark.asyncio
    async def test_scope_query(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` LIMIT 2")
        await cb_env.assert_rows(result, 2)
        result = cb_env.scope.query(f"SELECT * FROM {cb_env.fqdn} LIMIT 2")

    @pytest.mark.asyncio
    async def test_scope_query_with_named_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $batch LIMIT 1",
                                    QueryOptions(named_parameters={'batch': f'{cb_env.get_batch_id()}%'}))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_scope_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $1 LIMIT 1",
                                    QueryOptions(positional_parameters=[f'{cb_env.get_batch_id()}%']))
        await cb_env.assert_rows(result, 1)


class QueryTestSuite:
    TEST_MANIFEST = [
        'test_bad_query',
        'test_mixed_named_parameters',
        'test_mixed_positional_parameters',
        'test_non_blocking',
        'test_preserve_expiry',
        'test_query_error_context',
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

    @pytest_asyncio.fixture(name='setup_udf')
    async def setup_teardown_udf(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('query_user_defined_functions',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)
        await AsyncTestEnvironment.try_n_times(3,
                                               1,
                                               cb_env.load_udf)
        yield
        await AsyncTestEnvironment.try_n_times(3,
                                               1,
                                               cb_env.drop_udf)

    @pytest.fixture(scope='class')
    def check_preserve_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('preserve_expiry',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.mark.asyncio
    async def test_bad_query(self, cb_env):
        with pytest.raises(ParsingFailedException):
            await cb_env.cluster.query("I'm not N1QL!").execute()

    @pytest.mark.asyncio
    async def test_mixed_named_parameters(self, cb_env):
        batch_id = cb_env.get_batch_id()
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1',
                                      QueryOptions(named_parameters={'batch': 'xgfflq'}), batch=batch_id)
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_mixed_positional_parameters(self, cb_env):
        # we assume that positional overrides one in the Options
        result = cb_env.cluster.query(f'SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1',
                                      QueryOptions(positional_parameters=['xgfflq']), f'{cb_env.get_batch_id()}')
        await cb_env.assert_rows(result, 1)

    @pytest.mark.usefixtures('setup_udf')
    @pytest.mark.asyncio
    async def test_non_blocking(self, cb_env):
        async def run_query(cluster, idx):
            result = cluster.query("EXECUTE FUNCTION loop(1000000000)")
            rows = []
            async for r in result:
                rows.append(r)
            return {time.time(): idx}

        tasks = [asyncio.create_task(run_query(cb_env.cluster, idx)) for idx in range(10)]
        results = await asyncio.gather(*tasks)
        # order tasks via the finished timestamp, prior to PYCBC-1471, the queries
        # would return in order b/c we were blocking when pulling rows from the binding's
        # streaming result object.
        ordered_results = dict(sorted({k: v for r in results for k, v in r.items()}.items()))
        assert list(ordered_results.values()) != list(i for i in range(10))

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    @pytest.mark.asyncio
    async def test_preserve_expiry(self, cb_env):
        key = "imakey"
        content = {"a": "aaa", "b": "bbb"}

        await cb_env.collection.upsert(key, content, UpsertOptions(expiry=timedelta(days=1)))

        expiry_path = "$document.exptime"
        res = await AsyncTestEnvironment.try_n_times(10,
                                                     3,
                                                     cb_env.collection.lookup_in,
                                                     key,
                                                     (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        q_str = f"UPDATE `{cb_env.bucket.name}` AS content USE KEYS '{key}' SET content.a = 'aaaa'"
        await cb_env.cluster.query(q_str, QueryOptions(preserve_expiry=True)).execute()

        res = await AsyncTestEnvironment.try_n_times(10,
                                                     3,
                                                     cb_env.collection.lookup_in,
                                                     key,
                                                     (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2

    @pytest.mark.asyncio
    async def test_query_error_context(self, cb_env):
        try:
            await cb_env.cluster.query("SELECT * FROM no_such_bucket").execute()
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

    @pytest.mark.asyncio
    async def test_query_metadata(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        await cb_env.assert_rows(result, 2)
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

    @pytest.mark.asyncio
    async def test_query_raw_options(self, cb_env):
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        batch_id = cb_env.get_batch_id()
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT $1",
                                      QueryOptions(raw={'$batch': f'{batch_id}%', 'args': [2]}))
        await cb_env.assert_rows(result, 2)

        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1",
                                      QueryOptions(raw={'args': [f'{batch_id}%']}))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_ryow(self, cb_env):
        key, value = cb_env.get_new_doc()
        q_str = f'SELECT * FROM `{cb_env.bucket.name}` USE KEYS "{key}"'
        result = cb_env.cluster.query(q_str)
        await cb_env.assert_rows(result, 0)
        res = await cb_env.collection.insert(key, value)
        ms = MutationState().add_mutation_token(res.mutation_token())
        result = cb_env.cluster.query(q_str, QueryOptions(consistent_with=ms))
        await cb_env.assert_rows(result, 1)

        # prior to PYCBC-1477 the SDK _could_ crash w/ this this sort of MS creation
        key, value = cb_env.get_new_doc()
        result = cb_env.cluster.query(q_str)
        await cb_env.assert_rows(result, 0)
        res = await cb_env.collection.insert(key, value)
        ms = MutationState(res)
        result = cb_env.cluster.query(q_str, QueryOptions(consistent_with=ms))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(metrics=True))
        await cb_env.assert_rows(result, 1)
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

    @pytest.mark.asyncio
    async def test_query_with_profile(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 1", QueryOptions(profile=QueryProfile.TIMINGS))
        await cb_env.assert_rows(result, 1)
        assert result.metadata().profile() is not None

    @pytest.mark.asyncio
    async def test_simple_query(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        await cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('adhoc', None) is None

    @pytest.mark.asyncio
    async def test_simple_query_explain(self, cb_env):
        result = cb_env.cluster.query(f"EXPLAIN SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(metrics=True))
        rows = []
        async for r in result.rows():
            rows.append(r)

        assert len(rows) == 1
        assert 'plan' in rows[0]
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None

    @pytest.mark.asyncio
    async def test_simple_query_prepared(self, cb_env):
        # @TODO(CXXCBC-174)
        if cb_env.server_version_short < 6.5:
            pytest.skip(f'Skipped on server versions < 6.5 (using {cb_env.server_version_short}). Pending CXXCBC-174')
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        await cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None
        assert result._request.params.get('adhoc', None) is False

    @pytest.mark.asyncio
    async def test_simple_query_with_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 2",
                                      batch=f'{cb_env.get_batch_id()}%')
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_simple_query_with_named_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1",
                                      QueryOptions(named_parameters={'batch': f'{cb_env.get_batch_id()}%'}))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_simple_query_with_positional_params(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 2", f'{cb_env.get_batch_id()}%')
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_simple_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 2",
                                      QueryOptions(positional_parameters=[f'{cb_env.get_batch_id()}%']))
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_simple_query_without_options_with_kwargs_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $batch LIMIT 1",
                                      named_parameters={'batch': f'{cb_env.get_batch_id()}%'})
        await cb_env.assert_rows(result, 1)

    # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional
    # parameters for the query (once popping off the options if it is in there).  But this seems a bit tricky so for
    # now, kwargs override the corresponding value in the options, only.

    @pytest.mark.asyncio
    async def test_simple_query_without_options_with_kwargs_positional_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE batch LIKE $1 LIMIT 1",
                                      positional_parameters=[f'{cb_env.get_batch_id()}%'])
        await cb_env.assert_rows(result, 1)


class ClassicQueryCollectionTests(QueryCollectionTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicQueryCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicQueryCollectionTests) if valid_test_method(meth)]
        compare = set(QueryCollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.NAMED])
    async def couchbase_test_environment(self, acb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        acb_env = AsyncQueryTestEnvironment.from_environment(acb_base_env)
        acb_env.enable_query_mgmt()
        await acb_env.setup(request.param)
        yield acb_env
        await acb_env.teardown(request.param)


class ClassicQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicQueryTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicQueryTests) if valid_test_method(meth)]
        compare = set(QueryTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    async def couchbase_test_environment(self, acb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        acb_env = AsyncQueryTestEnvironment.from_environment(acb_base_env)
        acb_env.enable_query_mgmt()
        await acb_env.setup(request.param)
        yield acb_env
        await acb_env.teardown(request.param)
