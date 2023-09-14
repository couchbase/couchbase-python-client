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


from datetime import datetime, timedelta

import pytest
import pytest_asyncio

from couchbase.analytics import (AnalyticsMetaData,
                                 AnalyticsMetrics,
                                 AnalyticsStatus,
                                 AnalyticsWarning)
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  DatasetNotFoundException,
                                  DataverseNotFoundException)
from couchbase.options import AnalyticsOptions, UnsignedInt64
from tests.environments import CollectionType
from tests.environments.analytics_environment import AsyncAnalyticsTestEnvironment


class AnalyticsCollectionTestSuite:
    TEST_MANIFEST = [
        'test_analytics_metadata',
        'test_analytics_with_metrics',
        'test_bad_query_context',
        'test_bad_scope_query',
        'test_cluster_query_context',
        'test_query_fully_qualified',
        'test_scope_query',
        'test_scope_query_fqdn',
        'test_scope_query_with_named_params_in_options',
        'test_scope_query_with_positional_params_in_options',
    ]

    @pytest.mark.asyncio
    async def test_analytics_metadata(self, cb_env):
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2')
        await cb_env.assert_rows(result, 2)
        metadata = result.metadata()  # type: AnalyticsMetaData
        assert isinstance(metadata, AnalyticsMetaData)
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == AnalyticsStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), (list))
        for warning in metadata.warnings():
            assert isinstance(warning, AnalyticsWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    @pytest.mark.asyncio
    async def test_analytics_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` LIMIT 1')
        await cb_env.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: AnalyticsMetaData
        metrics = metadata.metrics()
        assert isinstance(metrics, AnalyticsMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.error_count: 0,
                           metrics.result_count: 1,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert count_result == UnsignedInt64(expected), fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert isinstance(metrics.processed_objects(), UnsignedInt64)
        assert metrics.error_count() == UnsignedInt64(0)

    @pytest.mark.asyncio
    async def test_bad_query_context(self, cb_env):
        # test w/ no context
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.collection.name} `LIMIT 2')
        # @TODO: DatasetNotFoundException
        with pytest.raises(DatasetNotFoundException):
            await cb_env.assert_rows(result, 2)

        # test w/ bad scope
        q_context = f'default:`{cb_env.bucket.name}`.`fake-scope`'
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2', AnalyticsOptions(query_context=q_context))
        # @TODO: DataverseNotFoundException
        with pytest.raises(DataverseNotFoundException):
            await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_bad_scope_query(self, cb_env):
        q_context = f'default:`{cb_env.bucket.name}`.`fake-scope`'
        result = cb_env.scope.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2',
                                              AnalyticsOptions(query_context=q_context))
        with pytest.raises(DataverseNotFoundException):
            await cb_env.assert_rows(result, 2)

        q_context = f'default:`fake-bucket`.`{cb_env.scope.name}`'
        result = cb_env.scope.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2',
                                              query_context=q_context)
        with pytest.raises(DataverseNotFoundException):
            await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_cluster_query_context(self, cb_env):
        q_context = f'default:`{cb_env.bucket.name}`.`{cb_env.scope.name}`'
        # test with QueryOptions
        a_opts = AnalyticsOptions(query_context=q_context)
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2', a_opts)
        await cb_env.assert_rows(result, 2)

        # test with kwargs
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2', query_context=q_context)
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_query_fully_qualified(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2')
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_scope_query(self, cb_env):
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` LIMIT 2')
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_scope_query_fqdn(self, cb_env):
        # @TODO:  look into this...
        if cb_env.server_version_short >= 7.1:
            pytest.skip("Analytics scope query format not allowed on server versions >= 7.1")

        result = cb_env.scope.analytics_query(f'SELECT * FROM {cb_env.fqdn} LIMIT 2', query_context='')
        await cb_env.assert_rows(result, 2)

    @pytest.mark.asyncio
    async def test_scope_query_with_named_params_in_options(self, cb_env):
        q_str = f'SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $batch LIMIT 1'
        result = cb_env.scope.analytics_query(q_str,
                                              AnalyticsOptions(named_parameters={'batch': f'{cb_env.get_batch_id()}%'}))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_scope_query_with_positional_params_in_options(self, cb_env):
        result = cb_env.scope.analytics_query(f'SELECT * FROM `{cb_env.collection.name}` WHERE batch LIKE $1 LIMIT 1',
                                              AnalyticsOptions(positional_parameters=[f'{cb_env.get_batch_id()}%']))
        await cb_env.assert_rows(result, 1)


class AnalyticsTestSuite:
    TEST_MANIFEST = [
        'test_analytics_metadata',
        'test_analytics_with_metrics',
        'test_query_named_parameters',
        'test_query_named_parameters_no_options',
        'test_query_named_parameters_override',
        'test_query_positional_params',
        'test_query_positional_params_no_option',
        'test_query_positional_params_override',
        'test_query_raw_options',
        'test_query_timeout',
        'test_simple_query',
    ]

    @pytest.mark.asyncio
    async def test_analytics_metadata(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` LIMIT 2')
        await cb_env.assert_rows(result, 2)
        metadata = result.metadata()  # type: AnalyticsMetaData
        isinstance(metadata, AnalyticsMetaData)
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            assert isinstance(id_res, str), fail_msg
        assert metadata.status() == AnalyticsStatus.SUCCESS
        assert isinstance(metadata.signature(), (str, dict))
        assert isinstance(metadata.warnings(), (list))
        for warning in metadata.warnings():
            assert isinstance(warning, AnalyticsWarning)
            assert isinstance(warning.message(), str)
            assert isinstance(warning.code(), int)

    @pytest.mark.asyncio
    async def test_analytics_with_metrics(self, cb_env):
        initial = datetime.now()
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` LIMIT 1')
        await cb_env.assert_rows(result, 1)
        taken = datetime.now() - initial
        metadata = result.metadata()  # type: AnalyticsMetaData
        metrics = metadata.metrics()
        assert isinstance(metrics, AnalyticsMetrics)
        assert isinstance(metrics.elapsed_time(), timedelta)
        assert metrics.elapsed_time() < taken
        assert metrics.elapsed_time() > timedelta(milliseconds=0)
        assert isinstance(metrics.execution_time(), timedelta)
        assert metrics.execution_time() < taken
        assert metrics.execution_time() > timedelta(milliseconds=0)

        expected_counts = {metrics.error_count: 0,
                           metrics.result_count: 1,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            assert isinstance(count_result, UnsignedInt64), fail_msg
            assert count_result == UnsignedInt64(expected), fail_msg
        assert metrics.result_size() > UnsignedInt64(0)
        assert isinstance(metrics.processed_objects(), UnsignedInt64)
        assert metrics.error_count() == UnsignedInt64(0)

    @pytest.mark.asyncio
    async def test_query_named_parameters(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $atype LIMIT 1',
                                                AnalyticsOptions(named_parameters={'atype': 'vehicle'}))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_named_parameters_no_options(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $atype LIMIT 1',
                                                atype='vehicle')
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_named_parameters_override(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $atype LIMIT 1',
                                                AnalyticsOptions(named_parameters={'atype': 'abcdefg'}),
                                                atype='vehicle')
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_positional_params(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $1 LIMIT 1',
                                                AnalyticsOptions(positional_parameters=["vehicle"]))
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_positional_params_no_option(self, cb_env):
        result = cb_env.cluster.analytics_query(
            f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $1 LIMIT 1', 'vehicle')
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_positional_params_override(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $1 LIMIT 1',
                                                AnalyticsOptions(positional_parameters=['abcdefg']), 'vehicle')
        await cb_env.assert_rows(result, 1)

    @pytest.mark.asyncio
    async def test_query_raw_options(self, cb_env):
        # via raw, we should be able to pass any option
        # if using named params, need to match full name param in query
        # which is different for when we pass in name_parameters via their specific
        # query option (i.e. include the $ when using raw)
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $atype LIMIT $1',
                                                AnalyticsOptions(raw={'$atype': 'vehicle', 'args': [1]}))
        await cb_env.assert_rows(result, 1)

        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` WHERE `type` = $1 LIMIT 1',
                                                AnalyticsOptions(raw={'args': ['vehicle']}))
        await cb_env.assert_rows(result, 1)

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_query_timeout(self, cb_env):
        from acouchbase.cluster import Cluster
        from couchbase.auth import PasswordAuthenticator
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions
        conn_string = cb_env.config.get_connection_string()
        username, pw = cb_env.config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        # Prior to PYCBC-1521, this test would fail as each request would override the cluster level analytics_timeout.
        # If a timeout was not provided in the request, the default 75s timeout would be used.  PYCBC-1521 corrects
        # this behavior so this test will pass as we are essentially forcing an AmbiguousTimeoutException because
        # we are setting the cluster level analytics_timeout such a small value.
        timeout_opts = ClusterTimeoutOptions(analytics_timeout=timedelta(milliseconds=1))
        cluster = await Cluster.connect(f'{conn_string}', ClusterOptions(auth, timeout_options=timeout_opts))
        # don't need to do this except for older server versions
        _ = cluster.bucket(f'{cb_env.bucket.name}')
        q_str = f'SELECT * FROM `{cb_env.DATASET_NAME}` LIMIT 1;'
        with pytest.raises(AmbiguousTimeoutException):
            res = cluster.analytics_query(q_str)
            [r async for r in res.rows()]
        # if we override the timeout w/in the request the query should succeed.
        res = cluster.analytics_query(q_str, timeout=timedelta(seconds=10))
        rows = [r async for r in res.rows()]
        assert len(rows) > 0

    @pytest.mark.asyncio
    async def test_simple_query(self, cb_env):
        result = cb_env.cluster.analytics_query(f'SELECT * FROM `{cb_env.DATASET_NAME}` LIMIT 1')
        await cb_env.assert_rows(result, 1)


class ClassicAnalyticsCollectionTests(AnalyticsCollectionTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAnalyticsCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAnalyticsCollectionTests) if valid_test_method(meth)]
        compare = set(AnalyticsCollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.NAMED])
    async def couchbase_test_environment(self, acb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        acb_env = AsyncAnalyticsTestEnvironment.from_environment(acb_base_env)
        acb_env.enable_analytics_mgmt()
        await acb_env.setup(request.param)
        yield acb_env
        await acb_env.teardown(request.param)


class ClassicAnalyticsTests(AnalyticsTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAnalyticsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAnalyticsTests) if valid_test_method(meth)]
        compare = set(AnalyticsTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    async def couchbase_test_environment(self, acb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        acb_env = AsyncAnalyticsTestEnvironment.from_environment(acb_base_env)
        acb_env.enable_analytics_mgmt()
        await acb_env.setup(request.param)
        yield acb_env
        await acb_env.teardown(request.param)
