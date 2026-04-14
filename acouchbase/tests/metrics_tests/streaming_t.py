#  Copyright 2016-2026. Couchbase, Inc.
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

import pytest
import pytest_asyncio

from couchbase.logic.observability import OpName
from couchbase.management.views import DesignDocumentNamespace
from couchbase.search import TermQuery
from tests.environments.metrics import AsyncMetricsEnvironment
from tests.environments.metrics.metrics_environment import MeterType
from tests.test_features import EnvironmentFeatures


class StreamingMetricsTestsSuite:

    TEST_MANIFEST = [
        'test_http_analytics_query_op',
        'test_http_query_op',
        'test_http_search_query_op',
        'test_http_view_query_op',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, acb_env: AsyncMetricsEnvironment):
        yield
        acb_env.http_meter_validator.reset()

    @pytest.fixture(scope='class')
    def check_analytics_supported(self, acb_env: AsyncMetricsEnvironment):
        EnvironmentFeatures.check_if_feature_supported('analytics',
                                                       acb_env.server_version_short,
                                                       acb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_query_supported(self, acb_env: AsyncMetricsEnvironment):
        EnvironmentFeatures.check_if_feature_supported('query',
                                                       acb_env.server_version_short,
                                                       acb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_search_supported(self, acb_env: AsyncMetricsEnvironment):
        EnvironmentFeatures.check_if_feature_supported('search',
                                                       acb_env.server_version_short,
                                                       acb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_views_supported(self, acb_env: AsyncMetricsEnvironment):
        EnvironmentFeatures.check_if_feature_supported('views',
                                                       acb_env.server_version_short,
                                                       acb_env.mock_server_type)

    @pytest.mark.usefixtures('check_analytics_supported')
    @pytest.mark.asyncio
    async def test_http_analytics_query_op(self, acb_env: AsyncMetricsEnvironment) -> None:
        validator = acb_env.http_meter_validator
        validator.reset(op_name=OpName.AnalyticsQuery)
        [r async for r in acb_env.cluster.analytics_query('SELECT 1=1;').rows()]
        validator.validate_http_op()

        statement = 'SELECT $named_param as param '
        validator.reset(op_name=OpName.AnalyticsQuery)
        [r async for r in acb_env.cluster.analytics_query(statement, named_parameters={'named_param': 5}).rows()]
        validator.validate_http_op()

        statement = 'SELECT $1 as param '
        validator.reset(op_name=OpName.AnalyticsQuery)
        [r async for r in acb_env.cluster.analytics_query(statement, positional_parameters=['hello']).rows()]
        validator.validate_http_op()

        validator.reset(op_name=OpName.AnalyticsQuery, validate_error=True)
        try:
            [r async for r in acb_env.cluster.analytics_query('This is not SQL++;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

        if not EnvironmentFeatures.is_feature_supported('collections',
                                                        acb_env.server_version_short,
                                                        acb_env.mock_server_type):
            return  # skip rest of test if collections not supported

        # dunno why we have this on analytics_query...buuuut we do...
        validator.reset(op_name=OpName.AnalyticsQuery,
                        validate_error=True,
                        bucket_name=acb_env.bucket.name,
                        scope_name=acb_env.scope.name)
        try:
            [r async for r in acb_env.scope.analytics_query('SELECT 1=1;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('check_query_supported')
    @pytest.mark.asyncio
    async def test_http_query_op(self, acb_env: AsyncMetricsEnvironment) -> None:
        validator = acb_env.http_meter_validator
        validator.reset(op_name=OpName.Query)
        [r async for r in acb_env.cluster.query('SELECT 1=1;').rows()]
        validator.validate_http_op()

        statement = 'SELECT $named_param as param '
        validator.reset(op_name=OpName.Query)
        [r async for r in acb_env.cluster.query(statement, named_parameters={'named_param': 5}).rows()]
        validator.validate_http_op()

        statement = 'SELECT $1 as param '
        validator.reset(op_name=OpName.Query)
        [r async for r in acb_env.cluster.query(statement, positional_parameters=['hello']).rows()]
        validator.validate_http_op()

        validator.reset(op_name=OpName.Query, validate_error=True)
        try:
            [r async for r in acb_env.cluster.query('This is not SQL++;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

        if not EnvironmentFeatures.is_feature_supported('collections',
                                                        acb_env.server_version_short,
                                                        acb_env.mock_server_type):
            return  # skip rest of test if collections not supported

        validator.reset(op_name=OpName.Query,
                        bucket_name=acb_env.bucket.name,
                        scope_name=acb_env.scope.name)
        [r async for r in acb_env.scope.query('SELECT 1=1;').rows()]
        validator.validate_http_op()

        statement = 'SELECT $named_param as param '
        validator.reset(op_name=OpName.Query,
                        bucket_name=acb_env.bucket.name,
                        scope_name=acb_env.scope.name)
        [r async for r in acb_env.scope.query(statement, named_parameters={'named_param': 5}).rows()]
        validator.validate_http_op()

        statement = 'SELECT $1 as param '
        validator.reset(op_name=OpName.Query,
                        bucket_name=acb_env.bucket.name,
                        scope_name=acb_env.scope.name)
        [r async for r in acb_env.scope.query(statement, positional_parameters=['hello']).rows()]
        validator.validate_http_op()

        validator.reset(op_name=OpName.Query,
                        validate_error=True,
                        bucket_name=acb_env.bucket.name,
                        scope_name=acb_env.scope.name)
        try:
            [r async for r in acb_env.scope.query('This is not SQL++;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('check_search_supported')
    @pytest.mark.asyncio
    async def test_http_search_query_op(self, acb_env: AsyncMetricsEnvironment) -> None:
        # search is a ROYAL PITA to setup, only testing errors (which we still send to the server)
        validator = acb_env.http_meter_validator
        validator.reset(op_name=OpName.SearchQuery, validate_error=True)
        try:
            [r async for r in acb_env.cluster.search('not-an-index', TermQuery('auto')).rows()]
        except Exception:
            pass
        validator.validate_http_op()

        if not EnvironmentFeatures.is_feature_supported('collections',
                                                        acb_env.server_version_short,
                                                        acb_env.mock_server_type):
            return  # skip rest of test if collections not supported

        validator.reset(op_name=OpName.SearchQuery,
                        bucket_name=acb_env.bucket.name,
                        scope_name=acb_env.scope.name,
                        validate_error=True)
        try:
            [r async for r in acb_env.scope.search('not-an-index', TermQuery('auto')).rows()]
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('check_views_supported')
    @pytest.mark.asyncio
    async def test_http_view_query_op(self, acb_env: AsyncMetricsEnvironment) -> None:
        # views are deprecated, so only minimal testing
        validator = acb_env.http_meter_validator
        validator.reset(op_name=OpName.ViewQuery,
                        validate_error=True,
                        bucket_name=acb_env.bucket.name)
        view_result = acb_env.bucket.view_query('fake-ddoc',
                                                'fake-view',
                                                limit=10,
                                                namespace=DesignDocumentNamespace.DEVELOPMENT)
        try:
            [r async for r in view_result]
        except Exception:
            pass
        validator.validate_http_op()


class ClassicStreamingMetricsTests(StreamingMetricsTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicStreamingMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicStreamingMetricsTests) if valid_test_method(meth)]
        test_list = set(StreamingMetricsTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest_asyncio.fixture(scope='class', name='acb_env', params=[MeterType.Basic, MeterType.NoOp])
    async def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        acb_env = await AsyncMetricsEnvironment.from_environment(cb_base_env, meter_type=request.param)
        await acb_env.setup(num_docs=50)
        yield acb_env
        await acb_env.teardown()
