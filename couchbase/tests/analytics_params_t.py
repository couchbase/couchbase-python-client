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

from datetime import timedelta

import pytest

from couchbase.analytics import AnalyticsQuery, AnalyticsScanConsistency
from couchbase.options import AnalyticsOptions
from tests.environments import CollectionType


class AnalyticsParamTestSuite:
    TEST_MANIFEST = [
        'test_encoded_consistency',
        'test_params_base',
        'test_params_client_context_id',
        'test_params_priority',
        'test_params_query_context',
        'test_params_read_only',
        'test_params_serializer',
        'test_params_timeout',
    ]

    @pytest.fixture(scope='class')
    def base_opts(self):
        return {'statement': 'SELECT * FROM default',
                'metrics': True}

    def test_encoded_consistency(self):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.REQUEST_PLUS)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == AnalyticsScanConsistency.REQUEST_PLUS.value
        assert query.consistency == AnalyticsScanConsistency.REQUEST_PLUS.value

        q_opts = AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.NOT_BOUNDED)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == AnalyticsScanConsistency.NOT_BOUNDED.value
        assert query.consistency == AnalyticsScanConsistency.NOT_BOUNDED.value

    def test_params_base(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions()
        query = AnalyticsQuery.create_query_object(q_str, q_opts)
        assert query.params == base_opts

    def test_params_client_context_id(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(client_context_id='test-string-id')
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['client_context_id'] = 'test-string-id'
        assert query.params == exp_opts

    def test_params_priority(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(priority=True)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['priority'] = True
        assert query.params == exp_opts

    def test_params_query_context(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(query_context='bucket.scope')
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scope_qualifier'] = 'bucket.scope'
        assert query.params == exp_opts

    def test_params_read_only(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(read_only=True)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['readonly'] = True
        assert query.params == exp_opts

    def test_params_serializer(self, base_opts):
        from couchbase.serializer import DefaultJsonSerializer

        # serializer
        serializer = DefaultJsonSerializer()
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(serializer=serializer)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['serializer'] = serializer
        assert query.params == exp_opts

    def test_params_timeout(self, base_opts):
        q_str = 'SELECT * FROM default'
        q_opts = AnalyticsOptions(timeout=timedelta(seconds=120))
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 120000000
        assert query.params == exp_opts

        q_opts = AnalyticsOptions(timeout=20)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        assert query.params == exp_opts

        q_opts = AnalyticsOptions(timeout=25.5)
        query = AnalyticsQuery.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 25500000
        assert query.params == exp_opts


class ClassicAnalyticsParamTests(AnalyticsParamTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAnalyticsParamTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAnalyticsParamTests) if valid_test_method(meth)]
        compare = set(AnalyticsParamTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.setup(request.param)
        yield cb_base_env
        cb_base_env.teardown(request.param)
