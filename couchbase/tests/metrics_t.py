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

import pytest

from couchbase.exceptions import CouchbaseException
from tests.environments.tracing_and_metrics_environment import TracingAndMetricsTestEnvironment


class MetricsTestSuite:

    TEST_MANIFEST = [
        'test_custom_logging_meter_kv',
    ]

    @pytest.fixture()
    def skip_if_mock(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip('Test needs real server')

    # @TODO(jc): CXXCBC-207
    # @pytest.fixture()
    # def setup_query(self, cb_env):
    #     cb_env.check_if_feature_supported('query_index_mgmt')
    #     ixm = cb_env.cluster.query_indexes()
    #     cb_env.try_n_times(10, 3, ixm.create_primary_index,
    #                 cb_env.bucket.name,
    #                 timeout=timedelta(seconds=60),
    #                 ignore_if_exists=True)
    #     yield
    #     cb_env.try_n_times_till_exception(10, 3,
    #                                 ixm.drop_primary_index,
    #                                 cb_env.bucket.name,
    #                                 expected_exceptions=(QueryIndexNotFoundException))

    @pytest.mark.parametrize('op', ['get', 'upsert', 'insert', 'replace', 'remove'])
    def test_custom_logging_meter_kv(self, cb_env, op):
        cb_env.meter.reset()
        operation = getattr(cb_env.collection, op)
        try:
            if op == 'insert':
                key, value = cb_env.get_new_doc()
                operation(key, value)
            elif op in ['get', 'remove']:
                key = cb_env.get_existing_doc(key_only=True)
                operation(key)
            else:
                key, value = cb_env.get_existing_doc()
                operation(key, value)
        except CouchbaseException:
            pass

        cb_env.validate_metrics(op)

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures('skip_if_mock')
    # @pytest.mark.usefixtures("setup_query")
    # def test_custom_logging_meter_query(self, cb_env):
    #     cb_env.meter.reset()
    #     result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2").execute()
    #     self._validate_metrics('n1ql')

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures('skip_if_mock')
    # def test_custom_logging_meter_analytics(self, cb_env):
    #     cb_env.meter.reset()

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures('skip_if_mock')
    # def test_custom_logging_meter_search(self, cb_env, default_kvp, new_kvp, op):
    #     cb_env.meter.reset()

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures('skip_if_mock')
    # def test_custom_logging_meter_views(self, cb_env, default_kvp, new_kvp, op):
    #     cb_env.meter.reset()


class ClassicMetricsTests(MetricsTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicMetricsTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicMetricsTests) if valid_test_method(meth)]
        compare = set(MetricsTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        # a new environment and cluster is created
        cb_env = TracingAndMetricsTestEnvironment.from_environment(cb_base_env,
                                                                   create_meter=True)
        cb_env.setup(num_docs=10)
        yield cb_env
        cb_env.teardown()
        cb_env.cluster.close()
