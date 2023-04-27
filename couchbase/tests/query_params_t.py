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

from datetime import timedelta

import pytest

from couchbase.exceptions import InvalidArgumentException
from couchbase.mutation_state import MutationState
from couchbase.n1ql import (N1QLQuery,
                            QueryProfile,
                            QueryScanConsistency)
from couchbase.options import QueryOptions
from couchbase.result import MutationToken
from tests.environments import CollectionType


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
        assert q_mt.pop() == mt.as_dict()

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
        assert q_mt.pop() == mt.as_dict()

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
        assert next((m for m in q_mt if m == mt2.as_dict()), None) is not None

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
