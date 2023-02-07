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
from couchbase.options import (AnalyticsOptions,
                               GetOptions,
                               InsertOptions,
                               QueryOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               SearchOptions,
                               UpsertOptions,
                               ViewOptions)
from couchbase.search import TermQuery
from tests.environments.tracing_and_metrics_environment import TracingAndMetricsTestEnvironment


class TracerTestsSuite:
    TEST_MANIFEST = [
        'test_http',
        'test_kv',
    ]

    @pytest.fixture(scope='class')
    def skip_if_mock(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip('Test needs real server')

    @pytest.mark.parametrize('op, span_name, opts, value', [
        ('get', 'cb.get', GetOptions, None),
        ('upsert', 'cb.upsert', UpsertOptions, {'some': 'thing'}),
        ('insert', 'cb.insert', InsertOptions, {'some': 'thing'}),
        ('replace', 'cb.replace', ReplaceOptions, {'some': 'thing'}),
        ('remove', 'cb.remove', RemoveOptions, None),
    ])
    @pytest.mark.parametrize("with_parent", [True, False])
    def test_kv(self, cb_env, op, span_name, opts, value, with_parent):
        # have to reset between parameterized runs
        cb_env.tracer.reset()
        cb = cb_env.collection
        parent = None
        if with_parent:
            parent = cb_env.tracer.start_span(f'parent_{op}')
        key = cb_env.get_existing_doc(key_only=True)
        options = opts(span=parent)
        operation = getattr(cb, op)
        try:
            if value:
                operation(key, value, options)
            else:
                operation(key, options)
        except CouchbaseException:
            pass  # insert will fail, who cares.

        spans = cb_env.tracer.spans()
        if with_parent:
            assert len(spans) == 2
            span = spans.pop(0)
            assert span == parent
            assert span.is_finished() is False

        assert len(spans) == 1
        assert spans[0].is_finished() is True
        assert spans[0].get_name() == span_name
        assert spans[0].get_parent() == parent

    @pytest.mark.parametrize('http_op, http_span_name, http_opts, query, extra', [
        ('query', 'cb.query', QueryOptions, 'Select 1', None),
        ('analytics_query', 'cb.analytics', AnalyticsOptions, "Select 1", None),
        ('search_query', 'cb.search', SearchOptions, 'whatever', TermQuery('foo')),
        ('view_query', 'cb.views', ViewOptions, 'whatever', 'whatever_else')
    ])
    @pytest.mark.parametrize('http_with_parent', [True, False])
    @pytest.mark.usefixtures('skip_if_mock')
    def test_http(self, cb_env, http_op, http_span_name, http_opts, query, extra, http_with_parent):
        cb_env.tracer.reset()
        cb = cb_env.bucket if http_op == 'view_query' else cb_env.cluster
        parent = None
        if http_with_parent:
            parent = cb_env.tracer.start_span(f'parent_{http_op}')
        options = http_opts(span=parent)
        operation = getattr(cb, http_op)
        result = None
        try:
            if extra:
                result = operation(query, extra, options).rows()
            else:
                result = operation(query, options).rows()
            for r in result:
                assert r is not None
        except CouchbaseException:
            pass
        spans = cb_env.tracer.spans()
        if http_with_parent:
            assert len(spans) == 2
            span = spans.pop(0)
            assert span == parent
            assert span.is_finished() is False
        assert len(spans) == 1
        assert spans[0].is_finished() is True
        assert spans[0].get_name() == http_span_name
        assert spans[0].get_parent() == parent


class ClassicTracerTests(TracerTestsSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicTracerTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicTracerTests) if valid_test_method(meth)]
        compare = set(TracerTestsSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        # a new environment and cluster is created
        cb_env = TracingAndMetricsTestEnvironment.from_environment(cb_base_env,
                                                                   create_tracer=True)
        cb_env.setup(num_docs=10)
        yield cb_env
        cb_env.teardown()
        cb_env.cluster.close()
