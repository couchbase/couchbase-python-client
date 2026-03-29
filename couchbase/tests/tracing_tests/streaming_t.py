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

from couchbase.logic.observability import OpName
from couchbase.management.views import DesignDocumentNamespace
from couchbase.options import (AnalyticsOptions,
                               QueryOptions,
                               SearchOptions,
                               ViewOptions)
from couchbase.search import TermQuery
from tests.environments.tracing import StreamingTracingEnvironment
from tests.environments.tracing.base_tracing_environment import TracingType
from tests.test_features import EnvironmentFeatures


class StreamingTracingTestsSuite:

    TEST_MANIFEST = [
        'test_http_analytics_query_op',
        'test_http_query_op',
        'test_http_search_query_op',
        'test_http_view_query_op',
    ]

    @pytest.fixture(autouse=True)
    def reset_validator(self, cb_env: StreamingTracingEnvironment):
        yield
        cb_env.http_span_validator.reset()

    @pytest.fixture(scope='class')
    def check_analytics_supported(self, cb_env: StreamingTracingEnvironment):
        EnvironmentFeatures.check_if_feature_supported('analytics',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_query_supported(self, cb_env: StreamingTracingEnvironment):
        EnvironmentFeatures.check_if_feature_supported('query',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_search_supported(self, cb_env: StreamingTracingEnvironment):
        EnvironmentFeatures.check_if_feature_supported('search',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_views_supported(self, cb_env: StreamingTracingEnvironment):
        EnvironmentFeatures.check_if_feature_supported('views',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.mark.usefixtures('check_analytics_supported')
    def test_http_analytics_query_op(self, cb_env: StreamingTracingEnvironment) -> None:
        validator = cb_env.http_span_validator
        validator.reset(op_name=OpName.AnalyticsQuery, clear_statement=True)
        [r for r in cb_env.cluster.analytics_query('SELECT 1=1;').rows()]
        validator.validate_http_op()

        statement = 'SELECT $named_param as param '
        validator.reset(full_reset=False, statement=statement)
        [r for r in cb_env.cluster.analytics_query(statement, named_parameters={'named_param': 5}).rows()]
        validator.validate_http_op()

        statement = 'SELECT $1 as param '
        validator.reset(full_reset=False, statement=statement)
        [r for r in cb_env.cluster.analytics_query(statement, positional_parameters=['hello']).rows()]
        validator.validate_http_op()

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.AnalyticsQuery.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.AnalyticsQuery,
                        parent_span=parent_span,
                        clear_statement=True)
        [r for r in cb_env.cluster.analytics_query('SELECT 1=1;', AnalyticsOptions(span=parent_span)).rows()]
        validator.validate_http_op(end_parent=True)

        validator.reset(op_name=OpName.AnalyticsQuery, clear_statement=True, validate_error=True)
        try:
            [r for r in cb_env.cluster.analytics_query('This is not SQL++;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

        if not EnvironmentFeatures.is_feature_supported('collections',
                                                        cb_env.server_version_short,
                                                        cb_env.mock_server_type):
            return  # skip rest of test if collections not supported

        # dunno why we have this on analytics_query...buuuut we do...
        validator.reset(op_name=OpName.AnalyticsQuery,
                        clear_statement=True,
                        validate_error=True,
                        bucket_name=cb_env.bucket.name,
                        scope_name=cb_env.scope.name)
        try:
            [r for r in cb_env.scope.analytics_query('SELECT 1=1;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.AnalyticsQuery.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.AnalyticsQuery,
                        parent_span=parent_span,
                        clear_statement=True,
                        bucket_name=cb_env.bucket.name,
                        scope_name=cb_env.scope.name)
        try:
            [r for r in cb_env.scope.analytics_query('SELECT 1=1;', AnalyticsOptions(span=parent_span)).rows()]
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

    @pytest.mark.usefixtures('check_query_supported')
    def test_http_query_op(self, cb_env: StreamingTracingEnvironment) -> None:
        validator = cb_env.http_span_validator
        validator.reset(op_name=OpName.Query, clear_statement=True)
        [r for r in cb_env.cluster.query('SELECT 1=1;').rows()]
        validator.validate_http_op()

        statement = 'SELECT $named_param as param '
        validator.reset(full_reset=False, statement=statement)
        [r for r in cb_env.cluster.query(statement, named_parameters={'named_param': 5}).rows()]
        validator.validate_http_op()

        statement = 'SELECT $1 as param '
        validator.reset(full_reset=False, statement=statement)
        [r for r in cb_env.cluster.query(statement, positional_parameters=['hello']).rows()]
        validator.validate_http_op()

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.Query.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.Query,
                        parent_span=parent_span,
                        clear_statement=True)
        [r for r in cb_env.cluster.query('SELECT 1=1;', QueryOptions(span=parent_span)).rows()]
        validator.validate_http_op(end_parent=True)

        validator.reset(op_name=OpName.Query, clear_statement=True, validate_error=True)
        try:
            [r for r in cb_env.cluster.query('This is not SQL++;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

        if not EnvironmentFeatures.is_feature_supported('collections',
                                                        cb_env.server_version_short,
                                                        cb_env.mock_server_type):
            return  # skip rest of test if collections not supported

        validator = cb_env.http_span_validator
        validator.reset(op_name=OpName.Query,
                        clear_statement=True,
                        bucket_name=cb_env.bucket.name,
                        scope_name=cb_env.scope.name)
        [r for r in cb_env.scope.query('SELECT 1=1;').rows()]
        validator.validate_http_op()

        statement = 'SELECT $named_param as param '
        validator.reset(full_reset=False,
                        statement=statement,
                        bucket_name=cb_env.bucket.name,
                        scope_name=cb_env.scope.name)
        [r for r in cb_env.scope.query(statement, named_parameters={'named_param': 5}).rows()]
        validator.validate_http_op()

        statement = 'SELECT $1 as param '
        validator.reset(full_reset=False,
                        statement=statement,
                        bucket_name=cb_env.bucket.name,
                        scope_name=cb_env.scope.name)
        [r for r in cb_env.scope.query(statement, positional_parameters=['hello']).rows()]
        validator.validate_http_op()

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.Query.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.Query,
                        parent_span=parent_span,
                        clear_statement=True,
                        bucket_name=cb_env.bucket.name,
                        scope_name=cb_env.scope.name)
        [r for r in cb_env.scope.query('SELECT 1=1;', QueryOptions(span=parent_span)).rows()]
        validator.validate_http_op(end_parent=True)

        validator.reset(op_name=OpName.Query,
                        clear_statement=True,
                        validate_error=True,
                        bucket_name=cb_env.bucket.name,
                        scope_name=cb_env.scope.name)
        try:
            [r for r in cb_env.scope.query('This is not SQL++;').rows()]
        except Exception:
            pass
        validator.validate_http_op()

    @pytest.mark.usefixtures('check_search_supported')
    def test_http_search_query_op(self, cb_env: StreamingTracingEnvironment) -> None:
        # search is a ROYAL PITA to setup, only testing errors (which we still send to the server)
        validator = cb_env.http_span_validator
        validator.reset(op_name=OpName.SearchQuery, clear_statement=True, validate_error=True)
        try:
            [r for r in cb_env.cluster.search('not-an-index', TermQuery('auto')).rows()]
        except Exception:
            pass
        validator.validate_http_op()

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.SearchQuery.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.SearchQuery,
                        parent_span=parent_span,
                        clear_statement=True,
                        validate_error=True)
        try:
            [r for r in cb_env.cluster.search('not-an-index', TermQuery('auto'),
                                              SearchOptions(span=parent_span)).rows()]
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

        if not EnvironmentFeatures.is_feature_supported('collections',
                                                        cb_env.server_version_short,
                                                        cb_env.mock_server_type):
            return  # skip rest of test if collections not supported

        # TODO(PYCBC-1753): The bucket and scope name are not passed when doing scope-level search queries.
        #                   This means we cannot validate the span attributes have db.namespace and couchbase.scope.name
        validator.reset(op_name=OpName.SearchQuery, clear_statement=True, validate_error=True)
        try:
            [r for r in cb_env.scope.search('not-an-index', TermQuery('auto')).rows()]
        except Exception:
            pass
        validator.validate_http_op()

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.SearchQuery.value}_span')
        # TODO(PYCBC-1753): The bucket and scope name are not passed when doing scope-level search queries.
        #                   This means we cannot validate the span attributes have db.namespace and couchbase.scope.name
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.SearchQuery,
                        parent_span=parent_span,
                        clear_statement=True,
                        validate_error=True)
        try:
            [r for r in cb_env.scope.search('not-an-index', TermQuery('auto'), SearchOptions(span=parent_span)).rows()]
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)

    @pytest.mark.usefixtures('check_views_supported')
    def test_http_view_query_op(self, cb_env: StreamingTracingEnvironment) -> None:
        # views are deprecated, so only minimal testing
        validator = cb_env.http_span_validator
        validator.reset(op_name=OpName.ViewQuery,
                        clear_statement=True,
                        validate_error=True,
                        bucket_name=cb_env.bucket.name)
        view_result = cb_env.bucket.view_query('fake-ddoc',
                                               'fake-view',
                                               limit=10,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)
        try:
            [r for r in view_result]
        except Exception:
            pass
        validator.validate_http_op()

        cb_env.tracer.clear_spans()
        parent_span = cb_env.tracer.request_span(f'parent_{OpName.ViewQuery.value}_span')
        validator.reset(do_not_clear_spans=True,
                        op_name=OpName.ViewQuery,
                        parent_span=parent_span,
                        clear_statement=True,
                        validate_error=True,
                        bucket_name=cb_env.bucket.name)
        view_result = cb_env.bucket.view_query('fake-ddoc',
                                               'fake-view',
                                               ViewOptions(span=parent_span),
                                               limit=10,
                                               namespace=DesignDocumentNamespace.DEVELOPMENT)
        try:
            [r for r in view_result]
        except Exception:
            pass
        validator.validate_http_op(end_parent=True)


class ClassicStreamingTracingTests(StreamingTracingTestsSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicStreamingTracingTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicStreamingTracingTests) if valid_test_method(meth)]
        test_list = set(StreamingTracingTestsSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest not validated.  Missing/extra tests: {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.ThresholdLogging,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.Modern
                                                          ])
    def couchbase_test_environment(self, cb_base_env, request):
        # a new environment and cluster is created
        cb_env = StreamingTracingEnvironment.from_environment(cb_base_env, tracing_type=request.param)
        cb_env.setup(num_docs=50)
        yield cb_env
        cb_env.teardown()
        # TODO(PYCBC-1746): Update once legacy tracing logic is removed.
        #                   See PYCBC-1748 for details on issue, this seems to mainly impact the error scenarios.
        if request.param == TracingType.Legacy:
            cb_env.cluster.close()
