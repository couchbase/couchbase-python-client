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
from couchbase.tracing import CouchbaseSpan, CouchbaseTracer

from ._test_utils import KVPair, TestEnvironment


class TestSpan(CouchbaseSpan):
    def __init__(self, name):
        super().__init__(None)
        self.finished_ = False
        self.name_ = name
        self.attributes_ = dict()
        self.parent_ = None
        self._span = None

    def set_attribute(self, key, value):
        self.attributes_[key] = value

    def set_parent(self, parent):
        self.parent_ = parent

    def get_parent(self):
        return self.parent_

    def finish(self):
        self.finished_ = True

    def is_finished(self):
        return self.finished_

    def get_attributes(self):
        return self.attributes_

    def get_name(self):
        return self.name_


class TestTracer(CouchbaseTracer):
    def __init__(self):
        self.spans_ = list()

    def start_span(self, name, parent=None, **kwargs):
        span = TestSpan(name)
        span.set_parent(parent)
        self.spans_.append(span)
        return span

    def reset(self):
        self.spans_ = list()

    def spans(self):
        return self.spans_


class TracerTests:
    TRACER = TestTracer()

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_buckets=True,
                                                 tracer=self.TRACER)
        cb_env.try_n_times(3, 5, cb_env.load_data)
        self.TRACER.reset()
        yield cb_env
        cb_env.try_n_times(3, 5, cb_env.purge_data)
        self.TRACER.reset()

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest.fixture(name="skip_if_mock")
    def skip_if_mock(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("Test needs real server")

    @pytest.mark.parametrize("op, span_name, opts, value", [
        ("get", "cb.get", GetOptions, None),
        ("upsert", "cb.upsert", UpsertOptions, {"some": "thing"}),
        ("insert", "cb.insert", InsertOptions, {"some": "thing"}),
        ("replace", "cb.replace", ReplaceOptions, {"some": "thing"}),
        ("remove", "cb.remove", RemoveOptions, None),
    ])
    @pytest.mark.parametrize("with_parent", [True, False])
    def test_kv(self, cb_env, default_kvp, op, span_name, opts, value, with_parent):
        # @TODO(): Pending CXXCBC-211 as recent changes do not allow for the parent_span to be passed in as an option
        if with_parent is True and op in ['upsert', 'insert', 'replace', 'remove']:
            pytest.skip("Pending CXXCBC-211")
        # have to reset between parameterized runs
        self.TRACER.reset()
        cb = cb_env.collection
        parent = None
        if with_parent:
            parent = self.TRACER.start_span(f'parent_{op}')
        key = default_kvp.key
        options = opts(span=parent)
        operation = getattr(cb, op)
        try:
            if value:
                operation(key, value, options)
            else:
                operation(key, options)
        except CouchbaseException:
            pass  # insert will fail, who cares.

        spans = self.TRACER.spans()
        if with_parent:
            assert len(spans) == 2
            span = spans.pop(0)
            assert span == parent
            assert span.is_finished() is False

        assert len(spans) == 1
        assert spans[0].is_finished() is True
        assert spans[0].get_name() == span_name
        assert spans[0].get_parent() == parent

    @pytest.mark.parametrize("http_op, http_span_name, http_opts, query, extra", [
        ("query", "cb.query", QueryOptions, "Select 1", None),
        ("analytics_query", "cb.analytics", AnalyticsOptions, "Select 1", None),
        ("search_query", "cb.search", SearchOptions, "whatever", TermQuery("foo")),
        ("view_query", "cb.views", ViewOptions, "whatever", "whatever_else")
    ])
    @pytest.mark.parametrize("http_with_parent", [True, False])
    @pytest.mark.usefixtures("skip_if_mock")
    def test_http(self, cb_env, http_op, http_span_name, http_opts, query, extra, http_with_parent):
        self.TRACER.reset()
        cb = cb_env.bucket if http_op == "view_query" else cb_env.cluster
        parent = None
        if http_with_parent:
            parent = self.TRACER.start_span(f'parent_{http_op}')
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
        spans = self.TRACER.spans()
        if http_with_parent:
            assert len(spans) == 2
            span = spans.pop(0)
            assert span == parent
            assert span.is_finished() is False
        assert len(spans) == 1
        assert spans[0].is_finished() is True
        assert spans[0].get_name() == http_span_name
        assert spans[0].get_parent() == parent
