# -*- coding:utf-8 -*-
#
# Copyright 2020, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from couchbase_tests.base import CollectionTestCase
from couchbase.exceptions import KVErrorContext, QueryErrorContext, SearchErrorContext, AnalyticsErrorContext, \
    ViewErrorContext, HTTPErrorContext, CouchbaseException, NotSupportedException
from couchbase.search import TermQuery
from unittest import SkipTest


class ErrorContextTests(CollectionTestCase):
    def setUp(self):
        super(ErrorContextTests, self).setUp()

    def test_kv_error(self):
        self.cb.upsert("foo", {"some":"content"})
        try:
            self.cb.insert("foo", {"some":"other content"})
            self.fail("expected an exception")
        except CouchbaseException as e:
            self.assertIsInstance(e.context, KVErrorContext)

    def test_query_error(self):
        try:
            self.cluster.query("I'm not n1ql").rows()
            self.fail("expected an exception")
        except NotSupportedException:
            raise SkipTest("query not supported in this cluster")
        except CouchbaseException as e:
            self.assertIsInstance(e.context, QueryErrorContext)

    def test_analytics_error(self):
        try:
            self.cluster.analytics_query("notanindex", "I'm also not n1ql").rows()
            self.fail("expected an exception")
        except NotSupportedException:
            raise SkipTest("analytics not supported in this cluster")
        except CouchbaseException as e:
            self.assertIsInstance(e.context, AnalyticsErrorContext)

    def test_search_error(self):
        try:
            for x in self.cluster.search_query("not_an_index", TermQuery("whatever")):
                pass
            self.fail("expected an exception")
        except NotSupportedException:
            raise SkipTest("search not supported in this cluster")
        except CouchbaseException as e:
            self.assertIsInstance(e.context, SearchErrorContext)

    def test_view_error(self):
        try:
            for x in self.cb.view_query("notadesigndoc", "notaview"):
                pass
            self.fail("expected an exception")
        except NotSupportedException:
            raise SkipTest("Views not supported in this cluster")
        except CouchbaseException as e:
            self.assertIsInstance(e.context, ViewErrorContext)

    def test_http_error(self):
        if self.is_mock:
            raise SkipTest('admin interface not supported with mock')
        try:
            self.cluster.buckets().get_bucket("imnotabucket")
            self.fail("expected an exception")
        except CouchbaseException as e:
            self.assertIsInstance(e.context, HTTPErrorContext)
