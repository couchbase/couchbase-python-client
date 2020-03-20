# Copyright 2015, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
from typing import NamedTuple

from twisted.internet import defer
from twisted.trial._synctest import SkipTest

import couchbase_core.fulltext as SEARCH
from couchbase_core.asynchronous.n1ql import AsyncN1QLRequest
from couchbase_tests.base import ConnectionTestCase, AnalyticsTestCaseBase
from txcouchbase.bucket import BatchedQueryResult, BatchedSearchResult, BatchedAnalyticsResult
from txcouchbase.tests.base import gen_base


class RowsHandler(AsyncN1QLRequest):
    def __init__(self, *args, **kwargs):
        super(RowsHandler, self).__init__(*args, **kwargs)
        self.rows = []
        self.done_called = False
        self.deferred = None
        self.cached_error = None

    def on_rows(self, rowiter):
        self.rows = list(rowiter)

    def on_done(self):
        assert not self.done_called
        self.done_called = True
        self.deferred.callback(None)

    def on_error(self, ex):
        self.cached_error = ex
        self.deferred.errback(ex)


Base = gen_base(ConnectionTestCase)


QueryParams = NamedTuple('QueryParams', [('statement', str), ('rowcount', int)])


class TxN1QLTests(Base):
    def setUp(self, *args, **kwargs):
        super(TxN1QLTests, self).setUp(*args, **kwargs)
        self.query_props = QueryParams('SELECT mockrow', 1) if self.is_mock else \
            QueryParams("SELECT * FROM `beer-sample` LIMIT 2", 2)  # type: QueryParams
        self.empty_query_props = QueryParams('SELECT emptyrow', 0) if self.is_mock else \
            QueryParams("SELECT * FROM `beer-sample` LIMIT 0", 0)

    @property
    def factory(self):
        return self.gen_cluster

    def testIncremental(self):
        cb = self.make_connection()
        d = defer.Deferred()
        o = cb.query_ex(RowsHandler, self.query_props.statement)
        self.assertIsInstance(o, RowsHandler)

        def verify(*args):
            self.assertEqual(len(o.rows), 1)
            self.assertTrue(o.done_called)

        o.deferred = d
        d.addCallback(verify)
        return d

    def testBatched(self  # type: Base
                    ):
        cb = self.make_connection()
        d = cb.query(self.query_props.statement)

        def verify(o):
            logging.error("Called back")

            self.assertIsInstance(o, BatchedQueryResult)
            rows = [r for r in o]
            self.assertEqual(self.query_props.rowcount, len(rows))
            logging.error("End of callback")

        result= d.addCallback(verify)
        logging.error("ready to return")
        return result

    def testBatchedSearch(self  # type: Base
                    ):
        if self.is_mock:
            raise SkipTest("No analytics on mock")
        cb = self.make_connection()
        d = cb.search_query("beer-search", SEARCH.TermQuery("category"),
                                      facets={'fred': SEARCH.TermFacet('category', 10)})

        def verify(o):
            logging.error("Called back")

            self.assertIsInstance(o, BatchedSearchResult)
            rows = [r for r in o]
            self.assertEqual(10, len(rows))
            logging.error("End of callback")

        result = d.addCallback(verify)
        logging.error("ready to return")
        return result

    def testEmpty(self):
        cb = self.make_connection()
        d = cb.query(self.empty_query_props.statement)

        def verify(o):
            self.assertIsInstance(o, BatchedQueryResult)
            rows = [r for r in o]
            self.assertEqual(0, len(rows))
        d.addCallback(verify)
        return d


class AnalyticsTest(gen_base(AnalyticsTestCaseBase)):
    @property
    def factory(self):
        return self.gen_cluster

    def testBatchedAnalytics(self  # type: Base
                             ):
        if self.is_mock:
            raise SkipTest("No analytics on mock")
        cb = self.make_connection()
        d = cb.analytics_query("SELECT * FROM `{}` LIMIT 1".format(self.dataset_name))

        def verify(o):
            logging.error("Called back")

            self.assertIsInstance(o, BatchedAnalyticsResult)
            self.assertEqual(1, len(o.rows()))
            logging.error("End of callback")

        result = d.addCallback(verify)
        logging.error("ready to return")
        return result