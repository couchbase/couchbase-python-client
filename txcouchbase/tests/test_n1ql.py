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

from twisted.internet import defer

from txcouchbase.bucket import BatchedN1QLRequest
from couchbase_core.asynchronous.n1ql import AsyncN1QLRequest

from couchbase_tests.base import MockTestCase
from txcouchbase.tests.base import gen_base
import logging
from txcouchbase.bucket import TxBucket


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


Base = gen_base(MockTestCase)


class TxN1QLTests(Base):
    @property
    def factory(self):
        return self.gen_bucket

    def testIncremental(self):
        cb = self.make_connection()
        d = defer.Deferred()
        o = cb.query_ex(RowsHandler, 'SELECT mockrow')
        self.assertIsInstance(o, RowsHandler)

        def verify(*args):
            self.assertEqual(len(o.rows), 1)
            self.assertTrue(o.done_called)

        o.deferred = d
        d.addCallback(verify)
        return d

    def testBatched(self):
        cb = self.make_connection()
        d = cb.query('SELECT mockrow')

        def verify(o):
            logging.error("Called back")

            self.assertIsInstance(o, BatchedN1QLRequest)
            rows = [r for r in o]
            self.assertEqual(1, len(rows))
            logging.error("End of callback")


        result= d.addCallback(verify)
        logging.error("ready to return")
        return result

    def testEmpty(self):
        cb = self.make_connection()
        d = cb.query('SELECT emptyrow')

        def verify(o):
            self.assertIsInstance(o, BatchedN1QLRequest)
            rows = [r for r in o]
            self.assertEqual(0, len(rows))
        return d.addCallback(verify)
