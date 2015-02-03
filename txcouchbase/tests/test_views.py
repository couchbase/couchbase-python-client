# Copyright 2013, Couchbase, Inc.
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

from txcouchbase.bucket import BatchedView
from couchbase.exceptions import HTTPError, ArgumentError
from couchbase.async.view import AsyncViewBase

from couchbase.tests.base import ViewTestCase
from txcouchbase.tests.base import gen_base

class RowsHandler(AsyncViewBase):
    def __init__(self, *args, **kwargs):
        super(RowsHandler, self).__init__(*args, **kwargs)
        self._rows_received = 0
        self._done_called = False
        self._call_count = 0
        self._cached_ex = None

    def on_rows(self, rows):
        l = list(rows)
        self._rows_received += len(l)
        self._call_count += 1

    def on_done(self):
        self._done_called = True
        self._d.callback(None)

    def on_error(self, ex):
        self._cached_ex = ex
        self._d.errback(ex)


class TxViewsTests(gen_base(ViewTestCase)):
    def make_connection(self, **kwargs):
        return super(TxViewsTests, self).make_connection(bucket='beer-sample')

    def testEmptyView(self):
        cb = self.make_connection()
        return cb.queryAll('beer', 'brewery_beers', limit=0)

    def testLimitView(self):
        cb = self.make_connection()
        d = cb.queryAll('beer', 'brewery_beers', limit=10)

        def _verify(o):
            self.assertIsInstance(o, BatchedView)
            rows = list(o)
            self.assertEqual(len(rows), 10)

        return d.addCallback(_verify)

    def testBadView(self):
        cb = self.make_connection()
        d = cb.queryAll('blah', 'blah_blah')
        self.assertFailure(d, HTTPError)
        return d

    def testIncrementalRows(self):
        d = defer.Deferred()
        cb = self.make_connection()
        o = cb.queryEx(RowsHandler, 'beer', 'brewery_beers')
        self.assertIsInstance(o, RowsHandler)

        def verify(unused):
            self.assertTrue(o.indexed_rows > 7000)
            self.assertEqual(o._rows_received, o.indexed_rows)

            ## Commented because we can't really verify this now,
            ## can we?
            #self.assertTrue(o._call_count > 1)

        d.addCallback(verify)
        o._d = d
        return d
