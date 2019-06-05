#
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

from __future__ import print_function

from couchbase_v2.exceptions import HTTPError
from couchbase_tests.base import MockTestCase
from couchbase_v2.n1ql import N1QLQuery
import json


class N1QLTest(MockTestCase):
    def test_onerow(self):
        row = self.cb.n1ql_query('SELECT mockrow').get_single_result()
        self.assertEqual('value', row['row'])

    def test_emptyrow(self):
        rv = self.cb.n1ql_query('SELECT emptyrow').get_single_result()
        self.assertEqual(None, rv)

    def test_meta(self):
        q = self.cb.n1ql_query('SELECT mockrow',meta_lookahead=False)
        self.assertRaises(RuntimeError, getattr, q, 'meta')
        q.execute()
        self.assertIsInstance(q.meta, dict)

    def test_metrics_switch(self):
        q = N1QLQuery('SELECT mockrow')
        q.metrics = False
        self.assertFalse(json.loads(q.encoded)['metrics'])
        q.metrics = True
        self.assertTrue(json.loads(q.encoded)['metrics'])

    def test_meta_lookahead(self):
        q = self.cb.n1ql_query('SELECT mockrow',meta_lookahead=True)
        self.assertIsInstance(q.meta, dict)

    def test_httperror_str(self):
        q = self.cb.n1ql_query('CREATE INDEX abc#123 ON abc (col_1)')

        with self.assertRaises(HTTPError) as c:
            q.execute()

        self.assertIn('0x3B', str(c.exception))
        ok, failed = c.exception.split_results()
        self.assertTrue(':nokey:' in k for k in failed.keys())

    def test_profile(self):
        query = N1QLQuery('SELECT 1')

        # default should be 'off'
        self.assertEqual('off', query.profile)

        # test setting each possible value
        query.profile = 'phases'
        self.assertEqual('phases', query.profile)
        query.profile = 'timings'
        self.assertEqual('timings', query.profile)
        query.profile = 'off'
        self.assertEqual('off', query.profile)

        # should raise error for unknown profile
        self.assertRaises(TypeError, query.profile, 'unknown')
