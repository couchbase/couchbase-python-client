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
import json

from couchbase.tests.base import CouchbaseTestCase
from couchbase.n1ql import N1QLQuery, CONSISTENCY_REQUEST, CONSISTENCY_NONE


class N1QLStringTest(CouchbaseTestCase):
    def setUp(self):
        super(N1QLStringTest, self).setUp()

    def test_encode_namedargs(self):
        qstr = 'SELECT * FROM default WHERE field1=$arg1 AND field2=$arg2'
        q = N1QLQuery(qstr, arg1='foo', arg2='bar')

        self.assertEqual(qstr, q.statement)

        dval = json.loads(q.encoded)
        self.assertEqual(qstr, dval['statement'])
        self.assertEqual('foo', dval['$arg1'])
        self.assertEqual('bar', dval['$arg2'])

    def test_encode_posargs(self):
        qstr = 'SELECT * FROM default WHERE field1=$1 AND field2=$arg2'
        q = N1QLQuery(qstr, 'foo', 'bar')
        dval = json.loads(q.encoded)
        self.assertEqual(qstr, dval['statement'])
        self.assertEqual('foo', dval['args'][0])
        self.assertEqual('bar', dval['args'][1])

    def test_encode_mixed_args(self):
        qstr = 'SELECT * FROM default WHERE field1=$1 AND field2=$arg2'
        q = N1QLQuery(qstr, 'foo', arg2='bar')
        dval = json.loads(q.encoded)
        self.assertEqual('bar', dval['$arg2'])
        self.assertEqual('foo', dval['args'][0])
        self.assertEqual(1, len(dval['args']))

    def test_encoded_consistency(self):
        qstr = 'SELECT * FROM default'
        q = N1QLQuery(qstr)
        q.consistency = CONSISTENCY_REQUEST
        dval = json.loads(q.encoded)
        self.assertEqual('request_plus', dval['scan_consistency'])

        q.consistency = CONSISTENCY_NONE
        dval = json.loads(q.encoded)
        self.assertEqual('none', dval['scan_consistency'])