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
from couchbase.n1ql import MutationState


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

    def test_encode_scanvec(self):
        # The value is a vbucket's sequence number,
        # and guard is a vbucket's UUID.

        q = N1QLQuery('SELECT * FROM default')
        ms = MutationState()
        ms._add_scanvec((42, 3004, 3, 'default'))
        q.consistent_with(ms)

        dval = json.loads(q.encoded)
        sv_exp = {
            'default': {'42': [3, '3004']}
        }

        self.assertEqual('at_plus', dval['scan_consistency'])
        self.assertEqual(sv_exp, dval['scan_vectors'])

        # Ensure the vb field gets updated. No duplicates!
        ms._add_scanvec((42, 3004, 4, 'default'))
        sv_exp['default']['42'] = [4, '3004']
        dval = json.loads(q.encoded)
        self.assertEqual(sv_exp, dval['scan_vectors'])

        ms._add_scanvec((91, 7779, 23, 'default'))
        dval = json.loads(q.encoded)
        sv_exp['default']['91'] = [23, '7779']
        self.assertEqual(sv_exp, dval['scan_vectors'])

        # Try with a second bucket
        sv_exp['other'] = {'666': [99, '5551212']}
        ms._add_scanvec((666, 5551212, 99, 'other'))
        dval = json.loads(q.encoded)
        self.assertEqual(sv_exp, dval['scan_vectors'])

    def test_timeout(self):
        q = N1QLQuery('SELECT foo')
        q.timeout = 3.75
        self.assertEqual('3.75s', q._body['timeout'])
        self.assertEqual(3.75, q.timeout)

        def setfn():
            q.timeout = "blah"

        self.assertRaises(ValueError, setfn)

        # Unset the timeout
        q.timeout = 0
        self.assertFalse('timeout' in q._body)