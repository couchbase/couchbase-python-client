#
# Copyright 2016, Couchbase, Inc.
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
import json


from couchbase.tests.base import ConnectionTestCase
from couchbase.connstr import ConnectionString
from couchbase._pyport import long
from couchbase.n1ql import MutationState


class MutationTokensTest(ConnectionTestCase):
    def make_connection(self, **overrides):
        no_mutinfo = overrides.pop('no_mutinfo', False)
        kwargs = self.make_connargs(**overrides)
        connstr = kwargs.get('connection_string', '')
        connstr = ConnectionString.parse(connstr)
        if not no_mutinfo:
            connstr.options['fetch_mutation_tokens'] = '1'
        kwargs['connection_string'] = connstr.encode()
        return super(MutationTokensTest, self).make_connection(**kwargs)

    def test_mutinfo_enabled(self):
        cb = self.cb
        key = self.gen_key('mutinfo')
        rv = cb.upsert(key, 'value')
        mutinfo = rv._mutinfo
        self.assertTrue(mutinfo)
        vb, uuid, seq, bktname = mutinfo
        self.assertIsInstance(vb, (int, long))
        self.assertIsInstance(uuid, (int, long))
        self.assertIsInstance(seq, (int, long))
        self.assertEqual(cb.bucket, bktname)

        # Get all the mutation tokens
        all_info = cb._mutinfo()
        self.assertTrue(all_info)
        self.assertEqual(1, len(all_info))
        vb, uuid, seq = all_info[0]
        self.assertIsInstance(vb, (int, long))
        self.assertIsInstance(uuid, (int, long))
        self.assertIsInstance(seq, (int, long))

    def test_mutinfo_disabled(self):
        cb = self.make_connection(no_mutinfo=True)
        key = self.gen_key('mutinfo')
        rv = cb.upsert(key, 'value')
        self.assertFalse(rv._mutinfo)
        self.assertEqual(0, len(cb._mutinfo()))

    def test_mutation_state(self):
        cb = self.cb
        key = self.gen_key('mutationState')
        rv = cb.upsert(key, 'value')

        d1 = json.loads(MutationState(rv).encode())
        ms = MutationState()
        ms.add_results(rv)
        d2 = json.loads(ms.encode())
        self.assertEqual(d1, d2)   # Ensure it's the same
        self.assertTrue(d1[cb.bucket])  # Ensure it's not empty

        vb, uuid, seq, _ = rv._mutinfo
        mt_got = d1[cb.bucket][str(vb)]
        self.assertEqual(seq, mt_got[0])
        self.assertEqual(str(uuid), mt_got[1])