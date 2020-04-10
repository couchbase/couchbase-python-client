#
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

from couchbase.exceptions import (DocumentNotFoundException, DeltaBadvalException)
from couchbase_tests.base import CollectionTestCase
from couchbase.collection import SignedInt64, DeltaValue

class ArithmeticTest(CollectionTestCase):
    def setUp(self):
        super(ArithmeticTest, self).setUp()
        self.bin_coll=self.cb.binary()
    def test_trivial_incrdecr(self):
        key = self.gen_key("trivial_incrdecr")
        self.cb.remove(key, quiet=True)
        rv_arith = self.bin_coll.increment(key, initial=SignedInt64(1), delta=DeltaValue(1))
        rv_get = self.cb.get(key)

        # self.assertEqual(rv_arith.content, 1) SDK3 MutationResult has no content
        self.assertEqual(int(rv_get.content), 1)

        rv = self.bin_coll.increment(key)
        # self.assertEqual(rv.content, 2)

        rv = self.bin_coll.decrement(key, delta=DeltaValue(1))
        # self.assertEqual(rv.content, 1)
        self.assertEqual(int(self.cb.get(key).content), 1)

        rv = self.bin_coll.decrement(key, delta=DeltaValue(1))
        # self.assertEqual(rv.content, 0)
        self.assertEqual(int(self.cb.get(key).content), 0)

    def test_incr_notfound(self):
        key = self.gen_key("incr_notfound")
        self.cb.remove(key, quiet=True)
        self.assertRaises(DocumentNotFoundException, self.bin_coll.increment, key)

    def test_incr_badval(self):
        key = self.gen_key("incr_badval")
        self.cb.upsert(key, "THIS IS SPARTA")
        self.assertRaises(DeltaBadvalException, self.bin_coll.increment, key)

    def test_incr_multi(self):
        keys = self.gen_key_list(amount=5, prefix="incr_multi")

        def _multi_lim_assert(expected):
            for k, v in self.cb.get_multi(keys).items():
                self.assertTrue(k in keys)
                self.assertEqual(v.content, expected)

        self.cb.remove_multi(keys, quiet=True)
        self.bin_coll.increment_multi(keys, initial=SignedInt64(5))
        _multi_lim_assert(5)

        self.bin_coll.increment_multi(keys)
        _multi_lim_assert(6)

        self.bin_coll.decrement_multi(keys, delta=DeltaValue(1))
        _multi_lim_assert(5)

        self.bin_coll.increment_multi(keys, delta=DeltaValue(10))
        _multi_lim_assert(15)

        self.bin_coll.decrement_multi(keys, delta=DeltaValue(6))
        _multi_lim_assert(9)

        self.cb.remove(keys[0])

        self.assertRaises(DocumentNotFoundException, self.bin_coll.increment_multi, keys)

    def test_incr_extended(self):
        key = self.gen_key("incr_extended")
        self.cb.remove(key, quiet=True)
        rv = self.bin_coll.increment(key, initial=SignedInt64(10))
        #self.assertEqual(rv.content, 10)
        srv = self.cb.upsert(key, "42", cas=rv.cas)
        self.assertTrue(srv.success)

        # test with multiple values?
        klist = self.gen_key_list(amount=5, prefix="incr_extended_list")
        try:
            self.bin_coll.increment_multi(klist)
        except DocumentNotFoundException:
            pass
        rvs = self.bin_coll.increment_multi(klist, initial=SignedInt64(40))
        # no content in MutationResults - perhaps this would be a good reason for it to have one?
        #[self.assertEqual(x.content, 40) for x in rvs.values()]

        self.assertEqual(sorted(list(rvs.keys())), sorted(klist))
