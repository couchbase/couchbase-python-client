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

from couchbase_v2.exceptions import (NotFoundError, DeltaBadvalError)
from couchbase_tests.base import ConnectionTestCase


class ArithmeticTest(ConnectionTestCase):

    def test_trivial_incrdecr(self):
        key = self.gen_key("trivial_incrdecr")
        self.cb.remove(key, quiet=True)
        rv_arith = self.cb.counter(key, initial=1, delta=1)
        rv_get = self.cb.get(key)

        self.assertEqual(rv_arith.value, 1)
        self.assertEqual(int(rv_get.value), 1)

        rv = self.cb.counter(key)
        self.assertEqual(rv.value, 2)

        rv = self.cb.counter(key, delta=-1)
        self.assertEqual(rv.value, 1)
        self.assertEqual(int(self.cb.get(key).value), 1)

        rv = self.cb.counter(key, delta=-1)
        self.assertEqual(rv.value, 0)
        self.assertEqual(int(self.cb.get(key).value), 0)

    def test_incr_notfound(self):
        key = self.gen_key("incr_notfound")
        self.cb.remove(key, quiet=True)
        self.assertRaises(NotFoundError, self.cb.counter, key)

    def test_incr_badval(self):
        key = self.gen_key("incr_badval")
        self.cb.upsert(key, "THIS IS SPARTA")
        self.assertRaises(DeltaBadvalError, self.cb.counter, key)

    def test_incr_multi(self):
        keys = self.gen_key_list(amount=5, prefix="incr_multi")

        def _multi_lim_assert(expected):
            for k, v in self.cb.get_multi(keys).items():
                self.assertTrue(k in keys)
                self.assertEqual(v.value, expected)

        self.cb.remove_multi(keys, quiet=True)
        self.cb.counter_multi(keys, initial=5)
        _multi_lim_assert(5)

        self.cb.counter_multi(keys)
        _multi_lim_assert(6)

        self.cb.counter_multi(keys, delta=-1)
        _multi_lim_assert(5)

        self.cb.counter_multi(keys, delta=10)
        _multi_lim_assert(15)

        self.cb.counter_multi(keys, delta=-6)
        _multi_lim_assert(9)

        self.cb.remove(keys[0])

        self.assertRaises(NotFoundError, self.cb.counter_multi, keys)

    def test_incr_extended(self):
        key = self.gen_key("incr_extended")
        self.cb.remove(key, quiet=True)
        rv = self.cb.counter(key, initial=10)
        self.assertEqual(rv.value, 10)
        srv = self.cb.upsert(key, "42", cas=rv.cas)
        self.assertTrue(srv.success)

        # test with multiple values?
        klist = self.gen_key_list(amount=5, prefix="incr_extended_list")
        self.cb.remove_multi(klist, quiet=True)
        rvs = self.cb.counter_multi(klist, initial=40)
        [ self.assertEqual(x.value, 40) for x in rvs.values() ]
        self.assertEqual(sorted(list(rvs.keys())), sorted(klist))


if __name__ == '__main__':
    unittest.main()
