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

from couchbase.exceptions import (NotFoundError, DeltaBadvalError)
from couchbase.libcouchbase import Connection

from tests.base import CouchbaseTestCase


class ConnectionArithmeticTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionArithmeticTest, self).setUp()
        self.cb = self.make_connection()

    def test_trivial_incrdecr(self):
        self.cb.delete("key", quiet=True)
        rv_arith = self.cb.incr("key", initial=1)
        rv_get = self.cb.get("key")

        self.assertEqual(rv_arith.value, 1)
        self.assertEqual(int(rv_get.value), 1)

        rv = self.cb.incr("key")
        self.assertEquals(rv.value, 2)

        rv = self.cb.decr("key")
        self.assertEquals(rv.value, 1)
        self.assertEquals(int(self.cb.get("key").value), 1)

        rv = self.cb.decr("key")
        self.assertEquals(rv.value, 0)
        self.assertEquals(int(self.cb.get("key").value), 0)

    def test_incr_notfound(self):
        self.cb.delete("key", quiet=True)
        self.assertRaises(NotFoundError,
                          self.cb.incr, "key")

    def test_incr_badval(self):
        self.cb.set("key", "THIS IS SPARTA")
        self.assertRaises(DeltaBadvalError,
                          self.cb.incr, "key")

    def test_incr_multi(self):
        keys = []
        for x in range(5):
            keys.append("Key_" + str(x))

        self.cb.delete_multi(keys, quiet=True)
        self.cb.incr_multi(keys, initial=5)
        for k in keys:
            rv = self.cb.get(k)
            self.assertEquals(int(rv.value), 5)

        self.cb.delete(keys[0])

        self.assertRaises(NotFoundError,
                          self.cb.incr_multi, keys)

    def test_incr_extended(self):
        self.cb.delete("key", quiet=True)
        rv = self.cb.incr("key", initial=10)
        self.assertEquals(rv.value, 10)
        srv = self.cb.set("key", "42", cas=rv.cas)
        self.assertTrue(srv.success)

        # test with multiple values?
        klist = [ "key_" + str(x) for x in range(10) ]
        self.cb.delete_multi(klist, quiet=True)
        rvs = self.cb.incr_multi(klist, initial=40)
        [ self.assertEquals(x.value, 40) for x in rvs.values() ]
        self.assertEquals(sorted(list(rvs.keys())), sorted(klist))


if __name__ == '__main__':
    unittest.main()
