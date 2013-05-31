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

from time import sleep

from couchbase.exceptions import (
    CouchbaseError, TemporaryFailError, KeyExistsError)

from tests.base import ConnectionTestCase


class ConnectionLockTest(ConnectionTestCase):

    def test_simple_lock(self):
        k = self.gen_key('lock')
        v = "locked_value"
        self.cb.set(k, v)
        rv = self.cb.lock(k, ttl=5)

        self.assertTrue(rv.success)
        self.assertEqual(rv.value, v)
        self.assertRaises(KeyExistsError, self.cb.set, k, v)

        self.assertRaises(TemporaryFailError, self.cb.lock, k, ttl=5)

        # Test set-while-locked
        self.assertRaises(KeyExistsError, self.cb.set, k, v)

        self.assertRaises(TemporaryFailError, self.cb.unlock, k, cas=0xdeadbeef)

        rv = self.cb.unlock(k, rv.cas)
        self.assertTrue(rv.success)

        # Unlocked with key already unlocked
        self.assertRaises(TemporaryFailError,
                          self.cb.unlock,
                          k,
                          rv.cas)

        rv = self.cb.set(k, v)
        self.assertTrue(rv.success)

    def test_timed_lock(self):
        self.slowTest()

        k = self.gen_key('lock')
        v = "locked_value"
        self.cb.set(k, v)
        rv = self.cb.lock(k, ttl=1)
        sleep(2)
        self.cb.set(k, v)

    def test_multi_lock(self):
        kvs = self.gen_kv_dict(prefix='lock_multi')

        self.cb.set_multi(kvs)
        rvs = self.cb.lock_multi(kvs.keys(), ttl=5)
        self.assertTrue(rvs.all_ok)
        self.assertEqual(len(rvs), len(kvs))
        for k, v in rvs.items():
            self.assertEqual(v.value, kvs[k])

        rvs = self.cb.unlock_multi(rvs)

    def test_unlock_multi(self):
        key = self.gen_key(prefix='unlock_multi')
        val = "lock_value"
        self.cb.set(key, val)

        rv = self.cb.lock(key, ttl=5)
        rvs = self.cb.unlock_multi({key:rv.cas})
        self.assertTrue(rvs.all_ok)
        self.assertTrue(rvs[key].success)

        rv = self.cb.lock(key, ttl=5)
        self.assertTrue(rv.success)
        rvs = self.cb.unlock_multi({key:rv})
        self.assertTrue(rvs.all_ok)
        self.assertTrue(rvs[key].success)
