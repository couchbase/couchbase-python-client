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
from threading import Thread
import time


from couchbase_v2.exceptions import ObjectThreadException
from couchbase_tests.base import CouchbaseTestCase, CollectionTestCase
from couchbase.options import LockMode

class LockmodeTest(CollectionTestCase):
    def test_lockmode_defaults(self):
        # default is LOCKMODE_EXC
        key = self.gen_key("lockmode_defaults")
        cb = self.make_connection()
        self.assertEqual(cb.lockmode, LockMode.EXC)
        cb._thr_lockop(0)
        cb._thr_lockop(1)
        cb.upsert(key, "value")

        cb = self.make_connection(lockmode=LockMode.NONE)
        self.assertEqual(cb.lockmode, LockMode.NONE)

        self.assertRaises(ObjectThreadException,
                          cb._thr_lockop, 1)
        self.assertRaises(ObjectThreadException,
                          cb._thr_lockop, 0)
        cb.upsert(key, "value")

        cb = self.make_connection(lockmode=LockMode.WAIT)
        self.assertEqual(cb.lockmode, LockMode.WAIT)
        cb._thr_lockop(0)
        cb._thr_lockop(1)
        cb.upsert(key, "value")

        cb = self.make_connection(lockmode=LockMode.WAIT, unlock_gil=False)
        self.assertEqual(cb.lockmode, LockMode.NONE)
        cb.upsert(key, "value")

    def test_lockmode_exc(self):
        key = self.gen_key("lockmode_exc")

        cb = self.make_connection()
        cb._thr_lockop(0)
        self.assertRaises(ObjectThreadException,
                          cb.upsert,
                          key, "bar")
        cb._thr_lockop(1)

        # Ensure the old value is not buffered
        cb.upsert(key, "baz")
        self.assertEqual(cb.get(key).content, "baz")

    def test_lockmode_wait(self):
        key = self.gen_key("lockmode_wait")
        cb = self.make_connection(lockmode=LockMode.WAIT, unlock_gil=True)

        d = {
            'ended' : 0
        }

        def runfunc():
            cb.upsert(key, "value")
            d['ended'] = time.time()

        cb._thr_lockop(0)
        t = Thread(target=runfunc)
        t.start()

        time.sleep(0.5)
        time_unlocked = time.time()
        cb._thr_lockop(1)

        t.join()
        self.assertTrue(d['ended'] >= time_unlocked)
