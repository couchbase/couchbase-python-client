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

from couchbase.tests.base import ConnectionTestCase, SkipTest
import couchbase.exceptions as E


class DatastructureTest(ConnectionTestCase):
    def test_map(self):
        key = self.gen_key('dsmap')
        self.cb.remove(key, quiet=True)

        cb = self.cb
        self.assertRaises(E.NotFoundError, cb.map_add, key, 'key1', 'val1')
        rv = cb.map_add(key, 'key1', 'val1', create=True)
        self.assertTrue(rv.success)
        self.assertNotEquals(0, rv.cas)

        rv = cb.map_get(key, 'key1')
        self.assertEqual('val1', rv.value)

        self.assertEqual(1, cb.map_size(key))

        self.assertRaises(IndexError, cb.map_remove, key, 'key2')
        rv = cb.map_remove(key, 'key1')
        self.assertTrue(rv.success)
        self.assertEqual(0, cb.map_size(key))

    def test_list(self):
        key = self.gen_key('dslist')
        self.cb.remove(key, quiet=True)

        cb = self.cb

        self.assertRaises(E.NotFoundError, cb.list_append, key, 'hello')
        rv = self.cb.list_append(key, 'hello', create=True)
        self.assertTrue(rv.success)
        self.assertNotEquals(0, rv.cas)

        rv = cb.list_get(key, 0)
        self.assertEqual('hello', rv.value)

        rv = cb.list_prepend(key, 'before')
        self.assertTrue(rv.success)
        self.assertEqual('before', cb.list_get(key, 0).value)
        self.assertEqual('hello', cb.list_get(key, 1).value)
        self.assertEqual(2, cb.list_size(key))

        rv = cb.list_remove(key, 1)
        self.assertTrue(rv.success)
        self.assertEqual(['before'], cb.get(key).value)

        rv = cb.list_append(key, 'world')
        self.assertTrue(rv.success)
        self.assertEqual(['before', 'world'], cb.get(key).value)

        rv = cb.list_set(key, 1, 'after')
        self.assertTrue(rv.success)
        self.assertEqual(['before', 'after'], cb.get(key).value)

    def test_sets(self):
        key = self.gen_key('dsset')
        self.cb.remove(key, quiet=True)
        cb = self.cb

        self.assertRaises(E.NotFoundError, cb.set_add, key, 123)
        rv = cb.set_add(key, 123, create=True)
        self.assertTrue(rv.success)
        rv = cb.set_add(key, 123)
        self.assertFalse(rv)
        self.assertEqual(1, cb.set_size(key))
        self.assertTrue(cb.set_contains(key, 123))

        rv = cb.set_remove(key, 123)
        self.assertTrue(rv.success)
        self.assertEqual(0, cb.set_size(key))
        rv = cb.set_remove(key, 123)
        self.assertFalse(rv)
        self.assertFalse(cb.set_contains(key, 123))

    def test_queue(self):
        key = self.gen_key('a_queue')
        self.cb.remove(key, quiet=True)

        cb = self.cb
        self.assertRaises(E.NotFoundError, cb.queue_push, key, 1)
        rv = cb.queue_push(key, 1, create=True)
        self.assertTrue(rv.success)
        cb.queue_push(key, 2)
        cb.queue_push(key, 3)

        # Pop the items now
        self.assertEqual(1, cb.queue_pop(key).value)
        self.assertEqual(2, cb.queue_pop(key).value)
        self.assertEqual(3, cb.queue_pop(key).value)
        self.assertRaises(E.QueueEmpty, cb.queue_pop, key)


