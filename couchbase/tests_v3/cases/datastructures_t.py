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
from couchbase.collection import CBCollection
from couchbase_tests.base import CollectionTestCase
import couchbase.exceptions as E


class DatastructureTest(CollectionTestCase):
    def test_map(self  # type: DatastructureTest
                 ):
        key = self.gen_key('dsmap')
        self.cb.remove(key, quiet=True)

        cb = self.cb  # type: CBCollection
        self.assertRaises(E.DocumentNotFoundException, cb.map_add, key, 'key1', 'val1')
        rv = cb.map_add(key, 'key1', 'val1', create=True)
        self.assertSuccess(rv)
        self.assertCas(rv)

        rv = cb.map_get(key, 'key1')
        self.assertDsValue('val1', rv)

        self.assertEqual(1, cb.map_size(key))

        self.assertRaises(IndexError, cb.map_remove, key, 'key2')
        rv = cb.map_remove(key, 'key1')
        self.assertSuccess(rv)
        self.assertEqual(0, cb.map_size(key))

    def test_list(self):
        key = self.gen_key('dslist')
        self.cb.remove(key, quiet=True)

        cb = self.cb

        self.assertRaises(E.DocumentNotFoundException, cb.list_append, key, 'hello')
        rv = self.cb.list_append(key, 'hello', create=True)
        self.assertSuccess(rv)
        self.assertCas(rv)

        rv = cb.list_get(key, 0)
        self.assertDsValue('hello', rv)

        rv = cb.list_prepend(key, 'before')
        self.assertSuccess(rv)
        self.assertDsValue('before', cb.list_get(key, 0))
        self.assertDsValue('hello', cb.list_get(key, 1))
        self.assertEqual(2, cb.list_size(key))

        rv = cb.list_remove(key, 1)
        self.assertSuccess(rv)
        self.assertValue(['before'], cb.get(key))

        rv = cb.list_append(key, 'world')
        self.assertSuccess(rv)
        self.assertValue(['before', 'world'], cb.get(key))

        rv = cb.list_set(key, 1, 'after')
        self.assertSuccess(rv)
        self.assertValue(['before', 'after'], cb.get(key))

    def test_sets(self):
        key = self.gen_key('dsset')
        self.cb.remove(key, quiet=True)
        cb = self.cb

        self.assertRaises(E.DocumentNotFoundException, cb.set_add, key, 123)
        rv = cb.set_add(key, 123, create=True)
        self.assertSuccess(rv)
        rv = cb.set_add(key, 123)
        self.assertFalse(rv)
        self.assertEqual(1, cb.set_size(key))
        self.assertTrue(cb.set_contains(key, 123))

        rv = cb.set_remove(key, 123)
        self.assertSuccess(rv)
        self.assertEqual(0, cb.set_size(key))
        rv = cb.set_remove(key, 123)
        self.assertFalse(rv)
        self.assertFalse(cb.set_contains(key, 123))

    def test_queue(self):
        key = self.gen_key('a_queue')
        self.cb.remove(key, quiet=True)

        cb = self.cb
        self.assertRaises(E.DocumentNotFoundException, cb.queue_push, key, 1)
        rv = cb.queue_push(key, 1, create=True)
        self.assertSuccess(rv)
        cb.queue_push(key, 2)
        cb.queue_push(key, 3)

        # Pop the items now
        self.assertDsValue(1, cb.queue_pop(key))
        self.assertDsValue(2, cb.queue_pop(key))
        self.assertDsValue(3, cb.queue_pop(key))
        self.assertRaises(E.QueueEmpty, cb.queue_pop, key)


