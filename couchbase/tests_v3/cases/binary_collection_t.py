#
# Copyright 2019, Couchbase, Inc.
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
from couchbase_tests.base import CollectionTestCase, SkipTest
from couchbase_core._libcouchbase import FMT_UTF8, FMT_BYTES
from couchbase.collection import IncrementOptions, DecrementOptions, DeltaValue, SignedInt64

from couchbase.exceptions import InvalidArgumentException, NotStoredException


class BinaryCollectionTests(CollectionTestCase):
    def setUp(self):
        super(BinaryCollectionTests, self).setUp()
        self.UTF8_KEY = self.gen_key("binary_collection_tests")
        self.coll.upsert(self.UTF8_KEY, "", format=FMT_UTF8)
        self.try_n_times(10, 1, self.coll.get, self.UTF8_KEY)
        self.BYTES_KEY = self.gen_key("binary_collection_tests")
        self.coll.upsert(self.BYTES_KEY, b'', format=FMT_BYTES)
        self.COUNTER_KEY = self.gen_key("binary_collection_tests")
        self.try_n_times(10, 1, self.coll.get, self.BYTES_KEY)
        try:
            self.coll.remove(self.COUNTER_KEY)
        except:
            pass
        self.try_n_times_till_exception(10, 1, self.coll.get, self.COUNTER_KEY)

    def tearDown(self):
        try:
            self.coll.remove(self.key)
        except:
            pass
        super(BinaryCollectionTests, self).tearDown()

    def test_append_string_nokey(self):
        self.coll.remove(self.UTF8_KEY)
        self.try_n_times_till_exception(10, 1, self.coll.get, self.UTF8_KEY)
        self.assertRaises(NotStoredException, self.coll.binary().append, self.UTF8_KEY, "foo")

    def test_prepend_string_nokey(self):
        self.coll.remove(self.UTF8_KEY)
        self.try_n_times_till_exception(10, 1, self.coll.get, self.UTF8_KEY)
        self.assertRaises(NotStoredException, self.coll.binary().prepend, self.UTF8_KEY, "foo")

    def test_append_string(self):
        result = self.coll.binary().append(self.UTF8_KEY, "foo")
        self.assertIsNotNone(result.cas)
        # make sure it really worked
        result = self.coll.get(self.UTF8_KEY).content_as[str]
        self.assertEqual("foo", result)

    def test_prepend_string(self):
        result = self.coll.binary().prepend(self.UTF8_KEY, "foo")
        self.assertIsNotNone(result.cas)
        self.assertEqual("foo", self.coll.get(self.UTF8_KEY).content_as[str])

    def test_append_string_not_empty(self):
        self.coll.upsert(self.UTF8_KEY, "XXXX", format=FMT_UTF8)
        self.assertIsNotNone(self.coll.binary().append(self.UTF8_KEY, "foo", format=FMT_UTF8).cas)
        self.assertEqual("XXXXfoo", self.coll.get(self.UTF8_KEY).content_as[str])

    def test_prepend_string_not_empty(self):
        self.coll.upsert(self.UTF8_KEY, "XXXX", format=FMT_UTF8)
        self.assertIsNotNone(self.coll.binary().prepend(self.UTF8_KEY, "foo").cas)
        self.assertEqual("fooXXXX", self.coll.get(self.UTF8_KEY).content_as[str])

    def test_prepend_bytes(self):
        result = self.coll.binary().prepend(self.BYTES_KEY, b'XXX', format=FMT_BYTES)
        self.assertIsNotNone(result.cas)
        result = self.coll.get(self.BYTES_KEY).content_as[bytes]
        self.assertEqual(b'XXX', result)

    def test_append_bytes(self):
        self.assertIsNotNone(self.coll.binary().append(self.BYTES_KEY, b'XXX', format=FMT_BYTES).cas)
        self.assertEqual(b'XXX', self.coll.get(self.BYTES_KEY).content_as[bytes])

    def test_prepend_bytes_not_empty(self):
        self.coll.upsert(self.BYTES_KEY, b'XXX', format=FMT_BYTES)
        self.assertIsNotNone(self.coll.binary().prepend(self.BYTES_KEY, b'foo', format=FMT_BYTES).cas)
        self.assertEqual(b'fooXXX', self.coll.get(self.BYTES_KEY).content_as[bytes])

    def test_append_bytes_not_empty(self):
        self.coll.upsert(self.BYTES_KEY, b'XXX', format=FMT_BYTES)
        self.assertIsNotNone(self.coll.binary().append(self.BYTES_KEY, b'foo', format=FMT_BYTES).cas)
        self.assertEqual(b'XXXfoo', self.coll.get(self.BYTES_KEY).content_as[bytes])

    def test_counter_increment_default_no_key(self):
        result = self.coll.binary().increment(self.COUNTER_KEY, IncrementOptions(initial=SignedInt64(100)))
        self.assertTrue(result.success)
        self.assertEqual(100, self.coll.get(self.COUNTER_KEY).content_as[int])

    def test_counter_decrement_default_no_key(self):
        result = self.coll.binary().decrement(self.COUNTER_KEY, DecrementOptions(initial=SignedInt64(100)))
        self.assertTrue(result.success)
        self.assertEqual(100, self.coll.get(self.COUNTER_KEY).content_as[int])

    def test_counter_increment(self):
        self.coll.upsert(self.COUNTER_KEY, 100)
        self.assertTrue(self.coll.binary().increment(self.COUNTER_KEY).success)
        self.assertEqual(101, self.coll.get(self.COUNTER_KEY).content_as[int])

    def test_counter_decrement(self):
        self.coll.upsert(self.COUNTER_KEY, 100)
        self.assertTrue(self.coll.binary().decrement(self.COUNTER_KEY).success)
        self.assertEqual(99, self.coll.get(self.COUNTER_KEY).content_as[int])

    def test_counter_increment_non_default(self):
        self.coll.upsert(self.COUNTER_KEY, 100)
        self.assertTrue(self.coll.binary().increment(self.COUNTER_KEY, IncrementOptions(delta=DeltaValue(3))).success)
        self.assertEqual(103, self.coll.get(self.COUNTER_KEY).content_as[int])

    def test_counter_decrement_non_default(self):
        self.coll.upsert(self.COUNTER_KEY, 100)
        self.assertTrue(self.coll.binary().decrement(self.COUNTER_KEY, DecrementOptions(delta=DeltaValue(3))).success)
        self.assertEqual(97, self.coll.get(self.COUNTER_KEY).content_as[int])

    def test_unsigned_int(self):
        self.assertRaises(InvalidArgumentException, DeltaValue, -1)
        self.assertRaises(InvalidArgumentException, DeltaValue, 0x7FFFFFFFFFFFFFFF + 1)
        self.assertEqual(5, DeltaValue(5).value)

    def test_signed_int_64(self):
        self.assertRaises(InvalidArgumentException, SignedInt64, -0x7FFFFFFFFFFFFFFF - 2)
        self.assertRaises(InvalidArgumentException, SignedInt64, 0x7FFFFFFFFFFFFFFF + 1)
        x = SignedInt64(0x7FFFFFFFFFFFFFFF)
        self.assertEqual(0x7FFFFFFFFFFFFFFF, x.value)
        x = SignedInt64(-0x7FFFFFFFFFFFFFFF-1)
        self.assertEqual(-0x7FFFFFFFFFFFFFFF-1, x.value)


