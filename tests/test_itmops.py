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

from tests.base import ConnectionTestCase
from couchbase.items import Item, ItemSequence, ItemOptionDict
from couchbase.exceptions import NotFoundError, ValueFormatError, ArgumentError
from couchbase.user_constants import FMT_BYTES, FMT_UTF8

class ConnectionItemTest(ConnectionTestCase):
    """
    This class tests the new 'Item' API
    """
    def test_construction(self):
        # Test whether we can construct a simple Item
        it = Item()
        it.key = "some_key"
        it.value = "some_value"
        it.cas = 123456
        it.flags = 1000

        self.assertEqual(it.key, "some_key")
        self.assertEqual(it.value, "some_value")
        self.assertEqual(it.cas, 123456)
        self.assertEqual(it.flags, 1000)
        hash(it)

    def test_simple_get(self):
        k = self.gen_key("itm_simple_get")

        it = Item()
        it.key = k
        it.value = "simple_value"

        rvs = self.cb.set_multi(ItemSequence([it]))
        self.assertTrue(rvs.all_ok)

        it_out = rvs[it.key]
        self.assertEqual(it_out, it)

        it = Item()
        it.key = k
        itcoll = ItemSequence([it])

        rvs = self.cb.get_multi(ItemSequence([it]))
        self.assertTrue(rvs.all_ok)
        it_out = rvs[it.key]
        self.assertEqual(it_out, it)
        self.assertEqual(it_out.value, "simple_value")

        # Now, set it again
        self.cb.replace_multi(itcoll)

        # Now, delete it
        self.cb.delete_multi(itcoll)

        self.assertRaises(NotFoundError,
                          self.cb.get_multi, itcoll)

    def test_item_format(self):
        # Tests whether things like 'CAS' and 'format' are honored
        k = self.gen_key("itm_format_options")
        it = Item()
        it.key = k
        it.value = {}
        itcoll = ItemOptionDict()
        itcoll.dict[it] = { "format" : FMT_BYTES }
        self.assertRaises(ValueFormatError, self.cb.set_multi, itcoll)

    def test_items_append(self):
        k = self.gen_key("itm_append")
        it = Item()
        it.key = k
        it.value = "MIDDLE"
        itcoll = ItemOptionDict()
        itcoll.add(it)

        self.cb.set_multi(itcoll, format=FMT_UTF8)

        itcoll.add(it, fragment="_END")
        self.cb.append_items(itcoll, format=FMT_UTF8)
        self.assertEqual(it.value, "MIDDLE_END")

        itcoll.add(it, fragment="BEGIN_")
        self.cb.prepend_items(itcoll, format=FMT_UTF8)
        self.assertEqual(it.value, "BEGIN_MIDDLE_END")

        rv = self.cb.get(it.key)
        self.assertEqual(rv.value, "BEGIN_MIDDLE_END")

        # Try without a 'fragment' specifier
        self.assertRaises(ArgumentError,
                          self.cb.append_items, ItemSequence([it]))
        itcoll.add(it)
        self.assertRaises(ArgumentError,
                          self.cb.append_items, itcoll)
