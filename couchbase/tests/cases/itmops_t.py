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

from couchbase.tests.base import ConnectionTestCase
from couchbase.items import Item, ItemSequence, ItemOptionDict
from couchbase.exceptions import (
    NotFoundError, ValueFormatError, ArgumentError, KeyExistsError)
from couchbase.user_constants import FMT_BYTES, FMT_UTF8

class ConnectionItemTest(ConnectionTestCase):
    """
    This class tests the new 'Item' API
    """

    def setUp(self):
        super(ConnectionItemTest, self).setUp()
        self.skipIfPyPy()

    def test_construction(self):
        # Test whether we can construct a simple Item
        it = Item("some_key", "some_value")
        it.cas = 123456
        it.flags = 1000

        self.assertEqual(it.key, "some_key")
        self.assertEqual(it.value, "some_value")
        self.assertEqual(it.cas, 123456)
        self.assertEqual(it.flags, 1000)
        hash(it)

    def test_simple_get(self):
        k = self.gen_key("itm_simple_get")
        it = Item(k, "simple_value")

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
        it = Item(k, {})
        itcoll = ItemOptionDict()
        itcoll.dict[it] = { "format" : FMT_BYTES }
        self.assertRaises(ValueFormatError, self.cb.set_multi, itcoll)

    def test_items_append(self):
        k = self.gen_key("itm_append")
        it = Item(k, "MIDDLE")
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

    def test_items_ignorecas(self):
        k = self.gen_key("itm_ignorecas")
        it = Item(k, "a value")
        itcoll = ItemOptionDict()
        itcoll.add(it)
        self.cb.set_multi(itcoll)
        self.assertTrue(it.cas)

        # Set it again
        rv = self.cb.set(it.key, it.value)
        self.assertTrue(rv.cas)
        self.assertFalse(rv.cas == it.cas)

        # Should raise an error without ignore_cas
        self.assertRaises(KeyExistsError, self.cb.set_multi, itcoll)
        self.assertTrue(it.cas)

        itcoll.add(it, ignore_cas=True)
        self.cb.set_multi(itcoll)
        rv = self.cb.get(it.key)
        self.assertEqual(rv.cas, it.cas)

    def test_subclass_descriptors(self):
        class MyItem(Item):
            def __init__(self):
                pass
            @property
            def value(self):
                return "This should not be present!!!"
            @value.setter
            def value(self, other):
                return

        k = self.gen_key("itm_desc")
        it = MyItem()
        it.key = k
        it.value = "hi!"
        self.assertRaises(ArgumentError,
                          self.cb.set_multi,
                          ItemSequence([it]))

    def test_apiwrap(self):
        it = Item(self.gen_key("item_apiwrap"))
        self.cb.set_multi(it.as_itcoll())
        self.assertTrue(it.cas)

        # Set with 'ignorecas'
        it.cas = 1234
        self.cb.set_multi(it.as_itcoll(ignore_cas=True))

        self.cb.set_multi(ItemSequence(it))
