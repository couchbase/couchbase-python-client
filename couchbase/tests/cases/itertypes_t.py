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
from couchbase.exceptions import ArgumentError, ValueFormatError
from couchbase.user_constants import FMT_UTF8

class ItertypeTest(ConnectionTestCase):

    def test_itertypes(self):
        kvs = self.gen_kv_dict(amount=10, prefix='itertypes')
        intlist = set(self.gen_key_list(amount=3, prefix='setobject'))

        self.cb.remove_multi(kvs.keys(), quiet=True)
        self.cb.upsert_multi(kvs)
        self.cb.get_multi(kvs.keys())
        self.cb.get_multi(kvs.values(), quiet=True)

        self.cb.counter_multi(intlist, initial=10)
        self.cb.get_multi(intlist)

    def test_bad_elements(self):
        badlist = ("key1", None, "key2")
        for fn in (self.cb.counter_multi,
                   self.cb.delete_multi,
                   self.cb.get_multi):
            self.assertRaises(
                (ArgumentError, ValueFormatError),
                fn, badlist)

        self.assertRaises(
            (ArgumentError, ValueFormatError),
            self.cb.set_multi,
            { None: "value" })

        self.assertRaises(ValueFormatError,
                          self.cb.set_multi,
                          { "Value" : None},
                          format=FMT_UTF8)

    def test_iterclass(self):
        class IterTemp(object):
            def __init__(self, gen_ints = False, badlen=False):
                self.current = 0
                self.max = 5
                self.gen_ints = gen_ints
                self.badlen = badlen

            def __iter__(self):
                while self.current < self.max:
                    ret = self.current
                    if not self.gen_ints:
                        ret = "Key_" + str(ret)
                    self.current += 1
                    yield ret

            def __len__(self):
                if self.badlen:
                    return 100
                return self.max

        self.cb.remove_multi(IterTemp(gen_ints=False), quiet=True)
        self.cb.counter_multi(IterTemp(gen_ints = False), initial=10)
        self.cb.get_multi(IterTemp(gen_ints=False))
        self.cb.remove_multi(IterTemp(gen_ints = False))

        # Try with a mismatched len-iter
        self.assertRaises(ArgumentError,
                          self.cb.get_multi,
                          IterTemp(gen_ints=False, badlen=True))
