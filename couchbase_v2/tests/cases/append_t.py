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

from couchbase_core import FMT_BYTES, FMT_UTF8

from couchbase_v2.exceptions import (ValueFormatError,
                                     NotFoundError,
                                     NotStoredError, CouchbaseError)

from couchbase_tests.base import ConnectionTestCase


class AppendTest(ConnectionTestCase):

    def test_append_prepend(self):
        key = self.gen_key("appendprepend")
        vbase = "middle"
        self.cb.upsert(key, vbase, format=FMT_UTF8)
        self.cb.prepend(key, "begin ")
        self.cb.append(key, " end")
        self.assertEqual(self.cb.get(key).value,
                          "begin middle end")


    def test_append_binary(self):
        kname = self.gen_key("binary_append")
        initial = b'\x10'
        self.cb.upsert(kname, initial, format=FMT_BYTES)
        self.cb.append(kname, b'\x20', format=FMT_BYTES)
        self.cb.prepend(kname, b'\x00', format=FMT_BYTES)

        res = self.cb.get(kname)
        self.assertEqual(res.value, b'\x00\x10\x20')

    def test_append_nostr(self):
        key = self.gen_key("append_nostr")
        self.cb.upsert(key, "value")
        rv = self.cb.append(key, "a_string")
        self.assertTrue(rv.cas)

        self.assertRaises(ValueFormatError,
                          self.cb.append, "key", { "some" : "object" })

    def test_append_enoent(self):
        key = self.gen_key("append_enoent")
        self.cb.remove(key, quiet=True)
        try:
            self.cb.append(key, "value")
            self.assertTrue(False, "Exception not thrown")
        except CouchbaseError as e:
            self.assertTrue(isinstance(e, NotStoredError)
                            or isinstance(e, NotFoundError))

    def test_append_multi(self):
        kv = self.gen_kv_dict(amount=4, prefix="append_multi")

        self.cb.upsert_multi(kv, format=FMT_UTF8)
        self.cb.append_multi(kv)
        self.cb.prepend_multi(kv)

        rvs = self.cb.get_multi(list(kv.keys()))
        self.assertTrue(rvs.all_ok)
        self.assertEqual(len(rvs), 4)

        for k, v in rvs.items():
            basekey = kv[k]
            self.assertEqual(v.value, basekey * 3)
