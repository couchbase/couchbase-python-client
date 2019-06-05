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

import json
import pickle

from couchbase_tests.base import ConnectionTestCase, SkipTest
from couchbase_v2.exceptions import ValueFormatError
from couchbase_core import FMT_AUTO, FMT_JSON, FMT_BYTES, FMT_UTF8, FMT_PICKLE

class FormatTest(ConnectionTestCase):

    def test_set_autoformat(self):
        key = self.gen_key("set_autoformat")
        jvals = (None, True, False, {}, [], tuple() )
        bvals = (b"\x01", bytearray([1,2,3]))
        uvals = (b"\x42".decode('utf-8'), b'\xea\x80\x80'.decode("utf-8"))
        pvals = (set([]), object())

        for jv in jvals:
            self.cb.upsert(key, jv, format=FMT_AUTO)
            rv = self.cb.get(key, no_format=True)
            self.assertEqual(rv.flags, FMT_JSON)
            # We need 'decode' because Python3's byte type
            self.assertEqual(rv.value.decode("utf-8"), json.dumps(jv))

        for bv in bvals:
            self.cb.upsert(key, bv, format=FMT_AUTO)
            rv = self.cb.get(key, no_format=True)
            self.assertEqual(rv.flags, FMT_BYTES)
            self.assertEqual(rv.value, bv)

        for uv in uvals:
            self.cb.upsert(key, uv, format=FMT_AUTO)
            rv = self.cb.get(key, no_format=True)
            self.assertEqual(rv.flags, FMT_UTF8)
            self.assertEqual(rv.value, uv.encode("utf-8"))

        for pv in pvals:
            self.cb.upsert(key, pv, format=FMT_AUTO)
            rv = self.cb.get(key, no_format=True)
            self.assertEqual(rv.flags, FMT_PICKLE)
            self.assertEqual(rv.value, pickle.dumps(pv))

    def test_set_format(self):
        key = self.gen_key('set_format')
        rv1 = self.cb.upsert(key, {'some': 'value1'}, format=FMT_JSON)
        self.assertTrue(rv1.cas > 0)

        self.assertRaises(ValueFormatError, self.cb.upsert,
                          key, object(), format=FMT_JSON)

        rv3 = self.cb.upsert(key, {'some': 'value3'},
                           format=FMT_PICKLE)
        self.assertTrue(rv3.cas > 0)
        rv4 = self.cb.upsert(key, object(), format=FMT_PICKLE)
        self.assertTrue(rv4.cas > 0)

        self.assertRaises(ValueFormatError, self.cb.upsert,
                          key, {'some': 'value5'},
                          format=FMT_BYTES)
        self.assertRaises(ValueFormatError, self.cb.upsert,
                          key, { 'some' : 'value5.1'},
                          format=FMT_UTF8)

        rv6 = self.cb.upsert(key, b'some value6', format=FMT_BYTES)
        self.assertTrue(rv6.cas > 0)

        rv7 = self.cb.upsert(key, b"\x42".decode('utf-8'),
                          format=FMT_UTF8)
        self.assertTrue(rv7.success)


    def test_get_noformat(self):
        k = self.gen_key("get_noformat")
        self.cb.upsert(k, {"foo":"bar"}, format=FMT_JSON)
        rv = self.cb.get(k, no_format=True)
        self.assertEqual(rv.value, b'{"foo":"bar"}')

        kl = self.gen_key_list(prefix="get_noformat")
        kv = {}
        for k in kl:
            kv[k] = {"foo" : "bar"}

        self.cb.upsert_multi(kv)
        rvs = self.cb.get_multi(kv.keys(), no_format=True)
        for k, v in rvs.items():
            self.assertEqual(v.value, b'{"foo":"bar"}')


    def test_get_format(self):

        raise(SkipTest("get-with-format not implemented"))

        self.cb.upsert('key_format1', {'some': 'value1'}, format=FMT_JSON)
        val1 = self.cb.get('key_format1')
        self.assertEqual(val1, {'some': 'value1'})

        self.cb.upsert('key_format2', {'some': 'value2'}, format=FMT_PICKLE)
        val2 = self.cb.get('key_format2')
        self.assertEqual(val2, {'some': 'value2'})

        self.cb.upsert('key_format3', b'some value3', format=FMT_BYTES)
        val3 = self.cb.get('key_format3')
        self.assertEqual(val3, b'some value3')


        self.cb.upsert('key_format4', {'some': 'value4'}, format=FMT_JSON)
        val4 = self.cb.get('key_format4', format=FMT_BYTES)
        self.assertEqual(val4, b'{"some": "value4"}')

        self.cb.upsert('key_format5', {'some': 'value5'}, format=FMT_PICKLE)
        val5 = self.cb.get('key_format5', format=FMT_BYTES)
        self.assertEqual(pickle.loads(val5), {'some': 'value5'})


        self.cb.upsert('key_format6', {'some': 'value6'}, format=FMT_JSON)
        self.assertRaises(ValueFormatError, self.cb.get, 'key_format6',
                          format=FMT_PICKLE)

        self.cb.upsert('key_format7', {'some': 'value7'}, format=FMT_PICKLE)
        self.assertRaises(ValueFormatError, self.cb.get, 'key_format7',
                          format=FMT_JSON)
