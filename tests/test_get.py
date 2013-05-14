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

import pickle
from time import sleep

from couchbase import (
    FMT_JSON, FMT_PICKLE, FMT_UTF8, FMT_BYTES)

from couchbase.exceptions import (
    CouchbaseError, ValueFormatError, NotFoundError)

from couchbase.libcouchbase import (
    Connection, MultiResult, Result)

from tests.base import CouchbaseTestCase
from nose.exc import SkipTest

class ConnectionGetTest(CouchbaseTestCase):
    def setUp(self):
        super(ConnectionGetTest, self).setUp()
        self.cb = self.make_connection()

    def test_trivial_get(self):
        self.cb.set('key_trivial1', 'value1')
        rv = self.cb.get('key_trivial1')
        self.assertEqual(rv.value, 'value1')

        rvs = self.cb.get_multi(['key_trivial1'])
        self.assertEqual(type(rvs), MultiResult)
        self.assertEqual(len(rvs), 1)
        self.assertEqual(rvs['key_trivial1'].value, 'value1')


    def test_get_missing_key(self):
        rv = self.cb.get('key_missing_1', quiet=True)
        self.assertIsNone(rv.value)
        self.assertFalse(rv.success)

        # Get with quiet=False
        self.assertRaises(NotFoundError, self.cb.get, 'key_missing_1',
                          quiet=False)

    def test_multi_get(self):
        kv = {'key_multi1': 'value1', 'key_multi3': 'value3',
                          'key_multi2': 'value2'}
        rvs = self.cb.set_multi(kv)
        self.assertTrue(rvs.all_ok)

        rvs1 = self.cb.get_multi(['key_multi2', 'key_multi3'])
        self.assertEqual(len(rvs1), 2)
        self.assertEqual(rvs1['key_multi2'].value, 'value2')
        self.assertEqual(rvs1['key_multi3'].value, 'value3')

        rv2 = self.cb.get_multi(['key_multi3', 'key_multi1', 'key_multi2'])
        self.assertEqual(rv2.keys(), kv.keys())


    def test_multi_mixed(self):
        kv_missing = { }
        kv_existing = { }

        for x in range(3):
            kv_missing["missing_key_" + str(x)] = "missing_value_" + str(x)
            kv_existing["existing_key_" + str(x)] = "existing_Value_" + str(x)

        self.cb.delete_multi(list(kv_missing.keys()) + list(kv_existing.keys()),
                             quiet=True)

        self.cb.set_multi(kv_existing)

        rvs = self.cb.get_multi(
            list(kv_existing.keys()) + list(kv_missing.keys()),
            quiet=True)


        self.assertFalse(rvs.all_ok)

        for k, v in kv_missing.items():
            self.assertTrue(k in rvs)
            self.assertFalse(rvs[k].success)
            self.assertTrue(rvs[k].value is None)
            self.assertTrue(CouchbaseError.rc_to_exctype(rvs[k].rc)
                            is NotFoundError)

        for k, v in kv_existing.items():
            self.assertTrue(k in rvs)
            self.assertTrue(rvs[k].success)
            self.assertEqual(rvs[k].value, kv_existing[k])
            self.assertEqual(rvs[k].rc, 0)

        # Try this again, but without quiet
        cb_exc = None
        try:
            self.cb.get_multi(list(kv_existing.keys()) + list(kv_missing.keys()))
        except NotFoundError as e:
            cb_exc = e

        self.assertTrue(cb_exc)
        all_res = cb_exc.all_results
        self.assertTrue(all_res)
        self.assertFalse(all_res.all_ok)

        for k, v in kv_existing.items():
            self.assertTrue(k in all_res)
            self.assertTrue(all_res[k].success)
            self.assertEqual(all_res[k].value, v)
            self.assertEqual(all_res[k].rc, 0)

        for k, v in kv_missing.items():
            self.assertTrue(k in all_res)
            self.assertFalse(all_res[k].success)
            self.assertTrue(all_res[k].value is None)

    def test_get_format(self):

        raise(SkipTest("get-with-format not implemented"))

        self.cb.set('key_format1', {'some': 'value1'}, format=FMT_JSON)
        val1 = self.cb.get('key_format1')
        self.assertEqual(val1, {'some': 'value1'})

        self.cb.set('key_format2', {'some': 'value2'}, format=FMT_PICKLE)
        val2 = self.cb.get('key_format2')
        self.assertEqual(val2, {'some': 'value2'})

        self.cb.set('key_format3', b'some value3', format=FMT_BYTES)
        val3 = self.cb.get('key_format3')
        self.assertEqual(val3, b'some value3')


        self.cb.set('key_format4', {'some': 'value4'}, format=FMT_JSON)
        val4 = self.cb.get('key_format4', format=FMT_BYTES)
        self.assertEqual(val4, b'{"some": "value4"}')

        self.cb.set('key_format5', {'some': 'value5'}, format=FMT_PICKLE)
        val5 = self.cb.get('key_format5', format=FMT_BYTES)
        self.assertEqual(pickle.loads(val5), {'some': 'value5'})


        self.cb.set('key_format6', {'some': 'value6'}, format=FMT_JSON)
        self.assertRaises(ValueFormatError, self.cb.get, 'key_format6',
                          format=FMT_PICKLE)

        self.cb.set('key_format7', {'some': 'value7'}, format=FMT_PICKLE)
        self.assertRaises(ValueFormatError, self.cb.get, 'key_format7',
                          format=FMT_JSON)

    def test_extended_get(self):
        orig_cas1 = self.cb.set('key_extended1', 'value1').cas
        rv = self.cb.get('key_extended1')
        val1, flags1, cas1 = rv.value, rv.flags, rv.cas
        self.assertEqual(val1, 'value1')
        self.assertEqual(flags1, 0x0)
        self.assertEqual(cas1, orig_cas1)

        # Test named tuples
        result1 = self.cb.get('key_extended1')
        self.assertEqual(result1.value, 'value1')
        self.assertEqual(result1.flags, 0x0)
        self.assertEqual(result1.cas, orig_cas1)

        # Single get as array
        result2 = self.cb.get_multi(['key_extended1'])
        self.assertEqual(type(result2), MultiResult)
        self.assertTrue('key_extended1' in result2)
        self.assertEqual(result2['key_extended1'].value, 'value1')
        self.assertEqual(result2['key_extended1'].flags, 0x0)
        self.assertEqual(result2['key_extended1'].cas, orig_cas1)

        cas2 = self.cb.set('key_extended2', 'value2').cas
        cas3 = self.cb.set('key_extended3', 'value3').cas
        results = self.cb.get_multi(['key_extended2', 'key_extended3'])

        self.assertEqual(results['key_extended3'].value, 'value3')
        self.assertEqual(results['key_extended3'].flags, 0x0)
        self.assertEqual(results['key_extended3'].cas, cas3)

        rv = self.cb.get('missing_key', quiet=True)
        val4, flags4, cas4 = rv.value, rv.flags, rv.cas
        self.assertEqual(val4, None)
        self.assertEqual(flags1, 0x0)
        self.assertEqual(cas4, 0)

    def test_get_ttl(self):
        self.cb.delete("key", quiet=True)
        self.cb.set("ttl_key", "a_value")
        rv = self.cb.get("ttl_key", ttl=1)
        self.assertEqual(rv.value, "a_value")
        sleep(2)
        rv = self.cb.get("ttl_key", quiet=True)
        self.assertFalse(rv.success)
        self.assertEqual(NotFoundError, CouchbaseError.rc_to_exctype(rv.rc))

    def test_get_multi_ttl(self):
        kvs = { "ttlkey_1" : "ttlvalue_1", "ttlkey_2" : "ttlvalue_2" }
        self.cb.set_multi(kvs)
        rvs = self.cb.get_multi(list(kvs.keys()), ttl=1)
        for k, v in rvs.items():
            self.assertEqual(v.value, kvs[k])

        sleep(2)
        rvs = self.cb.get_multi(list(kvs.keys()), quiet=True)
        for k, v in rvs.items():
            self.assertFalse(v.success)
            self.assertTrue(k in kvs)
            self.assertEqual(NotFoundError, CouchbaseError.rc_to_exctype(v.rc))


if __name__ == '__main__':
    unittest.main()
