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

from time import sleep

from nose.plugins.attrib import attr

from couchbase_core import FMT_JSON

from couchbase_v2.exceptions import (
    NotFoundError)

from couchbase_tests.base import ConnectionTestCase

class GetTest(ConnectionTestCase):
    def test_trivial_get(self):
        key = self.gen_key('trivial_get')
        self.cb.upsert(key, 'value1')
        rv = self.cb.get(key)
        self.assertEqual(rv.value, 'value1')

        rvs = self.cb.get_multi([key])
        self.assertIsInstance(rvs, self.cls_MultiResult)
        self.assertEqual(len(rvs), 1)
        self.assertEqual(rvs[key].value, 'value1')

    def test_get_missing_key(self):
        rv = self.cb.get('key_missing_1', quiet=True)
        self.assertIsNone(rv.value)
        self.assertFalse(rv.success)

        # Get with quiet=False
        self.assertRaises(NotFoundError, self.cb.get, 'key_missing_1',
                          quiet=False)

    def test_multi_get(self):
        kv = self.gen_kv_dict(amount=3, prefix='get_multi')
        rvs = self.cb.upsert_multi(kv)
        self.assertTrue(rvs.all_ok)

        k_subset = list(kv.keys())[:2]

        rvs1 = self.cb.get_multi(k_subset)
        self.assertEqual(len(rvs1), 2)
        self.assertEqual(rvs1[k_subset[0]].value, kv[k_subset[0]])
        self.assertEqual(rvs1[k_subset[1]].value, kv[k_subset[1]])

        rv2 = self.cb.get_multi(kv.keys())
        self.assertEqual(rv2.keys(), kv.keys())


    def test_multi_mixed(self):
        kv_missing = self.gen_kv_dict(amount=3, prefix='multi_missing_mixed')
        kv_existing = self.gen_kv_dict(amount=3, prefix='multi_existing_mixed')

        self.cb.remove_multi(list(kv_missing.keys()) + list(kv_existing.keys()),
                             quiet=True)

        self.cb.upsert_multi(kv_existing)

        rvs = self.cb.get_multi(
            list(kv_existing.keys()) + list(kv_missing.keys()),
            quiet=True)


        self.assertFalse(rvs.all_ok)

        for k, v in kv_missing.items():
            self.assertTrue(k in rvs)
            self.assertFalse(rvs[k].success)
            self.assertTrue(rvs[k].value is None)
            self.assertTrue(NotFoundError._can_derive(rvs[k].rc))

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

        del cb_exc


    def test_extended_get(self):
        key = self.gen_key(prefix='key_extended')
        orig_cas1 = self.cb.upsert(key, 'value1').cas
        rv = self.cb.get(key)
        val1, flags1, cas1 = rv.value, rv.flags, rv.cas
        self.assertEqual(val1, 'value1')
        self.assertEqual(flags1, FMT_JSON)
        self.assertEqual(cas1, orig_cas1)

        # Test named tuples
        result1 = self.cb.get(key)
        self.assertEqual(result1.value, 'value1')
        self.assertEqual(result1.flags, FMT_JSON)
        self.assertEqual(result1.cas, orig_cas1)

        # Single get as array
        result2 = self.cb.get_multi([key])
        self.assertIsInstance(result2, self.cls_MultiResult)
        self.assertTrue(key in result2)
        self.assertEqual(result2[key].value, 'value1')
        self.assertEqual(result2[key].flags, FMT_JSON)
        self.assertEqual(result2[key].cas, orig_cas1)

        key2 = self.gen_key('key_extended_2')
        cas2 = self.cb.upsert(key2, 'value2').cas

        key3 = self.gen_key('key_extended_3')
        cas3 = self.cb.upsert(key3, 'value3').cas
        results = self.cb.get_multi([key2, key3])

        self.assertEqual(results[key3].value, 'value3')
        self.assertEqual(results[key3].flags, FMT_JSON)
        self.assertEqual(results[key3].cas, cas3)

        rv = self.cb.get('missing_key', quiet=True)
        val4, flags4, cas4 = rv.value, rv.flags, rv.cas
        self.assertEqual(val4, None)
        self.assertEqual(flags4, 0x00)
        self.assertEqual(cas4, 0)

    @attr('slow')
    def test_get_ttl(self):
        key = self.gen_key('get_ttl')
        self.cb.remove(key, quiet=True)
        self.cb.upsert(key, "a_value")
        rv = self.cb.get(key, ttl=1)
        self.assertEqual(rv.value, "a_value")
        sleep(2)
        rv = self.cb.get(key, quiet=True)
        self.assertFalse(rv.success)
        self.assertTrue(NotFoundError._can_derive(rv.rc))

    @attr('slow')
    def test_get_multi_ttl(self):
        kvs = self.gen_kv_dict(amount=2, prefix='get_multi_ttl')

        self.cb.upsert_multi(kvs)
        rvs = self.cb.get_multi(list(kvs.keys()), ttl=1)
        for k, v in rvs.items():
            self.assertEqual(v.value, kvs[k])

        sleep(2)
        rvs = self.cb.get_multi(list(kvs.keys()), quiet=True)
        for k, v in rvs.items():
            self.assertFalse(v.success)
            self.assertTrue(k in kvs)
            self.assertTrue(NotFoundError._can_derive(v.rc))

if __name__ == '__main__':
    unittest.main()
