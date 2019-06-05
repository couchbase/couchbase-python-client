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

import couchbase_v2.exceptions as E
from couchbase_tests.base import ConnectionTestCase

# These tests try to see if the 'result' and 'all_results' appear properly
# also verify that other documented exception fields are present

class ExceptionsTest(ConnectionTestCase):

    def test_simple_excextra(self):
        exc = None
        key = self.gen_key("simple_excextra")
        self.cb.remove(key, quiet=True)

        try:
            self.cb.get(key, quiet=False)
        except E.CouchbaseError as e:
            exc = e

        self.assertTrue(exc)
        self.assertIsInstance(exc, E.CouchbaseError)
        self.assertTrue(exc.message)
        self.assertIsInstance(exc, E.NotFoundError)
        self.assertEqual(exc.key, key)
        self.assertIsInstance(exc.all_results, self.cls_MultiResult)
        self.assertTrue(key in exc.all_results)
        self.assertIsInstance(exc.all_results[key], self.cls_ValueResult)
        self.assertEqual(exc.all_results[key].rc, exc.rc)

        str(exc)
        repr(exc)
        del exc

    def test_multi_exc(self):
        kv_missing = self.gen_kv_dict(prefix="multi_exc_missing")
        kv_existing = self.gen_kv_dict(prefix="multi_exc_existing")
        self.cb.upsert_multi(kv_existing)
        exc = None
        try:
            self.cb.get_multi(list(kv_missing.keys()) + list(kv_existing.keys()),
                        quiet=False)
        except E.CouchbaseError as e:
            exc = e

        self.assertTrue(exc)
        self.assertIsInstance(exc, E.NotFoundError)
        self.assertEqual(len(exc.all_results),
                         len(kv_missing) + len(kv_existing))


        res_ok, res_fail = exc.split_results()
        all_results = exc.all_results
        for k, v in kv_missing.items():
            self.assertTrue(k in all_results)
            self.assertTrue(k in res_fail)
            self.assertFalse(k in res_ok)
            self.assertFalse(all_results[k].success)

        for k, v in kv_existing.items():
            self.assertTrue(k in all_results)
            self.assertTrue(k in res_ok)
            self.assertFalse(k in res_fail)
            self.assertTrue(all_results[k].success)
            self.assertTrue(all_results[k].value)
            self.assertEqual(v, all_results[k].value)

        str(exc)
        repr(exc)
        del exc
