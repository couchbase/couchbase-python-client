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
import time

from nose.plugins.attrib import attr

from couchbase_tests.base import ConnectionTestCase
import couchbase_v2.exceptions as E


@attr('slow')
class TouchTest(ConnectionTestCase):
    def setUp(self):
        super(TouchTest, self).setUp()
        self.cb = self.make_connection()

    def test_trivial_touch(self):
        key = self.gen_key("trivial_touch")
        self.cb.upsert(key, "value", ttl=1)
        rv = self.cb.touch(key, ttl=0)
        self.assertTrue(rv.success)
        self.sleep(2)
        rv = self.cb.get(key)
        self.assertTrue(rv.success)
        self.assertEqual(rv.value, "value")

        self.cb.touch(key, ttl=1)
        self.sleep(2)
        rv = self.cb.get(key, quiet=True)
        self.assertFalse(rv.success)
        self.assertTrue(E.NotFoundError._can_derive(rv.rc))

    def test_trivial_multi_touch(self):
        kv = self.gen_kv_dict(prefix="trivial_multi_touch")
        self.cb.upsert_multi(kv, ttl=1)
        self.sleep(10)
        rvs = self.cb.get_multi(kv.keys(), quiet=True)
        self.assertFalse(rvs.all_ok)

        self.cb.upsert_multi(kv, ttl=1)
        self.cb.touch_multi(kv.keys(), ttl=0)
        rvs = self.cb.get_multi(kv.keys())
        self.assertTrue(rvs.all_ok)

        self.cb.touch_multi(kv.keys(), ttl=1)
        self.sleep(10)
        rvs = self.cb.get_multi(kv.keys(), quiet=True)
        self.assertFalse(rvs.all_ok)

    def test_dict_touch_multi(self):
        k_missing = self.gen_key("dict_touch_multi_missing")
        k_existing = self.gen_key("dict_touch_multi_existing")

        self.cb.upsert_multi(
            {k_missing : "missing_val", k_existing : "existing_val"})

        self.cb.touch_multi({k_missing : 1, k_existing : 3})
        self.sleep(2)
        rvs = self.cb.get_multi([k_missing, k_existing], quiet=True)
        self.assertTrue(rvs[k_existing].success)
        self.assertFalse(rvs[k_missing].success)
        self.sleep(2)
        rv = self.cb.get(k_existing, quiet=True)
        self.assertFalse(rv.success)
