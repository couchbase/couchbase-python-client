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

from couchbase_v2.exceptions import (KeyExistsError, NotFoundError)
from couchbase_tests.base import ConnectionTestCase

class DeleteTest(ConnectionTestCase):

    def test_trivial_delete(self):
        # Try to delete a key that exists. Ensure that the operation
        # succeeds

        key = self.gen_key("trivial_delete")
        rv = self.cb.upsert(key, 'value')
        self.assertTrue(rv.success)
        self.assertTrue(rv.cas > 0)
        rv = self.cb.remove(key)
        self.assertTrue(rv.success)

    def test_delete_notfound(self):
        # Delete a key that does not exist.
        # With 'quiet' ensure that it returns false. Without 'quiet', ensure that
        # it raises a NotFoundError

        self.cb.remove("foo", quiet = True)
        rv = self.cb.remove("foo", quiet = True)
        self.assertFalse(rv.success)
        self.assertRaises(NotFoundError, self.cb.remove, 'foo')

    def test_delete_cas(self):
        # Delete with a CAS value. Ensure that it returns OK

        key = self.gen_key("delete_cas")
        rv1 = self.cb.upsert(key, 'bar')
        self.assertTrue(rv1.cas > 0)
        rv2 = self.cb.remove(key, cas = rv1.cas)
        self.assertTrue(rv2.success)

    def test_delete_badcas(self):
        # Simple delete with a bad CAS

        key = self.gen_key("delete_badcas")
        self.cb.upsert(key, 'bar')
        self.assertRaises(KeyExistsError,
                self.cb.remove, key, cas = 0xdeadbeef)

    def test_delete_multi(self):
        # Delete passing a list of keys

        kvlist = self.gen_kv_dict(amount=5, prefix='delete_multi')

        rvs = self.cb.upsert_multi(kvlist)
        self.assertTrue(len(rvs) == len(kvlist))
        rm_rvs = self.cb.remove_multi(list(rvs.keys()))
        self.assertTrue(len(rm_rvs) == len(kvlist))
        self.assertTrue(rm_rvs.all_ok)

        for k, v in rm_rvs.items():
            self.assertTrue(k in kvlist)
            self.assertTrue(v.success)

    def test_delete_dict(self):
        # Delete passing a dict of key:cas pairs

        kvlist = self.gen_kv_dict(amount=5, prefix='delete_dict')

        rvs = self.cb.upsert_multi(kvlist)
        self.assertTrue(rvs.all_ok)

        # We should just be able to pass it to 'delete'
        rm_rvs = self.cb.remove_multi(rvs)
        self.assertTrue(rm_rvs.all_ok)
        for k, v in rm_rvs.items():
            self.assertTrue(v.success)

    def test_delete_mixed(self):
        # Delete with mixed success-error keys.
        # Test with mixed found/not-found
        # Test with mixed cas-valid/cas-invalid

        self.cb.remove("foo", quiet = True)

        self.cb.upsert("bar", "a_value")
        # foo does not exit,

        rvs = self.cb.remove_multi(('foo', 'bar'), quiet = True)
        self.assertFalse(rvs.all_ok)
        self.assertTrue(rvs['bar'].success)
        self.assertFalse(rvs['foo'].success)

        # Now see what happens if we delete those with a bad CAS
        kvs = self.gen_kv_dict(amount=3, prefix="delete_mixed_badcas")
        keys = list(kvs.keys())
        cas_rvs = self.cb.upsert_multi(kvs)

        # Ensure set had no errors
        set_errors = []
        for k, v in cas_rvs.items():
            if not v.success:
                set_errors.append([k, v])
        self.assertTrue(len(set_errors) == 0)

        # Set one to have a bad CAS
        cas_rvs[keys[0]] = 0xdeadbeef
        self.assertRaises(KeyExistsError,
                          self.cb.remove_multi, cas_rvs)

if __name__ == '__main__':
    unittest.main()
