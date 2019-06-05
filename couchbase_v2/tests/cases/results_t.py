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

from couchbase_tests.base import ConnectionTestCase

INT_TYPES = None
try:
    INT_TYPES = (long, int)
except:
    INT_TYPES = (int,)

class ResultsTest(ConnectionTestCase):

    def __test_oprsesult(self, rv, check_exact=True, exprc=0):
        # Ensure they can be stringified
        self.assertIsInstance(rv, self.cls_OperationResult)
        self.assertIsInstance(rv, self.cls_Result)

        if check_exact:
            self.assertEqual(rv.__class__, self.cls_OperationResult)

        self.assertIsInstance(rv.cas, INT_TYPES)
        self.assertIsInstance(rv.rc, INT_TYPES)

        self.assertEqual(rv.rc, exprc)
        if exprc == 0:
            self.assertTrue(rv.success)

        self.assertIsInstance(rv.errstr, str)

        self.assertIsInstance(repr(rv), str)
        self.assertIsInstance(str(rv), str)

    def __test_valresult(self, rv, value):
        self.assertEqual(rv.__class__, self.cls_ValueResult)
        self.__test_oprsesult(rv, check_exact=False)

        self.assertEqual(rv.value, value)
        self.assertIsInstance(rv.flags, INT_TYPES)

    def test_results(self):
        # Test OperationResult/ValueResult fields
        key = self.gen_key("opresult")
        rv = self.cb.upsert(key, "value")
        self.__test_oprsesult(rv)

        rv = self.cb.remove(key)
        self.__test_oprsesult(rv)

        rv = self.cb.upsert(key, "value")
        self.__test_oprsesult(rv)

        rv = self.cb.lock(key, ttl=10)
        self.__test_valresult(rv, "value")
        rv = self.cb.unlock(key, rv.cas)
        self.__test_oprsesult(rv)
        rv = self.cb.get(key)
        self.__test_valresult(rv, "value")
        rv = self.cb.remove(key)
        self.__test_oprsesult(rv)

        rv = self.cb.counter(key, initial=10)
        self.__test_valresult(rv, 10)
        rv = self.cb.get(key)
        self.__test_valresult(rv, 10)

        rv = self.cb.touch(key)
        self.__test_oprsesult(rv)

    def test_multi_results(self):
        kvs = self.gen_kv_dict(prefix="multi_results")
        rvs = self.cb.upsert_multi(kvs)
        self.assertIsInstance(rvs, self.cls_MultiResult)
        [ self.__test_oprsesult(x) for x in rvs.values() ]
        repr(rvs)
        str(rvs)

        rvs = self.cb.get_multi(kvs.keys())
        self.assertIsInstance(rvs, self.cls_MultiResult)
        self.assertTrue(rvs.all_ok)

        [ self.__test_valresult(v, kvs[k]) for k, v in rvs.items()]

        rvs = self.cb.remove_multi(kvs.keys())
        [ self.__test_oprsesult(x) for x in rvs.values() ]
