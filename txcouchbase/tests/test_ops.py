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

from typing import *

from twisted.trial.unittest import TestCase

from couchbase.exceptions import DocumentNotFoundException
from couchbase.result import GetResult, MutationResult, MultiMutationResult
from couchbase_tests.base import AsyncClusterTestCase
from txcouchbase.tests.base import gen_base
from txcouchbase.tests.base import skip_PYCBC_894

Base = gen_base(AsyncClusterTestCase)  # type: Type[TestCase]


class OperationTestCase(Base):
    @skip_PYCBC_894
    def test_simple_set(self):
        cb = self.make_connection()
        key = self.gen_key("test_simple_set")
        d = cb.upsert(key, "simple_Value")

        def t(ret):
            self.assertIsInstance(ret, MutationResult)
            #self.assertEqual(ret.id, key) - seemingly MutationResults don't have IDs - recheck
            del ret

        d.addCallback(t)
        del cb
        return d

    @skip_PYCBC_894
    def test_simple_get(self):

        cb = self.make_connection()
        key = self.gen_key("test_simple_get")
        value = "simple_value"

        cb.upsert(key, value)
        d_get = cb.get(key)

        def t(ret  # type: GetResult
              ):
            self.assertIsInstance(ret, GetResult)
            self.assertEqual(ret.id, key)
            self.assertEqual(ret.content, value)

        d_get.addCallback(t)
        return d_get

    @skip_PYCBC_894
    def test_multi_set(self):
        cb = self.make_connection()
        kvs = self.gen_kv_dict(prefix="test_multi_set")
        d_set = cb.upsert_multi(kvs)

        def t(ret  # type: MultiMutationResult
              ):
            self.assertEqual(len(ret), len(kvs))
            self.assertEqual(ret.keys(), kvs.keys())
            #self.assertTrue(ret.all_ok)  # to be defined by SDK3 multi-ops RFC
            for k in kvs:
                #self.assertEqual(ret[k].id, k)  # MutationResult has no key or id
                self.assertTrue(ret[k].success)

            del ret

        d_set.addCallback(t)
        return d_set

    @skip_PYCBC_894
    def test_single_error(self):
        cb = self.make_connection()
        key = self.gen_key("test_single_error")

        d_del = cb.remove(key, quiet=True)

        d = cb.get(key, quiet=False)
        def t(err):
            self.assertIsInstance(err.value, DocumentNotFoundException)
            return True

        d.addCallback(lambda x: self.assertTrue(False))
        d.addErrback(t)
        return d

    @skip_PYCBC_894
    def test_multi_errors(self  # type: Base
                        ):
        orig_cb = self.make_connection()
        kv = self.gen_kv_dict(prefix = "test_multi_errors")

        cb = orig_cb.upsert_multi(kv)
        rmkey = list(kv.keys())[0]

        def t(err):
            self.assertIsInstance(err.value, DocumentNotFoundException)
            all_results = err.value.all_results
            for k, v in kv.items():
                self.assertTrue(k in all_results)
                res = all_results[k]
                self.assertEqual(res.key, k)
                if k != rmkey:
                    self.assertTrue(res.success)
                    self.assertEqual(res.value, v)

            res_fail = err.value.result
            self.assertFalse(res_fail.success)
            self.assertTrue(DocumentNotFoundException._can_derive(res_fail.rc))

        def respond(target):
            d = orig_cb.remove(rmkey)

            def and_then(*args, **kwargs):
                at = orig_cb.get_multi(kv.keys())
                at.addErrback(t)

                class Checker(object):
                    def __init__(self, parent):
                        self.count = 1
                        self._parent = parent

                    def __call__(self, *args, **kwargs):
                        try:
                            self._parent.assertTrue(success)
                        except Exception as e:
                            if not self.count:
                                raise
                            self.count -= 1

                at.addCallback(Checker(self))
                return at

            d.addCallback(and_then)
            return d

        cb.addCallback(respond)
        return cb
