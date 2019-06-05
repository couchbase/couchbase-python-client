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

from couchbase_tests.base import ConnectionTestCase
import couchbase_core
import couchbase_core._libcouchbase as LCB
from couchbase_core.user_constants import FMT_JSON

class ConverertSetTest(ConnectionTestCase):
    def _swap_converters(self, swapfunc, kbase, new_enc, new_dec):
        kencode = kbase + "_encode"
        kdecode = kbase + "_decode"

        old_enc = LCB._get_helper(kencode)
        old_dec = LCB._get_helper(kdecode)

        old = swapfunc(new_enc, new_dec)
        self.assertEqual(old[0], old_enc)
        self.assertEqual(old[1], old_dec)
        return old

    def test_json_conversions(self):
        d = {
            'encode' : 0,
            'decode' : 0
        }

        def _encode(val):
            d['encode'] += 1
            return json.dumps(val)

        def _decode(val):
            d['decode'] += 1
            return json.loads(val)

        old = self._swap_converters(couchbase_core.set_json_converters,
                                    "json",
                                    _encode,
                                    _decode)

        key = self.gen_key("test_json_conversion")

        self.cb.upsert(key, ["value"], format=FMT_JSON)
        rv = self.cb.get(key)
        self.assertEqual(rv.value, ["value"])
        self.assertEqual(1, d['encode'])
        self.assertEqual(1, d['decode'])

        self._swap_converters(couchbase_core.set_json_converters,
                              "json",
                              old[0],
                              old[1])

    def test_pickle_conversions(self):
        d = {
            'encode' : 0,
            'decode' : 0
        }

        def _encode(val):
            d['encode'] += 1
            return pickle.dumps(val)

        def _decode(val):
            d['decode'] += 1
            return pickle.loads(val)

        key = self.gen_key("test_pickle_conversions")
        old = self._swap_converters(couchbase_core.set_pickle_converters,
                                    "pickle",
                                    _encode,
                                    _decode)
        fn = set([1,2,3])
        self.cb.upsert(key, fn, format=couchbase_core.FMT_PICKLE)
        rv = self.cb.get(key)
        self.assertEqual(rv.value, fn)
        self.assertEqual(1, d['encode'])
        self.assertEqual(1, d['decode'])

        self._swap_converters(couchbase_core.set_pickle_converters,
                              "pickle",
                              old[0],
                              old[1])
