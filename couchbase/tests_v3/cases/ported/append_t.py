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

from couchbase_core._libcouchbase import FMT_UTF8
from couchbase_tests.base import CollectionTestCase


class AppendTest(CollectionTestCase):

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
            self.assertEqual(v.content, basekey * 3)
