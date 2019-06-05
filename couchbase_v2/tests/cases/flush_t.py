#
# Copyright 2015, Couchbase, Inc.
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

from couchbase_tests.base import MockTestCase


class FlushTest(MockTestCase):
    def test_flush(self):
        kv = self.gen_kv_dict(prefix='flush')
        self.cb.upsert_multi(kv)
        self.cb.flush()
        rvs = self.cb.get_multi(kv.keys(), quiet=True)
        for v in rvs.values():
            self.assertFalse(v.success)