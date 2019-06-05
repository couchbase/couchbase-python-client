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

from couchbase_tests.base import CouchbaseTestCase
from couchbase_core.iops.select import SelectIOPS

# For now, this just checks that basic set/get doesn't explode
# We'll definitely want to add more here before we consider it stable

class IopsTest(CouchbaseTestCase):
    def setUp(self):
        super(IopsTest, self).setUp()

    def _iops_connection(self, **kwargs):
        return self.make_connection(_iops=SelectIOPS(), **kwargs)

    def test_creation(self):
        self._iops_connection()
        self.assertTrue(True)

    def test_simple_ops(self):
        cb = self._iops_connection()
        key = self.gen_key("iops-simple")
        value = "some_value"
        cb.upsert(key, value)
        rv = cb.get(key)
        self.assertTrue(rv.success)
        self.assertEqual(rv.value, value)
