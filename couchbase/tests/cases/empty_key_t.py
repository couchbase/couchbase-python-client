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

from couchbase.exceptions import ArgumentError
from couchbase.tests.base import ConnectionTestCase

class EmptyKeyTest(ConnectionTestCase):

    def test_empty_key(self):
        fnargs = (
            (self.cb.set, ["", "value"]),
            (self.cb.get, [""]),
            (self.cb.lock, ["", {'ttl': 5}]),
            (self.cb.counter, [""]),
            (self.cb.unlock, ["", 1234]),
            (self.cb.delete, [""]),
            (self.cb.observe, [""]),
            (self.cb.set_multi, [{"": "value"}]),
            (self.cb.counter_multi, [("", "")]),
            (self.cb.delete_multi, [("", "")]),
            (self.cb.unlock_multi, [{"": 1234}]),
            (self.cb.observe_multi, [("")])
        )

        for fn, args in fnargs:
            self.assertRaises(ArgumentError, fn, *args)
