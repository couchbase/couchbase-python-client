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

from couchbase_v2.exceptions import InvalidArgumentException

from couchbase.exceptions import  NotSupportedException
from couchbase_tests.base import CollectionTestCase

class EmptyKeyTest(CollectionTestCase):

    def test_empty_key(self):
        fnargs = (
            (self.coll.upsert, ["", "value"]),
            (self.coll.get, [""]),
            (self.coll.lock, ["", {'ttl': 5}]),
            (self.coll.counter, [""]),
            (self.coll.unlock, ["", 1234]),
            (self.coll.remove, [""]),
            (self.coll.observe, [""]),
            (self.coll.upsert_multi, [{"": "value"}]),
            (self.coll.counter_multi, [("", "")]),
            (self.coll.remove_multi, [("", "")]),
            (self.coll.unlock_multi, [{"": 1234}]),
            (self.coll.observe_multi, [("")])
        )

        for fn, args in fnargs:
            try:
                self.assertRaises(InvalidArgumentException, fn, *args)
            except NotSupportedException:
                pass
