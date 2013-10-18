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

from couchbase.exceptions import NotFoundError, ArgumentError, TimeoutError
from couchbase.tests.base import MockTestCase

class ConnectionEndureTest(MockTestCase):
    #XXX: Require LCB 2.1.0

    def test_excessive(self):
        self.assertRaises(ArgumentError,
                          self.cb.set,
                          "foo", "bar",
                          persist_to=99, replicate_to=99)

    def test_embedded_endure(self):
        key = self.gen_key("embedded_endure")
        with self.cb.durability(persist_to=-1, replicate_to=-1, timeout=0.1):
            def cb1(res):
                self.mockclient.endure(key,
                                       replica_count=self.mock.replicas,
                                       value=90,
                                       cas=res.cas)

            self.cb._dur_testhook = cb1
            rv = self.cb.set(key, "blah blah")
            self.assertTrue(rv.success)


            def cb2(res):
                self.mockclient.unpersist(key, on_master=True,
                                          replica_count=self.mock.replicas)

            self.cb._dur_testhook = cb2
            self.assertRaises(TimeoutError, self.cb.set, key, "value")


    def test_single_poll(self):
        key = self.gen_key("endure_single_poll")
        self.mockclient.endure(key,
                               on_master=True,
                               replica_count=self.mock.replicas,
                               value=90,
                               cas=1234)

        rv = self.cb.endure(key,
                            persist_to=-1, replicate_to=-1)
        self.assertTrue(rv.success)

        # This will fail..
        self.mockclient.unpersist(key,
                                  on_master=True,
                                  replica_count=self.mock.replicas)

        obsres = self.cb.observe(key)
        self.assertRaises(TimeoutError,
                          self.cb.endure,
                          key, persist_to=1, replicate_to=0,
                          timeout=0.1)

        self.mockclient.persist(key, on_master=True, replica_count=0)
        rv = self.cb.endure(key, persist_to=1, replicate_to=0)
        self.assertTrue(rv.success)

        self.assertRaises(TimeoutError,
                          self.cb.endure,
                          key, persist_to=2,
                          replicate_to=0,
                          timeout=0.1)

        rv = self.cb.endure(key, persist_to=0,
                            replicate_to=self.mock.replicas)
        self.assertTrue(rv.success)
