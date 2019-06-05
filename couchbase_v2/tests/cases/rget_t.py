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

from couchbase_v2.exceptions import NotFoundError

from couchbase_tests.base import MockTestCase
from couchbase_core.mockserver import MockControlClient

class ReplicaGetTest(MockTestCase):
    def setUp(self):
        super(ReplicaGetTest, self).setUp()
        self.skipUnlessMock()
        self.skipLcbMin("2.0.7")
        self.mockclient = MockControlClient(self.mock.rest_port)

    def test_get_kw(self):
        key = self.gen_key("get_kw")
        # Set on all replicas
        self.mockclient.cache(key,
                              on_master=False,
                              replica_count=self.mock.replicas,
                              value=99,
                              cas=1234)

        self.assertRaises(NotFoundError,
                          self.cb.get, key)

        rv = self.cb.get(key, replica=True)
        self.assertTrue(rv.success)
        self.assertEqual(rv.value, 99)

    def _check_single_replica(self, ix):
        key = self.gen_key("get_kw_ix")

        # Ensure the key is removed...
        self.mockclient.purge(key,
                              on_master=True,
                              replica_count=self.mock.replicas)

        # Getting it should raise an error
        self.assertRaises(NotFoundError, self.cb.get, key)

        # So should getting it from any replica
        self.assertRaises(NotFoundError, self.cb.rget, key)

        # And so should getting it from a specific index
        for jx in range(self.mock.replicas):
            self.assertRaises(NotFoundError, self.cb.rget, key,
                              replica_index=jx)

        # Store the key on the desired replica
        self.mockclient.cache(key,
                              on_master=False,
                              replicas=[ix],
                              value=ix,
                              cas=12345)

        # Getting it from a replica should ultimately succeed
        self.cb.get(key, replica=True)
        rv = self.cb.rget(key)
        self.assertTrue(rv.success)
        self.assertEqual(rv.value, ix)

        # Getting it from our specified replica should succeed
        rv = self.cb.rget(key, replica_index=ix)
        self.assertTrue(rv.success)
        self.assertEqual(rv.value, ix)

        # Getting it from any other replica should fail
        for jx in range(self.mock.replicas):
            if jx == ix:
                continue

            self.assertRaises(NotFoundError,
                              self.cb.rget,
                              key,
                              replica_index=jx)


    def test_get_ix(self):
        key = self.gen_key("get_kw_ix")
        for ix in range(self.mock.replicas):
            self._check_single_replica(ix)
