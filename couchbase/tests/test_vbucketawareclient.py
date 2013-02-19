#
# Copyright 2012, Couchbase, Inc.
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

import uuid
import time

from nose.plugins.attrib import attr

from couchbase.vbucketawareclient import VBucketAwareClient
from couchbase.exception import MemcachedError
from couchbase.tests.base import Base
from couchbase.tests.test_memcachedclient import MemcachedClientTest


class VBucketAwareClientTest(MemcachedClientTest):
    def setUp(self):
        Base.setUp(self)
        # TODO: pull memcached port from config
        self.client = VBucketAwareClient(self.host)

    @attr(cbv="1.0.0")
    def test_getl(self):
        key, value = 'test_getl', str(uuid.uuid4())
        self.client.set(key, 0, 0, value)
        _, cas, rv = self.client.getl(key)
        self.assertEqual(rv, value)
        self.assertRaises(MemcachedError, self.client.set, key, 0, 0, value)
        # unlock the key
        self.client.cas(key, 0, 0, cas, value)
        # now that it's unlocked, clean it up
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_simple_touch(self):
        key, value = 'test_simple_touch', str(uuid.uuid4())
        self.client.set(key, 2, 0, value)
        self.client.touch(key, 5)
        time.sleep(3)
        self.assertTrue(self.client.get(key)[2] == value)
        self.client.delete(key)

    @attr(cbv="1.0.0")
    def test_gat(self):
        key, value = 'test_gat', str(uuid.uuid4())
        self.client.set(key, 2, 0, value)
        set_value = self.client.gat(key, 5)[2]
        self.assertTrue(set_value == value)
        time.sleep(3)
        self.assertTrue(self.client.get(key)[2] == value)
        self.client.delete(key)
