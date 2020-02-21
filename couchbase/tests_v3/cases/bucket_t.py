#
# Copyright 2019, Couchbase, Inc.
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
from couchbase_tests.base import CollectionTestCase, SkipTest
from couchbase.management.collections import CollectionManager
from couchbase.bucket import PingOptions
from couchbase.diagnostics import ServiceType
import logging

class BucketSimpleTest(CollectionTestCase):

    def setUp(self):
        super(BucketSimpleTest, self).setUp()

    def tearDown(self):
        super(BucketSimpleTest, self).tearDown()

    def test_name(self):
        self.assertEqual(self.bucket.name, 'default')

    def test_ping(self):
        result = self.bucket.ping()
        self.assertIsNotNone(result.sdk)
        self.assertIsNotNone(result.id)
        self.assertIsNotNone(result.version)
        endpoints = result.endpoints
        for k, vals in result.endpoints.items():
            for v in vals:
                self.assertIsNotNone(v)
                self.assertIsNotNone(v.id)
                self.assertIsNotNone(v.latency)
                self.assertIsNotNone(v.remote)
                self.assertIsNotNone(v.local)
                self.assertEqual(k, v.service_type)
                # Should really include ServiceType.View but lcb only
                # puts the scope in for KV.  TODO: file ticket or discuss
                if k in [ServiceType.KeyValue]:
                    self.assertEqual(self.bucket.name, v.namespace)
                else:
                    self.assertIsNone(v.namespace)

    def test_ping_report_id(self):
        report_id = "11111"
        result = self.bucket.ping(PingOptions(report_id=report_id))
        self.assertIn(report_id, result.id)

    def test_ping_restrict_services(self):
        services = [ServiceType.KeyValue]
        result = self.bucket.ping(PingOptions(service_types=services))
        keys = list(result.endpoints.keys())
        print(keys)
        self.assertEqual(1, len(keys))
        self.assertEqual(ServiceType.KeyValue, keys[0])

    def test_collection(self):
        if self.supports_collections():
            # return the one we know the CollectionTestCase upserts
            self.assertIsNotNone(self.bucket.collection('flintstones'))
        else:
            self.assertIsNotNone(self.bucket.collection('_default'))

    def test_scope(self):
        if self.supports_collections():
            # return the one we know the CollectionTestCase upserts
            self.assertIsNotNone(self.bucket.scope('bedrock'))
        else:
            self.assertIsNotNone(self.bucket.scope('_default'))

    def test_collections(self):
        self.assertIsInstance(self.bucket.collections(), CollectionManager)

    def test_view_query(self):
        raise SkipTest('cannot test view_query until a ViewManager has been created')
