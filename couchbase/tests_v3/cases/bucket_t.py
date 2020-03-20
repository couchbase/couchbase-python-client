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
from couchbase.diagnostics import ServiceType, PingState
from datetime import timedelta


class BucketSimpleTest(CollectionTestCase):

    def setUp(self):
        super(BucketSimpleTest, self).setUp()

    def tearDown(self):
        super(BucketSimpleTest, self).tearDown()

    def test_name(self):
        self.assertEqual('default', self.bucket.name)

    def test_ping(self):
        result = self.bucket.ping()
        self.assertIsNotNone(result.sdk)
        self.assertIsNotNone(result.id)
        self.assertIsNotNone(result.version)
        endpoints = result.endpoints
        for k, vals in endpoints.items():
            for v in vals:
                if v.state == PingState.OK:
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

    def test_view_query_timeout(self):
        self.bucket.view_timeout = timedelta(seconds=50)
        self.assertEqual(timedelta(seconds=50), self.bucket.view_timeout)

    def test_kv_timeout(self):
        self.bucket.kv_timeout = timedelta(seconds=50)
        self.assertEqual(timedelta(seconds=50), self.bucket.kv_timeout)

    def test_tracing_orphaned_queue_flush_interval(self):
        self.bucket.tracing_orphaned_queue_flush_interval = timedelta(seconds=1)
        self.assertEqual(timedelta(seconds=1), self.bucket.tracing_orphaned_queue_flush_interval)

    def test_tracing_orphaned_queue_size(self):
        self.bucket.tracing_orphaned_queue_size = 10
        self.assertEqual(10, self.bucket.tracing_orphaned_queue_size)

    def test_tracing_threshold_queue_flush_interval(self):
        self.bucket.tracing_threshold_queue_flush_interval = timedelta(seconds=10)
        self.assertEqual(timedelta(seconds=10), self.bucket.tracing_threshold_queue_flush_interval)

    def test_tracing_threshold_queue_size(self):
        self.bucket.tracing_threshold_queue_size = 100
        self.assertEqual(100, self.bucket.tracing_threshold_queue_size)

    def test_tracing_threshold_kv(self):
        self.bucket.tracing_threshold_kv = timedelta(seconds=1)
        self.assertEqual(timedelta(seconds=1), self.bucket.tracing_threshold_kv)

    def test_tracing_threshold_view(self):
        self.bucket.tracing_threshold_view = timedelta(seconds=1)
        self.assertEqual(timedelta(seconds=1), self.bucket.tracing_threshold_view)
