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

from couchbase.result import ViewResult, ViewRow
from couchbase.bucket import Bucket

from couchbase_tests.base import CollectionTestCase, SkipTest
from couchbase.management.collections import CollectionManager
from couchbase.bucket import PingOptions
from couchbase.result import PingResult
from couchbase.diagnostics import ServiceType, PingState
from datetime import timedelta
import json


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
                    self.assertEqual(v.state, PingState.OK)
                    # Should really include ServiceType.View but lcb only
                    # puts the scope in for KV.  TODO: file ticket or discuss
                    if k in [ServiceType.KeyValue]:
                        self.assertEqual(self.bucket.name, v.namespace)
                    else:
                        self.assertIsNone(v.namespace)

    def test_ping_timeout(self):
        self.skipIfMock()
        result = self.bucket.ping(PingOptions(timeout=timedelta(microseconds=1.0)))
        self.assertIsNotNone(result)
        for k, vals in result.endpoints.items():
            for v in vals:
                self.assertIsNotNone(v)
                self.assertIsNotNone(v.latency)
                self.assertIsNotNone(v.local)
                self.assertEqual(k, v.service_type)
                self.assertEqual(v.state, PingState.TIMEOUT)

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

    def test_ping_as_json(self):
        result = self.bucket.ping()
        self.assertIsInstance(result, PingResult)
        result_str = result.as_json()
        self.assertIsInstance(result_str, str)
        result_json = json.loads(result_str)
        self.assertIsNotNone(result_json['version'])
        self.assertIsNotNone(result_json['id'])
        self.assertIsNotNone(result_json['sdk'])
        self.assertIsNotNone(result_json['services'])
        for _, data in result_json['services'].items():
            if len(data):
                self.assertIsNotNone(data[0]['id'])
                self.assertIsNotNone(data[0]['latency_us'])
                self.assertIsNotNone(data[0]['remote'])
                self.assertIsNotNone(data[0]['local'])
                self.assertIsNotNone(data[0]['state'])

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

    def test_view_query(self  # type: BucketSimpleTest
                        ):
        beer_bucket = self.cluster.bucket('beer-sample')  # type: Bucket
        EXPECTED_ROW_COUNT=10
        view_result = beer_bucket.view_query("beer", "brewery_beers", limit=EXPECTED_ROW_COUNT)  # type: ViewResult

        count = 0
        for _ in view_result:
            x = _  # type: ViewRow
            if x.id:
                self.assertIsInstance(x.id, str)
            self.assertIsNotNone(x.key)
            count += 1

        self.assertEqual(count, EXPECTED_ROW_COUNT)

        metadata = view_result.metadata()

        self.assertEqual(EXPECTED_ROW_COUNT, metadata.total_rows())
        self.assertEqual(7303, metadata.debug()['total_rows'])
