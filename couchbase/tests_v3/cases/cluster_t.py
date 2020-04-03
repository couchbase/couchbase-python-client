# -*- coding:utf-8 -*-
#
# Copyright 2020, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from couchbase_tests.base import CollectionTestCase
from couchbase.cluster import ClassicAuthenticator
from couchbase.cluster import  DiagnosticsOptions, Cluster, ClusterOptions
from couchbase.diagnostics import ServiceType, EndpointState, ClusterState
from couchbase.exceptions import AlreadyShutdownException
from datetime import timedelta
from unittest import SkipTest
import couchbase_core._libcouchbase as _LCB


class ClusterTests(CollectionTestCase):
    def setUp(self):
        super(ClusterTests, self).setUp()

    def test_diagnostics(self):
        if self.is_mock or not self.supports_collections():
            raise SkipTest("diagnostics hangs forever with mock and < 6.5")
        result = self.cluster.diagnostics(DiagnosticsOptions(report_id="imareportid"))
        self.assertIn("imareportid", result.id)
        self.assertIsNotNone(result.sdk)
        self.assertIsNotNone(result.version)
        self.assertEquals(result.state, ClusterState.Online)
        # no matter what there should be a config service type in there...
        config = result.endpoints[ServiceType.Config]
        self.assertTrue(len(config) > 0)
        self.assertIsNotNone(config[0].id)
        self.assertIsNotNone(config[0].local)
        self.assertIsNotNone(config[0].remote)
        self.assertIsNotNone(config[0].last_activity)
        self.assertEqual(config[0].state, EndpointState.Connected)
        self.assertEqual(config[0].type, ServiceType.Config)

    def test_diagnostics_with_active_bucket(self):
        if self.is_mock or not self.supports_collections():
            raise SkipTest("diagnostics hangs forever with mock and < 6.5")
        query_result = self.cluster.query('SELECT * FROM `beer-sample` LIMIT 1')
        self.assertTrue(len(query_result.rows()) > 0)
        result = self.cluster.diagnostics(DiagnosticsOptions(report_id="imareportid"))
        self.assertIn("imareportid", result.id)

        # no matter what there should be a config service type in there...
        config = result.endpoints[ServiceType.Config]
        self.assertTrue(len(config) > 0)

        # but now, we have hit Query, so...
        q = result.endpoints[ServiceType.Query]
        self.assertTrue(len(q) > 0)
        self.assertIsNotNone(q[0].id)
        self.assertIsNotNone(q[0].local)
        self.assertIsNotNone(q[0].remote)
        self.assertIsNotNone(q[0].last_activity)
        self.assertEqual(q[0].state, EndpointState.Connected)
        self.assertEqual(q[0].type, ServiceType.Query)

    def test_disconnect(self):
        # for this test we need a new cluster...
        if self.is_mock:
            raise SkipTest("query not mocked")
        cluster = Cluster.connect(self.cluster.connstr, ClusterOptions(
            ClassicAuthenticator(self.cluster_info.admin_username, self.cluster_info.admin_password)))
        # verify that diagnostics returns a result
        result = cluster.query("SELECT * from `beer-sample` LIMIT 1")
        self.assertIsNotNone(len(result.rows()) > 0)
        # disconnect cluster
        cluster.disconnect()
        self.assertRaises(AlreadyShutdownException, cluster.query, "SELECT * FROM `beer-sample` LIMIT 1")

    def test_n1ql_default_timeout(self):
        self.cluster.n1ql_timeout = timedelta(seconds=50)
        self.assertEqual(timedelta(seconds=50), self.cluster.n1ql_timeout)

    def test_tracing_orphaned_queue_flush_interval(self):
        self.cluster.tracing_orphaned_queue_flush_interval = timedelta(seconds=1)
        self.assertEqual(timedelta(seconds=1), self.cluster.tracing_orphaned_queue_flush_interval)

    def test_tracing_orphaned_queue_size(self):
        self.cluster.tracing_orphaned_queue_size = 10
        self.assertEqual(10, self.cluster.tracing_orphaned_queue_size)

    def test_tracing_threshold_queue_flush_interval(self):
        self.cluster.tracing_threshold_queue_flush_interval = timedelta(seconds=10)
        self.assertEqual(timedelta(seconds=10), self.cluster.tracing_threshold_queue_flush_interval)

    def test_tracing_threshold_queue_size(self):
        self.cluster.tracing_threshold_queue_size = 100
        self.assertEqual(100, self.cluster.tracing_threshold_queue_size)

    def test_tracing_threshold_n1ql(self):
        self.cluster.tracing_threshold_n1ql = timedelta(seconds=1)
        self.assertEqual(timedelta(seconds=1), self.cluster.tracing_threshold_n1ql)

    def test_tracing_threshold_fts(self):
        self.cluster.tracing_threshold_fts = timedelta(seconds=1)
        self.assertEqual(timedelta(seconds=1), self.cluster.tracing_threshold_fts)

    def test_tracing_threshold_analytics(self):
        self.cluster.tracing_threshold_analytics = timedelta(seconds=1)
        self.assertEqual(timedelta(seconds=1), self.cluster.tracing_threshold_analytics)

    def test_compression(self):
        self.cluster.compression = _LCB.COMPRESS_NONE
        self.assertEqual(_LCB.COMPRESS_NONE, self.cluster.compression)

    def test_compression_min_size(self):
        self.cluster.compression_min_size = 5000
        self.assertEqual(5000, self.cluster.compression_min_size)

    def test_compression_min_ratio(self):
        self.cluster.compression_min_ratio = 0.5
        self.assertEqual(0.5, self.cluster.compression_min_ratio)

    def test_redaction(self):
        self.cluster.redaction = True
        self.assertTrue(self.cluster.redaction)

    def test_is_ssl(self):
        # well, our tests are not ssl, so...
        self.assertFalse(self.cluster.is_ssl)

