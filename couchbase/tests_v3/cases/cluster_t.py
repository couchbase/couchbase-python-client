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
from couchbase_core.cluster import ClassicAuthenticator
from couchbase.cluster import  DiagnosticsOptions, Cluster, ClusterOptions
from couchbase.diagnostics import ServiceType, EndpointState, ClusterState
from couchbase.exceptions import AlreadyShutdownException

from unittest import SkipTest


class ClusterTests(CollectionTestCase):
    def setUp(self):
        super(ClusterTests, self).setUp()

    def test_diagnostics(self):
        if self.is_mock:
            raise SkipTest("diagnostics hangs forever with mock")
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
        if self.is_mock:
            raise SkipTest("diagnostics hangs forever with mock")
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
