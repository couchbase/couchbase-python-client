import json

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase, async_test
from couchbase.cluster import DiagnosticsOptions
from couchbase.diagnostics import ServiceType, EndpointState, ClusterState, PingState
from couchbase.result import PingResult
from couchbase.bucket import PingOptions


class AcouchbaseClusterTests(AsyncioTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseClusterTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster)

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseClusterTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseClusterTests, self).setUp()

    @async_test
    async def test_ping(self):
        result = await self.cluster.ping()
        self.assertIsInstance(result, PingResult)

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

    @async_test
    async def test_ping_report_id(self):
        report_id = "11111"
        result = await self.cluster.ping(PingOptions(report_id=report_id))
        self.assertIn(report_id, result.id)

    @async_test
    async def test_ping_restrict_services(self):
        services = [ServiceType.KeyValue]
        result = await self.cluster.ping(PingOptions(service_types=services))
        keys = list(result.endpoints.keys())
        self.assertEqual(1, len(keys))
        self.assertEqual(ServiceType.KeyValue, keys[0])

    @async_test
    async def test_ping_as_json(self):
        result = await self.cluster.ping()
        self.assertIsInstance(result, PingResult)
        result_str = result.as_json()
        self.assertIsInstance(result_str, str)
        result_json = json.loads(result_str)
        self.assertIsNotNone(result_json['version'])
        self.assertIsNotNone(result_json['id'])
        self.assertIsNotNone(result_json['sdk'])
        self.assertIsNotNone(result_json['services'])
        # TODO:  see why the mock doesn't like this
        if not self.is_mock:
            for _, data in result_json['services'].items():
                if len(data):
                    self.assertIsNotNone(data[0]['id'])
                    self.assertIsNotNone(data[0]['latency_us'])
                    self.assertIsNotNone(data[0]['remote'])
                    self.assertIsNotNone(data[0]['local'])
                    self.assertIsNotNone(data[0]['state'])

    @async_test
    async def test_diagnostics(self):
        # call ping to get the service types we want to inspect
        await self.cluster.ping()
        result = self.cluster.diagnostics(
            DiagnosticsOptions(report_id="imareportid"))
        self.assertIn("imareportid", result.id)
        self.assertIsNotNone(result.sdk)
        self.assertIsNotNone(result.version)
        self.assertEqual(result.state, ClusterState.Online)
        if not self.is_mock:
            # no matter what there should be a config service type in there,
            # as long as we are not the mock.
            # no matter what there should be a config service type in there,
            # as long as we are not the mock.
            mgmt = result.endpoints[ServiceType.Management]
            self.assertTrue(len(mgmt) > 0)
            self.assertIsNotNone(mgmt[0].id)
            self.assertIsNotNone(mgmt[0].local)
            self.assertIsNotNone(mgmt[0].remote)
            self.assertIsNotNone(mgmt[0].last_activity)
            self.assertEqual(mgmt[0].state, EndpointState.Connected)
            self.assertEqual(mgmt[0].type, ServiceType.Management)

    @async_test
    async def test_diagnostics_with_active_bucket(self):
        query_result = self.cluster.query(
            'SELECT * FROM `beer-sample` LIMIT 1')
        if self.is_mock:
            try:
                [r async for r in query_result]
            except BaseException:
                pass
        else:
            self.assertTrue(len([r async for r in query_result]) > 0)
        result = self.cluster.diagnostics(
            DiagnosticsOptions(report_id="imareportid"))
        self.assertIn("imareportid", result.id)

        await self.cluster.ping()
        # we now open a bucket, and ping the services, in setUp's base classes.  So
        # there should always be a management endpoint...
        if not self.is_mock:
            # no matter what there should be a config service type in there,
            # as long as we are not the mock.
            mgmt = result.endpoints[ServiceType.Management]
            self.assertTrue(len(mgmt) > 0)

        # but now, we have hit Query, so...
        q = result.endpoints[ServiceType.Query]
        self.assertTrue(len(q) > 0)
        self.assertIsNotNone(q[0].id)
        self.assertIsNotNone(q[0].local)
        self.assertIsNotNone(q[0].remote)
        self.assertIsNotNone(q[0].last_activity)
        self.assertEqual(q[0].state, EndpointState.Connected)
        self.assertEqual(q[0].type, ServiceType.Query)

    @async_test
    async def test_diagnostics_as_json(self):
        # call ping to get the service types we want to inspect
        await self.cluster.ping()
        result = self.cluster.diagnostics(
            DiagnosticsOptions(report_id="imareportid"))
        self.assertIn("imareportid", result.id)
        self.assertIsNotNone(result.sdk)
        self.assertIsNotNone(result.version)
        self.assertEqual(result.state, ClusterState.Online)
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
                self.assertIsNotNone(data[0]['last_activity_us'])
                self.assertIsNotNone(data[0]['remote'])
                self.assertIsNotNone(data[0]['local'])
                self.assertIsNotNone(data[0]['state'])
