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
from couchbase.cluster import QueryOptions, QueryProfile, DiagnosticsOptions
from couchbase.diagnostics import ServiceType, EndpointState, ClusterState
from couchbase.exceptions import KeyNotFoundException
from datetime import timedelta
import time
from unittest import SkipTest


class ClusterTests(CollectionTestCase):
    def setUp(self):
        super(ClusterTests, self).setUp()

        # right now, these are all just query tests.  Lets make an
        # annotation to skip just those tests, once we flesh out this
        # test suite
        if not self.is_realserver:
            raise SkipTest('mock does not mock queries')
        # since we know that the CollectionTestCase loads beers, lets
        # use beer-sample bucket for our query tests.  NOTE: it isn't
        # clear to me that this is a great idea long-term, but we seem
        # to require this, and that bucket has a primary index, lets
        # use it.  Later when the querymgr works, and it can wait for the
        # index to exist, we can make this more isolated
        self.query_bucket = 'beer-sample'

    def test_diagnostics(self):
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
        query_result = self.cluster.query('SELECT * FROM `beer-sample` LIMIT 1')
        self.assertRows(query_result, 1)
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

    def assertRows(self, result, expected_count):
        count = 0
        self.assertIsNotNone(result)
        for row in result.rows():
            self.assertIsNotNone(row)
            count += 1
        self.assertEquals(count, expected_count)

    def test_simple_query(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` LIMIT 2")
        self.assertRows(result, 2)
        self.assertIsNone(result.metrics())
        self.assertIsNone(result.profile())

    def test_simple_query_with_positional_params(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1", '21st_amendment%')
        self.assertRows(result, 1)

    def test_simple_query_with_named_params(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1", brewery='21st_amendment%')
        self.assertRows(result, 1)

    def test_simple_query_with_positional_params_in_options(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1", QueryOptions(positional_parameters=['21st_amendment%']))
        self.assertRows(result, 1)

    def test_simple_query_with_named_params_in_options(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1", QueryOptions(named_parameters={'brewery':'21st_amendment%'}))
        self.assertRows(result, 1)

    # NOTE: Ideally I'd notice a set of positional parameters in the query call, and assume they were the positional parameters for the
    # query (once popping off the options if it is in there).  But this seems a bit tricky so for now, kwargs override the corresponding
    # value in the options, only.  TODO: ponder this interface more.  Soon.
    def test_simple_query_without_options_with_kwargs_positional_params(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1", positional_parameters=['21st_amendment%'])
        self.assertRows(result, 1)

    # NOTE: Ideally I'd notice that a named parameter wasn't an option parameter name, and just _assume_ that it is a named parameter
    # for the query.  However I worry about overlap being confusing, etc...
    # TODO: Lets ponder this interface a bit more.  Soon.
    def test_simple_query_without_options_with_kwargs_named_params(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1", named_parameters={'brewery':'21st_amendment%'})
        self.assertRows(result, 1)

    def test_query_with_profile(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` LIMIT 1", QueryOptions(profile=QueryProfile.timings()))
        self.assertRows(result, 1)
        self.assertIsNone(result.metrics())
        self.assertIsNotNone(result.profile())

    def test_query_with_metrics(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` LIMIT 1", QueryOptions(metrics=True))
        self.assertRows(result, 1)
        self.assertIsNotNone(result.metrics())
        self.assertIsNone(result.profile())

    def test_mixed_positional_parameters(self):
        # we assume that positional overrides one in the Options
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $1 LIMIT 1", QueryOptions(positional_parameters=['xgfflq']), '21st_am%')
        self.assertRows(result, 1)

    def test_mixed_named_parameters(self):
        result = self.cluster.query("SELECT * FROM `beer-sample` WHERE brewery_id LIKE $brewery LIMIT 1", QueryOptions(named_parameters={'brewery':'xxffqqlx'}), brewery='21st_am%')
        self.assertRows(result, 1)
