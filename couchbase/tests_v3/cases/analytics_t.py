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

import datetime
import time

from couchbase_tests.base import CollectionTestCase, SkipTest, AnalyticsTestCaseBase
from couchbase.management.analytics import DropDatasetOptions
from couchbase.analytics import AnalyticsOptions, AnalyticsMetaData, AnalyticsStatus, AnalyticsWarning
from couchbase.exceptions import DatasetNotFoundException, DataverseNotFoundException, NotSupportedException
from couchbase.n1ql import UnsignedInt64


class AnalyticsTestCase(AnalyticsTestCaseBase):

    def _drop_dataset(self, result, *args, **kwargs):
        return self.mgr.drop_dataset(self.dataset_name, DropDatasetOptions(ignore_if_not_exists=True))

    def tearDown(self):
        return self.try_n_times(10, 3, self.mgr.disconnect_link, on_success=self._drop_dataset)

    def assertQueryReturnsRows(self, query, *options, **kwargs):
        result = self.cluster.analytics_query(query, *options, **kwargs)

        def verify_rows(actual_result):
            rows = actual_result.rows()
            if len(rows) > 0:
                return rows
            raise Exception("no rows in result")
        return self.checkResult(result, verify_rows)

    def assertRows(self, query, *options, **kwargs):
        result = self.cluster.analytics_query(query, *options, **kwargs)

        def verify_rows(actual_result):
            rows = actual_result.rows()
            if len(rows) > 0:
                return actual_result
            raise Exception("no rows in result")
        return self.checkResult(result, verify_rows)

    def test_simple_query(self):
        return self.try_n_times(10, 3, self.assertQueryReturnsRows, "SELECT * FROM `{}` LIMIT 1".format(self.dataset_name))

    def test_query_positional_params(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                        'SELECT * FROM `{}` WHERE `type` = $1 LIMIT 1'.format(
                                            self.dataset_name),
                                        AnalyticsOptions(positional_parameters=["brewery"]))
        return self.checkResult(rows_attempt, lambda result:  self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_positional_params_no_option(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                        'SELECT * FROM `{}` WHERE `type` = $1 LIMIT 1'.format(
                                            self.dataset_name),
                                        "brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_positional_params_override(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                        'SELECT * FROM `{}` WHERE `type` = $1 LIMIT 1'.format(
                                            self.dataset_name),
                                        AnalyticsOptions(
                                            positional_parameters=["jfjfjfjfjfj"]),
                                        "brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_named_parameters(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                        "SELECT * FROM `{}` WHERE `type` = $btype LIMIT 1".format(
                                            self.dataset_name),
                                        AnalyticsOptions(named_parameters={"btype": "brewery"}))
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_named_parameters_no_options(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                        "SELECT * FROM `{}` WHERE `type` = $btype LIMIT 1".format(
                                            self.dataset_name),
                                        btype="brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_named_parameters_override(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                        "SELECT * FROM `{}` WHERE `type` = $btype LIMIT 1".format(
                                            self.dataset_name),
                                        AnalyticsOptions(named_parameters={
                                                         "btype": "jfjfjfjf"}),
                                        btype="brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_analytics_with_metrics(self):
        initial = datetime.datetime.now()
        result = self.try_n_times(10, 3, self.assertRows,
                                  "SELECT * FROM `{}` LIMIT 1".format(self.dataset_name))
        taken = datetime.datetime.now() - initial
        metadata = result.metadata()  # type: AnalyticsMetaData
        metrics = metadata.metrics()
        self.assertIsInstance(metrics.elapsed_time(), datetime.timedelta)
        self.assertLess(metrics.elapsed_time(), taken)
        self.assertGreater(metrics.elapsed_time(),
                           datetime.timedelta(milliseconds=0))
        self.assertIsInstance(metrics.execution_time(), datetime.timedelta)
        self.assertLess(metrics.execution_time(), taken)
        self.assertGreater(metrics.execution_time(),
                           datetime.timedelta(milliseconds=0))

        expected_counts = {metrics.error_count: 0,
                           metrics.result_count: 1,
                           metrics.processed_objects: 1,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            self.assertIsInstance(count_result, UnsignedInt64, msg=fail_msg)
            self.assertEqual(UnsignedInt64(expected),
                             count_result, msg=fail_msg)
        self.assertGreater(metrics.result_size(), UnsignedInt64(500))

        self.assertEqual(UnsignedInt64(0), metrics.error_count())

    def test_analytics_metadata(self):
        result = self.try_n_times(10, 3, self.assertRows,
                                  "SELECT * FROM `{}` LIMIT 2".format(self.dataset_name))
        metadata = result.metadata()  # type: AnalyticsMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            self.assertIsInstance(id_res, str, msg=fail_msg)
        self.assertEqual(AnalyticsStatus.SUCCESS, metadata.status())
        self.assertIsInstance(metadata.signature(), (str, dict))
        self.assertIsInstance(metadata.warnings(), (list))
        for warning in metadata.warnings():
            self.assertIsInstance(warning, AnalyticsWarning)
            self.assertIsInstance(warning.message, str)
            self.assertIsInstance(warning.code, int)


class AnalyticsCollectionTests(CollectionTestCase):
    def setUp(self):
        super(AnalyticsCollectionTests, self).setUp(bucket='beer-sample')

        if not self.is_realserver:
            raise SkipTest('Analytics not mocked')

        if int(self.get_cluster_version().split('.')[0]) < 6:
            raise SkipTest("no analytics in {}".format(
                self.get_cluster_version()))

        # SkipTest if collections not supported
        try:
            self.bucket.collections().get_all_scopes()
        except NotSupportedException:
            raise SkipTest('Cluster does not support collections')

        self.cm = self.bucket.collections()
        self._scope_name = 'beer-sample-scope'
        self.create_beer_sample_collections()
        self.create_analytics_collections()

        # make sure the collection loads...
        query_str = 'USE {}; SELECT COUNT(1) AS beers FROM beers;'.format(
            self.dataverse_fqdn)
        for _ in range(10):
            res = self.try_n_times(
                10, 10, self.cluster.analytics_query, query_str)
            beers = res.rows()[0]['beers']
            if beers > 100:
                break
            print('Found {} beers in collection, waiting a bit...'.format(beers))
            time.sleep(5)

    @classmethod
    def setUpClass(cls) -> None:
        super(AnalyticsCollectionTests, cls).setUpClass(True)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._beer_sample_collections:
            dataverse_fqdn = '`{}`.`{}`'.format(cls._cluster_resource.bucket_name,
                                                cls._beer_sample_collections.scope)
            beers_fqdn = '`{}`.`{}`.{}'.format(cls._cluster_resource.bucket_name,
                                               cls._beer_sample_collections.scope,
                                               cls._beer_sample_collections.beers.name)
            cluster = cls._cluster_resource.cluster

            query_str = """
            USE {};
            DISCONNECT LINK Local;
            """.format(dataverse_fqdn,
                        cls._beer_sample_collections.beers.name,
                        beers_fqdn)
            cluster.analytics_query(query_str).rows()

            query_str = "USE {};DROP DATASET {} IF EXISTS;".format(dataverse_fqdn,
                                                                   cls._beer_sample_collections.beers.name,
                                                                   beers_fqdn)
            cluster.analytics_query(query_str).rows()

            query_str = "DROP DATAVERSE {} IF EXISTS;".format(dataverse_fqdn)
            cluster.analytics_query(query_str).rows()
        super(AnalyticsCollectionTests, cls).tearDownClass()

    def assertRows(self,
                   result,  # type: AnalyticsResult
                   expected_count):
        count = 0
        self.assertIsNotNone(result)
        for row in result.rows():
            self.assertIsNotNone(row)
            count += 1
        self.assertEqual(count, expected_count)

    def create_analytics_collections(self):

        self.dataverse_fqdn = '`{}`.`{}`'.format(
            self.bucket_name, self.beer_sample_collections.scope)
        self.beers_fqdn = '`{}`.`{}`.{}'.format(self.bucket_name,
                                                self.beer_sample_collections.scope,
                                                self.beer_sample_collections.beers.name)

        query_str = "CREATE DATAVERSE {} IF NOT EXISTS;".format(
            self.dataverse_fqdn)
        self.cluster.analytics_query(query_str).rows()

        query_str = """USE {}; 
        CREATE DATASET IF NOT EXISTS {} ON {}""".format(self.dataverse_fqdn,
                                                        self.beer_sample_collections.beers.name,
                                                        self.beers_fqdn)
        self.cluster.analytics_query(query_str).rows()

        query_str = "USE {}; CONNECT LINK Local;".format(self.dataverse_fqdn)
        self.cluster.analytics_query(query_str).rows()

    def test_query_fully_qualified(self):
        result = self.cluster.analytics_query(
            "SELECT * FROM {} LIMIT 2".format(self.beers_fqdn))
        self.assertRows(result, 2)

    def test_cluster_query_context(self):
        q_context = 'default:{}'.format(self.dataverse_fqdn)
        # test with QueryOptions
        a_opts = AnalyticsOptions(query_context=q_context)
        result = self.cluster.analytics_query(
            "SELECT * FROM beers LIMIT 2", a_opts)
        self.assertRows(result, 2)

        # test with kwargs
        result = self.cluster.analytics_query(
            "SELECT * FROM beers LIMIT 2", query_context=q_context)
        self.assertRows(result, 2)

    def test_bad_query_context(self):
        # test w/ no context
        result = self.cluster.analytics_query("SELECT * FROM beers LIMIT 2")
        with self.assertRaises(DatasetNotFoundException):
            result.rows()

        # test w/ bad scope
        q_context = 'default:`{}`.`{}`'.format(self.bucket_name, 'fake-scope')
        result = self.cluster.analytics_query(
            "SELECT * FROM beers LIMIT 2", AnalyticsOptions(query_context=q_context))
        with self.assertRaises(DataverseNotFoundException):
            result.rows()

    def test_scope_query(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.analytics_query("SELECT * FROM beers LIMIT 2")
        self.assertRows(result, 2)
        result = scope.analytics_query(
            "SELECT * FROM {} LIMIT 2".format(self.beers_fqdn), query_context='')
        self.assertRows(result, 2)

    def test_bad_scope_query(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        q_context = 'default:`{}`.`{}`'.format(self.bucket_name, 'fake-scope')
        result = scope.analytics_query("SELECT * FROM beers LIMIT 2",
                                       AnalyticsOptions(query_context=q_context))
        with self.assertRaises(DataverseNotFoundException):
            result.rows()

        q_context = 'default:`{}`.`{}`'.format(
            'fake-bucket', self.beer_sample_collections.scope)
        result = scope.analytics_query("SELECT * FROM beers LIMIT 2",
                                       query_context=q_context)
        with self.assertRaises(DataverseNotFoundException):
            result.rows()

    def test_scope_query_with_positional_params_in_options(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.analytics_query("SELECT * FROM beers WHERE META().id LIKE $1 LIMIT 1",
                                       AnalyticsOptions(positional_parameters=['21st_amendment%']))
        self.assertRows(result, 1)

    def test_scope_query_with_named_params_in_options(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.analytics_query("SELECT * FROM beers WHERE META().id LIKE $brewery LIMIT 1",
                                       AnalyticsOptions(named_parameters={'brewery': '21st_amendment%'}))
        self.assertRows(result, 1)

    def test_analytics_with_metrics(self):
        initial = datetime.datetime.now()
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.analytics_query("SELECT * FROM beers LIMIT 1")
        self.assertRows(result, 1)
        taken = datetime.datetime.now() - initial
        metadata = result.metadata()  # type: AnalyticsMetaData
        metrics = metadata.metrics()
        self.assertIsInstance(metrics.elapsed_time(), datetime.timedelta)
        self.assertLess(metrics.elapsed_time(), taken)
        self.assertGreater(metrics.elapsed_time(),
                           datetime.timedelta(milliseconds=0))
        self.assertIsInstance(metrics.execution_time(), datetime.timedelta)
        self.assertLess(metrics.execution_time(), taken)
        self.assertGreater(metrics.execution_time(),
                           datetime.timedelta(milliseconds=0))

        expected_counts = {metrics.error_count: 0,
                           metrics.result_count: 1,
                           metrics.processed_objects: 1,
                           metrics.warning_count: 0}
        for method, expected in expected_counts.items():
            count_result = method()
            fail_msg = "{} failed".format(method)
            self.assertIsInstance(count_result, UnsignedInt64, msg=fail_msg)
            self.assertEqual(UnsignedInt64(expected),
                             count_result, msg=fail_msg)
        self.assertGreater(metrics.result_size(), UnsignedInt64(500))

        self.assertEqual(UnsignedInt64(0), metrics.error_count())

    def test_analytics_metadata(self):
        scope = self.bucket.scope(self.beer_sample_collections.scope)
        result = scope.analytics_query("SELECT * FROM beers LIMIT 2")
        self.assertRows(result, 2)
        metadata = result.metadata()  # type: AnalyticsMetaData
        for id_meth in (metadata.client_context_id, metadata.request_id):
            id_res = id_meth()
            fail_msg = "{} failed".format(id_meth)
            self.assertIsInstance(id_res, str, msg=fail_msg)
        self.assertEqual(AnalyticsStatus.SUCCESS, metadata.status())
        self.assertIsInstance(metadata.signature(), (str, dict))
        self.assertIsInstance(metadata.warnings(), (list))
        for warning in metadata.warnings():
            self.assertIsInstance(warning, AnalyticsWarning)
            self.assertIsInstance(warning.message, str)
            self.assertIsInstance(warning.code, int)
