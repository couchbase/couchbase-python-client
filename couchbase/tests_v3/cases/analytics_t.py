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
from couchbase_tests.base import CollectionTestCase, SkipTest, AnalyticsTestCaseBase
from couchbase.management.analytics import CreateDatasetOptions, DropDatasetOptions
from couchbase.analytics import AnalyticsOptions


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

    def assertRows(self, response):
        for r in response:
            if r:
                return
        self.fail("No rows in result!")

    def test_simple_query(self):
        return self.try_n_times(10, 3, self.assertQueryReturnsRows, "SELECT * FROM `{}` LIMIT 1".format(self.dataset_name))

    def test_query_positional_params(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                'SELECT * FROM `{}` WHERE `type` = $1 LIMIT 1'.format(self.dataset_name),
                                AnalyticsOptions(positional_parameters=["brewery"]))
        print(rows_attempt)
        return self.checkResult(rows_attempt, lambda result:  self.assertEqual("brewery", result[0][self.dataset_name]['type']))


    def test_query_positional_params_no_option(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                'SELECT * FROM `{}` WHERE `type` = $1 LIMIT 1'.format(self.dataset_name),
                                "brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_positional_params_override(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                'SELECT * FROM `{}` WHERE `type` = $1 LIMIT 1'.format(self.dataset_name),
                                AnalyticsOptions(positional_parameters=["jfjfjfjfjfj"]),
                                "brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_named_parameters(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                "SELECT * FROM `{}` WHERE `type` = $btype LIMIT 1".format(self.dataset_name),
                                AnalyticsOptions(named_parameters={"btype": "brewery"}))
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_named_parameters_no_options(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                "SELECT * FROM `{}` WHERE `type` = $btype LIMIT 1".format(self.dataset_name),
                                btype="brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))

    def test_query_named_parameters_override(self):
        rows_attempt = self.try_n_times(10, 3, self.assertQueryReturnsRows,
                                "SELECT * FROM `{}` WHERE `type` = $btype LIMIT 1".format(self.dataset_name),
                                AnalyticsOptions(named_parameters={"btype": "jfjfjfjf"}),
                                btype="brewery")
        return self.checkResult(rows_attempt, lambda result: self.assertEqual("brewery", result[0][self.dataset_name]['type']))



