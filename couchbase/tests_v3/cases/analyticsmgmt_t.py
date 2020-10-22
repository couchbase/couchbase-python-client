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
from couchbase_tests.base import CollectionTestCase, SkipTest
from couchbase.management.analytics import CreateDataverseOptions, DropDataverseOptions, CreateDatasetOptions, \
    CreateAnalyticsIndexOptions, DropAnalyticsIndexOptions, DropDatasetOptions, ConnectLinkOptions, DisconnectLinkOptions
from couchbase.exceptions import CouchbaseException, DataverseAlreadyExistsException, DataverseNotFoundException, \
    DatasetAlreadyExistsException, DatasetNotFoundException, NotSupportedException
from couchbase.analytics import AnalyticsDataType
import time


class AnalyticsIndexManagerTests(CollectionTestCase):
    def setUp(self):
        super(AnalyticsIndexManagerTests, self).setUp()
        if self.is_mock:
            raise SkipTest("mock doesn't mock management apis")

        if int(self.get_cluster_version().split('.')[0]) < 6:
            raise SkipTest("no analytics in {}".format(self.get_cluster_version()))

        self.mgr = self.cluster.analytics_indexes()
        self.dataverse_name = "test_dataverse"
        self.dataset_name = "test_breweries"
        # be sure the dataverse exists
        self.mgr.create_dataverse(self.dataverse_name, CreateDataverseOptions(ignore_if_exists=True))
        # now ensure our dataset in there
        self.mgr.create_dataset(self.dataset_name,
                                "beer-sample",
                                CreateDatasetOptions(dataverse_name=self.dataverse_name,
                                                     condition='`type` = "brewery"',
                                                     ignore_if_exists=True)
                                )
        try:
            self.mgr.disconnect_link(DisconnectLinkOptions(dataverse_name=self.dataverse_name))
        except:
            pass

    def tearDown(self):
        super(AnalyticsIndexManagerTests, self).tearDown()
        # be sure the dataset doesn't exist
        try:
            self.cluster.analytics_query("USE `{}`; DISCONNECT LINK Local;".format(self.dataverse_name)).metadata()
        except DataverseNotFoundException:
            pass
        try:
            self.mgr.disconnect_link(DisconnectLinkOptions(dataverse_name=self.dataverse_name))
        except:
            pass
        self.try_n_times(10, 3,
                         self.mgr.drop_dataverse, self.dataverse_name,
                         DropDatasetOptions(ignore_if_not_exists=True))

    def assertRows(self, query, iterations=10, pause_time=3):
        for _ in range(iterations):
            resp = self.cluster.analytics_query(query)
            for r in resp.rows():
                return
            time.sleep(pause_time)
        self.fail("query '{}' yielded no rows after {} attempts pausing {} sec between attempts"
                  .format(query, iterations, pause_time))

    def test_create_dataverse(self):
        # lets query for the existence of test-dataverse
        statement = 'SELECT * FROM Metadata.`Dataverse` WHERE DataverseName="{}";'.format(self.dataverse_name)
        result = self.cluster.analytics_query(statement)
        self.assertEqual(1, len(result.rows()))

    def test_create_dataverse_ignore_exists(self):
        self.assertRaises(DataverseAlreadyExistsException, self.mgr.create_dataverse, self.dataverse_name)
        self.mgr.create_dataverse(self.dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

    def test_drop_dataverse(self):
        self.mgr.drop_dataverse(self.dataverse_name)
        self.mgr.connect_link()
        statement = 'SELECT * FROM Metadata.`Dataverse` WHERE DataverseName="{}";'.format(self.dataverse_name)
        result = self.cluster.analytics_query(statement)
        self.assertEqual(0, len(result.rows()))

    def test_drop_dataverse_ignore_not_exists(self):
        self.mgr.drop_dataverse(self.dataverse_name)
        self.assertRaises(DataverseNotFoundException, self.mgr.drop_dataverse, self.dataverse_name)
        self.mgr.drop_dataverse(self.dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_create_dataset(self):
        # we put a dataset in during the setUp, so...
        datasets = self.mgr.get_all_datasets()
        for dataset in datasets:
            print(dataset)
            if dataset.dataset_name == self.dataset_name:
                return
        self.fail("didn't find {} in listing of all datasets".format(self.dataset_name))

    def test_create_dataset_ignore_exists(self):
        self.assertRaises(DatasetAlreadyExistsException, self.mgr.create_dataset, self.dataset_name, 'beer-sample',
                          CreateDatasetOptions(dataverse_name=self.dataverse_name))
        self.mgr.create_dataset(self.dataset_name, 'beer-sample',
                                CreateDatasetOptions(dataverse_name=self.dataverse_name), ignore_if_exists=True)

    def test_drop_dataset(self):
        self.mgr.drop_dataset(self.dataset_name, DropDatasetOptions(dataverse_name=self.dataverse_name))
        self.assertRaises(DatasetNotFoundException, self.mgr.drop_dataset, self.dataset_name,
                          DropDatasetOptions(dataverse_name=self.dataverse_name))
        self.mgr.drop_dataset(self.dataset_name, DropDatasetOptions(dataverse_name=self.dataverse_name,
                                                                    ignore_if_not_exists=True))

    def test_create_index(self):
        self.mgr.create_index("test_brewery_idx", self.dataset_name,
                              {'name': AnalyticsDataType.STRING, 'description': AnalyticsDataType.STRING },
                              CreateAnalyticsIndexOptions(dataverse_name=self.dataverse_name))

        def check_for_idx(idx):
            indexes = self.mgr.get_all_indexes()
            for index in indexes:
                print(index)
                if index.name == idx:
                    return
            raise Exception("unable to find 'test_brewery_idx' in list of all indexes")

        self.try_n_times(10, 3, check_for_idx, 'test_brewery_idx')

    def test_drop_index(self):
        # create one first, if not already there
        self.mgr.create_index("test_brewery_idx", self.dataset_name,
                              {'name': AnalyticsDataType.STRING, 'description': AnalyticsDataType.STRING },
                              CreateAnalyticsIndexOptions(dataverse_name=self.dataverse_name))

        def check_for_idx(idx):
            indexes = self.mgr.get_all_indexes()
            for index in indexes:
                print(index)
                if index.name == idx:
                    return
            raise Exception("unable to find 'test_brewery_idx' in list of all indexes")

        self.try_n_times(10, 3, check_for_idx, 'test_brewery_idx')
        self.mgr.drop_index("test_brewery_idx", self.dataset_name,
                            DropAnalyticsIndexOptions(dataverse_name=self.dataverse_name))
        self.try_n_times_till_exception(10, 3, check_for_idx, 'test_brewery_idx')

    def test_connect_link(self):
        self.mgr.connect_link(ConnectLinkOptions(dataverse_name=self.dataverse_name))

        # connect link should result in documents in the dataset, so...
        self.assertRows('USE `{}`; SELECT * FROM `{}` LIMIT 1'.format(self.dataverse_name, self.dataset_name))
        # manually stop it for now
        self.cluster.analytics_query(
            'USE `{}`; DISCONNECT LINK Local'.format(self.dataverse_name, self.dataset_name)).metadata()

    def test_get_pending_mutations(self):
        try:
            result = self.mgr.get_pending_mutations()
            # we expect no test_dataverse key yet
            print(result)
            self.assertFalse("test_dataverse" in result.keys())
            self.mgr.connect_link(ConnectLinkOptions(dataverse_name=self.dataverse_name))
            time.sleep(5)
            result = self.mgr.get_pending_mutations()
            print(result)
            self.assertTrue("test_dataverse" in result.keys())
        except NotSupportedException:
            raise SkipTest("get pending mutations not supported on this cluster")

