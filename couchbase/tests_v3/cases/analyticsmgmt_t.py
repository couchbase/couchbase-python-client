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
import time
import os

from couchbase_core import ulp
from couchbase_tests.base import CollectionTestCase, SkipTest
from couchbase.management.analytics import (CreateDataverseOptions, DropDataverseOptions, CreateDatasetOptions,
                                            CreateAnalyticsIndexOptions, DropAnalyticsIndexOptions,
                                            DropDatasetOptions, ConnectLinkOptions, DisconnectLinkOptions,
                                            GetLinksAnalyticsOptions)
from couchbase.exceptions import (AnalyticsLinkExistsException, DataverseAlreadyExistsException,
                                  DataverseNotFoundException, DatasetAlreadyExistsException, DatasetNotFoundException,
                                  InvalidArgumentException, NotSupportedException, CompilationFailedException,
                                  ParsingFailedException, AnalyticsLinkNotFoundException)
from couchbase.analytics import (AnalyticsDataType, AnalyticsEncryptionLevel,
                                 AnalyticsLink, AnalyticsLinkType, AzureBlobExternalAnalyticsLink,
                                 CouchbaseAnalyticsEncryptionSettings, CouchbaseRemoteAnalyticsLink, S3ExternalAnalyticsLink)


class AnalyticsIndexManagerTests(CollectionTestCase):
    def setUp(self):
        super(AnalyticsIndexManagerTests, self).setUp()
        self._enable_print_statements = False
        if self.is_mock:
            raise SkipTest("mock doesn't mock management apis")

        if int(self.get_cluster_version().split('.')[0]) < 6:
            raise SkipTest("no analytics in {}".format(
                self.get_cluster_version()))

        self.mgr = self.cluster.analytics_indexes()
        self.dataverse_name = "test/dataverse" if int(
            self.get_cluster_version().split('.')[0]) == 7 else "test_dataverse"
        self.dataset_name = "test_breweries"
        # be sure the dataverse exists
        self.mgr.create_dataverse(
            self.dataverse_name, CreateDataverseOptions(ignore_if_exists=True))
        # now ensure our dataset in there
        self.mgr.create_dataset(self.dataset_name,
                                "beer-sample",
                                CreateDatasetOptions(dataverse_name=self.dataverse_name,
                                                     condition='`type` = "brewery"',
                                                     ignore_if_exists=True)
                                )
        try:
            self.mgr.disconnect_link(DisconnectLinkOptions(
                dataverse_name=self.dataverse_name))
        except:
            pass

    def tearDown(self):
        super(AnalyticsIndexManagerTests, self).tearDown()
        # be sure the dataverse doesn't exist
        try:
            dataverse_name = self.mgr._scrub_dataverse_name(
                self.dataverse_name)
            self.cluster.analytics_query(
                "USE {}; DISCONNECT LINK Local;".format(dataverse_name)).metadata()
        except DataverseNotFoundException:
            pass
        try:
            self.mgr.disconnect_link(DisconnectLinkOptions(
                dataverse_name=self.dataverse_name))
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
        statement = 'SELECT * FROM Metadata.`Dataverse` WHERE DataverseName="{}";'.format(
            self.dataverse_name)
        result = self.cluster.analytics_query(statement)
        self.assertEqual(1, len(result.rows()))

    def test_create_dataverse_ignore_exists(self):
        self.assertRaises(DataverseAlreadyExistsException,
                          self.mgr.create_dataverse, self.dataverse_name)
        self.mgr.create_dataverse(
            self.dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

    def test_drop_dataverse(self):
        self.mgr.drop_dataverse(self.dataverse_name)
        self.mgr.connect_link()
        statement = 'SELECT * FROM Metadata.`Dataverse` WHERE DataverseName="{}";'.format(
            self.dataverse_name)
        result = self.cluster.analytics_query(statement)
        self.assertEqual(0, len(result.rows()))

    def test_drop_dataverse_ignore_not_exists(self):
        self.mgr.drop_dataverse(self.dataverse_name)
        self.assertRaises(DataverseNotFoundException,
                          self.mgr.drop_dataverse, self.dataverse_name)
        self.mgr.drop_dataverse(
            self.dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_create_dataset(self):
        # we put a dataset in during the setUp, so...
        datasets = self.mgr.get_all_datasets()
        for dataset in datasets:
            if self._enable_print_statements:
                print(dataset)
            if dataset.dataset_name == self.dataset_name:
                return
        self.fail("didn't find {} in listing of all datasets".format(
            self.dataset_name))

    def test_create_dataset_ignore_exists(self):
        self.assertRaises(DatasetAlreadyExistsException, self.mgr.create_dataset, self.dataset_name, 'beer-sample',
                          CreateDatasetOptions(dataverse_name=self.dataverse_name))
        self.mgr.create_dataset(self.dataset_name, 'beer-sample',
                                CreateDatasetOptions(dataverse_name=self.dataverse_name), ignore_if_exists=True)

    def test_drop_dataset(self):
        self.mgr.drop_dataset(self.dataset_name, DropDatasetOptions(
            dataverse_name=self.dataverse_name))
        self.assertRaises(DatasetNotFoundException, self.mgr.drop_dataset, self.dataset_name,
                          DropDatasetOptions(dataverse_name=self.dataverse_name))
        self.mgr.drop_dataset(self.dataset_name, DropDatasetOptions(dataverse_name=self.dataverse_name,
                                                                    ignore_if_not_exists=True))

    def test_create_index(self):
        self.mgr.create_index("test_brewery_idx", self.dataset_name,
                              {'name': AnalyticsDataType.STRING,
                                  'description': AnalyticsDataType.STRING},
                              CreateAnalyticsIndexOptions(dataverse_name=self.dataverse_name))

        def check_for_idx(idx):
            indexes = self.mgr.get_all_indexes()
            for index in indexes:
                if self._enable_print_statements:
                    print(index)
                if index.name == idx:
                    return
            raise Exception(
                "unable to find 'test_brewery_idx' in list of all indexes")

        self.try_n_times(10, 3, check_for_idx, 'test_brewery_idx')

    def test_drop_index(self):
        # create one first, if not already there
        self.mgr.create_index("test_brewery_idx", self.dataset_name,
                              {'name': AnalyticsDataType.STRING,
                                  'description': AnalyticsDataType.STRING},
                              CreateAnalyticsIndexOptions(dataverse_name=self.dataverse_name))

        def check_for_idx(idx):
            indexes = self.mgr.get_all_indexes()
            for index in indexes:
                if self._enable_print_statements:
                    print(index)
                if index.name == idx:
                    return
            raise Exception(
                "unable to find 'test_brewery_idx' in list of all indexes")

        self.try_n_times(10, 3, check_for_idx, 'test_brewery_idx')
        self.mgr.drop_index("test_brewery_idx", self.dataset_name,
                            DropAnalyticsIndexOptions(dataverse_name=self.dataverse_name))
        self.try_n_times_till_exception(
            10, 3, check_for_idx, 'test_brewery_idx')

    def test_connect_link(self):
        self.mgr.connect_link(ConnectLinkOptions(
            dataverse_name=self.dataverse_name))

        # connect link should result in documents in the dataset, so...
        dataverse_name = self.mgr._scrub_dataverse_name(self.dataverse_name)
        self.assertRows(
            'USE {}; SELECT * FROM `{}` LIMIT 1'.format(dataverse_name, self.dataset_name))
        # manually stop it for now
        self.cluster.analytics_query(
            'USE {}; DISCONNECT LINK Local'.format(dataverse_name, self.dataset_name)).metadata()

    def test_get_pending_mutations(self):
        try:
            result = self.mgr.get_pending_mutations()
            if self._enable_print_statements:
                # we expect no test_dataverse key yet
                print(result)
            self.assertFalse("test_dataverse" in result.keys())
            self.mgr.connect_link(ConnectLinkOptions(
                dataverse_name=self.dataverse_name))
            time.sleep(5)
            result = self.mgr.get_pending_mutations()
            if self._enable_print_statements:
                print(result)
            dataverse_name = self.mgr._scrub_dataverse_name(
                self.dataverse_name).replace("`", "")
            self.assertTrue(dataverse_name in result.keys())
        except NotSupportedException:
            raise SkipTest(
                "get pending mutations not supported on this cluster")

    def test_v6_dataverse_name_parsing(self):
        if int(self.cluster_version.split('.')[0]) != 6:
            raise SkipTest("Test only for 6.x versions")

        with self.assertRaises(CompilationFailedException):
            # test.beer_sample => `test.beer_sample` which is not valid prior to 7.0
            self.mgr.create_dataverse(
                "test.beer_sample", CreateDataverseOptions(ignore_if_exists=True))

        # wish the analytics service was consistent here :/
        with self.assertRaises(ParsingFailedException):
            # test/beer_sample => `test`.`beer_sample` which is not valid prior to 7.0
            self.mgr.create_dataverse(
                "test/beer_sample", CreateDataverseOptions(ignore_if_exists=True))

    def test_v7_dataverse_name_parsing(self):
        if int(self.cluster_version.split('.')[0]) != 7:
            raise SkipTest("Test only for 7.x versions")

        # test.beer_sample => `test.beer_sample` which is valid >= 7.0
        self.mgr.create_dataverse(
            "test.beer_sample", CreateDataverseOptions(ignore_if_exists=True))

        statement = 'SELECT * FROM Metadata.`Dataverse` WHERE DataverseName="test.beer_sample";'.format(
            self.dataverse_name)
        result = self.cluster.analytics_query(statement)
        self.assertEqual(1, len(result.rows()))
        self.mgr.drop_dataverse("test.beer_sample")

        # test/beer_sample => `test`.`beer_sample` which is valid >= 7.0
        self.mgr.create_dataverse(
            "test/beer_sample", CreateDataverseOptions(ignore_if_exists=True))
        statement = 'SELECT * FROM Metadata.`Dataverse` WHERE DataverseName="test/beer_sample";'.format(
            self.dataverse_name)
        result = self.cluster.analytics_query(statement)
        self.assertEqual(1, len(result.rows()))
        self.mgr.drop_dataverse("test/beer_sample")


class AnalyticsIndexManagerLinkTests(CollectionTestCase):
    def setUp(self):
        super(AnalyticsIndexManagerLinkTests, self).setUp()
        if self.is_mock:
            raise SkipTest("mock doesn't mock management apis")

        if int(self.cluster_version.split('.')[0]) < 6:
            raise SkipTest("no analytics in {}".format(
                self.cluster_version))

        if int(self.cluster_version.split('.')[0]) < 7:
            raise SkipTest("No analytics link management API in {}".format(
                self.cluster_version))

        self.mgr = self.cluster.analytics_indexes()

    def test_couchbase_remote_link_encode(self):
        link = CouchbaseRemoteAnalyticsLink("test_dataverse",
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        encoded = link.form_encode().decode()
        query_str = ulp.parse_qs(encoded)
        self.assertEqual("localhost", query_str.get("hostname")[0])
        self.assertEqual(AnalyticsLinkType.CouchbaseRemote.value,
                         query_str.get("type")[0])
        self.assertEqual(AnalyticsEncryptionLevel.NONE.value,
                         query_str.get("encryption")[0])
        self.assertEqual("Administrator", query_str.get("username")[0])
        self.assertEqual("password", query_str.get("password")[0])

        link = CouchbaseRemoteAnalyticsLink("test_dataverse",
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.FULL,
                                                certificate=bytes(
                                                    'certificate', 'utf-8'),
                                                client_certificate=bytes(
                                                    'clientcertificate', 'utf-8'),
                                                client_key=bytes('clientkey', 'utf-8')),
                                            )

        encoded = link.form_encode().decode()
        query_str = ulp.parse_qs(encoded)
        self.assertEqual("localhost", query_str.get("hostname")[0])
        self.assertEqual(AnalyticsLinkType.CouchbaseRemote.value,
                         query_str.get("type")[0])
        self.assertEqual(AnalyticsEncryptionLevel.FULL.value,
                         query_str.get("encryption")[0])
        self.assertEqual("certificate", query_str.get("certificate")[0])
        self.assertEqual("clientcertificate",
                         query_str.get("clientCertificate")[0])
        self.assertEqual("clientkey", query_str.get("clientKey")[0])

    def test_s3_external_link(self):
        dataverse_name = "test_dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = S3ExternalAnalyticsLink(dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        link1 = S3ExternalAnalyticsLink(dataverse_name,
                                        "s3link1",
                                        "accesskey1",
                                        "us-east-2",
                                        secret_access_key="mysupersecretkey1",
                                        )

        self.mgr.create_link(link)
        self.mgr.create_link(link1)

        links = self.mgr.get_links()
        self.assertEqual(2, len(links))
        for l in links:
            link_match = (l.dataverse_name() == link.dataverse_name()
                          and l.name() == link.name()
                          and l.link_type() == AnalyticsLinkType.S3External
                          and l._region == link._region
                          and l._access_key_id == link._access_key_id)
            link1_match = (l.dataverse_name() == link1.dataverse_name()
                           and l.name() == link1.name()
                           and l.link_type() == AnalyticsLinkType.S3External
                           and l._region == link1._region
                           and l._access_key_id == link1._access_key_id)

            self.assertTrue(link_match or link1_match)

        links = self.mgr.get_links(GetLinksAnalyticsOptions(
            dataverse_name=dataverse_name, name=link.name()))

        self.assertEqual(1, len(links))
        self.assertTrue(links[0].dataverse_name() == link.dataverse_name()
                        and links[0].name() == link.name()
                        and links[0].link_type() == AnalyticsLinkType.S3External
                        and links[0]._region == link._region
                        and links[0]._access_key_id == link._access_key_id)

        new_link = S3ExternalAnalyticsLink(dataverse_name,
                                           "s3link",
                                           "accesskey",
                                           "eu-west-2",
                                           secret_access_key="mysupersecretkey1",
                                           )

        self.mgr.replace_link(new_link)

        links = self.mgr.get_links()
        self.assertEqual(2, len(links))

        links = self.mgr.get_links(GetLinksAnalyticsOptions(
            dataverse_name=dataverse_name, name=new_link.name()))

        self.assertEqual(1, len(links))
        self.assertTrue(links[0].dataverse_name() == new_link.dataverse_name()
                        and links[0].name() == new_link.name()
                        and links[0].link_type() == AnalyticsLinkType.S3External
                        and links[0]._region == new_link._region
                        and links[0]._access_key_id == new_link._access_key_id)

        self.mgr.drop_link("s3link", dataverse_name)
        self.mgr.drop_link("s3link1", dataverse_name)

        links = self.mgr.get_links()
        self.assertEqual(0, len(links))

        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_s3_external_link_compound_dataverse(self):
        dataverse_name = "test/dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = S3ExternalAnalyticsLink(dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        link1 = S3ExternalAnalyticsLink(dataverse_name,
                                        "s3link1",
                                        "accesskey1",
                                        "us-east-2",
                                        secret_access_key="mysupersecretkey1",
                                        )

        self.mgr.create_link(link)
        self.mgr.create_link(link1)

        links = self.mgr.get_links()
        self.assertEqual(2, len(links))
        for l in links:
            link_match = (l.dataverse_name() == link.dataverse_name()
                          and l.name() == link.name()
                          and l.link_type() == AnalyticsLinkType.S3External
                          and l._region == link._region
                          and l._access_key_id == link._access_key_id)
            link1_match = (l.dataverse_name() == link1.dataverse_name()
                           and l.name() == link1.name()
                           and l.link_type() == AnalyticsLinkType.S3External
                           and l._region == link1._region
                           and l._access_key_id == link1._access_key_id)

            self.assertTrue(link_match or link1_match)

        links = self.mgr.get_links(GetLinksAnalyticsOptions(
            dataverse_name=dataverse_name, name=link.name()))

        self.assertEqual(1, len(links))
        self.assertTrue(links[0].dataverse_name() == link.dataverse_name()
                        and links[0].name() == link.name()
                        and links[0].link_type() == AnalyticsLinkType.S3External
                        and links[0]._region == link._region
                        and links[0]._access_key_id == link._access_key_id)

        new_link = S3ExternalAnalyticsLink(dataverse_name,
                                           "s3link",
                                           "accesskey",
                                           "eu-west-2",
                                           secret_access_key="mysupersecretkey1",
                                           )

        self.mgr.replace_link(new_link)

        links = self.mgr.get_links()
        self.assertEqual(2, len(links))

        links = self.mgr.get_links(GetLinksAnalyticsOptions(
            dataverse_name=dataverse_name, name=new_link.name()))

        self.assertEqual(1, len(links))
        self.assertTrue(links[0].dataverse_name() == new_link.dataverse_name()
                        and links[0].name() == new_link.name()
                        and links[0].link_type() == AnalyticsLinkType.S3External
                        and links[0]._region == new_link._region
                        and links[0]._access_key_id == new_link._access_key_id)

        self.mgr.drop_link("s3link", dataverse_name)
        self.mgr.drop_link("s3link1", dataverse_name)

        links = self.mgr.get_links()
        self.assertEqual(0, len(links))

        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_create_link_fail_link_exists(self):
        dataverse_name = "test_dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = S3ExternalAnalyticsLink(dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        self.mgr.create_link(link)

        with self.assertRaises(AnalyticsLinkExistsException):
            self.mgr.create_link(link)

        self.mgr.drop_link("s3link", dataverse_name)
        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_link_fail_dataverse_not_found(self):
        dataverse_name = "test_dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = S3ExternalAnalyticsLink("notadataverse",
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.create_link(link)

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.replace_link(link)

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.drop_link(link.name(), link.dataverse_name())

        link = CouchbaseRemoteAnalyticsLink("notadataverse",
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.create_link(link)

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.replace_link(link)

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.drop_link(link.name(), link.dataverse_name())

        link = AzureBlobExternalAnalyticsLink("notadataverse",
                                              "azurebloblink",
                                              account_name="myaccount",
                                              account_key="myaccountkey")

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.create_link(link)

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.replace_link(link)

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.drop_link(link.name(), link.dataverse_name())

        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_create_couchbase_link_fail_invalid_argument(self):
        dataverse_name = "test_dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = CouchbaseRemoteAnalyticsLink("",
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            password="password")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.HALF),
                                            password="password")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.HALF),
                                            username="Administrator")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.FULL)
                                            )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.FULL,
                                                certificate=bytes('certificate', 'utf-8'))
                                            )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.FULL,
                                                certificate=bytes(
                                                    'certificate', 'utf-8'),
                                                client_certificate=bytes('clientcert', 'utf-8'))
                                            )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.FULL,
                                                certificate=bytes(
                                                    'certificate', 'utf-8'),
                                                client_key=bytes('clientkey', 'utf-8'))
                                            )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_create_s3_link_fail_invalid_argument(self):
        dataverse_name = "test_dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = S3ExternalAnalyticsLink("",
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = S3ExternalAnalyticsLink(dataverse_name,
                                       "",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = S3ExternalAnalyticsLink(dataverse_name,
                                       "s3link",
                                       "",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = S3ExternalAnalyticsLink(dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "",
                                       secret_access_key="mysupersecretkey",
                                       )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = S3ExternalAnalyticsLink("",
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       )

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_create_azureblob_link_fail_invalid_argument(self):
        dataverse_name = "test_dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = AzureBlobExternalAnalyticsLink("",
                                              "azurebloblink",
                                              account_name="myaccount",
                                              account_key="myaccountkey")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = AzureBlobExternalAnalyticsLink(dataverse_name,
                                              "",
                                              account_name="myaccount",
                                              account_key="myaccountkey")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = AzureBlobExternalAnalyticsLink(dataverse_name,
                                              "azurebloblink")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = AzureBlobExternalAnalyticsLink(dataverse_name,
                                              "azurebloblink",
                                              account_name="myaccount")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = AzureBlobExternalAnalyticsLink(dataverse_name,
                                              "azurebloblink",
                                              account_key="myaccountkey")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        link = AzureBlobExternalAnalyticsLink(dataverse_name,
                                              "azurebloblink",
                                              shared_access_signature="sharedaccesssignature")

        with self.assertRaises(InvalidArgumentException):
            self.mgr.create_link(link)

        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_link_fail_link_not_found(self):
        dataverse_name = "test_dataverse"
        self.mgr.create_dataverse(
            dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        link = S3ExternalAnalyticsLink(dataverse_name,
                                       "notalink",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        with self.assertRaises(AnalyticsLinkNotFoundException):
            self.mgr.replace_link(link)

        with self.assertRaises(AnalyticsLinkNotFoundException):
            self.mgr.drop_link(link.name(), link.dataverse_name())

        link = CouchbaseRemoteAnalyticsLink(dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        with self.assertRaises(AnalyticsLinkNotFoundException):
            self.mgr.replace_link(link)

        with self.assertRaises(AnalyticsLinkNotFoundException):
            self.mgr.drop_link(link.name(), link.dataverse_name())

        link = AzureBlobExternalAnalyticsLink(dataverse_name,
                                              "azurebloblink",
                                              account_name="myaccount",
                                              account_key="myaccountkey")

        with self.assertRaises(AnalyticsLinkNotFoundException):
            self.mgr.replace_link(link)

        with self.assertRaises(AnalyticsLinkNotFoundException):
            self.mgr.drop_link(link.name(), link.dataverse_name())

        self.mgr.drop_dataverse(
            dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    def test_get_links_fail(self):

        with self.assertRaises(DataverseNotFoundException):
            self.mgr.get_links(GetLinksAnalyticsOptions(
                dataverse_name="notadataverse"))

        with self.assertRaises(InvalidArgumentException):
            self.mgr.get_links(GetLinksAnalyticsOptions(name="mylink"))
