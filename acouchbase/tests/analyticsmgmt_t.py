#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.exceptions import (AnalyticsLinkExistsException,
                                  AnalyticsLinkNotFoundException,
                                  CouchbaseException,
                                  DatasetAlreadyExistsException,
                                  DatasetNotFoundException,
                                  DataverseAlreadyExistsException,
                                  DataverseNotFoundException,
                                  InternalServerFailureException,
                                  InvalidArgumentException)
from couchbase.management.analytics import (AnalyticsDataType,
                                            AnalyticsEncryptionLevel,
                                            AnalyticsLinkType,
                                            AzureBlobExternalAnalyticsLink,
                                            CouchbaseAnalyticsEncryptionSettings,
                                            CouchbaseRemoteAnalyticsLink,
                                            S3ExternalAnalyticsLink)
from couchbase.management.options import (ConnectLinkOptions,
                                          CreateAnalyticsIndexOptions,
                                          CreateDatasetOptions,
                                          CreateDataverseOptions,
                                          DisconnectLinkOptions,
                                          DropAnalyticsIndexOptions,
                                          DropDatasetOptions,
                                          DropDataverseOptions,
                                          GetLinksAnalyticsOptions)

from ._test_utils import TestEnvironment


class AnalyticsManagementTests:
    DATASET_NAME = 'test-dataset'

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_analytics=True)
        yield cb_env

    @pytest.fixture(scope="class")
    def empty_dataverse_name(self, cb_env):
        if cb_env.server_version_short >= 7.0:
            name = 'empty/dataverse'
        else:
            name = 'empty_dataverse'
        return name

    @pytest_asyncio.fixture()
    async def create_empty_dataverse(self, cb_env, empty_dataverse_name):
        await cb_env.am.create_dataverse(empty_dataverse_name, ignore_if_exists=True)

    @pytest_asyncio.fixture()
    async def drop_empty_dataverse(self, cb_env, empty_dataverse_name):
        yield
        await cb_env.am.drop_dataverse(empty_dataverse_name, ignore_if_not_exists=True)

    @pytest_asyncio.fixture()
    async def create_empty_dataset(self, cb_env):
        await cb_env.am.create_dataset(self.DATASET_NAME, cb_env.bucket.name, ignore_if_exists=True)

    @pytest_asyncio.fixture()
    async def drop_empty_dataset(self, cb_env):
        yield
        await cb_env.am.drop_dataset(self.DATASET_NAME, ignore_if_not_exists=True)

    @pytest_asyncio.fixture()
    async def clean_drop(self, cb_env, empty_dataverse_name):
        yield
        await cb_env.am.drop_dataset(self.DATASET_NAME, ignore_if_not_exists=True)
        await cb_env.am.drop_dataset(self.DATASET_NAME,
                                     DropDatasetOptions(ignore_if_not_exists=True,
                                                        dataverse_name=empty_dataverse_name))
        await cb_env.am.drop_dataverse(empty_dataverse_name, ignore_if_not_exists=True)

    @pytest.mark.usefixtures("drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_create_dataverse(self, cb_env, empty_dataverse_name):
        await cb_env.am.create_dataverse(empty_dataverse_name)

    @pytest.mark.usefixtures("drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_create_dataverse_ignore_exists(self, cb_env, empty_dataverse_name):
        await cb_env.am.create_dataverse(
            empty_dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        with pytest.raises(DataverseAlreadyExistsException):
            await cb_env.am.create_dataverse(empty_dataverse_name)

    @pytest.mark.usefixtures("create_empty_dataverse")
    @pytest.mark.usefixtures("drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_drop_dataverse(self, cb_env, empty_dataverse_name):
        await cb_env.am.drop_dataverse(empty_dataverse_name)

    @pytest.mark.usefixtures("create_empty_dataverse")
    @pytest.mark.usefixtures("drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_drop_dataverse_ignore_not_exists(self, cb_env, empty_dataverse_name):
        await cb_env.am.drop_dataverse(empty_dataverse_name)
        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.drop_dataverse(empty_dataverse_name)
        await cb_env.am.drop_dataverse(empty_dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    @pytest.mark.usefixtures("drop_empty_dataset")
    @pytest.mark.asyncio
    async def test_create_dataset(self, cb_env):
        await cb_env.am.create_dataset(self.DATASET_NAME, cb_env.bucket.name)

    @pytest.mark.usefixtures("drop_empty_dataset")
    @pytest.mark.asyncio
    async def test_create_dataset_ignore_exists(self, cb_env):
        await cb_env.am.create_dataset(self.DATASET_NAME, cb_env.bucket.name)
        with pytest.raises(DatasetAlreadyExistsException):
            await cb_env.am.create_dataset(self.DATASET_NAME, cb_env.bucket.name)

        await cb_env.am.create_dataset(self.DATASET_NAME,
                                       cb_env.bucket.name,
                                       CreateDatasetOptions(ignore_if_exists=True))

    @pytest.mark.usefixtures("create_empty_dataset")
    @pytest.mark.usefixtures("drop_empty_dataset")
    @pytest.mark.asyncio
    async def test_drop_dataset(self, cb_env):
        await cb_env.am.drop_dataset(self.DATASET_NAME)

    @pytest.mark.usefixtures("create_empty_dataset")
    @pytest.mark.usefixtures("drop_empty_dataset")
    @pytest.mark.asyncio
    async def test_drop_dataset_ignore_not_exists(self, cb_env):
        await cb_env.am.drop_dataset(self.DATASET_NAME)
        with pytest.raises(DatasetNotFoundException):
            await cb_env.am.drop_dataset(self.DATASET_NAME)
        await cb_env.am.drop_dataset(self.DATASET_NAME, DropDatasetOptions(ignore_if_not_exists=True))

    @pytest.mark.usefixtures("create_empty_dataverse")
    @pytest.mark.usefixtures("clean_drop")
    @pytest.mark.asyncio
    async def test_get_all_datasets(self, cb_env, empty_dataverse_name):
        await cb_env.am.create_dataset(self.DATASET_NAME, cb_env.bucket.name, ignore_if_exists=True)
        await cb_env.am.create_dataset(self.DATASET_NAME,
                                       cb_env.bucket.name,
                                       CreateDatasetOptions(dataverse_name=empty_dataverse_name,
                                                            ignore_if_exists=True))

        datasets = await cb_env.am.get_all_datasets()
        local_ds = [ds for ds in datasets if ds.dataset_name == self.DATASET_NAME]
        assert len(local_ds) == 2
        assert any(map(lambda ds: ds.dataverse_name == 'Default', local_ds)) is True
        assert any(map(lambda ds: ds.dataverse_name == empty_dataverse_name, local_ds)) is True

    @pytest.mark.usefixtures("create_empty_dataverse")
    @pytest.mark.usefixtures("clean_drop")
    @pytest.mark.asyncio
    async def test_create_index(self, cb_env, empty_dataverse_name):
        await cb_env.am.create_dataset(self.DATASET_NAME,
                                       cb_env.bucket.name,
                                       CreateDatasetOptions(dataverse_name=empty_dataverse_name, ignore_if_exists=True))
        await cb_env.am.create_index("test_idx", self.DATASET_NAME,
                                     {'name': AnalyticsDataType.STRING,
                                      'description': AnalyticsDataType.STRING},
                                     CreateAnalyticsIndexOptions(dataverse_name=empty_dataverse_name))

        async def check_for_idx(idx):
            indexes = await cb_env.am.get_all_indexes()
            for index in indexes:
                if index.name == idx:
                    return
            raise Exception(
                "unable to find 'test_idx' in list of all indexes")

        await cb_env.try_n_times(10, 3, check_for_idx, 'test_idx')

    @pytest.mark.usefixtures("create_empty_dataverse")
    @pytest.mark.usefixtures("clean_drop")
    @pytest.mark.asyncio
    async def test_drop_index(self, cb_env, empty_dataverse_name):
        # create one first, if not already there
        await cb_env.am.create_dataset(self.DATASET_NAME,
                                       cb_env.bucket.name,
                                       CreateDatasetOptions(dataverse_name=empty_dataverse_name, ignore_if_exists=True))
        await cb_env.am.create_index("test_idx", self.DATASET_NAME,
                                     {'name': AnalyticsDataType.STRING,
                                      'description': AnalyticsDataType.STRING},
                                     CreateAnalyticsIndexOptions(dataverse_name=empty_dataverse_name))

        async def check_for_idx(idx):
            indexes = await cb_env.am.get_all_indexes()
            for index in indexes:
                if index.name == idx:
                    return
            raise Exception(
                "unable to find 'test_idx' in list of all indexes")

        await cb_env.try_n_times(10, 3, check_for_idx, 'test_idx')
        await cb_env.am.drop_index("test_idx", self.DATASET_NAME,
                                   DropAnalyticsIndexOptions(dataverse_name=empty_dataverse_name))
        await cb_env.try_n_times_till_exception(
            10, 3, check_for_idx, 'test_idx')

    @pytest.mark.usefixtures("create_empty_dataverse")
    @pytest.mark.usefixtures("clean_drop")
    @pytest.mark.asyncio
    async def test_connect_disconnect_link(self, cb_env, empty_dataverse_name):
        await cb_env.am.create_dataset(self.DATASET_NAME,
                                       cb_env.bucket.name,
                                       CreateDatasetOptions(dataverse_name=empty_dataverse_name,
                                                            ignore_if_exists=True))
        await cb_env.am.connect_link(ConnectLinkOptions(dataverse_name=empty_dataverse_name))

        # # connect link should result in documents in the dataset, so...
        # dataverse_name = self.mgr._scrub_dataverse_name(self.dataverse_name)
        # self.assertRows(
        #     'USE {}; SELECT * FROM `{}` LIMIT 1'.format(dataverse_name, self.dataset_name))
        # # manually stop it for now
        # self.cluster.analytics_query(
        #     'USE {}; DISCONNECT LINK Local'.format(dataverse_name, self.dataset_name)).metadata()
        await cb_env.am.disconnect_link(DisconnectLinkOptions(dataverse_name=empty_dataverse_name))

    @pytest.mark.usefixtures("create_empty_dataverse")
    @pytest.mark.usefixtures("clean_drop")
    @pytest.mark.asyncio
    async def test_get_pending_mutations(self, cb_env, empty_dataverse_name):
        cb_env.check_if_feature_supported('analytics_pending_mutations')
        dv_name = empty_dataverse_name.replace('/', '.')
        key = f'{dv_name}.{self.DATASET_NAME}'
        result = await cb_env.am.get_pending_mutations()
        assert key not in result.keys()
        await cb_env.am.create_dataset(self.DATASET_NAME,
                                       cb_env.bucket.name,
                                       CreateDatasetOptions(dataverse_name=empty_dataverse_name,
                                                            ignore_if_exists=True))
        await cb_env.am.connect_link(ConnectLinkOptions(dataverse_name=empty_dataverse_name))
        await asyncio.sleep(1)
        result = await cb_env.am.get_pending_mutations()
        assert key in result.keys()
        await cb_env.am.disconnect_link(DisconnectLinkOptions(dataverse_name=empty_dataverse_name))

    @pytest.mark.asyncio
    async def test_v6_dataverse_name_parsing(self, cb_env):
        if cb_env.server_version_short >= 7.0:
            pytest.skip("Test only for 6.x versions")

        # test.test_dataverse, valid format which is valid >= 6.0, but not on 6.6...weird
        if cb_env.server_version_short >= 6.6:
            with pytest.raises(CouchbaseException):
                await cb_env.am.create_dataverse(
                    "test.test_dataverse", CreateDataverseOptions(ignore_if_exists=True))
        else:
            await cb_env.am.create_dataverse(
                "test.test_dataverse", CreateDataverseOptions(ignore_if_exists=True))

        # test/test_dataverse, invalid format < 7.0
        with pytest.raises((InternalServerFailureException, CouchbaseException)):
            await cb_env.am.create_dataverse(
                "test/test_dataverse", CreateDataverseOptions(ignore_if_exists=True))

    @pytest.mark.asyncio
    async def test_v7_dataverse_name_parsing(self, cb_env):
        if cb_env.server_version_short < 7.0:
            pytest.skip("Test only for 7.x versions")

        # test.test_dataverse, valid format which is valid >= 6.6
        await cb_env.am.create_dataverse(
            "test.test_dataverse", CreateDataverseOptions(ignore_if_exists=True))
        await cb_env.am.drop_dataverse("test.test_dataverse")

        # test/test_dataverse, valideformat which is valid >= 7.0
        await cb_env.am.create_dataverse(
            "test/test_dataverse", CreateDataverseOptions(ignore_if_exists=True))
        await cb_env.am.drop_dataverse("test/test_dataverse")


class AnalyticsManagementLinksTests:
    DATASET_NAME = 'test-dataset'

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_analytics=True)

        cb_env.check_if_feature_supported('analytics_link_mgmt')
        yield cb_env

    @pytest.fixture(scope="class")
    def empty_dataverse_name(self, cb_env):
        if cb_env.server_version_short >= 7.0:
            name = 'empty/dataverse'
        else:
            name = 'empty_dataverse'
        return name

    @pytest_asyncio.fixture()
    async def create_drop_empty_dataverse(self, cb_env, empty_dataverse_name):
        await cb_env.am.create_dataverse(empty_dataverse_name, ignore_if_exists=True)
        yield
        await cb_env.am.drop_dataverse(empty_dataverse_name, ignore_if_not_exists=True)

    @pytest.mark.usefixtures('cb_env')
    def test_couchbase_remote_link_encode(self):
        link = CouchbaseRemoteAnalyticsLink("test_dataverse",
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        encoded = link.as_dict()
        assert isinstance(encoded, dict)
        assert encoded.get('hostname') == 'localhost'
        assert encoded.get('link_type') == AnalyticsLinkType.CouchbaseRemote.value
        link_encryption = encoded.get('encryption', None)
        assert isinstance(link_encryption, dict)
        assert link_encryption.get('encryption_level') == AnalyticsEncryptionLevel.NONE.value
        assert encoded.get('username') == 'Administrator'
        assert encoded.get('password') == 'password'

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

        encoded = link.as_dict()
        assert isinstance(encoded, dict)
        assert encoded.get('hostname') == 'localhost'
        assert encoded.get('link_type') == AnalyticsLinkType.CouchbaseRemote.value
        link_encryption = encoded.get('encryption', None)
        assert isinstance(link_encryption, dict)
        assert link_encryption.get('encryption_level') == AnalyticsEncryptionLevel.FULL.value
        assert link_encryption.get('certificate') == 'certificate'
        assert link_encryption.get('client_certificate') == 'clientcertificate'
        assert link_encryption.get('client_key') == 'clientkey'

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_create_s3_external_link(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        await cb_env.am.create_link(link)

        links = await cb_env.am.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == link.dataverse_name()
        assert links[0].name() == link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == link._region
        assert links[0]._access_key_id == link._access_key_id

        await cb_env.am.drop_link(link.name(), empty_dataverse_name)

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_replace_s3_external_link(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        await cb_env.am.create_link(link)

        links = await cb_env.am.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == link.dataverse_name()
        assert links[0].name() == link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == link._region
        assert links[0]._access_key_id == link._access_key_id

        new_link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                           "s3link",
                                           "accesskey",
                                           "eu-west-2",
                                           secret_access_key="mysupersecretkey1",
                                           )

        await cb_env.am.replace_link(new_link)

        links = await cb_env.am.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == new_link.dataverse_name()
        assert links[0].name() == new_link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == new_link._region
        assert links[0]._access_key_id == new_link._access_key_id

        await cb_env.am.drop_link(link.name(), empty_dataverse_name)

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_drop_s3_external_link(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        await cb_env.am.create_link(link)

        links = await cb_env.am.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == link.dataverse_name()
        assert links[0].name() == link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == link._region
        assert links[0]._access_key_id == link._access_key_id

        await cb_env.am.drop_link(link.name(), empty_dataverse_name)

        links = await cb_env.am.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 0

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_create_link_fail_link_exists(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        await cb_env.am.create_link(link)

        with pytest.raises(AnalyticsLinkExistsException):
            await cb_env.am.create_link(link)

        await cb_env.am.drop_link(link.name(), empty_dataverse_name)

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_s3_link_fail_dataverse_not_found(self, cb_env):

        link = S3ExternalAnalyticsLink("notadataverse",
                                       "s3link",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.create_link(link)

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.replace_link(link)

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_couchbase_link_fail_dataverse_not_found(self, cb_env):

        link = CouchbaseRemoteAnalyticsLink("notadataverse",
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.create_link(link)

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.replace_link(link)

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_azure_link_fail_dataverse_not_found(self, cb_env):

        link = AzureBlobExternalAnalyticsLink("notadataverse",
                                              "azurebloblink",
                                              account_name="myaccount",
                                              account_key="myaccountkey")

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.create_link(link)

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.replace_link(link)

        with pytest.raises(DataverseNotFoundException):
            await cb_env.am.drop_link(link.name(), link.dataverse_name())

    @pytest.fixture()
    def bad_couchbase_remote_links(self, empty_dataverse_name):
        links = []
        links.append(CouchbaseRemoteAnalyticsLink("",
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username="Administrator",
                                                  password="password"))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username="Administrator",
                                                  password="password"))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username="Administrator",
                                                  password="password"))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  password="password"))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username="Administrator"))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.HALF),
                                                  password="password"))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.HALF),
                                                  username="Administrator"))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL)
                                                  ))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL,
                                                      certificate=bytes('certificate', 'utf-8'))
                                                  ))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL,
                                                      certificate=bytes(
                                                          'certificate', 'utf-8'),
                                                      client_certificate=bytes('clientcert', 'utf-8'))
                                                  ))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  "cbremote",
                                                  "localhost",
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL,
                                                      certificate=bytes(
                                                          'certificate', 'utf-8'),
                                                      client_key=bytes('clientkey', 'utf-8'))
                                                  ))

        return links

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_create_couchbase_link_fail_invalid_argument(self, cb_env, bad_couchbase_remote_links):
        for link in bad_couchbase_remote_links:
            with pytest.raises(InvalidArgumentException):
                await cb_env.am.create_link(link)

    @pytest.fixture()
    def bad_s3_external_links(self, empty_dataverse_name):
        links = []
        links.append(S3ExternalAnalyticsLink("",
                                             "s3link",
                                             "accesskey",
                                             "us-west-2",
                                             secret_access_key="mysupersecretkey",
                                             ))

        links.append(S3ExternalAnalyticsLink(empty_dataverse_name,
                                             "",
                                             "accesskey",
                                             "us-west-2",
                                             secret_access_key="mysupersecretkey",
                                             ))

        links.append(S3ExternalAnalyticsLink(empty_dataverse_name,
                                             "s3link",
                                             "",
                                             "us-west-2",
                                             secret_access_key="mysupersecretkey",
                                             ))

        links.append(S3ExternalAnalyticsLink(empty_dataverse_name,
                                             "s3link",
                                             "accesskey",
                                             "",
                                             secret_access_key="mysupersecretkey",
                                             ))

        links.append(S3ExternalAnalyticsLink("",
                                             "s3link",
                                             "accesskey",
                                             "us-west-2",
                                             ))
        return links

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_create_s3_link_fail_invalid_argument(self, cb_env, bad_s3_external_links):
        for link in bad_s3_external_links:
            with pytest.raises(InvalidArgumentException):
                await cb_env.am.create_link(link)

    @pytest.fixture()
    def bad_azure_blob_external_links(self, empty_dataverse_name):
        links = []
        links.append(AzureBlobExternalAnalyticsLink("",
                                                    "azurebloblink",
                                                    account_name="myaccount",
                                                    account_key="myaccountkey"))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    "",
                                                    account_name="myaccount",
                                                    account_key="myaccountkey"))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    "azurebloblink"))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    "azurebloblink",
                                                    account_name="myaccount"))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    "azurebloblink",
                                                    account_key="myaccountkey"))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    "azurebloblink",
                                                    shared_access_signature="sharedaccesssignature"))
        return links

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_create_azure_block_link_fail_invalid_argument(self, cb_env, bad_azure_blob_external_links):
        for link in bad_azure_blob_external_links:
            with pytest.raises(InvalidArgumentException):
                await cb_env.am.create_link(link)

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_s3_link_fail_link_not_found(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       "notalink",
                                       "accesskey",
                                       "us-west-2",
                                       secret_access_key="mysupersecretkey",
                                       )

        with pytest.raises(AnalyticsLinkNotFoundException):
            await cb_env.am.replace_link(link)

        with pytest.raises(AnalyticsLinkNotFoundException):
            await cb_env.am.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_couchbase_link_fail_link_not_found(self, cb_env, empty_dataverse_name):

        link = CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                            "cbremote",
                                            "localhost",
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username="Administrator",
                                            password="password")

        with pytest.raises(AnalyticsLinkNotFoundException):
            await cb_env.am.replace_link(link)

        with pytest.raises(AnalyticsLinkNotFoundException):
            await cb_env.am.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures("create_drop_empty_dataverse")
    @pytest.mark.asyncio
    async def test_azure_link_fail_link_not_found(self, cb_env, empty_dataverse_name):

        link = AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                              "azurebloblink",
                                              account_name="myaccount",
                                              account_key="myaccountkey")

        with pytest.raises(AnalyticsLinkNotFoundException):
            await cb_env.am.replace_link(link)

        with pytest.raises(AnalyticsLinkNotFoundException):
            await cb_env.am.drop_link(link.name(), link.dataverse_name())

    # @TODO:  neither situation raises an exception...
    # @pytest.mark.usefixtures("create_drop_empty_dataverse")
    # @pytest.mark.asyncio
    # async def test_get_links_fail(self, cb_env):

    #     with pytest.raises(DataverseNotFoundException):
    #         await cb_env.am.get_links(GetLinksAnalyticsOptions(
    #             dataverse_name="notadataverse"))

    #     with pytest.raises(InvalidArgumentException):
    #         await cb_env.am.get_links(GetLinksAnalyticsOptions(name="mylink"))
