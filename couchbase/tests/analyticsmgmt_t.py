#  Copyright 2016-2023. Couchbase, Inc.
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

import pytest

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
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class AnalyticsManagementLinksTestSuite:
    TEST_MANIFEST = [
        'test_azure_link_fail_dataverse_not_found',
        'test_azure_link_fail_link_not_found',
        'test_couchbase_link_fail_dataverse_not_found',
        'test_couchbase_link_fail_link_not_found',
        'test_couchbase_remote_link_encode',
        'test_create_azure_block_link_fail_invalid_argument',
        'test_create_couchbase_link_fail_invalid_argument',
        'test_create_link_fail_link_exists',
        'test_create_s3_external_link',
        'test_create_s3_link_fail_invalid_argument',
        'test_drop_s3_external_link',
        'test_replace_s3_external_link',
        'test_s3_link_fail_dataverse_not_found',
        'test_s3_link_fail_link_not_found',
    ]

    @pytest.fixture()
    def bad_azure_blob_external_links(self, empty_dataverse_name):
        links = []
        links.append(AzureBlobExternalAnalyticsLink('',
                                                    'azurebloblink',
                                                    account_name='myaccount',
                                                    account_key='myaccountkey'))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    '',
                                                    account_name='myaccount',
                                                    account_key='myaccountkey'))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    'azurebloblink'))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    'azurebloblink',
                                                    account_name='myaccount'))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    'azurebloblink',
                                                    account_key='myaccountkey'))

        links.append(AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                                    'azurebloblink',
                                                    shared_access_signature='sharedaccesssignature'))
        return links

    @pytest.fixture()
    def bad_couchbase_remote_links(self, empty_dataverse_name):
        links = []
        links.append(CouchbaseRemoteAnalyticsLink('',
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username='Administrator',
                                                  password='password'))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  '',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username='Administrator',
                                                  password='password'))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  '',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username='Administrator',
                                                  password='password'))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  password='password'))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.NONE),
                                                  username='Administrator'))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.HALF),
                                                  password='password'))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.HALF),
                                                  username='Administrator'))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL)
                                                  ))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL,
                                                      certificate=bytes('certificate', 'utf-8'))
                                                  ))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL,
                                                      certificate=bytes(
                                                          'certificate', 'utf-8'),
                                                      client_certificate=bytes('clientcert', 'utf-8'))
                                                  ))

        links.append(CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                                  'cbremote',
                                                  'localhost',
                                                  CouchbaseAnalyticsEncryptionSettings(
                                                      AnalyticsEncryptionLevel.FULL,
                                                      certificate=bytes(
                                                          'certificate', 'utf-8'),
                                                      client_key=bytes('clientkey', 'utf-8'))
                                                  ))

        return links

    @pytest.fixture()
    def bad_s3_external_links(self, empty_dataverse_name):
        links = []
        links.append(S3ExternalAnalyticsLink('',
                                             's3link',
                                             'accesskey',
                                             'us-west-2',
                                             secret_access_key='mysupersecretkey',
                                             ))

        links.append(S3ExternalAnalyticsLink(empty_dataverse_name,
                                             '',
                                             'accesskey',
                                             'us-west-2',
                                             secret_access_key='mysupersecretkey',
                                             ))

        links.append(S3ExternalAnalyticsLink(empty_dataverse_name,
                                             's3link',
                                             '',
                                             'us-west-2',
                                             secret_access_key='mysupersecretkey',
                                             ))

        links.append(S3ExternalAnalyticsLink(empty_dataverse_name,
                                             's3link',
                                             'accesskey',
                                             '',
                                             secret_access_key='mysupersecretkey',
                                             ))

        links.append(S3ExternalAnalyticsLink('',
                                             's3link',
                                             'accesskey',
                                             'us-west-2',
                                             ))
        return links

    @pytest.fixture()
    def create_drop_empty_dataverse(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataverse(empty_dataverse_name, ignore_if_exists=True)
        yield
        cb_env.aixm.drop_dataverse(empty_dataverse_name, ignore_if_not_exists=True)

    @pytest.fixture(scope='class')
    def empty_dataverse_name(self, cb_env):
        if cb_env.server_version_short >= 7.0:
            name = 'empty/dataverse'
        else:
            name = 'empty_dataverse'
        return name

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_azure_link_fail_dataverse_not_found(self, cb_env):

        link = AzureBlobExternalAnalyticsLink('notadataverse',
                                              'azurebloblink',
                                              account_name='myaccount',
                                              account_key='myaccountkey')

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.create_link(link)

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.replace_link(link)

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_azure_link_fail_link_not_found(self, cb_env, empty_dataverse_name):

        link = AzureBlobExternalAnalyticsLink(empty_dataverse_name,
                                              'azurebloblink',
                                              account_name='myaccount',
                                              account_key='myaccountkey')

        with pytest.raises(AnalyticsLinkNotFoundException):
            cb_env.aixm.replace_link(link)

        with pytest.raises(AnalyticsLinkNotFoundException):
            cb_env.aixm.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_couchbase_link_fail_dataverse_not_found(self, cb_env):

        link = CouchbaseRemoteAnalyticsLink("notadataverse",
                                            'cbremote',
                                            'localhost',
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username='Administrator',
                                            password='password')

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.create_link(link)

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.replace_link(link)

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_couchbase_link_fail_link_not_found(self, cb_env, empty_dataverse_name):

        link = CouchbaseRemoteAnalyticsLink(empty_dataverse_name,
                                            'cbremote',
                                            'localhost',
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username='Administrator',
                                            password='password')

        with pytest.raises(AnalyticsLinkNotFoundException):
            cb_env.aixm.replace_link(link)

        with pytest.raises(AnalyticsLinkNotFoundException):
            cb_env.aixm.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures('cb_env')
    def test_couchbase_remote_link_encode(self):
        link = CouchbaseRemoteAnalyticsLink('test_dataverse',
                                            'cbremote',
                                            'localhost',
                                            CouchbaseAnalyticsEncryptionSettings(
                                                AnalyticsEncryptionLevel.NONE),
                                            username='Administrator',
                                            password='password')

        encoded = link.as_dict()
        assert isinstance(encoded, dict)
        assert encoded.get('hostname') == 'localhost'
        assert encoded.get('link_type') == AnalyticsLinkType.CouchbaseRemote.value
        link_encryption = encoded.get('encryption', None)
        assert isinstance(link_encryption, dict)
        assert link_encryption.get('encryption_level') == AnalyticsEncryptionLevel.NONE.value
        assert encoded.get('username') == 'Administrator'
        assert encoded.get('password') == 'password'

        link = CouchbaseRemoteAnalyticsLink('test_dataverse',
                                            'cbremote',
                                            'localhost',
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

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_create_azure_block_link_fail_invalid_argument(self, cb_env, bad_azure_blob_external_links):
        for link in bad_azure_blob_external_links:
            with pytest.raises(InvalidArgumentException):
                cb_env.aixm.create_link(link)

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_create_couchbase_link_fail_invalid_argument(self, cb_env, bad_couchbase_remote_links):
        for link in bad_couchbase_remote_links:
            with pytest.raises(InvalidArgumentException):
                cb_env.aixm.create_link(link)

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_create_link_fail_link_exists(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       's3link',
                                       'accesskey',
                                       'us-west-2',
                                       secret_access_key='mysupersecretkey',
                                       )

        cb_env.aixm.create_link(link)

        with pytest.raises(AnalyticsLinkExistsException):
            cb_env.aixm.create_link(link)

        cb_env.aixm.drop_link(link.name(), empty_dataverse_name)

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_create_s3_external_link(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       's3link',
                                       'accesskey',
                                       'us-west-2',
                                       secret_access_key='mysupersecretkey',
                                       )

        cb_env.aixm.create_link(link)

        links = cb_env.aixm.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == link.dataverse_name()
        assert links[0].name() == link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == link._region
        assert links[0]._access_key_id == link._access_key_id

        cb_env.aixm.drop_link(link.name(), empty_dataverse_name)

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_create_s3_link_fail_invalid_argument(self, cb_env, bad_s3_external_links):
        for link in bad_s3_external_links:
            with pytest.raises(InvalidArgumentException):
                cb_env.aixm.create_link(link)

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_drop_s3_external_link(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       's3link',
                                       'accesskey',
                                       'us-west-2',
                                       secret_access_key='mysupersecretkey',
                                       )

        cb_env.aixm.create_link(link)

        links = cb_env.aixm.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == link.dataverse_name()
        assert links[0].name() == link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == link._region
        assert links[0]._access_key_id == link._access_key_id

        cb_env.aixm.drop_link(link.name(), empty_dataverse_name)

        links = cb_env.aixm.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 0

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_replace_s3_external_link(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       's3link',
                                       'accesskey',
                                       'us-west-2',
                                       secret_access_key='mysupersecretkey',
                                       )

        cb_env.aixm.create_link(link)

        links = cb_env.aixm.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == link.dataverse_name()
        assert links[0].name() == link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == link._region
        assert links[0]._access_key_id == link._access_key_id

        new_link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                           's3link',
                                           'accesskey',
                                           'eu-west-2',
                                           secret_access_key='mysupersecretkey1',
                                           )

        cb_env.aixm.replace_link(new_link)

        links = cb_env.aixm.get_links(GetLinksAnalyticsOptions(
            dataverse_name=empty_dataverse_name, name=link.name()))

        assert len(links) == 1
        assert links[0].dataverse_name() == new_link.dataverse_name()
        assert links[0].name() == new_link.name()
        assert links[0].link_type() == AnalyticsLinkType.S3External
        assert links[0]._region == new_link._region
        assert links[0]._access_key_id == new_link._access_key_id

        cb_env.aixm.drop_link(link.name(), empty_dataverse_name)

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_s3_link_fail_dataverse_not_found(self, cb_env):

        link = S3ExternalAnalyticsLink("notadataverse",
                                       's3link',
                                       'accesskey',
                                       'us-west-2',
                                       secret_access_key='mysupersecretkey',
                                       )

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.create_link(link)

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.replace_link(link)

        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.drop_link(link.name(), link.dataverse_name())

    @pytest.mark.usefixtures('create_drop_empty_dataverse')
    def test_s3_link_fail_link_not_found(self, cb_env, empty_dataverse_name):

        link = S3ExternalAnalyticsLink(empty_dataverse_name,
                                       'notalink',
                                       'accesskey',
                                       'us-west-2',
                                       secret_access_key='mysupersecretkey',
                                       )

        with pytest.raises(AnalyticsLinkNotFoundException):
            cb_env.aixm.replace_link(link)

        with pytest.raises(AnalyticsLinkNotFoundException):
            cb_env.aixm.drop_link(link.name(), link.dataverse_name())


class AnalyticsManagementTestSuite:
    DATASET_NAME = 'test-dataset'

    TEST_MANIFEST = [
        'test_connect_disconnect_link',
        'test_create_dataset',
        'test_create_dataset_ignore_exists',
        'test_create_dataverse',
        'test_create_dataverse_ignore_exists',
        'test_create_index',
        'test_drop_dataset',
        'test_drop_dataset_ignore_not_exists',
        'test_drop_dataverse',
        'test_drop_dataverse_ignore_not_exists',
        'test_drop_index',
        'test_get_all_datasets',
        'test_get_pending_mutations',
        'test_v6_dataverse_name_parsing',
        'test_v7_dataverse_name_parsing',
    ]

    @pytest.fixture()
    def clean_drop(self, cb_env, empty_dataverse_name):
        yield
        cb_env.aixm.drop_dataset(self.DATASET_NAME, ignore_if_not_exists=True)
        cb_env.aixm.drop_dataset(self.DATASET_NAME,
                                 DropDatasetOptions(ignore_if_not_exists=True,
                                                    dataverse_name=empty_dataverse_name))
        cb_env.aixm.drop_dataverse(empty_dataverse_name, ignore_if_not_exists=True)

    @pytest.fixture()
    def create_drop_empty_dataverse(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataverse(empty_dataverse_name, ignore_if_exists=True)
        yield
        cb_env.aixm.drop_dataverse(empty_dataverse_name, ignore_if_not_exists=True)

    @pytest.fixture()
    def create_empty_dataset(self, cb_env):
        cb_env.aixm.create_dataset(self.DATASET_NAME, cb_env.bucket.name, ignore_if_exists=True)

    @pytest.fixture()
    def create_empty_dataverse(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataverse(empty_dataverse_name, ignore_if_exists=True)

    @pytest.fixture()
    def drop_empty_dataset(self, cb_env):
        yield
        cb_env.aixm.drop_dataset(self.DATASET_NAME, ignore_if_not_exists=True)

    @pytest.fixture()
    def drop_empty_dataverse(self, cb_env, empty_dataverse_name):
        yield
        cb_env.aixm.drop_dataverse(empty_dataverse_name, ignore_if_not_exists=True)

    @pytest.fixture(scope='class')
    def empty_dataverse_name(self, cb_env):
        if cb_env.server_version_short >= 7.0:
            name = 'empty/dataverse'
        else:
            name = 'empty_dataverse'
        return name

    @pytest.mark.usefixtures('create_empty_dataverse')
    @pytest.mark.usefixtures('clean_drop')
    def test_connect_disconnect_link(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataset(self.DATASET_NAME,
                                   cb_env.bucket.name,
                                   CreateDatasetOptions(dataverse_name=empty_dataverse_name,
                                                        ignore_if_exists=True))
        cb_env.aixm.connect_link(ConnectLinkOptions(dataverse_name=empty_dataverse_name))

        # # connect link should result in documents in the dataset, so...
        # dataverse_name = self.mgr._scrub_dataverse_name(self.dataverse_name)
        # self.assertRows(
        #     'USE {}; SELECT * FROM `{}` LIMIT 1'.format(dataverse_name, self.dataset_name))
        # # manually stop it for now
        # self.cluster.analytics_query(
        #     'USE {}; DISCONNECT LINK Local'.format(dataverse_name, self.dataset_name)).metadata()
        cb_env.aixm.disconnect_link(DisconnectLinkOptions(dataverse_name=empty_dataverse_name))

    @pytest.mark.usefixtures("drop_empty_dataset")
    def test_create_dataset(self, cb_env):
        cb_env.aixm.create_dataset(self.DATASET_NAME, cb_env.bucket.name)

    @pytest.mark.usefixtures("drop_empty_dataset")
    def test_create_dataset_ignore_exists(self, cb_env):
        cb_env.aixm.create_dataset(self.DATASET_NAME, cb_env.bucket.name)
        with pytest.raises(DatasetAlreadyExistsException):
            cb_env.aixm.create_dataset(self.DATASET_NAME, cb_env.bucket.name)

        cb_env.aixm.create_dataset(self.DATASET_NAME,
                                   cb_env.bucket.name,
                                   CreateDatasetOptions(ignore_if_exists=True))

    @pytest.mark.usefixtures('drop_empty_dataverse')
    def test_create_dataverse(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataverse(empty_dataverse_name)

    @pytest.mark.usefixtures('drop_empty_dataverse')
    def test_create_dataverse_ignore_exists(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataverse(
            empty_dataverse_name, CreateDataverseOptions(ignore_if_exists=True))

        with pytest.raises(DataverseAlreadyExistsException):
            cb_env.aixm.create_dataverse(empty_dataverse_name)

    @pytest.mark.usefixtures('create_empty_dataverse')
    @pytest.mark.usefixtures('clean_drop')
    def test_create_index(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataset(self.DATASET_NAME,
                                   cb_env.bucket.name,
                                   CreateDatasetOptions(dataverse_name=empty_dataverse_name, ignore_if_exists=True))
        cb_env.aixm.create_index("test_idx", self.DATASET_NAME,
                                 {'name': AnalyticsDataType.STRING,
                                  'description': AnalyticsDataType.STRING},
                                 CreateAnalyticsIndexOptions(dataverse_name=empty_dataverse_name))

        def check_for_idx(idx):
            indexes = cb_env.aixm.get_all_indexes()
            for index in indexes:
                if index.name == idx:
                    return
            raise Exception(
                "unable to find 'test_idx' in list of all indexes")

        TestEnvironment.try_n_times(10, 3, check_for_idx, 'test_idx')

    @pytest.mark.usefixtures('create_empty_dataset')
    @pytest.mark.usefixtures("drop_empty_dataset")
    def test_drop_dataset(self, cb_env):
        cb_env.aixm.drop_dataset(self.DATASET_NAME)

    @pytest.mark.usefixtures('create_empty_dataset')
    @pytest.mark.usefixtures("drop_empty_dataset")
    def test_drop_dataset_ignore_not_exists(self, cb_env):
        cb_env.aixm.drop_dataset(self.DATASET_NAME)
        with pytest.raises(DatasetNotFoundException):
            cb_env.aixm.drop_dataset(self.DATASET_NAME)
        cb_env.aixm.drop_dataset(self.DATASET_NAME, DropDatasetOptions(ignore_if_not_exists=True))

    @pytest.mark.usefixtures('create_empty_dataverse')
    @pytest.mark.usefixtures('drop_empty_dataverse')
    def test_drop_dataverse(self, cb_env, empty_dataverse_name):
        cb_env.aixm.drop_dataverse(empty_dataverse_name)

    @pytest.mark.usefixtures('create_empty_dataverse')
    @pytest.mark.usefixtures('drop_empty_dataverse')
    def test_drop_dataverse_ignore_not_exists(self, cb_env, empty_dataverse_name):
        cb_env.aixm.drop_dataverse(empty_dataverse_name)
        with pytest.raises(DataverseNotFoundException):
            cb_env.aixm.drop_dataverse(empty_dataverse_name)
        cb_env.aixm.drop_dataverse(empty_dataverse_name, DropDataverseOptions(ignore_if_not_exists=True))

    @pytest.mark.usefixtures('create_empty_dataverse')
    @pytest.mark.usefixtures('clean_drop')
    def test_drop_index(self, cb_env, empty_dataverse_name):
        # create one first, if not already there
        cb_env.aixm.create_dataset(self.DATASET_NAME,
                                   cb_env.bucket.name,
                                   CreateDatasetOptions(dataverse_name=empty_dataverse_name, ignore_if_exists=True))
        cb_env.aixm.create_index('test_idx', self.DATASET_NAME,
                                 {'name': AnalyticsDataType.STRING,
                                  'description': AnalyticsDataType.STRING},
                                 CreateAnalyticsIndexOptions(dataverse_name=empty_dataverse_name))

        def check_for_idx(idx):
            indexes = cb_env.aixm.get_all_indexes()
            for index in indexes:
                if index.name == idx:
                    return
            raise Exception(
                "unable to find 'test_idx' in list of all indexes")

        TestEnvironment.try_n_times(10, 3, check_for_idx, 'test_idx')
        cb_env.aixm.drop_index("test_idx",
                               self.DATASET_NAME,
                               DropAnalyticsIndexOptions(dataverse_name=empty_dataverse_name))
        TestEnvironment.try_n_times_till_exception(10, 3, check_for_idx, 'test_idx')

    @pytest.mark.usefixtures('create_empty_dataverse')
    @pytest.mark.usefixtures('clean_drop')
    def test_get_all_datasets(self, cb_env, empty_dataverse_name):
        cb_env.aixm.create_dataset(self.DATASET_NAME, cb_env.bucket.name, ignore_if_exists=True)
        cb_env.aixm.create_dataset(self.DATASET_NAME,
                                   cb_env.bucket.name,
                                   CreateDatasetOptions(dataverse_name=empty_dataverse_name,
                                                        ignore_if_exists=True))

        datasets = cb_env.aixm.get_all_datasets()
        local_ds = [ds for ds in datasets if ds.dataset_name == self.DATASET_NAME]
        assert len(local_ds) == 2
        assert any(map(lambda ds: ds.dataverse_name == 'Default', local_ds)) is True
        assert any(map(lambda ds: ds.dataverse_name == empty_dataverse_name, local_ds)) is True

    @pytest.mark.usefixtures('create_empty_dataverse')
    @pytest.mark.usefixtures('clean_drop')
    def test_get_pending_mutations(self, cb_env, empty_dataverse_name):
        EnvironmentFeatures.check_if_feature_supported('analytics_pending_mutations',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)
        dv_name = empty_dataverse_name.replace('/', '.')
        key = f'{dv_name}.{self.DATASET_NAME}'
        result = cb_env.aixm.get_pending_mutations()
        assert key not in result.keys()
        cb_env.aixm.create_dataset(self.DATASET_NAME,
                                   cb_env.bucket.name,
                                   CreateDatasetOptions(dataverse_name=empty_dataverse_name,
                                                        ignore_if_exists=True))
        cb_env.aixm.connect_link(ConnectLinkOptions(dataverse_name=empty_dataverse_name))
        TestEnvironment.sleep(1)
        result = cb_env.aixm.get_pending_mutations()
        assert key in result.keys()
        cb_env.aixm.disconnect_link(DisconnectLinkOptions(dataverse_name=empty_dataverse_name))

    def test_v6_dataverse_name_parsing(self, cb_env):
        if cb_env.server_version_short >= 7.0:
            pytest.skip('Test only for 6.x versions')

        # test.test_dataverse, valid format which is valid >= 6.0, but not on 6.6...weird
        if cb_env.server_version_short >= 6.6:
            with pytest.raises(CouchbaseException):
                cb_env.aixm.create_dataverse(
                    'test.test_dataverse', CreateDataverseOptions(ignore_if_exists=True))
        else:
            cb_env.aixm.create_dataverse(
                'test.test_dataverse', CreateDataverseOptions(ignore_if_exists=True))

        cb_env.aixm.drop_dataverse('test.test_dataverse', ignore_if_not_exists=True)

        # test/test_dataverse, invalid format < 7.0
        with pytest.raises((InternalServerFailureException, CouchbaseException)):
            cb_env.aixm.create_dataverse(
                'test/test_dataverse', CreateDataverseOptions(ignore_if_exists=True))

    def test_v7_dataverse_name_parsing(self, cb_env):
        if cb_env.server_version_short < 7.0:
            pytest.skip('Test only for 7.x versions')

        # test.test_dataverse, valid format which is valid >= 6.6
        cb_env.aixm.create_dataverse(
            'test.test_dataverse', CreateDataverseOptions(ignore_if_exists=True))
        cb_env.aixm.drop_dataverse('test.test_dataverse')

        # test/test_dataverse, valideformat which is valid >= 7.0
        cb_env.aixm.create_dataverse(
            'test/test_dataverse', CreateDataverseOptions(ignore_if_exists=True))
        cb_env.aixm.drop_dataverse('test/test_dataverse')


class ClassicAnalyticsManagementLinksTests(AnalyticsManagementLinksTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAnalyticsManagementLinksTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAnalyticsManagementLinksTests) if valid_test_method(meth)]
        compare = set(AnalyticsManagementLinksTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        EnvironmentFeatures.check_if_feature_supported('analytics_link_mgmt',
                                                       cb_base_env.server_version_short,
                                                       cb_base_env.mock_server_type)

        cb_base_env.enable_analytics_mgmt()
        yield cb_base_env
        cb_base_env.disable_analytics_mgmt()


class ClassicAnalyticsManagementTests(AnalyticsManagementTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicAnalyticsManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicAnalyticsManagementTests) if valid_test_method(meth)]
        compare = set(AnalyticsManagementTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.enable_analytics_mgmt()
        yield cb_base_env
        cb_base_env.disable_analytics_mgmt()
