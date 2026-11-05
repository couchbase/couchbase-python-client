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

from __future__ import annotations

__all__ = [
    'AnalyticsDataType',
    'AnalyticsDataset',
    'AnalyticsEncryptionLevel',
    'AnalyticsIndex',
    'AnalyticsLink',
    'AnalyticsLinkType',
    'AzureBlobExternalAnalyticsLink',
    'CouchbaseAnalyticsEncryptionSettings',
    'CouchbaseRemoteAnalyticsLink',
    'S3ExternalAnalyticsLink',
]

import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from couchbase.common._utils import (fields_repr,
                                     is_null_or_empty,
                                     redacted,
                                     to_form_str)
from couchbase.common.exceptions import InvalidArgumentException


class AnalyticsDataType(Enum):
    """The type of a field an analytics index covers."""

    STRING = 'string'
    INT64 = 'int64'
    DOUBLE = 'double'


class AnalyticsLinkType(Enum):
    """The kind of data source an analytics link connects to."""

    S3External = 's3'
    AzureBlobExternal = 'azureblob'
    CouchbaseRemote = 'couchbase'


class AnalyticsEncryptionLevel(Enum):
    """How a remote Couchbase analytics link encrypts its connection.

    ``NONE`` sends everything in plain text, ``HALF`` encrypts only the credentials, and ``FULL`` encrypts the whole
    connection with TLS.
    """

    NONE = 'none'
    HALF = 'half'
    FULL = 'full'


@dataclass
class AnalyticsDataset:
    """An analytics dataset, as the analytics service describes it.

    Attributes:
        dataset_name (str | None): The name of the dataset.
        dataverse_name (str | None): The name of the dataverse the dataset is in.
        link_name (str | None): The name of the link the dataset reads from.
        bucket_name (str | None): The name of the bucket the dataset reads from.
    """

    dataset_name: str | None = None
    dataverse_name: str | None = None
    link_name: str | None = None
    bucket_name: str | None = None

    @classmethod
    def from_server(cls, json_data: dict[str, str]) -> AnalyticsDataset:
        """**INTERNAL** The dataset the analytics service described."""
        return cls(json_data['name'],
                   json_data['dataverse_name'],
                   json_data['link_name'],
                   json_data['bucket_name'])


@dataclass
class AnalyticsIndex:
    """An analytics index, as the analytics service describes it.

    Attributes:
        name (str | None): The name of the index.
        dataset_name (str | None): The name of the dataset the index is on.
        dataverse_name (str | None): The name of the dataverse the dataset is in.
        is_primary (bool | None): Whether the index is the dataset's primary index.
    """

    name: str | None = None
    dataset_name: str | None = None
    dataverse_name: str | None = None
    is_primary: bool | None = None


class AnalyticsLink(ABC):
    """A link from the analytics service to a source of data outside the local cluster.

    A link is one of :class:`.CouchbaseRemoteAnalyticsLink`, :class:`.S3ExternalAnalyticsLink` or
    :class:`.AzureBlobExternalAnalyticsLink`. Links are available on Couchbase Server 7.0 and later.
    """

    @abstractmethod
    def name(self) -> str:
        """Get the name of the link.

        Returns:
            str: The name of the link.
        """
        pass

    @abstractmethod
    def dataverse_name(self) -> str:
        """Get the name of the dataverse the link is in.

        Returns:
            str: The name of the dataverse the link is in.
        """
        pass

    @abstractmethod
    def form_encode(self) -> bytes:
        """**INTERNAL** The link as the form-encoded body of a create or replace request."""
        pass

    @abstractmethod
    def validate(self) -> None:
        """Check that the link has every setting its kind requires.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If a required setting is missing.
        """
        pass

    @abstractmethod
    def link_type(self) -> AnalyticsLinkType:
        """Get the kind of data source the link connects to.

        Returns:
            :class:`.AnalyticsLinkType`: The kind of data source the link connects to.
        """
        pass

    @abstractmethod
    def as_dict(self) -> dict[str, Any]:
        """**INTERNAL** The link as the dict the management request is built from."""
        pass


class CouchbaseAnalyticsEncryptionSettings:
    """The encryption a remote Couchbase analytics link uses.

    Args:
        encryption_level (:class:`.AnalyticsEncryptionLevel`, optional): How the link encrypts its connection. Defaults
            to ``NONE``.
        certificate (bytes | bytearray, optional): The remote cluster's certificate, required for ``FULL``.
        client_certificate (bytes | bytearray, optional): The client certificate, required for ``FULL``.
        client_key (bytes | bytearray, optional): The client certificate's key, required for ``FULL``.
    """

    def __init__(self,
                 encryption_level: AnalyticsEncryptionLevel | None = None,
                 certificate: bytes | bytearray | None = None,
                 client_certificate: bytes | bytearray | None = None,
                 client_key: bytes | bytearray | None = None,
                 ) -> None:
        self._encryption_level = encryption_level if encryption_level is not None else AnalyticsEncryptionLevel.NONE
        self._certificate = certificate
        self._client_certificate = client_certificate
        self._client_key = client_key

    @property
    def encryption_level(self) -> AnalyticsEncryptionLevel:
        """How the link encrypts its connection."""
        return self._encryption_level

    @encryption_level.setter
    def encryption_level(self, value: AnalyticsEncryptionLevel) -> None:
        self._encryption_level = value

    @property
    def certificate(self) -> bytes | bytearray | None:
        """The remote cluster's certificate."""
        return self._certificate

    @certificate.setter
    def certificate(self, value: bytes | bytearray) -> None:
        self._certificate = value

    @property
    def client_certificate(self) -> bytes | bytearray | None:
        """The client certificate."""
        return self._client_certificate

    @client_certificate.setter
    def client_certificate(self, value: bytes | bytearray) -> None:
        self._client_certificate = value

    @property
    def client_key(self) -> bytes | bytearray | None:
        """The client certificate's key."""
        return self._client_key

    @client_key.setter
    def client_key(self, value: bytes | bytearray) -> None:
        self._client_key = value

    def as_dict(self) -> dict[str, str]:
        """**INTERNAL** The settings as the dict the management request is built from."""
        encryption_dict = {
            'encryption_level': self.encryption_level.value
        }
        if self.certificate:
            encryption_dict['certificate'] = self.certificate.decode('utf-8')
        if self.client_certificate:
            encryption_dict['client_certificate'] = self.client_certificate.decode('utf-8')
        if self.client_key:
            encryption_dict['client_key'] = self.client_key.decode('utf-8')
        return encryption_dict

    @classmethod
    def from_server_json(cls, raw_data: dict[str, Any]) -> CouchbaseAnalyticsEncryptionSettings:
        """**INTERNAL** The settings the analytics service described."""
        encryption_settings = CouchbaseAnalyticsEncryptionSettings(AnalyticsEncryptionLevel(raw_data['level']))

        certificate = raw_data.get('certificate', None)
        if isinstance(certificate, str) and not is_null_or_empty(certificate):
            encryption_settings.certificate = bytes(certificate, 'utf-8')

        client_certificate = raw_data.get('client_certificate', None)
        if isinstance(client_certificate, str) and not is_null_or_empty(client_certificate):
            encryption_settings.client_certificate = bytes(client_certificate, 'utf-8')

        return encryption_settings

    def __repr__(self) -> str:
        return fields_repr(self, encryption_level=self._encryption_level, certificate=self._certificate,
                           client_certificate=self._client_certificate, client_key=redacted(self._client_key))


class CouchbaseRemoteAnalyticsLink(AnalyticsLink):
    """A link to a bucket on another Couchbase cluster.

    Args:
        dataverse (str): The name of the dataverse the link is in.
        link_name (str): The name of the link.
        hostname (str): The hostname of the remote cluster.
        encryption (:class:`.CouchbaseAnalyticsEncryptionSettings`): How the link encrypts its connection.
        username (str, optional): The username to authenticate with, required unless the encryption level is ``FULL``.
        password (str, optional): The password to authenticate with, required unless the encryption level is ``FULL``.
    """

    def __init__(self,
                 dataverse: str,
                 link_name: str,
                 hostname: str,
                 encryption: CouchbaseAnalyticsEncryptionSettings,
                 username: str | None = None,
                 password: str | None = None
                 ) -> None:
        super().__init__()
        self._dataverse = dataverse
        self._link_name = link_name
        self._hostname = hostname
        self._encryption = encryption
        self._username = username
        self._password = password

    @override
    def name(self) -> str:
        return self._link_name

    @override
    def dataverse_name(self) -> str:
        return self._dataverse

    @override
    def form_encode(self) -> bytes:
        params: dict[str, str | None] = {}

        if "/" not in self._dataverse:
            params["dataverse"] = self._dataverse
            params["name"] = self._link_name

        params["hostname"] = self._hostname
        params["type"] = AnalyticsLinkType.CouchbaseRemote.value
        params["encryption"] = self._encryption.encryption_level.value

        if not is_null_or_empty(self._username):
            params["username"] = self._username

        if not is_null_or_empty(self._password):
            params["password"] = self._password

        if self._encryption.certificate and len(
                self._encryption.certificate) > 0:
            params["certificate"] = self._encryption.certificate.decode(
                "utf-8")

        if self._encryption.client_certificate and len(
                self._encryption.client_certificate) > 0:
            params["clientCertificate"] = self._encryption.client_certificate.decode(
                "utf-8")

        if self._encryption.client_key and len(
                self._encryption.client_key) > 0:
            params["clientKey"] = self._encryption.client_key.decode("utf-8")

        return to_form_str(params).encode()

    @override
    def validate(self) -> None:
        if is_null_or_empty(self._dataverse):
            raise InvalidArgumentException(message="Dataverse must be set for couchbase analytics links.")

        if is_null_or_empty(self._link_name):
            raise InvalidArgumentException(message="Link name must be set for couchbase analytics links.")

        if is_null_or_empty(self._hostname):
            raise InvalidArgumentException(message="Hostname must be set for couchbase analytics links.")

        if self._encryption.encryption_level in [
                AnalyticsEncryptionLevel.NONE, AnalyticsEncryptionLevel.HALF]:
            if is_null_or_empty(
                    self._username) or is_null_or_empty(self._password):
                raise InvalidArgumentException(
                    message=("When encryption level is half or none, "
                             "username and password must be set for couchbase analytics links."))
        elif self._encryption.encryption_level == AnalyticsEncryptionLevel.FULL:
            if not (self._encryption.certificate and len(
                    self._encryption.certificate) > 0):
                raise InvalidArgumentException(
                    message="When encryption level is full a certificate must be set for couchbase analytics links.")
            if not ((self._encryption.client_certificate and len(self._encryption.client_certificate) > 0)
                    and (self._encryption.client_key and len(self._encryption.client_key) > 0)):
                raise InvalidArgumentException(
                    message=("When encryption level is full the client "
                             "certificate and key must be set for couchbase analytics links."))

    @override
    def link_type(self) -> AnalyticsLinkType:
        return AnalyticsLinkType.CouchbaseRemote

    @override
    def as_dict(self) -> dict[str, Any]:
        link_dict: dict[str, Any] = {
            'link_name': self.name(),
            'dataverse': self.dataverse_name(),
            'hostname': self._hostname,
            'link_type': AnalyticsLinkType.CouchbaseRemote.value
        }
        if self._username:
            link_dict['username'] = self._username
        if self._password:
            link_dict['password'] = self._password
        if self._encryption:
            link_dict['encryption'] = self._encryption.as_dict()

        return link_dict

    @classmethod
    def link_from_server_json(cls, raw_data: dict[str, Any]) -> CouchbaseRemoteAnalyticsLink:
        """**INTERNAL** The link the analytics service described."""
        dataverse = raw_data['dataverse']
        link_name = raw_data['link_name']
        hostname = raw_data['hostname']
        encryption = CouchbaseAnalyticsEncryptionSettings.from_server_json(
            raw_data['encryption'])
        username = raw_data.get('username', None)

        return CouchbaseRemoteAnalyticsLink(
            dataverse, link_name, hostname, encryption, username)

    def __repr__(self) -> str:
        return fields_repr(self, dataverse=self._dataverse, link_name=self._link_name,
                           hostname=self._hostname, encryption=self._encryption, username=self._username,
                           password=redacted(self._password))


class S3ExternalAnalyticsLink(AnalyticsLink):
    """A link to data in Amazon S3.

    Args:
        dataverse (str): The name of the dataverse the link is in.
        link_name (str): The name of the link.
        access_key_id (str): The AWS access key ID.
        region (str): The AWS region the data is in.
        secret_access_key (str, optional): The AWS secret access key. The link is not valid without it.
        session_token (str, optional): The AWS session token, for temporary credentials.
        service_endpoint (str, optional): The S3 endpoint, in place of the region's default.
    """

    def __init__(
            self,
            dataverse: str,
            link_name: str,
            access_key_id: str,
            region: str,
            secret_access_key: str | None = None,
            session_token: str | None = None,
            service_endpoint: str | None = None
    ) -> None:
        super().__init__()
        self._dataverse = dataverse
        self._link_name = link_name
        self._access_key_id = access_key_id
        self._region = region
        self._secret_access_key = secret_access_key
        self._session_token = session_token
        self._service_endpoint = service_endpoint

    @override
    def name(self) -> str:
        return self._link_name

    @override
    def dataverse_name(self) -> str:
        return self._dataverse

    @override
    def form_encode(self) -> bytes:
        params: dict[str, str | None] = {}

        if "/" not in self._dataverse:
            params["dataverse"] = self._dataverse
            params["name"] = self._link_name

        params["type"] = AnalyticsLinkType.S3External.value
        params["accessKeyId"] = self._access_key_id
        params["secretAccessKey"] = self._secret_access_key
        params["region"] = self._region

        if not is_null_or_empty(self._session_token):
            params["sessionToken"] = self._session_token

        if not is_null_or_empty(self._service_endpoint):
            params["serviceEndpoint"] = self._service_endpoint

        return to_form_str(params).encode()

    @override
    def validate(self) -> None:
        if is_null_or_empty(self._dataverse):
            raise InvalidArgumentException(
                message="Dataverse must be set for S3 external analytics links.")

        if is_null_or_empty(self._link_name):
            raise InvalidArgumentException(
                message="Link name must be set for S3 external analytics links.")

        if is_null_or_empty(self._access_key_id):
            raise InvalidArgumentException(
                message="Access key id must be set for S3 external analytics links.")

        if is_null_or_empty(self._secret_access_key):
            raise InvalidArgumentException(
                message="Secret access key must be set for S3 external analytics links.")

        if is_null_or_empty(self._region):
            raise InvalidArgumentException(
                message="Region must be set for S3 external analytics links.")

    @override
    def link_type(self) -> AnalyticsLinkType:
        return AnalyticsLinkType.S3External

    @override
    def as_dict(self) -> dict[str, Any]:
        link_dict = {
            'link_name': self.name(),
            'dataverse': self.dataverse_name(),
            'access_key_id': self._access_key_id,
            'secret_access_key': self._secret_access_key,
            'region': self._region,
            'link_type': AnalyticsLinkType.S3External.value
        }
        if self._session_token:
            link_dict['session_token'] = self._session_token
        if self._service_endpoint:
            link_dict['service_endpoint'] = self._service_endpoint

        return link_dict

    @classmethod
    def link_from_server_json(cls, raw_data: dict[str, Any]) -> S3ExternalAnalyticsLink:
        """**INTERNAL** The link the analytics service described."""
        dataverse = raw_data['dataverse']
        link_name = raw_data['link_name']
        access_key_id = raw_data['access_key_id']
        region = raw_data['region']
        service_endpoint = raw_data.get('service_endpoint', None)

        return S3ExternalAnalyticsLink(
            dataverse, link_name, access_key_id, region, service_endpoint=service_endpoint)

    def __repr__(self) -> str:
        return fields_repr(self, dataverse=self._dataverse, link_name=self._link_name,
                           access_key_id=self._access_key_id, region=self._region,
                           secret_access_key=redacted(self._secret_access_key),
                           session_token=redacted(self._session_token),
                           service_endpoint=self._service_endpoint)


class AzureBlobExternalAnalyticsLink(AnalyticsLink):
    """A link to data in Azure Blob Storage.

    The link authenticates with a connection string, or with an account name and either an account key or a shared
    access signature.

    Args:
        dataverse (str): The name of the dataverse the link is in.
        link_name (str): The name of the link.
        connection_string (str, optional): The Azure connection string.
        account_name (str, optional): The storage account name.
        account_key (str, optional): The storage account key.
        shared_access_signature (str, optional): A shared access signature for the account.
        blob_endpoint (str, optional): The blob service endpoint.
        endpoint_suffix (str, optional): The endpoint suffix.
    """

    def __init__(
            self,
            dataverse: str,
            link_name: str,
            connection_string: str | None = None,
            account_name: str | None = None,
            account_key: str | None = None,
            shared_access_signature: str | None = None,
            blob_endpoint: str | None = None,
            endpoint_suffix: str | None = None,
    ) -> None:
        super().__init__()
        self._dataverse = dataverse
        self._link_name = link_name
        self._connection_string = connection_string
        self._account_name = account_name
        self._account_key = account_key
        self._shared_access_signature = shared_access_signature
        self._blob_endpoint = blob_endpoint
        self._endpoint_suffix = endpoint_suffix

    @override
    def name(self) -> str:
        return self._link_name

    @override
    def dataverse_name(self) -> str:
        return self._dataverse

    @override
    def form_encode(self) -> bytes:
        params: dict[str, str | None] = {}

        if "/" not in self._dataverse:
            params["dataverse"] = self._dataverse
            params["name"] = self._link_name

        params["type"] = AnalyticsLinkType.AzureBlobExternal.value

        if not is_null_or_empty(self._connection_string):
            params["connectionString"] = self._connection_string

        if not is_null_or_empty(self._account_name):
            params["accountName"] = self._account_name

        if not is_null_or_empty(self._account_key):
            params["accountKey"] = self._account_key

        if not is_null_or_empty(self._shared_access_signature):
            params["sharedAccessSignature"] = self._shared_access_signature

        if not is_null_or_empty(self._blob_endpoint):
            params["blobEndpoint"] = self._blob_endpoint

        if not is_null_or_empty(self._endpoint_suffix):
            params["endpointSuffix"] = self._endpoint_suffix

        return to_form_str(params).encode()

    @override
    def validate(self) -> None:
        if is_null_or_empty(self._dataverse):
            raise InvalidArgumentException("Dataverse must be set for Azure blob external analytics links.")

        if is_null_or_empty(self._link_name):
            raise InvalidArgumentException("Link name must be set for Azure blob external analytics links.")

        if is_null_or_empty(self._connection_string):
            acct_name_and_key = not (is_null_or_empty(self._account_name)
                                     or is_null_or_empty(self._account_key))
            acct_name_and_sas = not (is_null_or_empty(self._account_name)
                                     or is_null_or_empty(self._shared_access_signature))

            if not (acct_name_and_key or acct_name_and_sas):
                raise InvalidArgumentException("AccessKeyId must be set for Azure blob external analytics links.")

    @override
    def link_type(self) -> AnalyticsLinkType:
        return AnalyticsLinkType.AzureBlobExternal

    @override
    def as_dict(self) -> dict[str, Any]:
        link_dict = {
            'link_name': self.name(),
            'dataverse': self.dataverse_name(),
        }
        if self._connection_string:
            link_dict['connection_string'] = self._connection_string
        if self._account_name:
            link_dict['account_name'] = self._account_name
        if self._account_key:
            link_dict['account_key'] = self._account_key
        if self._shared_access_signature:
            link_dict['shared_access_signature'] = self._shared_access_signature
        if self._blob_endpoint:
            link_dict['blob_endpoint'] = self._blob_endpoint
        if self._endpoint_suffix:
            link_dict['endpoint_suffix'] = self._endpoint_suffix

        return link_dict

    @classmethod
    def link_from_server_json(cls, raw_data: dict[str, Any]) -> AzureBlobExternalAnalyticsLink:
        """**INTERNAL** The link the analytics service described."""
        dataverse = raw_data['dataverse']
        link_name = raw_data['link_name']
        account_name = raw_data.get('account_name', None)
        blob_endpoint = raw_data.get('blob_endpoint', None)
        endpoint_suffix = raw_data.get('endpoint_suffix', None)

        return AzureBlobExternalAnalyticsLink(dataverse,
                                              link_name,
                                              account_name=account_name,
                                              blob_endpoint=blob_endpoint,
                                              endpoint_suffix=endpoint_suffix)

    def __repr__(self) -> str:
        return fields_repr(self, dataverse=self._dataverse, link_name=self._link_name,
                           connection_string=redacted(self._connection_string),
                           account_name=self._account_name, account_key=redacted(self._account_key),
                           shared_access_signature=redacted(self._shared_access_signature),
                           blob_endpoint=self._blob_endpoint, endpoint_suffix=self._endpoint_suffix)
