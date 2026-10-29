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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

from couchbase.common._utils import is_null_or_empty, to_form_str
from couchbase.common.exceptions import InvalidArgumentException


class AnalyticsDataType(Enum):
    STRING = 'string'
    INT64 = 'int64'
    DOUBLE = 'double'


class AnalyticsLinkType(Enum):
    S3External = 's3'
    AzureBlobExternal = 'azureblob'
    CouchbaseRemote = 'couchbase'


class AnalyticsEncryptionLevel(Enum):
    NONE = 'none'
    HALF = 'half'
    FULL = 'full'


@dataclass
class AnalyticsDataset:
    dataset_name: str | None = None
    dataverse_name: str | None = None
    link_name: str | None = None
    bucket_name: str | None = None

    @classmethod
    def from_server(cls, json_data: dict[str, str]) -> AnalyticsDataset:
        return cls(json_data['name'],
                   json_data['dataverse_name'],
                   json_data['link_name'],
                   json_data['bucket_name'])


@dataclass
class AnalyticsIndex:
    name: str | None = None
    dataset_name: str | None = None
    dataverse_name: str | None = None
    is_primary: bool | None = None


class AnalyticsLink(ABC):
    """AnalytcsLinks are only available on Couchbase Server 7.0+
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the :class:`couchbase.analytics.AnalyticsLink`

        Returns:
            The name of the :class:`couchbase.analytics.AnalyticsLink`
        """
        pass

    @abstractmethod
    def dataverse_name(self) -> str:
        """Returns the name of the dataverse the :class:`couchbase.analytics.AnalyticsLink` belongs to

        Returns:
            The name of the dataverse
        """
        pass

    @abstractmethod
    def form_encode(self) -> bytes:
        """Encodes the :class:`couchbase.analytics.AnalyticsLink` into a form data representation,
            to send as the body of a :func:`couchbase.management.analytics.CreateLink` or
            :func:`couchbase.management.analytics.ReplaceLink`

        Returns:
            A form encoded :class:`couchbase.analytics.AnalyticsLink`
        """
        pass

    @abstractmethod
    def validate(self) -> None:
        """Ensures the :class:`couchbase.analytics.AnalyticsLink` is valid.
        Raises a :class:`couchbase.exceptions.InvalidArgumentException` if link is invalid.

        Returns:
            None

        Raises:
            :class:`couchbase.exceptions.InvalidArgumentException`: If the link is invalid.
        """
        pass

    @abstractmethod
    def link_type(self) -> AnalyticsLinkType:
        """Returns the :class:`couchbase.analytics.AnalyticsLinkType` of
        the :class:`couchbase.analytics.AnalyticsLink`

        Returns:
            The corresponding :class:`couchbase.analytics.AnalyticsLinkType`
            of the :class:`couchbase.analytics.AnalyticsLink`
        """
        pass

    @abstractmethod
    def as_dict(self) -> dict[str, Any]:
        """Returns the :class:`couchbase.analytics.AnalyticsLink` as a dict,
            in the form the management API sends it in

        Returns:
            A dict representation of the :class:`couchbase.analytics.AnalyticsLink`
        """
        pass


class CouchbaseAnalyticsEncryptionSettings:

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
        return self._encryption_level

    @encryption_level.setter
    def encryption_level(self, value: AnalyticsEncryptionLevel) -> None:
        self._encryption_level = value

    @property
    def certificate(self) -> bytes | bytearray | None:
        return self._certificate

    @certificate.setter
    def certificate(self, value: bytes | bytearray) -> None:
        self._certificate = value

    @property
    def client_certificate(self) -> bytes | bytearray | None:
        return self._client_certificate

    @client_certificate.setter
    def client_certificate(self, value: bytes | bytearray) -> None:
        self._client_certificate = value

    @property
    def client_key(self) -> bytes | bytearray | None:
        return self._client_key

    @client_key.setter
    def client_key(self, value: bytes | bytearray) -> None:
        self._client_key = value

    def as_dict(self) -> dict[str, str]:
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

        encryption_settings = CouchbaseAnalyticsEncryptionSettings(AnalyticsEncryptionLevel(raw_data['level']))

        certificate = raw_data.get('certificate', None)
        if isinstance(certificate, str) and not is_null_or_empty(certificate):
            encryption_settings.certificate = bytes(certificate, 'utf-8')

        client_certificate = raw_data.get('client_certificate', None)
        if isinstance(client_certificate, str) and not is_null_or_empty(client_certificate):
            encryption_settings.client_certificate = bytes(client_certificate, 'utf-8')

        return encryption_settings


class CouchbaseRemoteAnalyticsLink(AnalyticsLink):
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

    def name(self,) -> str:
        return self._link_name

    def dataverse_name(self) -> str:
        return self._dataverse

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

    def link_type(self) -> AnalyticsLinkType:
        return AnalyticsLinkType.CouchbaseRemote

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

        dataverse = raw_data['dataverse']
        link_name = raw_data['link_name']
        hostname = raw_data['hostname']
        encryption = CouchbaseAnalyticsEncryptionSettings.from_server_json(
            raw_data['encryption'])
        username = raw_data.get('username', None)

        return CouchbaseRemoteAnalyticsLink(
            dataverse, link_name, hostname, encryption, username)


class S3ExternalAnalyticsLink(AnalyticsLink):
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

    def name(self) -> str:
        return self._link_name

    def dataverse_name(self) -> str:
        return self._dataverse

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

    def link_type(self) -> AnalyticsLinkType:
        return AnalyticsLinkType.S3External

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

        dataverse = raw_data['dataverse']
        link_name = raw_data['link_name']
        access_key_id = raw_data['access_key_id']
        region = raw_data['region']
        service_endpoint = raw_data.get('service_endpoint', None)

        return S3ExternalAnalyticsLink(
            dataverse, link_name, access_key_id, region, service_endpoint=service_endpoint)


class AzureBlobExternalAnalyticsLink(AnalyticsLink):
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

    def name(self) -> str:
        return self._link_name

    def dataverse_name(self) -> str:
        return self._dataverse

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

    def link_type(self) -> AnalyticsLinkType:
        return AnalyticsLinkType.AzureBlobExternal

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
