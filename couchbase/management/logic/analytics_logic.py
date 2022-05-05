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

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    Union)

from couchbase._utils import is_null_or_empty, to_form_str
from couchbase.exceptions import (AnalyticsLinkExistsException,
                                  AnalyticsLinkNotFoundException,
                                  DatasetAlreadyExistsException,
                                  DatasetNotFoundException,
                                  DataverseAlreadyExistsException,
                                  DataverseNotFoundException,
                                  InvalidArgumentException)
from couchbase.options import forward_args
from couchbase.pycbc_core import (analytics_mgmt_operations,
                                  management_operation,
                                  mgmt_operations)

if TYPE_CHECKING:
    from couchbase.management.options import (ConnectLinkOptions,
                                              CreateAnalyticsIndexOptions,
                                              CreateDatasetOptions,
                                              CreateDataverseOptions,
                                              CreateLinkAnalyticsOptions,
                                              DisconnectLinkOptions,
                                              DropAnalyticsIndexOptions,
                                              DropDatasetOptions,
                                              DropDataverseOptions,
                                              DropLinkAnalyticsOptions,
                                              GetAllAnalyticsIndexesOptions,
                                              GetAllDatasetOptions,
                                              GetLinksAnalyticsOptions,
                                              GetPendingMutationsOptions,
                                              ReplaceLinkAnalyticsOptions)


class AnalyticsManagerLogic:

    _ERROR_MAPPING = {r'.*24055.*already exists': AnalyticsLinkExistsException,
                      r'.*24006.*does not exist': AnalyticsLinkNotFoundException,
                      r'.*24034.*Cannot find': DataverseNotFoundException,
                      r'.*24025.*Cannot find analytics (collection)*': DatasetNotFoundException,
                      r'.*24039.*An analytics\s+(scope)*.*already exists': DataverseAlreadyExistsException,
                      r'.*24040.*An analytics\s+(collection)*.*already exists': DatasetAlreadyExistsException}

    def __init__(self, connection):
        self._connection = connection

    def create_dataverse(self,
                         dataverse_name,    # type: str
                         *options,      # type: CreateDataverseOptions
                         **kwargs       # type: Dict[str, Any]
                         ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {
            "dataverse_name": dataverse_name,
        }

        if final_args.get("ignore_if_exists", False) is True:
            op_args['ignore_if_exists'] = final_args.get('ignore_if_exists')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.CREATE_DATAVERSE.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_dataverse(self,
                       dataverse_name,    # type: str
                       *options,      # type: DropDataverseOptions
                       **kwargs       # type: Dict[str, Any]
                       ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {
            "dataverse_name": dataverse_name,
        }

        if final_args.get("ignore_if_not_exists", False) is True:
            op_args['ignore_if_does_not_exist'] = final_args.get('ignore_if_not_exists')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.DROP_DATAVERSE.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       *options,    # type: CreateDatasetOptions
                       **kwargs         # type: Dict[str, Any]
                       ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {
            "dataset_name": dataset_name,
            "bucket_name": bucket_name,
        }

        if final_args.get("dataverse_name", None) is not None:
            op_args['dataverse_name'] = final_args.get('dataverse_name')

        if final_args.get("ignore_if_exists", False) is True:
            op_args['ignore_if_exists'] = final_args.get('ignore_if_exists')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.CREATE_DATASET.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_dataset(self,
                     dataset_name,  # type: str
                     *options,  # type: DropDatasetOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {
            "dataset_name": dataset_name,
        }

        if final_args.get("dataverse_name", None) is not None:
            op_args['dataverse_name'] = final_args.get('dataverse_name')

        if final_args.get("ignore_if_not_exists", False) is True:
            op_args['ignore_if_does_not_exist'] = final_args.get('ignore_if_not_exists')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.DROP_DATASET.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_datasets(self,
                         *options,   # type: GetAllDatasetOptions
                         **kwargs   # type: Dict[str, Any]
                         ) -> Optional[Iterable[AnalyticsDataset]]:

        final_args = forward_args(kwargs, *options)

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.GET_ALL_DATASETS.value,
            "op_args": {}
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type: Dict[str, AnalyticsDataType]
                     *options,      # type: CreateAnalyticsIndexOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> None:

        final_args = forward_args(kwargs, *options)

        fields = {k: v.value for k, v in fields.items()}

        op_args = {
            "index_name": index_name,
            "dataset_name": dataset_name,
            "fields": fields
        }

        if final_args.get("dataverse_name", None) is not None:
            op_args['dataverse_name'] = final_args.get('dataverse_name')

        if final_args.get("ignore_if_exists", False) is True:
            op_args['ignore_if_exists'] = final_args.get('ignore_if_exists')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.CREATE_INDEX.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_index(self,
                   index_name,    # type: str
                   dataset_name,  # type: str
                   *options,      # type: DropAnalyticsIndexOptions
                   **kwargs       # type: Dict[str, Any]
                   ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {
            "index_name": index_name,
            "dataset_name": dataset_name
        }

        if final_args.get("dataverse_name", None) is not None:
            op_args['dataverse_name'] = final_args.get('dataverse_name')

        if final_args.get("ignore_if_not_exists", False) is True:
            op_args['ignore_if_does_not_exist'] = final_args.get('ignore_if_not_exists')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.DROP_INDEX.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_indexes(self,
                        *options,   # type: GetAllAnalyticsIndexesOptions
                        **kwargs   # type: Dict[str, Any]
                        ) -> Optional[Iterable[AnalyticsIndex]]:

        final_args = forward_args(kwargs, *options)

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.GET_ALL_INDEXES.value,
            "op_args": {}
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def connect_link(self,
                     *options,  # type: ConnectLinkOptions
                     **kwargs   # type: Dict[str, Any]
                     ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {}

        if final_args.get("dataverse_name", None) is not None:
            op_args['dataverse_name'] = final_args.get('dataverse_name')

        if final_args.get("link_name", None) is not None:
            op_args['link_name'] = final_args.get('link_name')

        if final_args.get("force", False) is True:
            op_args['force'] = final_args.get('force')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.LINK_CONNECT.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def disconnect_link(self,
                        *options,   # type: DisconnectLinkOptions
                        **kwargs   # type: Dict[str, Any]
                        ) -> None:

        final_args = forward_args(kwargs, *options)

        op_args = {}

        if final_args.get("dataverse_name", None) is not None:
            op_args['dataverse_name'] = final_args.get('dataverse_name')

        if final_args.get("link_name", None) is not None:
            op_args['link_name'] = final_args.get('link_name')

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.LINK_DISCONNECT.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_pending_mutations(self,
                              *options,     # type: GetPendingMutationsOptions
                              **kwargs     # type: Dict[str, Any]
                              ) -> Optional[Dict[str, int]]:

        final_args = forward_args(kwargs, *options)

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.GET_PENDING_MUTATIONS.value,
            "op_args": {}
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def create_link(
        self,
        link,  # type: AnalyticsLink
        *options,     # type: CreateLinkAnalyticsOptions
        **kwargs
    ) -> None:
        link.validate()
        final_args = forward_args(kwargs, *options)

        op_args = {
            "link": link.as_dict(),
            "link_type": link.link_type().value
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.LINK_CREATE.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def replace_link(
        self,
        link,  # type: AnalyticsLink
        *options,     # type: ReplaceLinkAnalyticsOptions
        **kwargs
    ) -> None:
        link.validate()
        final_args = forward_args(kwargs, *options)

        op_args = {
            "link": link.as_dict(),
            "link_type": link.link_type().value
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.LINK_REPLACE.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_link(
        self,
        link_name,  # type: str
        dataverse_name,     # type: str
        *options,  # type: DropLinkAnalyticsOptions
        **kwargs
    ) -> None:
        final_args = forward_args(kwargs, *options)

        op_args = {
            "link_name": link_name,
            "dataverse_name": dataverse_name
        }

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.DROP_LINK.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_links(
        self,
        *options,  # type: GetLinksAnalyticsOptions
        **kwargs
    ) -> Optional[Iterable[AnalyticsLink]]:

        final_args = forward_args(kwargs, *options)

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.ANALYTICS.value,
            "op_type": analytics_mgmt_operations.GET_ALL_LINKS.value,
            "op_args": {}
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)


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
    dataset_name: str = None
    dataverse_name: str = None
    link_name: str = None
    bucket_name: str = None


@dataclass
class AnalyticsIndex:
    name: str = None
    dataset_name: str = None
    dataverse_name: str = None
    is_primary: bool = None


class AnalyticsLink(ABC):
    """AnalytcsLinks are only available on Couchbase Server 7.0+
    """

    @abstractmethod
    def name(
        self,  # type: "AnalyticsLink"
    ) -> str:
        """Returns the name of the :class:`couchbase.analytics.AnalyticsLink`

        :return: The name of the :class:`couchbase.analytics.AnalyticsLink`
        """
        pass

    @abstractmethod
    def dataverse_name(self) -> str:
        """Returns the name of the dataverse the :class:`couchbase.analytics.AnalyticsLink` belongs to

        :return: The name of the dataverse
        """
        pass

    @abstractmethod
    def form_encode(self) -> bytes:
        """Encodes the :class:`couchbase.analytics.AnalyticsLink` into a form data representation,
            to send as the body of a :func:`couchbase.management.analytics.CreateLink` or
            :func:`couchbase.management.analytics.ReplaceLink`

        :return: A form encoded :class:`couchbase.analytics.AnalyticsLink`
        """
        pass

    @abstractmethod
    def validate(self) -> None:
        """Ensures the :class:`couchbase.analytics.AnalyticsLink` is valid.
        Raises a :class:`couchbase.exceptions.InvalidArgumentException` if link is invalid.

        :return: None

        :raises: :class:`couchbase.exceptions.InvalidArgumentException`
        """
        pass

    @abstractmethod
    def link_type(self) -> AnalyticsLinkType:
        """Returns the :class:`couchbase.analytics.AnalyticsLinkType` of
        the :class:`couchbase.analytics.AnalyticsLink`

        :return: The corresponding :class:`couchbase.analytics.AnalyticsLinkType`
        of the :class:`couchbase.analytics.AnalyticsLink`
        """
        pass


class CouchbaseAnalyticsEncryptionSettings(object):

    def __init__(self,
                 encryption_level=None,  # type: Optional[AnalyticsEncryptionLevel]
                 certificate=None,  # type: Optional[Union[bytes, bytearray]]
                 client_certificate=None,  # type: Optional[Union[bytes, bytearray]]
                 client_key=None,  # type: Optional[Union[bytes, bytearray]]
                 ):
        self._encryption_level = encryption_level
        if self._encryption_level is None:
            self._encryption_level = AnalyticsEncryptionLevel.NONE
        self._certificate = certificate
        self._client_certificate = client_certificate
        self._client_key = client_key

    @property
    def encryption_level(self) -> AnalyticsEncryptionLevel:
        return self._encryption_level

    @encryption_level.setter
    def encryption_level(self, value  # type: AnalyticsEncryptionLevel
                         ):
        self._encryption_level = value

    @property
    def certificate(self) -> Optional[bytes]:
        return self._certificate

    @certificate.setter
    def certificate(self, value  # type: Union[bytes, bytearray]
                    ):
        self._certificate = value

    @property
    def client_certificate(self) -> Optional[bytes]:
        return self._client_certificate

    @client_certificate.setter
    def client_certificate(self, value  # type: Union[bytes, bytearray]
                           ):
        self._client_certificate = value

    @property
    def client_key(self) -> Optional[bytes]:
        return self._client_key

    @client_key.setter
    def client_key(self, value  # type: Union[bytes, bytearray]
                   ):
        self._client_key = value

    def as_dict(self):
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
    def from_server_json(
        cls,
        raw_data  # type: dict
    ) -> CouchbaseAnalyticsEncryptionSettings:

        encryption_settings = CouchbaseAnalyticsEncryptionSettings()
        if raw_data['encryption_level'] == AnalyticsEncryptionLevel.NONE.value:
            encryption_settings.encryption_level = AnalyticsEncryptionLevel.NONE
        elif raw_data['encryption_level'] == AnalyticsEncryptionLevel.HALF.value:
            encryption_settings.encryption_level = AnalyticsEncryptionLevel.HALF
        elif raw_data['encryption_level'] == AnalyticsEncryptionLevel.FULL.value:
            encryption_settings.encryption_level = AnalyticsEncryptionLevel.FULL

        if 'certificate' in raw_data and not is_null_or_empty(raw_data['certificate']):
            encryption_settings.certificate = bytes(
                raw_data['certificate'], 'utf-8')

        if 'client_certificate' in raw_data and not is_null_or_empty(raw_data['client_certificate']):
            encryption_settings.certificate = bytes(
                raw_data['client_certificate'], 'utf-8')

        return encryption_settings


class CouchbaseRemoteAnalyticsLink(AnalyticsLink):
    def __init__(self,
                 dataverse,  # type: str
                 link_name,  # type: str
                 hostname,  # type: str
                 encryption,  # type: CouchbaseAnalyticsEncryptionSettings
                 username=None,  # type: Optional[str]
                 password=None,  # type: Optional[str]

                 ):
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
        params = {}

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

    def as_dict(self):
        link_dict = {
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
    def link_from_server_json(
        cls,
        raw_data,  # type: dict
    ) -> CouchbaseRemoteAnalyticsLink:

        dataverse = raw_data['dataverse']
        link_name = raw_data['link_name']
        hostname = raw_data['hostname']
        encryption = CouchbaseAnalyticsEncryptionSettings.from_server_json(
            raw_data['encryption_settings'])
        username = raw_data.get('username', None)

        return CouchbaseRemoteAnalyticsLink(
            dataverse, link_name, hostname, encryption, username)


class S3ExternalAnalyticsLink(AnalyticsLink):
    def __init__(
            self,
            dataverse,  # type: str
            link_name,  # type: str
            access_key_id,  # type: str
            region,  # type: str
            secret_access_key=None,  # type: Optional[str]
            session_token=None,  # type: Optional[str]
            service_endpoint=None,  # type: Optional[str]

    ):
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
        params = {}

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

    def as_dict(self):
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
    def link_from_server_json(cls,
                              raw_data,  # type: dict
                              ) -> S3ExternalAnalyticsLink:

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
            dataverse,  # type: str
            link_name,  # type: str
            connection_string=None,  # type: Optional[str]
            account_name=None,  # type: Optional[str]
            account_key=None,  # type: Optional[str]
            shared_access_signature=None,  # type: Optional[str]
            blob_endpoint=None,  # type: Optional[str]
            endpiont_suffix=None,  # type: Optional[str]

    ):
        super().__init__()
        self._dataverse = dataverse
        self._link_name = link_name
        self._connection_string = connection_string
        self._account_name = account_name
        self._account_key = account_key
        self._shared_access_signature = shared_access_signature
        self._blob_endpoint = blob_endpoint
        self._endpiont_suffix = endpiont_suffix

    def name(self) -> str:
        return self._link_name

    def dataverse_name(self) -> str:
        return self._dataverse

    def form_encode(self) -> bytes:
        params = {}

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

        if not is_null_or_empty(self._endpiont_suffix):
            params["endpointSuffix"] = self._endpiont_suffix

        return to_form_str(params).encode()

    def validate(self) -> None:
        if is_null_or_empty(self._dataverse):
            raise InvalidArgumentException(
                "Dataverse must be set for Azure blob external analytics links.")

        if is_null_or_empty(self._link_name):
            raise InvalidArgumentException(
                "Link name must be set for Azure blob external analytics links.")

        if is_null_or_empty(self._connection_string):
            acct_name_and_key = not (is_null_or_empty(self._account_name)
                                     or is_null_or_empty(self._account_key))
            acct_name_and_sas = not (is_null_or_empty(self._account_name)
                                     or is_null_or_empty(self._shared_access_signature))

            if not (acct_name_and_key or acct_name_and_sas):
                raise InvalidArgumentException(
                    "AccessKeyId must be set for Azure blob external analytics links.")

    def link_type(self) -> AnalyticsLinkType:
        return AnalyticsLinkType.AzureBlobExternal

    def as_dict(self):
        link_dict = {
            'link_name': self.name(),
            'dataverse': self.dataverse_name(),
            'link_type': AnalyticsLinkType.AzureBlobExternal.value
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
        if self._endpiont_suffix:
            link_dict['endpiont_suffix'] = self._endpiont_suffix

        return link_dict

    @classmethod
    def link_from_server_json(cls,
                              raw_data,  # type: dict
                              ) -> AzureBlobExternalAnalyticsLink:

        dataverse = raw_data['dataverse']
        link_name = raw_data['link_name']
        account_name = raw_data.get('account_name', None)
        blob_endpoint = raw_data.get('blob_endpoint', None)
        endpoint_suffix = raw_data.get('endpoint_suffix', None)

        return AzureBlobExternalAnalyticsLink(dataverse,
                                              link_name,
                                              account_name=account_name,
                                              blob_endpoint=blob_endpoint,
                                              endpiont_suffix=endpoint_suffix)
