from abc import ABC, abstractmethod
from typing import *
from datetime import timedelta
from enum import Enum

from durationpy import from_str

from couchbase.options import QueryBaseOptions, enum_value
from couchbase_core.mapper import identity
from .n1ql import *
from couchbase_core.n1ql import N1QLRequest
from couchbase_core.analytics import AnalyticsQuery, AnalyticsRequest
from couchbase_core import iterable_wrapper, mk_formstr
from couchbase.exceptions import InvalidArgumentException


class AnalyticsIndex(dict):
    def __init__(self, **kwargs):
        #print("creating index from {}".format(kwargs))
        super(AnalyticsIndex, self).__init__(**kwargs['Index'])

    @property
    def name(self):
        return self.get("IndexName", None)

    @property
    def dataset_name(self):
        return self.get("DatasetName", None)

    @property
    def dataverse_name(self):
        return self.get("DataverseName", None)

    @property
    def is_primary(self):
        return self.get("IsPrimary", None)


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


class AnalyticsDataset(dict):
    def __init__(self, **kwargs):
        super(AnalyticsDataset, self).__init__(**kwargs)

    @property
    def dataset_name(self):
        return self.get("DatasetName", None)

    @property
    def dataverse_name(self):
        return self.get('DataverseName', None)

    @property
    def link_name(self):
        return self.get('LinkName', None)

    @property
    def bucket_name(self):
        return self.get('BucketName', None)


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
    def dataverse_name(
        self,  # type: "AnalyticsLink"
    ) -> str:
        """Returns the name of the dataverse the :class:`couchbase.analytics.AnalyticsLink` belongs to

        :return: The name of the dataverse
        """
        pass

    @abstractmethod
    def form_encode(
        self,  # type: "AnalyticsLink"
    ) -> bytes:
        """Encodes the :class:`couchbase.analytics.AnalyticsLink` into a form data representation, 
            to send as the body of a :func:`couchbase.management.analytics.CreateLink` or 
            :func:`couchbase.management.analytics.ReplaceLink`

        :return: A form encoded :class:`couchbase.analytics.AnalyticsLink`
        """
        pass

    @abstractmethod
    def validate(
        self,  # type: "AnalyticsLink"
    ):
        """Ensures the :class:`couchbase.analytics.AnalyticsLink` is valid.  Raises a :class:`couchbase.exceptions.InvalidArgumentException` if link is invalid.

        :return: None

        :raises: :class:`couchbase.exceptions.InvalidArgumentException`
        """
        pass

    @abstractmethod
    def link_type(
        self,  # type: "AnalyticsLink"
    ) -> AnalyticsLinkType:
        """Returns the :class:`couchbase.analytics.AnalyticsLinkType` of the :class:`couchbase.analytics.AnalyticsLink`

        :return: The corresponding :class:`couchbase.analytics.AnalyticsLinkType` of the :class:`couchbase.analytics.AnalyticsLink`
        """
        pass


class CouchbaseAnalyticsEncryptionSettings(object):
    """The settings available for setting encryption level on a link.

    :param encryption_level: The level of encryption to apply, defaults to :class:`couchbase.analytics.AnalyticsEncryptionLevel`.NONE
    :type encryption_level: :class:`couchbase.analytics.AnalyticsEncryptionLevel`
    :param certificate: The certificate to use when encryption level is set to full. Must be set if encryption level is set to full.  Defaults to None.
    :type certificate: bytes | bytearray
    :param client_certificate: The client certificate to use when encryption level is set to full. Cannot be used if username and password are also used. Defaults to None
    :type client_certificate: bytes | bytearray
    :param client_key: The client key to use when encryption level is set to full.  Cannot be used if username and password are also used. Defaults to None
    :type client_key: bytes | bytearray
    """

    def __init__(
        self,  # type: "CouchbaseAnalyticsEncryptionSettings"
        encryption_level=None,  # type: AnalyticsEncryptionLevel
        certificate=None,  # type: Union[bytes, bytearray]
        client_certificate=None,  # type: Union[bytes, bytearray]
        client_key=None,  # type: Union[bytes, bytearray]
    ):
        self._encryption_level = encryption_level
        if self._encryption_level is None:
            self._encryption_level = AnalyticsEncryptionLevel.NONE
        self._certificate = certificate
        self._client_certificate = client_certificate
        self._client_key = client_key

    @property
    def encryption_level(self):
        return self._encryption_level

    @encryption_level.setter
    def encryption_level(self, value):
        self._encryption_level = value

    @property
    def certificate(self):
        return self._certificate

    @certificate.setter
    def certificate(self, value):
        self._certificate = value

    @property
    def client_certificate(self):
        return self._client_certificate

    @client_certificate.setter
    def client_certificate(self, value):
        self._client_certificate = value

    @property
    def client_key(self):
        return self._client_key

    @client_key.setter
    def client_key(self, value):
        self._client_key = value

    @classmethod
    def from_server_json(
        cls,  # type: "CouchbaseAnalyticsEncryptionSettings"
        raw_data  # type: dict
    ) -> "CouchbaseAnalyticsEncryptionSettings":

        encryption_settings = CouchbaseAnalyticsEncryptionSettings()
        if raw_data["encryption"] == AnalyticsEncryptionLevel.NONE.value:
            encryption_settings.encryption_level = AnalyticsEncryptionLevel.NONE
        elif raw_data["encryption"] == AnalyticsEncryptionLevel.HALF.value:
            encryption_settings.encryption_level = AnalyticsEncryptionLevel.HALF
        elif raw_data["encryption"] == AnalyticsEncryptionLevel.FULL.value:
            encryption_settings.encryption_level = AnalyticsEncryptionLevel.FULL

        if "certificate" in raw_data and raw_data["certificate"] and raw_data["certificate"].split():
            encryption_settings.certificate = bytes(
                raw_data["certificate"], "utf-8")

        if "clientCertificate" in raw_data and raw_data["clientCertificate"] and raw_data["clientCertificate"].split():
            encryption_settings.certificate = bytes(
                raw_data["clientCertificate"], "utf-8")

        return encryption_settings


def is_null_or_empty(
    value  # type: str
) -> bool:
    return not (value and value.split())


class CouchbaseRemoteAnalyticsLink(AnalyticsLink):
    def __init__(
            self,  # type: "CouchbaseRemoteAnalyticsLink"
            dataverse,  # type: str
            link_name,  # type: str
            hostname,  # type: str
            encryption,  # type: CouchbaseAnalyticsEncryptionSettings
            username=None,  # type: str
            password=None,  # type: str

    ):
        super().__init__()
        self._dataverse = dataverse
        self._link_name = link_name
        self._hostname = hostname
        self._encryption = encryption
        self._username = username
        self._password = password

    def name(
        self,  # type: "CouchbaseRemoteAnalyticsLink"
    ) -> str:
        return self._link_name

    def dataverse_name(
        self,  # type: "CouchbaseRemoteAnalyticsLink"
    ) -> str:
        return self._dataverse

    def form_encode(self: "CouchbaseRemoteAnalyticsLink") -> bytes:
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

        if self._encryption.certificate and len(self._encryption.certificate) > 0:
            params["certificate"] = self._encryption.certificate.decode(
                "utf-8")

        if self._encryption.client_certificate and len(self._encryption.client_certificate) > 0:
            params["clientCertificate"] = self._encryption.client_certificate.decode(
                "utf-8")

        if self._encryption.client_key and len(self._encryption.client_key) > 0:
            params["clientKey"] = self._encryption.client_key.decode("utf-8")

        return mk_formstr(params).encode()

    def validate(self: "CouchbaseRemoteAnalyticsLink"):
        if is_null_or_empty(self._dataverse):
            raise InvalidArgumentException(
                "Dataverse must be set for couchbase analytics links.")

        if is_null_or_empty(self._link_name):
            raise InvalidArgumentException(
                "Link name must be set for couchbase analytics links.")

        if is_null_or_empty(self._hostname):
            raise InvalidArgumentException(
                "Hostname must be set for couchbase analytics links.")

        if self._encryption.encryption_level in [AnalyticsEncryptionLevel.NONE, AnalyticsEncryptionLevel.HALF]:
            if is_null_or_empty(self._username) or is_null_or_empty(self._password):
                raise InvalidArgumentException(
                    "When encryption level is half or none, username and password must be set for couchbase analytics links.")
        elif self._encryption.encryption_level == AnalyticsEncryptionLevel.FULL:
            if not (self._encryption.certificate and len(self._encryption.certificate) > 0):
                raise InvalidArgumentException(
                    "When encryption level is full a certificate must be set for couchbase analytics links.")
            if not ((self._encryption.client_certificate and len(self._encryption.client_certificate) > 0)
                    and (self._encryption.client_key and len(self._encryption.client_key) > 0)):
                raise InvalidArgumentException(
                    "When encryption level is full the client certificate and key must be set for couchbase analytics links.")

    def link_type(self: "CouchbaseRemoteAnalyticsLink") -> AnalyticsLinkType:
        return AnalyticsLinkType.CouchbaseRemote

    @classmethod
    def link_from_server_json(
        cls,  # type: "CouchbaseRemoteAnalyticsLink"
        raw_data,  # type: dict
    ) -> "CouchbaseRemoteAnalyticsLink":

        dataverse = raw_data["dataverse"] if "dataverse" in raw_data else raw_data["scope"]
        link_name = raw_data["name"]
        hostname = raw_data["activeHostname"]
        encryption = CouchbaseAnalyticsEncryptionSettings.from_server_json(
            raw_data)
        username = raw_data["username"]

        return CouchbaseRemoteAnalyticsLink(dataverse, link_name, hostname, encryption, username)


class S3ExternalAnalyticsLink(AnalyticsLink):
    def __init__(
            self,  # type: "S3ExternalAnalyticsLink"
            dataverse,  # type: str
            link_name,  # type: str
            access_key_id,  # type: str
            region,  # type: str
            secret_access_key=None,  # type: str
            session_token=None,  # type: str
            service_endpoint=None,  # type: str

    ):
        super().__init__()
        self._dataverse = dataverse
        self._link_name = link_name
        self._access_key_id = access_key_id
        self._region = region
        self._secret_access_key = secret_access_key
        self._session_token = session_token
        self._service_endpoint = service_endpoint

    def name(
        self,  # type: "S3ExternalAnalyticsLink"
    ) -> str:
        return self._link_name

    def dataverse_name(
        self,  # type: "S3ExternalAnalyticsLink"
    ) -> str:
        return self._dataverse

    def form_encode(self: "S3ExternalAnalyticsLink") -> bytes:
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

        return mk_formstr(params).encode()

    def validate(self: "S3ExternalAnalyticsLink"):
        if is_null_or_empty(self._dataverse):
            raise InvalidArgumentException(
                "Dataverse must be set for S3 external analytics links.")

        if is_null_or_empty(self._link_name):
            raise InvalidArgumentException(
                "Link name must be set for S3 external analytics links.")

        if is_null_or_empty(self._access_key_id):
            raise InvalidArgumentException(
                "Access key id must be set for S3 external analytics links.")

        if is_null_or_empty(self._secret_access_key):
            raise InvalidArgumentException(
                "Secret access key must be set for S3 external analytics links.")

        if is_null_or_empty(self._region):
            raise InvalidArgumentException(
                "Region must be set for S3 external analytics links.")

    def link_type(self: "S3ExternalAnalyticsLink") -> AnalyticsLinkType:
        return AnalyticsLinkType.S3External

    @classmethod
    def link_from_server_json(
        cls,  # type: "S3ExternalAnalyticsLink"
        raw_data,  # type: dict
    ) -> "S3ExternalAnalyticsLink":

        dataverse = raw_data["dataverse"] if "dataverse" in raw_data else raw_data["scope"]
        link_name = raw_data["name"]
        access_key_id = raw_data["accessKeyId"]
        region = raw_data["region"]
        service_endpoint = raw_data["serviceEndpoint"]

        return S3ExternalAnalyticsLink(dataverse, link_name, access_key_id, region, service_endpoint=service_endpoint)


class AzureBlobExternalAnalyticsLink(AnalyticsLink):
    def __init__(
            self,  # type: "AzureBlobExternalAnalyticsLink"
            dataverse,  # type: str
            link_name,  # type: str
            connection_string=None,  # type: str
            account_name=None,  # type: str
            account_key=None,  # type: str
            shared_access_signature=None,  # type: str
            blob_endpoint=None,  # type: str
            endpiont_suffix=None,  # type: str

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

    def name(
        self,  # type: "AzureBlobExternalAnalyticsLink"
    ) -> str:
        return self._link_name

    def dataverse_name(
        self,  # type: "AzureBlobExternalAnalyticsLink"
    ) -> str:
        return self._dataverse

    def form_encode(self: "AzureBlobExternalAnalyticsLink") -> bytes:
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

        return mk_formstr(params).encode()

    def validate(self: "AzureBlobExternalAnalyticsLink"):
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

    def link_type(self: "AzureBlobExternalAnalyticsLink") -> AnalyticsLinkType:
        return AnalyticsLinkType.AzureBlobExternal

    @classmethod
    def link_from_server_json(
        cls,  # type: "AzureBlobExternalAnalyticsLink"
        raw_data,  # type: dict
    ) -> "AzureBlobExternalAnalyticsLink":

        dataverse = raw_data["dataverse"] if "dataverse" in raw_data else raw_data["scope"]
        link_name = raw_data["name"]
        account_name = raw_data["accountName"]
        blob_endpoint = raw_data["blobEndpoint"]
        endpoint_suffix = raw_data["endpointSuffix"]

        return AzureBlobExternalAnalyticsLink(dataverse,
                                              link_name,
                                              account_name=account_name,
                                              blob_endpoint=blob_endpoint,
                                              endpiont_suffix=endpoint_suffix)


class AnalyticsResult(iterable_wrapper(AnalyticsRequest)):
    def __init__(self,
                 *args, **kwargs  # type: N1QLRequest
                 ):
        super(AnalyticsResult, self).__init__(*args, **kwargs)

    def metadata(self  # type: AnalyticsResult
                 ):
        # type: (...) -> AnalyticsMetaData
        return AnalyticsMetaData(self)


class AnalyticsScanConsistency(enum.Enum):
    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"


class AnalyticsOptions(QueryBaseOptions):
    VALID_OPTS = {'timeout': {'timeout': timedelta.seconds},
                  'read_only': {'readonly': identity},
                  'scan_consistency': {'consistency': enum_value},
                  'client_context_id': {'client_context_id': identity},
                  'priority': {'priority': identity},
                  'positional_parameters': {},
                  'named_parameters': {},
                  'query_context': {'query_context': identity},
                  'raw': {}}

    TARGET_CLASS = AnalyticsQuery

    @overload
    def __init__(self,
                 timeout=None,  # type: timedelta
                 read_only=None,  # type: bool
                 scan_consistency=None,  # type: AnalyticsScanConsistency
                 client_context_id=None,  # type: str
                 priority=None,  # type: bool
                 positional_parameters=None,  # type: Iterable[str]
                 named_parameters=None,  # type: Dict[str, str]
                 query_context=None,          # type: str
                 raw=None,  # type: Dict[str,Any]
                 ):
        """

        :param timedelta timeout:
        :param bool read_only:
        :param AnalyticsScanConsistency scan_consistency:
        :param str client_context_id:
        :param bool priority:
        :param Iterable[JSON] positional_parameters:
        :param dict[str,JSON] named_parameters:
        :param str query_context:
        :param dict[str,JSON] raw:
        """
        pass

    def __init__(self,
                 **kwargs
                 ):
        super(AnalyticsOptions, self).__init__(**kwargs)


class AnalyticsStatus(enum.Enum):
    RUNNING = ()
    SUCCESS = ()
    ERRORS = ()
    COMPLETED = ()
    STOPPED = ()
    TIMEOUT = ()
    CLOSED = ()
    FATAL = ()
    ABORTED = ()
    UNKNOWN = ()


class AnalyticsWarning(object):
    def __init__(self, raw_warning):
        self._raw_warning = raw_warning

    def code(self):
        # type: (...) -> int
        return self._raw_warning.get('code')

    def message(self):
        # type: (...) -> str
        return self._raw_warning.get('msg')


class AnalyticsMetrics(object):
    def __init__(self,
                 parent  # type: AnalyticsResult
                 ):
        self._parentquery = parent

    @property
    def _raw_metrics(self):
        return self._parentquery.metrics

    def _as_timedelta(self, time_str):
        return from_str(self._raw_metrics.get(time_str))

    def elapsed_time(self):
        # type: (...) -> timedelta
        return self._as_timedelta('elapsedTime')

    def execution_time(self):
        # type: (...) -> timedelta
        return self._as_timedelta('executionTime')

    def result_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('resultCount', 0))

    def result_size(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('resultSize', 0))

    def error_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('errorCount', 0))

    def processed_objects(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('processedObjects', 0))

    def warning_count(self):
        # type: (...) -> UnsignedInt64
        return UnsignedInt64(self._raw_metrics.get('warningCount', 0))


class AnalyticsMetaData(object):
    def __init__(self,
                 parent  # type: AnalyticsResult
                 ):
        self._parentquery_for_metadata = parent

    def request_id(self):
        # type: (...) -> str
        return self._parentquery_for_metadata.meta.get('requestID')

    def client_context_id(self):
        # type: (...) -> str
        return self._parentquery_for_metadata.meta.get('clientContextID')

    def signature(self):
        # type: (...) -> Optional[JSON]
        return self._parentquery_for_metadata.meta.get('signature')

    def status(self):
        # type: (...) -> AnalyticsStatus
        return AnalyticsStatus[self._parentquery_for_metadata.meta.get('status').upper()]

    def warnings(self):
        # type: (...) -> List[AnalyticsWarning]
        return list(map(AnalyticsWarning, self._parentquery_for_metadata.meta.get('warnings', [])))

    def metrics(self):
        # type: (...) -> Optional[AnalyticsMetrics]
        return AnalyticsMetrics(self._parentquery_for_metadata)
