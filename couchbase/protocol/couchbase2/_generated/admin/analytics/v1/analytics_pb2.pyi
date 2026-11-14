#  Copyright 2016-2026. Couchbase, Inc.
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

# !! AUTO-GENERATED from tools/autogen/config/protos.yaml: DO NOT EDIT MANUALLY !!
# Regenerate with `python -m tools.autogen protos generate --write`.

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LinkStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LINK_STATUS_UNKNOWN: _ClassVar[LinkStatus]
    LINK_STATUS_HEALTHY: _ClassVar[LinkStatus]
    LINK_STATUS_STOPPED: _ClassVar[LinkStatus]
    LINK_STATUS_UNHEALTHY: _ClassVar[LinkStatus]
    LINK_STATUS_SUSPENDED: _ClassVar[LinkStatus]

class CouchbaseEncryptionMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COUCHBASE_ENCRYPTION_MODE_NONE: _ClassVar[CouchbaseEncryptionMode]
    COUCHBASE_ENCRYPTION_MODE_HALF: _ClassVar[CouchbaseEncryptionMode]
    COUCHBASE_ENCRYPTION_MODE_FULL: _ClassVar[CouchbaseEncryptionMode]
LINK_STATUS_UNKNOWN: LinkStatus
LINK_STATUS_HEALTHY: LinkStatus
LINK_STATUS_STOPPED: LinkStatus
LINK_STATUS_UNHEALTHY: LinkStatus
LINK_STATUS_SUSPENDED: LinkStatus
COUCHBASE_ENCRYPTION_MODE_NONE: CouchbaseEncryptionMode
COUCHBASE_ENCRYPTION_MODE_HALF: CouchbaseEncryptionMode
COUCHBASE_ENCRYPTION_MODE_FULL: CouchbaseEncryptionMode

class CreateLinkRequest(_message.Message):
    __slots__ = ("analytics_scope_name", "link_name", "couchbase_link", "s3_link", "azureblob_link", "gcs_link")
    class CouchbaseLink(_message.Message):
        __slots__ = ("hostname", "encryption_mode", "username", "password", "certificate", "client_certificate", "client_key")
        HOSTNAME_FIELD_NUMBER: _ClassVar[int]
        ENCRYPTION_MODE_FIELD_NUMBER: _ClassVar[int]
        USERNAME_FIELD_NUMBER: _ClassVar[int]
        PASSWORD_FIELD_NUMBER: _ClassVar[int]
        CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
        CLIENT_CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
        CLIENT_KEY_FIELD_NUMBER: _ClassVar[int]
        hostname: str
        encryption_mode: CouchbaseEncryptionMode
        username: str
        password: str
        certificate: str
        client_certificate: str
        client_key: str
        def __init__(self, hostname: _Optional[str] = ..., encryption_mode: _Optional[_Union[CouchbaseEncryptionMode, str]] = ..., username: _Optional[str] = ..., password: _Optional[str] = ..., certificate: _Optional[str] = ..., client_certificate: _Optional[str] = ..., client_key: _Optional[str] = ...) -> None: ...
    class S3Link(_message.Message):
        __slots__ = ("access_key_id", "secret_access_key", "session_token", "region", "service_endpoint")
        ACCESS_KEY_ID_FIELD_NUMBER: _ClassVar[int]
        SECRET_ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
        SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
        REGION_FIELD_NUMBER: _ClassVar[int]
        SERVICE_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        access_key_id: str
        secret_access_key: str
        session_token: str
        region: str
        service_endpoint: str
        def __init__(self, access_key_id: _Optional[str] = ..., secret_access_key: _Optional[str] = ..., session_token: _Optional[str] = ..., region: _Optional[str] = ..., service_endpoint: _Optional[str] = ...) -> None: ...
    class AzureBlobLink(_message.Message):
        __slots__ = ("account_name", "account_key", "shared_access_signature", "managed_identity_id", "client_id", "tenant_id", "client_secret", "client_certificate_password", "endpoint")
        ACCOUNT_NAME_FIELD_NUMBER: _ClassVar[int]
        ACCOUNT_KEY_FIELD_NUMBER: _ClassVar[int]
        SHARED_ACCESS_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
        MANAGED_IDENTITY_ID_FIELD_NUMBER: _ClassVar[int]
        CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
        TENANT_ID_FIELD_NUMBER: _ClassVar[int]
        CLIENT_SECRET_FIELD_NUMBER: _ClassVar[int]
        CLIENT_CERTIFICATE_PASSWORD_FIELD_NUMBER: _ClassVar[int]
        ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        account_name: str
        account_key: str
        shared_access_signature: str
        managed_identity_id: str
        client_id: str
        tenant_id: str
        client_secret: str
        client_certificate_password: str
        endpoint: str
        def __init__(self, account_name: _Optional[str] = ..., account_key: _Optional[str] = ..., shared_access_signature: _Optional[str] = ..., managed_identity_id: _Optional[str] = ..., client_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., client_secret: _Optional[str] = ..., client_certificate_password: _Optional[str] = ..., endpoint: _Optional[str] = ...) -> None: ...
    class GcsLink(_message.Message):
        __slots__ = ("json_credentials",)
        JSON_CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
        json_credentials: str
        def __init__(self, json_credentials: _Optional[str] = ...) -> None: ...
    ANALYTICS_SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    LINK_NAME_FIELD_NUMBER: _ClassVar[int]
    COUCHBASE_LINK_FIELD_NUMBER: _ClassVar[int]
    S3_LINK_FIELD_NUMBER: _ClassVar[int]
    AZUREBLOB_LINK_FIELD_NUMBER: _ClassVar[int]
    GCS_LINK_FIELD_NUMBER: _ClassVar[int]
    analytics_scope_name: str
    link_name: str
    couchbase_link: CreateLinkRequest.CouchbaseLink
    s3_link: CreateLinkRequest.S3Link
    azureblob_link: CreateLinkRequest.AzureBlobLink
    gcs_link: CreateLinkRequest.GcsLink
    def __init__(self, analytics_scope_name: _Optional[str] = ..., link_name: _Optional[str] = ..., couchbase_link: _Optional[_Union[CreateLinkRequest.CouchbaseLink, _Mapping]] = ..., s3_link: _Optional[_Union[CreateLinkRequest.S3Link, _Mapping]] = ..., azureblob_link: _Optional[_Union[CreateLinkRequest.AzureBlobLink, _Mapping]] = ..., gcs_link: _Optional[_Union[CreateLinkRequest.GcsLink, _Mapping]] = ...) -> None: ...

class CreateLinkResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetLinkRequest(_message.Message):
    __slots__ = ("analytics_scope_name", "link_name")
    ANALYTICS_SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    LINK_NAME_FIELD_NUMBER: _ClassVar[int]
    analytics_scope_name: str
    link_name: str
    def __init__(self, analytics_scope_name: _Optional[str] = ..., link_name: _Optional[str] = ...) -> None: ...

class GetLinkResponse(_message.Message):
    __slots__ = ("couchbase_link", "s3_link", "azureblob_link", "gcs_link")
    class CouchbaseLink(_message.Message):
        __slots__ = ("uuid", "active_hostname", "bootstrap_hostname", "is_bootstrap_alternate_address", "certificate", "encryption_mode", "username")
        UUID_FIELD_NUMBER: _ClassVar[int]
        ACTIVE_HOSTNAME_FIELD_NUMBER: _ClassVar[int]
        BOOTSTRAP_HOSTNAME_FIELD_NUMBER: _ClassVar[int]
        IS_BOOTSTRAP_ALTERNATE_ADDRESS_FIELD_NUMBER: _ClassVar[int]
        CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
        ENCRYPTION_MODE_FIELD_NUMBER: _ClassVar[int]
        USERNAME_FIELD_NUMBER: _ClassVar[int]
        uuid: str
        active_hostname: str
        bootstrap_hostname: str
        is_bootstrap_alternate_address: bool
        certificate: str
        encryption_mode: CouchbaseEncryptionMode
        username: str
        def __init__(self, uuid: _Optional[str] = ..., active_hostname: _Optional[str] = ..., bootstrap_hostname: _Optional[str] = ..., is_bootstrap_alternate_address: bool = ..., certificate: _Optional[str] = ..., encryption_mode: _Optional[_Union[CouchbaseEncryptionMode, str]] = ..., username: _Optional[str] = ...) -> None: ...
    class S3Link(_message.Message):
        __slots__ = ("access_key_id", "region", "service_endpoint")
        ACCESS_KEY_ID_FIELD_NUMBER: _ClassVar[int]
        REGION_FIELD_NUMBER: _ClassVar[int]
        SERVICE_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        access_key_id: str
        region: str
        service_endpoint: str
        def __init__(self, access_key_id: _Optional[str] = ..., region: _Optional[str] = ..., service_endpoint: _Optional[str] = ...) -> None: ...
    class AzureBlobLink(_message.Message):
        __slots__ = ("account_name", "client_id", "endpoint", "managed_identity_id", "tenant_id")
        ACCOUNT_NAME_FIELD_NUMBER: _ClassVar[int]
        CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
        ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        MANAGED_IDENTITY_ID_FIELD_NUMBER: _ClassVar[int]
        TENANT_ID_FIELD_NUMBER: _ClassVar[int]
        account_name: str
        client_id: str
        endpoint: str
        managed_identity_id: str
        tenant_id: str
        def __init__(self, account_name: _Optional[str] = ..., client_id: _Optional[str] = ..., endpoint: _Optional[str] = ..., managed_identity_id: _Optional[str] = ..., tenant_id: _Optional[str] = ...) -> None: ...
    class GcsLink(_message.Message):
        __slots__ = ("use_application_default_credentials", "endpoint")
        USE_APPLICATION_DEFAULT_CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
        ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        use_application_default_credentials: bool
        endpoint: str
        def __init__(self, use_application_default_credentials: bool = ..., endpoint: _Optional[str] = ...) -> None: ...
    COUCHBASE_LINK_FIELD_NUMBER: _ClassVar[int]
    S3_LINK_FIELD_NUMBER: _ClassVar[int]
    AZUREBLOB_LINK_FIELD_NUMBER: _ClassVar[int]
    GCS_LINK_FIELD_NUMBER: _ClassVar[int]
    couchbase_link: GetLinkResponse.CouchbaseLink
    s3_link: GetLinkResponse.S3Link
    azureblob_link: GetLinkResponse.AzureBlobLink
    gcs_link: GetLinkResponse.GcsLink
    def __init__(self, couchbase_link: _Optional[_Union[GetLinkResponse.CouchbaseLink, _Mapping]] = ..., s3_link: _Optional[_Union[GetLinkResponse.S3Link, _Mapping]] = ..., azureblob_link: _Optional[_Union[GetLinkResponse.AzureBlobLink, _Mapping]] = ..., gcs_link: _Optional[_Union[GetLinkResponse.GcsLink, _Mapping]] = ...) -> None: ...

class UpdateLinkRequest(_message.Message):
    __slots__ = ("analytics_scope_name", "link_name", "couchbase_link", "s3_link", "azureblob_link", "gcs_link")
    class CouchbaseLink(_message.Message):
        __slots__ = ("hostname", "encryption_mode", "username", "password", "certificate", "client_certificate", "client_key")
        HOSTNAME_FIELD_NUMBER: _ClassVar[int]
        ENCRYPTION_MODE_FIELD_NUMBER: _ClassVar[int]
        USERNAME_FIELD_NUMBER: _ClassVar[int]
        PASSWORD_FIELD_NUMBER: _ClassVar[int]
        CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
        CLIENT_CERTIFICATE_FIELD_NUMBER: _ClassVar[int]
        CLIENT_KEY_FIELD_NUMBER: _ClassVar[int]
        hostname: str
        encryption_mode: CouchbaseEncryptionMode
        username: str
        password: str
        certificate: str
        client_certificate: str
        client_key: str
        def __init__(self, hostname: _Optional[str] = ..., encryption_mode: _Optional[_Union[CouchbaseEncryptionMode, str]] = ..., username: _Optional[str] = ..., password: _Optional[str] = ..., certificate: _Optional[str] = ..., client_certificate: _Optional[str] = ..., client_key: _Optional[str] = ...) -> None: ...
    class S3Link(_message.Message):
        __slots__ = ("access_key_id", "secret_access_key", "session_token", "region", "service_endpoint")
        ACCESS_KEY_ID_FIELD_NUMBER: _ClassVar[int]
        SECRET_ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
        SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
        REGION_FIELD_NUMBER: _ClassVar[int]
        SERVICE_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        access_key_id: str
        secret_access_key: str
        session_token: str
        region: str
        service_endpoint: str
        def __init__(self, access_key_id: _Optional[str] = ..., secret_access_key: _Optional[str] = ..., session_token: _Optional[str] = ..., region: _Optional[str] = ..., service_endpoint: _Optional[str] = ...) -> None: ...
    class AzureBlobLink(_message.Message):
        __slots__ = ("account_name", "account_key", "shared_access_signature", "managed_identity_id", "client_id", "tenant_id", "client_secret", "client_certificate_password", "endpoint")
        ACCOUNT_NAME_FIELD_NUMBER: _ClassVar[int]
        ACCOUNT_KEY_FIELD_NUMBER: _ClassVar[int]
        SHARED_ACCESS_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
        MANAGED_IDENTITY_ID_FIELD_NUMBER: _ClassVar[int]
        CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
        TENANT_ID_FIELD_NUMBER: _ClassVar[int]
        CLIENT_SECRET_FIELD_NUMBER: _ClassVar[int]
        CLIENT_CERTIFICATE_PASSWORD_FIELD_NUMBER: _ClassVar[int]
        ENDPOINT_FIELD_NUMBER: _ClassVar[int]
        account_name: str
        account_key: str
        shared_access_signature: str
        managed_identity_id: str
        client_id: str
        tenant_id: str
        client_secret: str
        client_certificate_password: str
        endpoint: str
        def __init__(self, account_name: _Optional[str] = ..., account_key: _Optional[str] = ..., shared_access_signature: _Optional[str] = ..., managed_identity_id: _Optional[str] = ..., client_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., client_secret: _Optional[str] = ..., client_certificate_password: _Optional[str] = ..., endpoint: _Optional[str] = ...) -> None: ...
    class GcsLink(_message.Message):
        __slots__ = ("json_credentials",)
        JSON_CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
        json_credentials: str
        def __init__(self, json_credentials: _Optional[str] = ...) -> None: ...
    ANALYTICS_SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    LINK_NAME_FIELD_NUMBER: _ClassVar[int]
    COUCHBASE_LINK_FIELD_NUMBER: _ClassVar[int]
    S3_LINK_FIELD_NUMBER: _ClassVar[int]
    AZUREBLOB_LINK_FIELD_NUMBER: _ClassVar[int]
    GCS_LINK_FIELD_NUMBER: _ClassVar[int]
    analytics_scope_name: str
    link_name: str
    couchbase_link: UpdateLinkRequest.CouchbaseLink
    s3_link: UpdateLinkRequest.S3Link
    azureblob_link: UpdateLinkRequest.AzureBlobLink
    gcs_link: UpdateLinkRequest.GcsLink
    def __init__(self, analytics_scope_name: _Optional[str] = ..., link_name: _Optional[str] = ..., couchbase_link: _Optional[_Union[UpdateLinkRequest.CouchbaseLink, _Mapping]] = ..., s3_link: _Optional[_Union[UpdateLinkRequest.S3Link, _Mapping]] = ..., azureblob_link: _Optional[_Union[UpdateLinkRequest.AzureBlobLink, _Mapping]] = ..., gcs_link: _Optional[_Union[UpdateLinkRequest.GcsLink, _Mapping]] = ...) -> None: ...

class UpdateLinkResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeleteLinkRequest(_message.Message):
    __slots__ = ("analytics_scope_name", "link_name")
    ANALYTICS_SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    LINK_NAME_FIELD_NUMBER: _ClassVar[int]
    analytics_scope_name: str
    link_name: str
    def __init__(self, analytics_scope_name: _Optional[str] = ..., link_name: _Optional[str] = ...) -> None: ...

class DeleteLinkResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetIngestionStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetIngestionStatusResponse(_message.Message):
    __slots__ = ("links",)
    class Link(_message.Message):
        __slots__ = ("link_name", "analytics_scope_name", "status", "states")
        class State(_message.Message):
            __slots__ = ("timestamp", "progress", "time_lag_ms", "items_processed", "seqno_advances", "scopes")
            class Scope(_message.Message):
                __slots__ = ("scope_name", "collections")
                class Collection(_message.Message):
                    __slots__ = ("collection_name",)
                    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
                    collection_name: str
                    def __init__(self, collection_name: _Optional[str] = ...) -> None: ...
                SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
                COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
                scope_name: str
                collections: _containers.RepeatedCompositeFieldContainer[GetIngestionStatusResponse.Link.State.Scope.Collection]
                def __init__(self, scope_name: _Optional[str] = ..., collections: _Optional[_Iterable[_Union[GetIngestionStatusResponse.Link.State.Scope.Collection, _Mapping]]] = ...) -> None: ...
            TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
            PROGRESS_FIELD_NUMBER: _ClassVar[int]
            TIME_LAG_MS_FIELD_NUMBER: _ClassVar[int]
            ITEMS_PROCESSED_FIELD_NUMBER: _ClassVar[int]
            SEQNO_ADVANCES_FIELD_NUMBER: _ClassVar[int]
            SCOPES_FIELD_NUMBER: _ClassVar[int]
            timestamp: _timestamp_pb2.Timestamp
            progress: float
            time_lag_ms: int
            items_processed: int
            seqno_advances: int
            scopes: _containers.RepeatedCompositeFieldContainer[GetIngestionStatusResponse.Link.State.Scope]
            def __init__(self, timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., progress: _Optional[float] = ..., time_lag_ms: _Optional[int] = ..., items_processed: _Optional[int] = ..., seqno_advances: _Optional[int] = ..., scopes: _Optional[_Iterable[_Union[GetIngestionStatusResponse.Link.State.Scope, _Mapping]]] = ...) -> None: ...
        LINK_NAME_FIELD_NUMBER: _ClassVar[int]
        ANALYTICS_SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        STATES_FIELD_NUMBER: _ClassVar[int]
        link_name: str
        analytics_scope_name: str
        status: LinkStatus
        states: _containers.RepeatedCompositeFieldContainer[GetIngestionStatusResponse.Link.State]
        def __init__(self, link_name: _Optional[str] = ..., analytics_scope_name: _Optional[str] = ..., status: _Optional[_Union[LinkStatus, str]] = ..., states: _Optional[_Iterable[_Union[GetIngestionStatusResponse.Link.State, _Mapping]]] = ...) -> None: ...
    LINKS_FIELD_NUMBER: _ClassVar[int]
    links: _containers.RepeatedCompositeFieldContainer[GetIngestionStatusResponse.Link]
    def __init__(self, links: _Optional[_Iterable[_Union[GetIngestionStatusResponse.Link, _Mapping]]] = ...) -> None: ...
