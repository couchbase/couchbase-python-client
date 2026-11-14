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

class ClusterEdition(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLUSTER_EDITION_COMMUNITY: _ClassVar[ClusterEdition]
    CLUSTER_EDITION_ENTERPRISE: _ClassVar[ClusterEdition]
    CLUSTER_EDITION_MIXED: _ClassVar[ClusterEdition]

class ConflictResolutionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONFLICT_RESOLUTION_TYPE_TIMESTAMP: _ClassVar[ConflictResolutionType]
    CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER: _ClassVar[ConflictResolutionType]
    CONFLICT_RESOLUTION_TYPE_CUSTOM: _ClassVar[ConflictResolutionType]

class BucketType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BUCKET_TYPE_COUCHBASE: _ClassVar[BucketType]
    BUCKET_TYPE_EPHEMERAL: _ClassVar[BucketType]

class ContentType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTENT_TYPE_NONJSON: _ClassVar[ContentType]
    CONTENT_TYPE_JSON: _ClassVar[ContentType]
CLUSTER_EDITION_COMMUNITY: ClusterEdition
CLUSTER_EDITION_ENTERPRISE: ClusterEdition
CLUSTER_EDITION_MIXED: ClusterEdition
CONFLICT_RESOLUTION_TYPE_TIMESTAMP: ConflictResolutionType
CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER: ConflictResolutionType
CONFLICT_RESOLUTION_TYPE_CUSTOM: ConflictResolutionType
BUCKET_TYPE_COUCHBASE: BucketType
BUCKET_TYPE_EPHEMERAL: BucketType
CONTENT_TYPE_NONJSON: ContentType
CONTENT_TYPE_JSON: ContentType

class HeartbeatRequest(_message.Message):
    __slots__ = ("payload",)
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    def __init__(self, payload: _Optional[bytes] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetClusterInfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetClusterInfoResponse(_message.Message):
    __slots__ = ("cluster_uuid", "edition")
    CLUSTER_UUID_FIELD_NUMBER: _ClassVar[int]
    EDITION_FIELD_NUMBER: _ClassVar[int]
    cluster_uuid: str
    edition: ClusterEdition
    def __init__(self, cluster_uuid: _Optional[str] = ..., edition: _Optional[_Union[ClusterEdition, str]] = ...) -> None: ...

class GetBucketInfoRequest(_message.Message):
    __slots__ = ("bucket_name",)
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    def __init__(self, bucket_name: _Optional[str] = ...) -> None: ...

class GetBucketInfoResponse(_message.Message):
    __slots__ = ("bucket_uuid", "num_vbuckets", "conflict_resolution_type", "cross_cluster_versioning_enabled", "bucket_type")
    BUCKET_UUID_FIELD_NUMBER: _ClassVar[int]
    NUM_VBUCKETS_FIELD_NUMBER: _ClassVar[int]
    CONFLICT_RESOLUTION_TYPE_FIELD_NUMBER: _ClassVar[int]
    CROSS_CLUSTER_VERSIONING_ENABLED_FIELD_NUMBER: _ClassVar[int]
    BUCKET_TYPE_FIELD_NUMBER: _ClassVar[int]
    bucket_uuid: str
    num_vbuckets: int
    conflict_resolution_type: ConflictResolutionType
    cross_cluster_versioning_enabled: bool
    bucket_type: BucketType
    def __init__(self, bucket_uuid: _Optional[str] = ..., num_vbuckets: _Optional[int] = ..., conflict_resolution_type: _Optional[_Union[ConflictResolutionType, str]] = ..., cross_cluster_versioning_enabled: bool = ..., bucket_type: _Optional[_Union[BucketType, str]] = ...) -> None: ...

class GetVbucketInfoRequest(_message.Message):
    __slots__ = ("bucket_name", "vbucket_ids", "include_history", "include_max_cas")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    VBUCKET_IDS_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_HISTORY_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_MAX_CAS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    vbucket_ids: _containers.RepeatedScalarFieldContainer[int]
    include_history: bool
    include_max_cas: bool
    def __init__(self, bucket_name: _Optional[str] = ..., vbucket_ids: _Optional[_Iterable[int]] = ..., include_history: bool = ..., include_max_cas: bool = ...) -> None: ...

class GetVbucketInfoResponse(_message.Message):
    __slots__ = ("vbuckets",)
    class HistoryEntry(_message.Message):
        __slots__ = ("uuid", "seqno")
        UUID_FIELD_NUMBER: _ClassVar[int]
        SEQNO_FIELD_NUMBER: _ClassVar[int]
        uuid: int
        seqno: int
        def __init__(self, uuid: _Optional[int] = ..., seqno: _Optional[int] = ...) -> None: ...
    class VbucketState(_message.Message):
        __slots__ = ("vbucket_id", "uuid", "high_seqno", "history", "max_cas")
        VBUCKET_ID_FIELD_NUMBER: _ClassVar[int]
        UUID_FIELD_NUMBER: _ClassVar[int]
        HIGH_SEQNO_FIELD_NUMBER: _ClassVar[int]
        HISTORY_FIELD_NUMBER: _ClassVar[int]
        MAX_CAS_FIELD_NUMBER: _ClassVar[int]
        vbucket_id: int
        uuid: int
        high_seqno: int
        history: _containers.RepeatedCompositeFieldContainer[GetVbucketInfoResponse.HistoryEntry]
        max_cas: int
        def __init__(self, vbucket_id: _Optional[int] = ..., uuid: _Optional[int] = ..., high_seqno: _Optional[int] = ..., history: _Optional[_Iterable[_Union[GetVbucketInfoResponse.HistoryEntry, _Mapping]]] = ..., max_cas: _Optional[int] = ...) -> None: ...
    VBUCKETS_FIELD_NUMBER: _ClassVar[int]
    vbuckets: _containers.RepeatedCompositeFieldContainer[GetVbucketInfoResponse.VbucketState]
    def __init__(self, vbuckets: _Optional[_Iterable[_Union[GetVbucketInfoResponse.VbucketState, _Mapping]]] = ...) -> None: ...

class WatchCollectionsRequest(_message.Message):
    __slots__ = ("bucket_name",)
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    def __init__(self, bucket_name: _Optional[str] = ...) -> None: ...

class WatchCollectionsResponse(_message.Message):
    __slots__ = ("manifest_uid", "scopes")
    class Collection(_message.Message):
        __slots__ = ("collection_id", "collection_name")
        COLLECTION_ID_FIELD_NUMBER: _ClassVar[int]
        COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
        collection_id: int
        collection_name: str
        def __init__(self, collection_id: _Optional[int] = ..., collection_name: _Optional[str] = ...) -> None: ...
    class Scope(_message.Message):
        __slots__ = ("scope_id", "scope_name", "collections")
        SCOPE_ID_FIELD_NUMBER: _ClassVar[int]
        SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
        COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
        scope_id: int
        scope_name: str
        collections: _containers.RepeatedCompositeFieldContainer[WatchCollectionsResponse.Collection]
        def __init__(self, scope_id: _Optional[int] = ..., scope_name: _Optional[str] = ..., collections: _Optional[_Iterable[_Union[WatchCollectionsResponse.Collection, _Mapping]]] = ...) -> None: ...
    MANIFEST_UID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    manifest_uid: int
    scopes: _containers.RepeatedCompositeFieldContainer[WatchCollectionsResponse.Scope]
    def __init__(self, manifest_uid: _Optional[int] = ..., scopes: _Optional[_Iterable[_Union[WatchCollectionsResponse.Scope, _Mapping]]] = ...) -> None: ...

class GetDocumentRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "include_content", "include_xattrs")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_CONTENT_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_XATTRS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    include_content: bool
    include_xattrs: bool
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., include_content: bool = ..., include_xattrs: bool = ...) -> None: ...

class GetDocumentResponse(_message.Message):
    __slots__ = ("cas", "expiry", "is_deleted", "datatype", "content_flags", "content_compressed", "revno", "xattrs")
    class XattrsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    CAS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_FIELD_NUMBER: _ClassVar[int]
    IS_DELETED_FIELD_NUMBER: _ClassVar[int]
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    REVNO_FIELD_NUMBER: _ClassVar[int]
    XATTRS_FIELD_NUMBER: _ClassVar[int]
    cas: int
    expiry: _timestamp_pb2.Timestamp
    is_deleted: bool
    datatype: int
    content_flags: int
    content_compressed: bytes
    revno: int
    xattrs: _containers.ScalarMap[str, bytes]
    def __init__(self, cas: _Optional[int] = ..., expiry: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., is_deleted: bool = ..., datatype: _Optional[int] = ..., content_flags: _Optional[int] = ..., content_compressed: _Optional[bytes] = ..., revno: _Optional[int] = ..., xattrs: _Optional[_Mapping[str, bytes]] = ...) -> None: ...

class CheckDocumentRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "store_cas", "revno", "content_flags", "has_xattrs", "expiry_time", "is_deleted", "vb_uuid")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    STORE_CAS_FIELD_NUMBER: _ClassVar[int]
    REVNO_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    HAS_XATTRS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    IS_DELETED_FIELD_NUMBER: _ClassVar[int]
    VB_UUID_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    store_cas: int
    revno: int
    content_flags: int
    has_xattrs: bool
    expiry_time: _timestamp_pb2.Timestamp
    is_deleted: bool
    vb_uuid: int
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., store_cas: _Optional[int] = ..., revno: _Optional[int] = ..., content_flags: _Optional[int] = ..., has_xattrs: bool = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., is_deleted: bool = ..., vb_uuid: _Optional[int] = ...) -> None: ...

class CheckDocumentResponse(_message.Message):
    __slots__ = ("exists",)
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    exists: bool
    def __init__(self, exists: bool = ...) -> None: ...

class PushDocumentRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "check_cas", "store_cas", "revno", "content_flags", "content_type", "content_uncompressed", "content_compressed", "xattrs", "expiry_time", "is_deleted", "vb_uuid")
    class XattrsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CHECK_CAS_FIELD_NUMBER: _ClassVar[int]
    STORE_CAS_FIELD_NUMBER: _ClassVar[int]
    REVNO_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_UNCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    XATTRS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    IS_DELETED_FIELD_NUMBER: _ClassVar[int]
    VB_UUID_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    check_cas: int
    store_cas: int
    revno: int
    content_flags: int
    content_type: ContentType
    content_uncompressed: bytes
    content_compressed: bytes
    xattrs: _containers.ScalarMap[str, bytes]
    expiry_time: _timestamp_pb2.Timestamp
    is_deleted: bool
    vb_uuid: int
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., check_cas: _Optional[int] = ..., store_cas: _Optional[int] = ..., revno: _Optional[int] = ..., content_flags: _Optional[int] = ..., content_type: _Optional[_Union[ContentType, str]] = ..., content_uncompressed: _Optional[bytes] = ..., content_compressed: _Optional[bytes] = ..., xattrs: _Optional[_Mapping[str, bytes]] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., is_deleted: bool = ..., vb_uuid: _Optional[int] = ...) -> None: ...

class PushDocumentResponse(_message.Message):
    __slots__ = ("cas", "seqno")
    CAS_FIELD_NUMBER: _ClassVar[int]
    SEQNO_FIELD_NUMBER: _ClassVar[int]
    cas: int
    seqno: int
    def __init__(self, cas: _Optional[int] = ..., seqno: _Optional[int] = ...) -> None: ...
