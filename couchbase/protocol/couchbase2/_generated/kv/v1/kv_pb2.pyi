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

from google.rpc import status_pb2 as _status_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DurabilityLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DURABILITY_LEVEL_MAJORITY: _ClassVar[DurabilityLevel]
    DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE: _ClassVar[DurabilityLevel]
    DURABILITY_LEVEL_PERSIST_TO_MAJORITY: _ClassVar[DurabilityLevel]

class CompressionEnabled(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMPRESSION_ENABLED_OPTIONAL: _ClassVar[CompressionEnabled]
    COMPRESSION_ENABLED_ALWAYS: _ClassVar[CompressionEnabled]
DURABILITY_LEVEL_MAJORITY: DurabilityLevel
DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE: DurabilityLevel
DURABILITY_LEVEL_PERSIST_TO_MAJORITY: DurabilityLevel
COMPRESSION_ENABLED_OPTIONAL: CompressionEnabled
COMPRESSION_ENABLED_ALWAYS: CompressionEnabled

class LegacyDurabilitySpec(_message.Message):
    __slots__ = ("num_replicated", "num_persisted")
    NUM_REPLICATED_FIELD_NUMBER: _ClassVar[int]
    NUM_PERSISTED_FIELD_NUMBER: _ClassVar[int]
    num_replicated: int
    num_persisted: int
    def __init__(self, num_replicated: _Optional[int] = ..., num_persisted: _Optional[int] = ...) -> None: ...

class MutationToken(_message.Message):
    __slots__ = ("bucket_name", "vbucket_id", "vbucket_uuid", "seq_no")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    VBUCKET_ID_FIELD_NUMBER: _ClassVar[int]
    VBUCKET_UUID_FIELD_NUMBER: _ClassVar[int]
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    vbucket_id: int
    vbucket_uuid: int
    seq_no: int
    def __init__(self, bucket_name: _Optional[str] = ..., vbucket_id: _Optional[int] = ..., vbucket_uuid: _Optional[int] = ..., seq_no: _Optional[int] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "project", "compression")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    project: _containers.RepeatedScalarFieldContainer[str]
    compression: CompressionEnabled
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., project: _Optional[_Iterable[str]] = ..., compression: _Optional[_Union[CompressionEnabled, str]] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("content_uncompressed", "content_compressed", "content_flags", "cas", "expiry")
    CONTENT_UNCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_FIELD_NUMBER: _ClassVar[int]
    content_uncompressed: bytes
    content_compressed: bytes
    content_flags: int
    cas: int
    expiry: _timestamp_pb2.Timestamp
    def __init__(self, content_uncompressed: _Optional[bytes] = ..., content_compressed: _Optional[bytes] = ..., content_flags: _Optional[int] = ..., cas: _Optional[int] = ..., expiry: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetAndTouchRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "expiry_time", "expiry_secs", "compression")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    compression: CompressionEnabled
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ..., compression: _Optional[_Union[CompressionEnabled, str]] = ...) -> None: ...

class GetAndTouchResponse(_message.Message):
    __slots__ = ("content_uncompressed", "content_compressed", "content_flags", "cas", "expiry")
    CONTENT_UNCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_FIELD_NUMBER: _ClassVar[int]
    content_uncompressed: bytes
    content_compressed: bytes
    content_flags: int
    cas: int
    expiry: _timestamp_pb2.Timestamp
    def __init__(self, content_uncompressed: _Optional[bytes] = ..., content_compressed: _Optional[bytes] = ..., content_flags: _Optional[int] = ..., cas: _Optional[int] = ..., expiry: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetAndLockRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "lock_time_secs", "compression")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    LOCK_TIME_SECS_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    lock_time_secs: int
    compression: CompressionEnabled
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., lock_time_secs: _Optional[int] = ..., compression: _Optional[_Union[CompressionEnabled, str]] = ...) -> None: ...

class GetAndLockResponse(_message.Message):
    __slots__ = ("content_uncompressed", "content_compressed", "content_flags", "cas", "expiry")
    CONTENT_UNCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_FIELD_NUMBER: _ClassVar[int]
    content_uncompressed: bytes
    content_compressed: bytes
    content_flags: int
    cas: int
    expiry: _timestamp_pb2.Timestamp
    def __init__(self, content_uncompressed: _Optional[bytes] = ..., content_compressed: _Optional[bytes] = ..., content_flags: _Optional[int] = ..., cas: _Optional[int] = ..., expiry: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UnlockRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "cas")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    cas: int
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., cas: _Optional[int] = ...) -> None: ...

class UnlockResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TouchRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "expiry_time", "expiry_secs")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ...) -> None: ...

class TouchResponse(_message.Message):
    __slots__ = ("cas", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class ExistsRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class ExistsResponse(_message.Message):
    __slots__ = ("result", "cas")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    result: bool
    cas: int
    def __init__(self, result: bool = ..., cas: _Optional[int] = ...) -> None: ...

class InsertRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "content_uncompressed", "content_compressed", "content_flags", "expiry_time", "expiry_secs", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_UNCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    content_uncompressed: bytes
    content_compressed: bytes
    content_flags: int
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., content_uncompressed: _Optional[bytes] = ..., content_compressed: _Optional[bytes] = ..., content_flags: _Optional[int] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class InsertResponse(_message.Message):
    __slots__ = ("cas", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class UpsertRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "content_uncompressed", "content_compressed", "content_flags", "expiry_time", "expiry_secs", "preserve_expiry_on_existing", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_UNCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    PRESERVE_EXPIRY_ON_EXISTING_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    content_uncompressed: bytes
    content_compressed: bytes
    content_flags: int
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    preserve_expiry_on_existing: bool
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., content_uncompressed: _Optional[bytes] = ..., content_compressed: _Optional[bytes] = ..., content_flags: _Optional[int] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ..., preserve_expiry_on_existing: bool = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class UpsertResponse(_message.Message):
    __slots__ = ("cas", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class ReplaceRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "content_uncompressed", "content_compressed", "content_flags", "cas", "expiry_time", "expiry_secs", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_UNCOMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_COMPRESSED_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    content_uncompressed: bytes
    content_compressed: bytes
    content_flags: int
    cas: int
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., content_uncompressed: _Optional[bytes] = ..., content_compressed: _Optional[bytes] = ..., content_flags: _Optional[int] = ..., cas: _Optional[int] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class ReplaceResponse(_message.Message):
    __slots__ = ("cas", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class RemoveRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "cas", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    cas: int
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., cas: _Optional[int] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class RemoveResponse(_message.Message):
    __slots__ = ("cas", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class IncrementRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "delta", "expiry_time", "expiry_secs", "initial", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    delta: int
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    initial: int
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., delta: _Optional[int] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ..., initial: _Optional[int] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class IncrementResponse(_message.Message):
    __slots__ = ("cas", "content", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    content: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., content: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class DecrementRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "delta", "expiry_time", "expiry_secs", "initial", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    delta: int
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    initial: int
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., delta: _Optional[int] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ..., initial: _Optional[int] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class DecrementResponse(_message.Message):
    __slots__ = ("cas", "content", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    content: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., content: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class AppendRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "content", "cas", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    content: bytes
    cas: int
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., content: _Optional[bytes] = ..., cas: _Optional[int] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class AppendResponse(_message.Message):
    __slots__ = ("cas", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class PrependRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "content", "cas", "durability_level")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    content: bytes
    cas: int
    durability_level: DurabilityLevel
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., content: _Optional[bytes] = ..., cas: _Optional[int] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ...) -> None: ...

class PrependResponse(_message.Message):
    __slots__ = ("cas", "mutation_token")
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    cas: int
    mutation_token: MutationToken
    def __init__(self, cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class LookupInRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "specs", "flags")
    class Spec(_message.Message):
        __slots__ = ("operation", "path", "flags")
        class Operation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            OPERATION_GET: _ClassVar[LookupInRequest.Spec.Operation]
            OPERATION_EXISTS: _ClassVar[LookupInRequest.Spec.Operation]
            OPERATION_COUNT: _ClassVar[LookupInRequest.Spec.Operation]
        OPERATION_GET: LookupInRequest.Spec.Operation
        OPERATION_EXISTS: LookupInRequest.Spec.Operation
        OPERATION_COUNT: LookupInRequest.Spec.Operation
        class Flags(_message.Message):
            __slots__ = ("xattr",)
            XATTR_FIELD_NUMBER: _ClassVar[int]
            xattr: bool
            def __init__(self, xattr: bool = ...) -> None: ...
        OPERATION_FIELD_NUMBER: _ClassVar[int]
        PATH_FIELD_NUMBER: _ClassVar[int]
        FLAGS_FIELD_NUMBER: _ClassVar[int]
        operation: LookupInRequest.Spec.Operation
        path: str
        flags: LookupInRequest.Spec.Flags
        def __init__(self, operation: _Optional[_Union[LookupInRequest.Spec.Operation, str]] = ..., path: _Optional[str] = ..., flags: _Optional[_Union[LookupInRequest.Spec.Flags, _Mapping]] = ...) -> None: ...
    class Flags(_message.Message):
        __slots__ = ("access_deleted",)
        ACCESS_DELETED_FIELD_NUMBER: _ClassVar[int]
        access_deleted: bool
        def __init__(self, access_deleted: bool = ...) -> None: ...
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    SPECS_FIELD_NUMBER: _ClassVar[int]
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    specs: _containers.RepeatedCompositeFieldContainer[LookupInRequest.Spec]
    flags: LookupInRequest.Flags
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., specs: _Optional[_Iterable[_Union[LookupInRequest.Spec, _Mapping]]] = ..., flags: _Optional[_Union[LookupInRequest.Flags, _Mapping]] = ...) -> None: ...

class LookupInResponse(_message.Message):
    __slots__ = ("specs", "cas")
    class Spec(_message.Message):
        __slots__ = ("status", "content")
        STATUS_FIELD_NUMBER: _ClassVar[int]
        CONTENT_FIELD_NUMBER: _ClassVar[int]
        status: _status_pb2.Status
        content: bytes
        def __init__(self, status: _Optional[_Union[_status_pb2.Status, _Mapping]] = ..., content: _Optional[bytes] = ...) -> None: ...
    SPECS_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    specs: _containers.RepeatedCompositeFieldContainer[LookupInResponse.Spec]
    cas: int
    def __init__(self, specs: _Optional[_Iterable[_Union[LookupInResponse.Spec, _Mapping]]] = ..., cas: _Optional[int] = ...) -> None: ...

class MutateInRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key", "specs", "store_semantic", "durability_level", "cas", "flags", "expiry_time", "expiry_secs")
    class StoreSemantic(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STORE_SEMANTIC_REPLACE: _ClassVar[MutateInRequest.StoreSemantic]
        STORE_SEMANTIC_UPSERT: _ClassVar[MutateInRequest.StoreSemantic]
        STORE_SEMANTIC_INSERT: _ClassVar[MutateInRequest.StoreSemantic]
    STORE_SEMANTIC_REPLACE: MutateInRequest.StoreSemantic
    STORE_SEMANTIC_UPSERT: MutateInRequest.StoreSemantic
    STORE_SEMANTIC_INSERT: MutateInRequest.StoreSemantic
    class Spec(_message.Message):
        __slots__ = ("operation", "path", "content", "flags")
        class Operation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            OPERATION_INSERT: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_UPSERT: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_REPLACE: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_REMOVE: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_ARRAY_APPEND: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_ARRAY_PREPEND: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_ARRAY_INSERT: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_ARRAY_ADD_UNIQUE: _ClassVar[MutateInRequest.Spec.Operation]
            OPERATION_COUNTER: _ClassVar[MutateInRequest.Spec.Operation]
        OPERATION_INSERT: MutateInRequest.Spec.Operation
        OPERATION_UPSERT: MutateInRequest.Spec.Operation
        OPERATION_REPLACE: MutateInRequest.Spec.Operation
        OPERATION_REMOVE: MutateInRequest.Spec.Operation
        OPERATION_ARRAY_APPEND: MutateInRequest.Spec.Operation
        OPERATION_ARRAY_PREPEND: MutateInRequest.Spec.Operation
        OPERATION_ARRAY_INSERT: MutateInRequest.Spec.Operation
        OPERATION_ARRAY_ADD_UNIQUE: MutateInRequest.Spec.Operation
        OPERATION_COUNTER: MutateInRequest.Spec.Operation
        class Flags(_message.Message):
            __slots__ = ("create_path", "xattr")
            CREATE_PATH_FIELD_NUMBER: _ClassVar[int]
            XATTR_FIELD_NUMBER: _ClassVar[int]
            create_path: bool
            xattr: bool
            def __init__(self, create_path: bool = ..., xattr: bool = ...) -> None: ...
        OPERATION_FIELD_NUMBER: _ClassVar[int]
        PATH_FIELD_NUMBER: _ClassVar[int]
        CONTENT_FIELD_NUMBER: _ClassVar[int]
        FLAGS_FIELD_NUMBER: _ClassVar[int]
        operation: MutateInRequest.Spec.Operation
        path: str
        content: bytes
        flags: MutateInRequest.Spec.Flags
        def __init__(self, operation: _Optional[_Union[MutateInRequest.Spec.Operation, str]] = ..., path: _Optional[str] = ..., content: _Optional[bytes] = ..., flags: _Optional[_Union[MutateInRequest.Spec.Flags, _Mapping]] = ...) -> None: ...
    class Flags(_message.Message):
        __slots__ = ("access_deleted",)
        ACCESS_DELETED_FIELD_NUMBER: _ClassVar[int]
        access_deleted: bool
        def __init__(self, access_deleted: bool = ...) -> None: ...
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    SPECS_FIELD_NUMBER: _ClassVar[int]
    STORE_SEMANTIC_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_TIME_FIELD_NUMBER: _ClassVar[int]
    EXPIRY_SECS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    specs: _containers.RepeatedCompositeFieldContainer[MutateInRequest.Spec]
    store_semantic: MutateInRequest.StoreSemantic
    durability_level: DurabilityLevel
    cas: int
    flags: MutateInRequest.Flags
    expiry_time: _timestamp_pb2.Timestamp
    expiry_secs: int
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ..., specs: _Optional[_Iterable[_Union[MutateInRequest.Spec, _Mapping]]] = ..., store_semantic: _Optional[_Union[MutateInRequest.StoreSemantic, str]] = ..., durability_level: _Optional[_Union[DurabilityLevel, str]] = ..., cas: _Optional[int] = ..., flags: _Optional[_Union[MutateInRequest.Flags, _Mapping]] = ..., expiry_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., expiry_secs: _Optional[int] = ...) -> None: ...

class MutateInResponse(_message.Message):
    __slots__ = ("specs", "cas", "mutation_token")
    class Spec(_message.Message):
        __slots__ = ("content",)
        CONTENT_FIELD_NUMBER: _ClassVar[int]
        content: bytes
        def __init__(self, content: _Optional[bytes] = ...) -> None: ...
    SPECS_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    specs: _containers.RepeatedCompositeFieldContainer[MutateInResponse.Spec]
    cas: int
    mutation_token: MutationToken
    def __init__(self, specs: _Optional[_Iterable[_Union[MutateInResponse.Spec, _Mapping]]] = ..., cas: _Optional[int] = ..., mutation_token: _Optional[_Union[MutationToken, _Mapping]] = ...) -> None: ...

class GetAllReplicasRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "key")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    key: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., key: _Optional[str] = ...) -> None: ...

class GetAllReplicasResponse(_message.Message):
    __slots__ = ("is_replica", "content", "content_flags", "cas")
    IS_REPLICA_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CAS_FIELD_NUMBER: _ClassVar[int]
    is_replica: bool
    content: bytes
    content_flags: int
    cas: int
    def __init__(self, is_replica: bool = ..., content: _Optional[bytes] = ..., content_flags: _Optional[int] = ..., cas: _Optional[int] = ...) -> None: ...
