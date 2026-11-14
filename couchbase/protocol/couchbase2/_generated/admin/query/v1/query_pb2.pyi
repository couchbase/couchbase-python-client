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

from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IndexType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INDEX_TYPE_VIEW: _ClassVar[IndexType]
    INDEX_TYPE_GSI: _ClassVar[IndexType]

class IndexState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INDEX_STATE_DEFERRED: _ClassVar[IndexState]
    INDEX_STATE_BUILDING: _ClassVar[IndexState]
    INDEX_STATE_PENDING: _ClassVar[IndexState]
    INDEX_STATE_ONLINE: _ClassVar[IndexState]
    INDEX_STATE_OFFLINE: _ClassVar[IndexState]
    INDEX_STATE_ABRIDGED: _ClassVar[IndexState]
    INDEX_STATE_SCHEDULED: _ClassVar[IndexState]
INDEX_TYPE_VIEW: IndexType
INDEX_TYPE_GSI: IndexType
INDEX_STATE_DEFERRED: IndexState
INDEX_STATE_BUILDING: IndexState
INDEX_STATE_PENDING: IndexState
INDEX_STATE_ONLINE: IndexState
INDEX_STATE_OFFLINE: IndexState
INDEX_STATE_ABRIDGED: IndexState
INDEX_STATE_SCHEDULED: IndexState

class GetAllIndexesRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ...) -> None: ...

class GetAllIndexesResponse(_message.Message):
    __slots__ = ("indexes",)
    class Index(_message.Message):
        __slots__ = ("bucket_name", "scope_name", "collection_name", "name", "is_primary", "type", "state", "fields", "condition", "partition")
        BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
        SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
        COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        IS_PRIMARY_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        STATE_FIELD_NUMBER: _ClassVar[int]
        FIELDS_FIELD_NUMBER: _ClassVar[int]
        CONDITION_FIELD_NUMBER: _ClassVar[int]
        PARTITION_FIELD_NUMBER: _ClassVar[int]
        bucket_name: str
        scope_name: str
        collection_name: str
        name: str
        is_primary: bool
        type: IndexType
        state: IndexState
        fields: _containers.RepeatedScalarFieldContainer[str]
        condition: str
        partition: str
        def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., name: _Optional[str] = ..., is_primary: bool = ..., type: _Optional[_Union[IndexType, str]] = ..., state: _Optional[_Union[IndexState, str]] = ..., fields: _Optional[_Iterable[str]] = ..., condition: _Optional[str] = ..., partition: _Optional[str] = ...) -> None: ...
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    indexes: _containers.RepeatedCompositeFieldContainer[GetAllIndexesResponse.Index]
    def __init__(self, indexes: _Optional[_Iterable[_Union[GetAllIndexesResponse.Index, _Mapping]]] = ...) -> None: ...

class CreatePrimaryIndexRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "name", "num_replicas", "deferred", "ignore_if_exists")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    NUM_REPLICAS_FIELD_NUMBER: _ClassVar[int]
    DEFERRED_FIELD_NUMBER: _ClassVar[int]
    IGNORE_IF_EXISTS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    name: str
    num_replicas: int
    deferred: bool
    ignore_if_exists: bool
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., name: _Optional[str] = ..., num_replicas: _Optional[int] = ..., deferred: bool = ..., ignore_if_exists: bool = ...) -> None: ...

class CreatePrimaryIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CreateIndexRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "name", "num_replicas", "fields", "deferred", "ignore_if_exists")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    NUM_REPLICAS_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    DEFERRED_FIELD_NUMBER: _ClassVar[int]
    IGNORE_IF_EXISTS_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    name: str
    num_replicas: int
    fields: _containers.RepeatedScalarFieldContainer[str]
    deferred: bool
    ignore_if_exists: bool
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., name: _Optional[str] = ..., num_replicas: _Optional[int] = ..., fields: _Optional[_Iterable[str]] = ..., deferred: bool = ..., ignore_if_exists: bool = ...) -> None: ...

class CreateIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DropPrimaryIndexRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "name", "ignore_if_missing")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    IGNORE_IF_MISSING_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    name: str
    ignore_if_missing: bool
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., name: _Optional[str] = ..., ignore_if_missing: bool = ...) -> None: ...

class DropPrimaryIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DropIndexRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "name", "ignore_if_missing")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    IGNORE_IF_MISSING_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    name: str
    ignore_if_missing: bool
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., name: _Optional[str] = ..., ignore_if_missing: bool = ...) -> None: ...

class DropIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class BuildDeferredIndexesRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ...) -> None: ...

class BuildDeferredIndexesResponse(_message.Message):
    __slots__ = ("indexes",)
    class Index(_message.Message):
        __slots__ = ("bucket_name", "scope_name", "collection_name", "name")
        BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
        SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
        COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        bucket_name: str
        scope_name: str
        collection_name: str
        name: str
        def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    indexes: _containers.RepeatedCompositeFieldContainer[BuildDeferredIndexesResponse.Index]
    def __init__(self, indexes: _Optional[_Iterable[_Union[BuildDeferredIndexesResponse.Index, _Mapping]]] = ...) -> None: ...

class WaitForIndexOnlineRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "collection_name", "name")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    collection_name: str
    name: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class WaitForIndexOnlineResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
