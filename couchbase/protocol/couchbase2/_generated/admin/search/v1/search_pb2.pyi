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
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Index(_message.Message):
    __slots__ = ("name", "params", "plan_params", "source_name", "source_params", "source_type", "source_uuid", "type", "uuid")
    class ParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class PlanParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class SourceParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    PLAN_PARAMS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_UUID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    name: str
    params: _containers.ScalarMap[str, bytes]
    plan_params: _containers.ScalarMap[str, bytes]
    source_name: str
    source_params: _containers.ScalarMap[str, bytes]
    source_type: str
    source_uuid: str
    type: str
    uuid: str
    def __init__(self, name: _Optional[str] = ..., params: _Optional[_Mapping[str, bytes]] = ..., plan_params: _Optional[_Mapping[str, bytes]] = ..., source_name: _Optional[str] = ..., source_params: _Optional[_Mapping[str, bytes]] = ..., source_type: _Optional[str] = ..., source_uuid: _Optional[str] = ..., type: _Optional[str] = ..., uuid: _Optional[str] = ...) -> None: ...

class CreateIndexRequest(_message.Message):
    __slots__ = ("name", "params", "plan_params", "prev_index_uuid", "source_name", "source_params", "source_type", "source_uuid", "type", "bucket_name", "scope_name")
    class ParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class PlanParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class SourceParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    PLAN_PARAMS_FIELD_NUMBER: _ClassVar[int]
    PREV_INDEX_UUID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_UUID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    params: _containers.ScalarMap[str, bytes]
    plan_params: _containers.ScalarMap[str, bytes]
    prev_index_uuid: str
    source_name: str
    source_params: _containers.ScalarMap[str, bytes]
    source_type: str
    source_uuid: str
    type: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., params: _Optional[_Mapping[str, bytes]] = ..., plan_params: _Optional[_Mapping[str, bytes]] = ..., prev_index_uuid: _Optional[str] = ..., source_name: _Optional[str] = ..., source_params: _Optional[_Mapping[str, bytes]] = ..., source_type: _Optional[str] = ..., source_uuid: _Optional[str] = ..., type: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class CreateIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UpdateIndexRequest(_message.Message):
    __slots__ = ("index", "bucket_name", "scope_name")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    index: Index
    bucket_name: str
    scope_name: str
    def __init__(self, index: _Optional[_Union[Index, _Mapping]] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class UpdateIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetIndexRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class GetIndexResponse(_message.Message):
    __slots__ = ("index",)
    INDEX_FIELD_NUMBER: _ClassVar[int]
    index: Index
    def __init__(self, index: _Optional[_Union[Index, _Mapping]] = ...) -> None: ...

class ListIndexesRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name")
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class ListIndexesResponse(_message.Message):
    __slots__ = ("indexes",)
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    indexes: _containers.RepeatedCompositeFieldContainer[Index]
    def __init__(self, indexes: _Optional[_Iterable[_Union[Index, _Mapping]]] = ...) -> None: ...

class DeleteIndexRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class DeleteIndexResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AnalyzeDocumentRequest(_message.Message):
    __slots__ = ("name", "doc", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DOC_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    doc: bytes
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., doc: _Optional[bytes] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class AnalyzeDocumentResponse(_message.Message):
    __slots__ = ("status", "analyzed")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ANALYZED_FIELD_NUMBER: _ClassVar[int]
    status: str
    analyzed: bytes
    def __init__(self, status: _Optional[str] = ..., analyzed: _Optional[bytes] = ...) -> None: ...

class GetIndexedDocumentsCountRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class GetIndexedDocumentsCountResponse(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...

class PauseIndexIngestRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class PauseIndexIngestResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResumeIndexIngestRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class ResumeIndexIngestResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AllowIndexQueryingRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class AllowIndexQueryingResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DisallowIndexQueryingRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class DisallowIndexQueryingResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FreezeIndexPlanRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class FreezeIndexPlanResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UnfreezeIndexPlanRequest(_message.Message):
    __slots__ = ("name", "bucket_name", "scope_name")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    bucket_name: str
    scope_name: str
    def __init__(self, name: _Optional[str] = ..., bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ...) -> None: ...

class UnfreezeIndexPlanResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
