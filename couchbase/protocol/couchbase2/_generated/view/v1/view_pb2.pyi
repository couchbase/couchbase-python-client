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

class ViewQueryRequest(_message.Message):
    __slots__ = ("bucket_name", "design_document_name", "view_name", "skip", "limit", "scan_consistency", "reduce", "group", "group_level", "key", "keys", "start_key", "end_key", "inclusive_end", "start_key_doc_id", "end_key_doc_id", "on_error", "debug", "namespace", "order")
    class ScanConsistency(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SCAN_CONSISTENCY_NOT_BOUNDED: _ClassVar[ViewQueryRequest.ScanConsistency]
        SCAN_CONSISTENCY_REQUEST_PLUS: _ClassVar[ViewQueryRequest.ScanConsistency]
        SCAN_CONSISTENCY_UPDATE_AFTER: _ClassVar[ViewQueryRequest.ScanConsistency]
    SCAN_CONSISTENCY_NOT_BOUNDED: ViewQueryRequest.ScanConsistency
    SCAN_CONSISTENCY_REQUEST_PLUS: ViewQueryRequest.ScanConsistency
    SCAN_CONSISTENCY_UPDATE_AFTER: ViewQueryRequest.ScanConsistency
    class ErrorMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ERROR_MODE_CONTINUE: _ClassVar[ViewQueryRequest.ErrorMode]
        ERROR_MODE_STOP: _ClassVar[ViewQueryRequest.ErrorMode]
    ERROR_MODE_CONTINUE: ViewQueryRequest.ErrorMode
    ERROR_MODE_STOP: ViewQueryRequest.ErrorMode
    class Namespace(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NAMESPACE_PRODUCTION: _ClassVar[ViewQueryRequest.Namespace]
        NAMESPACE_DEVELOPMENT: _ClassVar[ViewQueryRequest.Namespace]
    NAMESPACE_PRODUCTION: ViewQueryRequest.Namespace
    NAMESPACE_DEVELOPMENT: ViewQueryRequest.Namespace
    class Order(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ORDER_ASCENDING: _ClassVar[ViewQueryRequest.Order]
        ORDER_DESCENDING: _ClassVar[ViewQueryRequest.Order]
    ORDER_ASCENDING: ViewQueryRequest.Order
    ORDER_DESCENDING: ViewQueryRequest.Order
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    DESIGN_DOCUMENT_NAME_FIELD_NUMBER: _ClassVar[int]
    VIEW_NAME_FIELD_NUMBER: _ClassVar[int]
    SKIP_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    SCAN_CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    REDUCE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    GROUP_LEVEL_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    START_KEY_FIELD_NUMBER: _ClassVar[int]
    END_KEY_FIELD_NUMBER: _ClassVar[int]
    INCLUSIVE_END_FIELD_NUMBER: _ClassVar[int]
    START_KEY_DOC_ID_FIELD_NUMBER: _ClassVar[int]
    END_KEY_DOC_ID_FIELD_NUMBER: _ClassVar[int]
    ON_ERROR_FIELD_NUMBER: _ClassVar[int]
    DEBUG_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    design_document_name: str
    view_name: str
    skip: int
    limit: int
    scan_consistency: ViewQueryRequest.ScanConsistency
    reduce: bool
    group: bool
    group_level: int
    key: bytes
    keys: _containers.RepeatedScalarFieldContainer[bytes]
    start_key: bytes
    end_key: bytes
    inclusive_end: bool
    start_key_doc_id: str
    end_key_doc_id: str
    on_error: ViewQueryRequest.ErrorMode
    debug: bool
    namespace: ViewQueryRequest.Namespace
    order: ViewQueryRequest.Order
    def __init__(self, bucket_name: _Optional[str] = ..., design_document_name: _Optional[str] = ..., view_name: _Optional[str] = ..., skip: _Optional[int] = ..., limit: _Optional[int] = ..., scan_consistency: _Optional[_Union[ViewQueryRequest.ScanConsistency, str]] = ..., reduce: bool = ..., group: bool = ..., group_level: _Optional[int] = ..., key: _Optional[bytes] = ..., keys: _Optional[_Iterable[bytes]] = ..., start_key: _Optional[bytes] = ..., end_key: _Optional[bytes] = ..., inclusive_end: bool = ..., start_key_doc_id: _Optional[str] = ..., end_key_doc_id: _Optional[str] = ..., on_error: _Optional[_Union[ViewQueryRequest.ErrorMode, str]] = ..., debug: bool = ..., namespace: _Optional[_Union[ViewQueryRequest.Namespace, str]] = ..., order: _Optional[_Union[ViewQueryRequest.Order, str]] = ...) -> None: ...

class ViewQueryResponse(_message.Message):
    __slots__ = ("rows", "meta_data")
    class Row(_message.Message):
        __slots__ = ("id", "key", "value")
        ID_FIELD_NUMBER: _ClassVar[int]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        id: str
        key: bytes
        value: bytes
        def __init__(self, id: _Optional[str] = ..., key: _Optional[bytes] = ..., value: _Optional[bytes] = ...) -> None: ...
    class MetaData(_message.Message):
        __slots__ = ("total_rows", "debug")
        TOTAL_ROWS_FIELD_NUMBER: _ClassVar[int]
        DEBUG_FIELD_NUMBER: _ClassVar[int]
        total_rows: int
        debug: bytes
        def __init__(self, total_rows: _Optional[int] = ..., debug: _Optional[bytes] = ...) -> None: ...
    ROWS_FIELD_NUMBER: _ClassVar[int]
    META_DATA_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[ViewQueryResponse.Row]
    meta_data: ViewQueryResponse.MetaData
    def __init__(self, rows: _Optional[_Iterable[_Union[ViewQueryResponse.Row, _Mapping]]] = ..., meta_data: _Optional[_Union[ViewQueryResponse.MetaData, _Mapping]] = ...) -> None: ...
