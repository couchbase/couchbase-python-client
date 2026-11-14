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

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AnalyticsQueryRequest(_message.Message):
    __slots__ = ("analytics_scope_name", "statement", "read_only", "client_context_id", "priority", "scan_consistency", "positional_parameters", "named_parameters", "plan_format", "include_logical_plan", "include_organized_logical_plan", "include_expression_tree", "include_rewritten_expression_tree", "max_warnings")
    class ScanConsistency(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SCAN_CONSISTENCY_NOT_BOUNDED: _ClassVar[AnalyticsQueryRequest.ScanConsistency]
        SCAN_CONSISTENCY_REQUEST_PLUS: _ClassVar[AnalyticsQueryRequest.ScanConsistency]
    SCAN_CONSISTENCY_NOT_BOUNDED: AnalyticsQueryRequest.ScanConsistency
    SCAN_CONSISTENCY_REQUEST_PLUS: AnalyticsQueryRequest.ScanConsistency
    class PlanFormat(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PLAN_FORMAT_JSON: _ClassVar[AnalyticsQueryRequest.PlanFormat]
        PLAN_FORMAT_STRING: _ClassVar[AnalyticsQueryRequest.PlanFormat]
    PLAN_FORMAT_JSON: AnalyticsQueryRequest.PlanFormat
    PLAN_FORMAT_STRING: AnalyticsQueryRequest.PlanFormat
    class NamedParametersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    ANALYTICS_SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_FIELD_NUMBER: _ClassVar[int]
    READ_ONLY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    SCAN_CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    POSITIONAL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    NAMED_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    PLAN_FORMAT_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_LOGICAL_PLAN_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_ORGANIZED_LOGICAL_PLAN_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_EXPRESSION_TREE_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_REWRITTEN_EXPRESSION_TREE_FIELD_NUMBER: _ClassVar[int]
    MAX_WARNINGS_FIELD_NUMBER: _ClassVar[int]
    analytics_scope_name: str
    statement: str
    read_only: bool
    client_context_id: str
    priority: bool
    scan_consistency: AnalyticsQueryRequest.ScanConsistency
    positional_parameters: _containers.RepeatedScalarFieldContainer[bytes]
    named_parameters: _containers.ScalarMap[str, bytes]
    plan_format: AnalyticsQueryRequest.PlanFormat
    include_logical_plan: bool
    include_organized_logical_plan: bool
    include_expression_tree: bool
    include_rewritten_expression_tree: bool
    max_warnings: int
    def __init__(self, analytics_scope_name: _Optional[str] = ..., statement: _Optional[str] = ..., read_only: bool = ..., client_context_id: _Optional[str] = ..., priority: bool = ..., scan_consistency: _Optional[_Union[AnalyticsQueryRequest.ScanConsistency, str]] = ..., positional_parameters: _Optional[_Iterable[bytes]] = ..., named_parameters: _Optional[_Mapping[str, bytes]] = ..., plan_format: _Optional[_Union[AnalyticsQueryRequest.PlanFormat, str]] = ..., include_logical_plan: bool = ..., include_organized_logical_plan: bool = ..., include_expression_tree: bool = ..., include_rewritten_expression_tree: bool = ..., max_warnings: _Optional[int] = ...) -> None: ...

class AnalyticsQueryResponse(_message.Message):
    __slots__ = ("rows", "meta_data")
    class Metrics(_message.Message):
        __slots__ = ("elapsed_time", "execution_time", "result_count", "result_size", "mutation_count", "sort_count", "error_count", "warning_count", "processed_objects")
        ELAPSED_TIME_FIELD_NUMBER: _ClassVar[int]
        EXECUTION_TIME_FIELD_NUMBER: _ClassVar[int]
        RESULT_COUNT_FIELD_NUMBER: _ClassVar[int]
        RESULT_SIZE_FIELD_NUMBER: _ClassVar[int]
        MUTATION_COUNT_FIELD_NUMBER: _ClassVar[int]
        SORT_COUNT_FIELD_NUMBER: _ClassVar[int]
        ERROR_COUNT_FIELD_NUMBER: _ClassVar[int]
        WARNING_COUNT_FIELD_NUMBER: _ClassVar[int]
        PROCESSED_OBJECTS_FIELD_NUMBER: _ClassVar[int]
        elapsed_time: _duration_pb2.Duration
        execution_time: _duration_pb2.Duration
        result_count: int
        result_size: int
        mutation_count: int
        sort_count: int
        error_count: int
        warning_count: int
        processed_objects: int
        def __init__(self, elapsed_time: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., execution_time: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., result_count: _Optional[int] = ..., result_size: _Optional[int] = ..., mutation_count: _Optional[int] = ..., sort_count: _Optional[int] = ..., error_count: _Optional[int] = ..., warning_count: _Optional[int] = ..., processed_objects: _Optional[int] = ...) -> None: ...
    class MetaData(_message.Message):
        __slots__ = ("request_id", "client_context_id", "metrics", "signature", "warnings", "status", "logical_plan", "optimized_logical_plan", "expression_tree", "rewritten_expression_tree")
        class Warning(_message.Message):
            __slots__ = ("code", "message")
            CODE_FIELD_NUMBER: _ClassVar[int]
            MESSAGE_FIELD_NUMBER: _ClassVar[int]
            code: int
            message: str
            def __init__(self, code: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...
        REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
        CLIENT_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
        METRICS_FIELD_NUMBER: _ClassVar[int]
        SIGNATURE_FIELD_NUMBER: _ClassVar[int]
        WARNINGS_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        LOGICAL_PLAN_FIELD_NUMBER: _ClassVar[int]
        OPTIMIZED_LOGICAL_PLAN_FIELD_NUMBER: _ClassVar[int]
        EXPRESSION_TREE_FIELD_NUMBER: _ClassVar[int]
        REWRITTEN_EXPRESSION_TREE_FIELD_NUMBER: _ClassVar[int]
        request_id: str
        client_context_id: str
        metrics: AnalyticsQueryResponse.Metrics
        signature: bytes
        warnings: _containers.RepeatedCompositeFieldContainer[AnalyticsQueryResponse.MetaData.Warning]
        status: str
        logical_plan: bytes
        optimized_logical_plan: bytes
        expression_tree: str
        rewritten_expression_tree: str
        def __init__(self, request_id: _Optional[str] = ..., client_context_id: _Optional[str] = ..., metrics: _Optional[_Union[AnalyticsQueryResponse.Metrics, _Mapping]] = ..., signature: _Optional[bytes] = ..., warnings: _Optional[_Iterable[_Union[AnalyticsQueryResponse.MetaData.Warning, _Mapping]]] = ..., status: _Optional[str] = ..., logical_plan: _Optional[bytes] = ..., optimized_logical_plan: _Optional[bytes] = ..., expression_tree: _Optional[str] = ..., rewritten_expression_tree: _Optional[str] = ...) -> None: ...
    ROWS_FIELD_NUMBER: _ClassVar[int]
    META_DATA_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedScalarFieldContainer[bytes]
    meta_data: AnalyticsQueryResponse.MetaData
    def __init__(self, rows: _Optional[_Iterable[bytes]] = ..., meta_data: _Optional[_Union[AnalyticsQueryResponse.MetaData, _Mapping]] = ...) -> None: ...
