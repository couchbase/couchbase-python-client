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
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2 as _kv_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryRequest(_message.Message):
    __slots__ = ("bucket_name", "scope_name", "statement", "read_only", "prepared", "tuning_options", "client_context_id", "scan_consistency", "positional_parameters", "named_parameters", "flex_index", "preserve_expiry", "consistent_with", "durability_level", "profile_mode")
    class ScanConsistency(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SCAN_CONSISTENCY_NOT_BOUNDED: _ClassVar[QueryRequest.ScanConsistency]
        SCAN_CONSISTENCY_REQUEST_PLUS: _ClassVar[QueryRequest.ScanConsistency]
    SCAN_CONSISTENCY_NOT_BOUNDED: QueryRequest.ScanConsistency
    SCAN_CONSISTENCY_REQUEST_PLUS: QueryRequest.ScanConsistency
    class DurabilityLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DURABILITY_LEVEL_NONE: _ClassVar[QueryRequest.DurabilityLevel]
        DURABILITY_LEVEL_MAJORITY: _ClassVar[QueryRequest.DurabilityLevel]
        DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE: _ClassVar[QueryRequest.DurabilityLevel]
        DURABILITY_LEVEL_PERSIST_TO_MAJORITY: _ClassVar[QueryRequest.DurabilityLevel]
    DURABILITY_LEVEL_NONE: QueryRequest.DurabilityLevel
    DURABILITY_LEVEL_MAJORITY: QueryRequest.DurabilityLevel
    DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE: QueryRequest.DurabilityLevel
    DURABILITY_LEVEL_PERSIST_TO_MAJORITY: QueryRequest.DurabilityLevel
    class ProfileMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PROFILE_MODE_OFF: _ClassVar[QueryRequest.ProfileMode]
        PROFILE_MODE_PHASES: _ClassVar[QueryRequest.ProfileMode]
        PROFILE_MODE_TIMINGS: _ClassVar[QueryRequest.ProfileMode]
    PROFILE_MODE_OFF: QueryRequest.ProfileMode
    PROFILE_MODE_PHASES: QueryRequest.ProfileMode
    PROFILE_MODE_TIMINGS: QueryRequest.ProfileMode
    class TuningOptions(_message.Message):
        __slots__ = ("max_parallelism", "pipeline_batch", "pipeline_cap", "scan_wait", "scan_cap", "disable_metrics")
        MAX_PARALLELISM_FIELD_NUMBER: _ClassVar[int]
        PIPELINE_BATCH_FIELD_NUMBER: _ClassVar[int]
        PIPELINE_CAP_FIELD_NUMBER: _ClassVar[int]
        SCAN_WAIT_FIELD_NUMBER: _ClassVar[int]
        SCAN_CAP_FIELD_NUMBER: _ClassVar[int]
        DISABLE_METRICS_FIELD_NUMBER: _ClassVar[int]
        max_parallelism: int
        pipeline_batch: int
        pipeline_cap: int
        scan_wait: _duration_pb2.Duration
        scan_cap: int
        disable_metrics: bool
        def __init__(self, max_parallelism: _Optional[int] = ..., pipeline_batch: _Optional[int] = ..., pipeline_cap: _Optional[int] = ..., scan_wait: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., scan_cap: _Optional[int] = ..., disable_metrics: bool = ...) -> None: ...
    class NamedParametersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    SCOPE_NAME_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_FIELD_NUMBER: _ClassVar[int]
    READ_ONLY_FIELD_NUMBER: _ClassVar[int]
    PREPARED_FIELD_NUMBER: _ClassVar[int]
    TUNING_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    CLIENT_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    SCAN_CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    POSITIONAL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    NAMED_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    FLEX_INDEX_FIELD_NUMBER: _ClassVar[int]
    PRESERVE_EXPIRY_FIELD_NUMBER: _ClassVar[int]
    CONSISTENT_WITH_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_LEVEL_FIELD_NUMBER: _ClassVar[int]
    PROFILE_MODE_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    scope_name: str
    statement: str
    read_only: bool
    prepared: bool
    tuning_options: QueryRequest.TuningOptions
    client_context_id: str
    scan_consistency: QueryRequest.ScanConsistency
    positional_parameters: _containers.RepeatedScalarFieldContainer[bytes]
    named_parameters: _containers.ScalarMap[str, bytes]
    flex_index: bool
    preserve_expiry: bool
    consistent_with: _containers.RepeatedCompositeFieldContainer[_kv_pb2.MutationToken]
    durability_level: QueryRequest.DurabilityLevel
    profile_mode: QueryRequest.ProfileMode
    def __init__(self, bucket_name: _Optional[str] = ..., scope_name: _Optional[str] = ..., statement: _Optional[str] = ..., read_only: bool = ..., prepared: bool = ..., tuning_options: _Optional[_Union[QueryRequest.TuningOptions, _Mapping]] = ..., client_context_id: _Optional[str] = ..., scan_consistency: _Optional[_Union[QueryRequest.ScanConsistency, str]] = ..., positional_parameters: _Optional[_Iterable[bytes]] = ..., named_parameters: _Optional[_Mapping[str, bytes]] = ..., flex_index: bool = ..., preserve_expiry: bool = ..., consistent_with: _Optional[_Iterable[_Union[_kv_pb2.MutationToken, _Mapping]]] = ..., durability_level: _Optional[_Union[QueryRequest.DurabilityLevel, str]] = ..., profile_mode: _Optional[_Union[QueryRequest.ProfileMode, str]] = ...) -> None: ...

class QueryResponse(_message.Message):
    __slots__ = ("rows", "meta_data")
    class MetaData(_message.Message):
        __slots__ = ("request_id", "client_context_id", "metrics", "status", "warnings", "profile", "signature")
        class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            STATUS_RUNNING: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_SUCCESS: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_ERRORS: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_COMPLETED: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_STOPPED: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_TIMEOUT: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_CLOSED: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_FATAL: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_ABORTED: _ClassVar[QueryResponse.MetaData.Status]
            STATUS_UNKNOWN: _ClassVar[QueryResponse.MetaData.Status]
        STATUS_RUNNING: QueryResponse.MetaData.Status
        STATUS_SUCCESS: QueryResponse.MetaData.Status
        STATUS_ERRORS: QueryResponse.MetaData.Status
        STATUS_COMPLETED: QueryResponse.MetaData.Status
        STATUS_STOPPED: QueryResponse.MetaData.Status
        STATUS_TIMEOUT: QueryResponse.MetaData.Status
        STATUS_CLOSED: QueryResponse.MetaData.Status
        STATUS_FATAL: QueryResponse.MetaData.Status
        STATUS_ABORTED: QueryResponse.MetaData.Status
        STATUS_UNKNOWN: QueryResponse.MetaData.Status
        class Warning(_message.Message):
            __slots__ = ("code", "message")
            CODE_FIELD_NUMBER: _ClassVar[int]
            MESSAGE_FIELD_NUMBER: _ClassVar[int]
            code: int
            message: str
            def __init__(self, code: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...
        class Metrics(_message.Message):
            __slots__ = ("elapsed_time", "execution_time", "result_count", "result_size", "mutation_count", "sort_count", "error_count", "warning_count")
            ELAPSED_TIME_FIELD_NUMBER: _ClassVar[int]
            EXECUTION_TIME_FIELD_NUMBER: _ClassVar[int]
            RESULT_COUNT_FIELD_NUMBER: _ClassVar[int]
            RESULT_SIZE_FIELD_NUMBER: _ClassVar[int]
            MUTATION_COUNT_FIELD_NUMBER: _ClassVar[int]
            SORT_COUNT_FIELD_NUMBER: _ClassVar[int]
            ERROR_COUNT_FIELD_NUMBER: _ClassVar[int]
            WARNING_COUNT_FIELD_NUMBER: _ClassVar[int]
            elapsed_time: _duration_pb2.Duration
            execution_time: _duration_pb2.Duration
            result_count: int
            result_size: int
            mutation_count: int
            sort_count: int
            error_count: int
            warning_count: int
            def __init__(self, elapsed_time: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., execution_time: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., result_count: _Optional[int] = ..., result_size: _Optional[int] = ..., mutation_count: _Optional[int] = ..., sort_count: _Optional[int] = ..., error_count: _Optional[int] = ..., warning_count: _Optional[int] = ...) -> None: ...
        REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
        CLIENT_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
        METRICS_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        WARNINGS_FIELD_NUMBER: _ClassVar[int]
        PROFILE_FIELD_NUMBER: _ClassVar[int]
        SIGNATURE_FIELD_NUMBER: _ClassVar[int]
        request_id: str
        client_context_id: str
        metrics: QueryResponse.MetaData.Metrics
        status: QueryResponse.MetaData.Status
        warnings: _containers.RepeatedCompositeFieldContainer[QueryResponse.MetaData.Warning]
        profile: bytes
        signature: bytes
        def __init__(self, request_id: _Optional[str] = ..., client_context_id: _Optional[str] = ..., metrics: _Optional[_Union[QueryResponse.MetaData.Metrics, _Mapping]] = ..., status: _Optional[_Union[QueryResponse.MetaData.Status, str]] = ..., warnings: _Optional[_Iterable[_Union[QueryResponse.MetaData.Warning, _Mapping]]] = ..., profile: _Optional[bytes] = ..., signature: _Optional[bytes] = ...) -> None: ...
    ROWS_FIELD_NUMBER: _ClassVar[int]
    META_DATA_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedScalarFieldContainer[bytes]
    meta_data: QueryResponse.MetaData
    def __init__(self, rows: _Optional[_Iterable[bytes]] = ..., meta_data: _Optional[_Union[QueryResponse.MetaData, _Mapping]] = ...) -> None: ...
