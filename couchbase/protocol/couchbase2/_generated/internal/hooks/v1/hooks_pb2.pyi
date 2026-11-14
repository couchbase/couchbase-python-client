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

from google.protobuf import any_pb2 as _any_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ComparisonOperator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMPARISON_OPERATOR_EQUAL: _ClassVar[ComparisonOperator]
    COMPARISON_OPERATOR_NOT_EQUAL: _ClassVar[ComparisonOperator]
    COMPARISON_OPERATOR_GREATER_THAN: _ClassVar[ComparisonOperator]
    COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL: _ClassVar[ComparisonOperator]
    COMPARISON_OPERATOR_LESS_THAN: _ClassVar[ComparisonOperator]
    COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL: _ClassVar[ComparisonOperator]
COMPARISON_OPERATOR_EQUAL: ComparisonOperator
COMPARISON_OPERATOR_NOT_EQUAL: ComparisonOperator
COMPARISON_OPERATOR_GREATER_THAN: ComparisonOperator
COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL: ComparisonOperator
COMPARISON_OPERATOR_LESS_THAN: ComparisonOperator
COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL: ComparisonOperator

class CreateHooksContextRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class CreateHooksContextResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DestroyHooksContextRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class DestroyHooksContextResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ValueRef(_message.Message):
    __slots__ = ("request_field", "counter_value", "json_value")
    REQUEST_FIELD_FIELD_NUMBER: _ClassVar[int]
    COUNTER_VALUE_FIELD_NUMBER: _ClassVar[int]
    JSON_VALUE_FIELD_NUMBER: _ClassVar[int]
    request_field: str
    counter_value: str
    json_value: bytes
    def __init__(self, request_field: _Optional[str] = ..., counter_value: _Optional[str] = ..., json_value: _Optional[bytes] = ...) -> None: ...

class HookCondition(_message.Message):
    __slots__ = ("left", "op", "right")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    left: ValueRef
    op: ComparisonOperator
    right: ValueRef
    def __init__(self, left: _Optional[_Union[ValueRef, _Mapping]] = ..., op: _Optional[_Union[ComparisonOperator, str]] = ..., right: _Optional[_Union[ValueRef, _Mapping]] = ...) -> None: ...

class HookAction(_message.Message):
    __slots__ = ("counter", "wait_on_barrier", "signal_barrier", "return_response", "return_error", "execute")
    class If(_message.Message):
        __slots__ = ("cond", "match", "no_match")
        COND_FIELD_NUMBER: _ClassVar[int]
        MATCH_FIELD_NUMBER: _ClassVar[int]
        NO_MATCH_FIELD_NUMBER: _ClassVar[int]
        cond: _containers.RepeatedCompositeFieldContainer[HookCondition]
        match: _containers.RepeatedCompositeFieldContainer[HookAction]
        no_match: _containers.RepeatedCompositeFieldContainer[HookAction]
        def __init__(self, cond: _Optional[_Iterable[_Union[HookCondition, _Mapping]]] = ..., match: _Optional[_Iterable[_Union[HookAction, _Mapping]]] = ..., no_match: _Optional[_Iterable[_Union[HookAction, _Mapping]]] = ...) -> None: ...
    class Counter(_message.Message):
        __slots__ = ("counter_id", "delta")
        COUNTER_ID_FIELD_NUMBER: _ClassVar[int]
        DELTA_FIELD_NUMBER: _ClassVar[int]
        counter_id: str
        delta: int
        def __init__(self, counter_id: _Optional[str] = ..., delta: _Optional[int] = ...) -> None: ...
    class WaitOnBarrier(_message.Message):
        __slots__ = ("barrier_id",)
        BARRIER_ID_FIELD_NUMBER: _ClassVar[int]
        barrier_id: str
        def __init__(self, barrier_id: _Optional[str] = ...) -> None: ...
    class SignalBarrier(_message.Message):
        __slots__ = ("barrier_id", "signal_all")
        BARRIER_ID_FIELD_NUMBER: _ClassVar[int]
        SIGNAL_ALL_FIELD_NUMBER: _ClassVar[int]
        barrier_id: str
        signal_all: bool
        def __init__(self, barrier_id: _Optional[str] = ..., signal_all: bool = ...) -> None: ...
    class ReturnResponse(_message.Message):
        __slots__ = ("value",)
        VALUE_FIELD_NUMBER: _ClassVar[int]
        value: _any_pb2.Any
        def __init__(self, value: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
    class ReturnError(_message.Message):
        __slots__ = ("code", "message", "details")
        CODE_FIELD_NUMBER: _ClassVar[int]
        MESSAGE_FIELD_NUMBER: _ClassVar[int]
        DETAILS_FIELD_NUMBER: _ClassVar[int]
        code: int
        message: str
        details: _containers.RepeatedCompositeFieldContainer[_any_pb2.Any]
        def __init__(self, code: _Optional[int] = ..., message: _Optional[str] = ..., details: _Optional[_Iterable[_Union[_any_pb2.Any, _Mapping]]] = ...) -> None: ...
    class Execute(_message.Message):
        __slots__ = ()
        def __init__(self) -> None: ...
    IF_FIELD_NUMBER: _ClassVar[int]
    COUNTER_FIELD_NUMBER: _ClassVar[int]
    WAIT_ON_BARRIER_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_BARRIER_FIELD_NUMBER: _ClassVar[int]
    RETURN_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    RETURN_ERROR_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_FIELD_NUMBER: _ClassVar[int]
    counter: HookAction.Counter
    wait_on_barrier: HookAction.WaitOnBarrier
    signal_barrier: HookAction.SignalBarrier
    return_response: HookAction.ReturnResponse
    return_error: HookAction.ReturnError
    execute: HookAction.Execute
    def __init__(self, counter: _Optional[_Union[HookAction.Counter, _Mapping]] = ..., wait_on_barrier: _Optional[_Union[HookAction.WaitOnBarrier, _Mapping]] = ..., signal_barrier: _Optional[_Union[HookAction.SignalBarrier, _Mapping]] = ..., return_response: _Optional[_Union[HookAction.ReturnResponse, _Mapping]] = ..., return_error: _Optional[_Union[HookAction.ReturnError, _Mapping]] = ..., execute: _Optional[_Union[HookAction.Execute, _Mapping]] = ..., **kwargs) -> None: ...

class Hook(_message.Message):
    __slots__ = ("name", "description", "target_method", "actions")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TARGET_METHOD_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    target_method: str
    actions: _containers.RepeatedCompositeFieldContainer[HookAction]
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., target_method: _Optional[str] = ..., actions: _Optional[_Iterable[_Union[HookAction, _Mapping]]] = ...) -> None: ...

class AddHooksRequest(_message.Message):
    __slots__ = ("hooks_context_id", "hooks")
    HOOKS_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    HOOKS_FIELD_NUMBER: _ClassVar[int]
    hooks_context_id: str
    hooks: _containers.RepeatedCompositeFieldContainer[Hook]
    def __init__(self, hooks_context_id: _Optional[str] = ..., hooks: _Optional[_Iterable[_Union[Hook, _Mapping]]] = ...) -> None: ...

class AddHooksResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WatchBarrierRequest(_message.Message):
    __slots__ = ("hooks_context_id", "barrier_id")
    HOOKS_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    BARRIER_ID_FIELD_NUMBER: _ClassVar[int]
    hooks_context_id: str
    barrier_id: str
    def __init__(self, hooks_context_id: _Optional[str] = ..., barrier_id: _Optional[str] = ...) -> None: ...

class WatchBarrierResponse(_message.Message):
    __slots__ = ("wait_id",)
    WAIT_ID_FIELD_NUMBER: _ClassVar[int]
    wait_id: str
    def __init__(self, wait_id: _Optional[str] = ...) -> None: ...

class SignalBarrierRequest(_message.Message):
    __slots__ = ("hooks_context_id", "barrier_id", "wait_id")
    HOOKS_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    BARRIER_ID_FIELD_NUMBER: _ClassVar[int]
    WAIT_ID_FIELD_NUMBER: _ClassVar[int]
    hooks_context_id: str
    barrier_id: str
    wait_id: str
    def __init__(self, hooks_context_id: _Optional[str] = ..., barrier_id: _Optional[str] = ..., wait_id: _Optional[str] = ...) -> None: ...

class SignalBarrierResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WatchRequestsRequest(_message.Message):
    __slots__ = ("hooks_context_id",)
    HOOKS_CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    hooks_context_id: str
    def __init__(self, hooks_context_id: _Optional[str] = ...) -> None: ...

class WatchRequestsResponse(_message.Message):
    __slots__ = ("meta_data", "full_method", "payload")
    class MetaDataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    META_DATA_FIELD_NUMBER: _ClassVar[int]
    FULL_METHOD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    meta_data: _containers.RepeatedCompositeFieldContainer[WatchRequestsResponse.MetaDataEntry]
    full_method: str
    payload: _any_pb2.Any
    def __init__(self, meta_data: _Optional[_Iterable[_Union[WatchRequestsResponse.MetaDataEntry, _Mapping]]] = ..., full_method: _Optional[str] = ..., payload: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...
