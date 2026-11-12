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

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Literal,
                    Protocol,
                    TypedDict,
                    runtime_checkable)

if TYPE_CHECKING:
    from couchbase.common._observability.handler import (ObservableRequestHandlerNoOpMeterImpl,
                                                         ObservableRequestHandlerNoOpTracerImpl)


@runtime_checkable
class LegacySpanProtocol(Protocol):

    def set_attribute(self, name: str, value: Any) -> None: ...

    def finish(self) -> None: ...


@runtime_checkable
class LegacyTracerProtocol(Protocol):

    def start_span(self,
                   name: str,
                   parent: LegacySpanProtocol | None = None,
                   start_time: int | None = None) -> LegacySpanProtocol: ...


@runtime_checkable
class RequestSpanProtocol(Protocol):

    @property
    def name(self) -> str: ...

    def set_attribute(self, key: str, value: Any) -> None: ...

    def set_attributes(self, attributes: Mapping[str, Any]) -> None: ...

    def add_event(self, name: str, value: Any) -> None: ...

    def set_status(self, status: Any) -> None: ...

    def end(self, timestamp: int | None) -> None: ...


@runtime_checkable
class RequestTracerProtocol(Protocol):

    def request_span(self,
                     name: str,
                     parent_span: RequestSpanProtocol | None = None,
                     start_time: int | None = None
                     ) -> RequestSpanProtocol:
        ...


SpanProtocol = LegacySpanProtocol | RequestSpanProtocol
TracerProtocol = LegacyTracerProtocol | RequestTracerProtocol


@runtime_checkable
class ValueRecorderProtocol(Protocol):

    def record_value(self, value: int) -> None: ...


@runtime_checkable
class MeterProtocol(Protocol):

    def value_recorder(self, name: str, tags: Mapping[str, str]) -> ValueRecorderProtocol: ...


class CoreChildSpan(TypedDict, total=False):
    """One span the core recorded inside a request, with the spans it recorded inside that."""

    name: str
    start: int
    end: int
    attributes: dict[str, str | int] | None
    children: list[CoreChildSpan] | None


class CoreSpan(TypedDict, total=False):
    """What a protocol's core reports about one request, which the handler rebuilds as SDK spans."""

    attributes: dict[str, str | int] | None
    children: list[CoreChildSpan] | None


@dataclass
class WrappedTracer:
    tracer: TracerProtocol
    is_legacy: bool


@dataclass
class ObservabilityInstruments:
    tracer: WrappedTracer
    meter: MeterProtocol
    get_cluster_labels_fn: Callable[[], Mapping[str, str]] | None = None
    # set to True when both tracer and meter or no-op
    is_noop: bool = False
    # Populated on first use by ObservableRequestHandler.  Literal[False] rather than bool is what
    # lets `is not False` narrow to the impl type, leaving no Literal[True] arm behind.
    _cached_noop_tracer_impl: ObservableRequestHandlerNoOpTracerImpl | Literal[False] | None = field(
        default=None, repr=False, compare=False)
    _cached_noop_meter_impl: ObservableRequestHandlerNoOpMeterImpl | Literal[False] | None = field(
        default=None, repr=False, compare=False)


class OpAttributeName(Enum):
    BucketName = 'db.namespace'
    ClusterName = 'couchbase.cluster.name'
    ClusterUUID = 'couchbase.cluster.uuid'
    CollectionName = 'couchbase.collection.name'
    DispatchSpanName = 'dispatch_to_server'
    DurabilityLevel = 'couchbase.durability'
    EncodingSpanName = 'request_encoding'
    ErrorType = 'error.type'
    MeterOperationDuration = 'db.client.operation.duration'
    OperationName = 'db.operation.name'
    QueryStatement = 'db.query.text'
    ReservedUnit = '__unit'
    ReservedUnitSeconds = 's'
    RetryCount = 'couchbase.retries'
    ScopeName = 'couchbase.scope.name'
    Service = 'couchbase.service'
    SystemName = 'db.system.name'


# OpAttributeName cached values
_ATTR_BUCKET_NAME = OpAttributeName.BucketName.value
_ATTR_CLUSTER_NAME = OpAttributeName.ClusterName.value
_ATTR_CLUSTER_UUID = OpAttributeName.ClusterUUID.value
_ATTR_COLLECTION_NAME = OpAttributeName.CollectionName.value
_ATTR_DISPATCH_SPAN_NAME = OpAttributeName.DispatchSpanName.value
_ATTR_DURABILITY_LEVEL = OpAttributeName.DurabilityLevel.value
_ATTR_ENCODING_SPAN_NAME = OpAttributeName.EncodingSpanName.value
_ATTR_ERROR_TYPE = OpAttributeName.ErrorType.value
_ATTR_METER_OP_DURATION = OpAttributeName.MeterOperationDuration.value
_ATTR_OPERATION_NAME = OpAttributeName.OperationName.value
_ATTR_QUERY_STATEMENT = OpAttributeName.QueryStatement.value
_ATTR_RESERVED_UNIT = OpAttributeName.ReservedUnit.value
_ATTR_RESERVED_UNIT_SECONDS = OpAttributeName.ReservedUnitSeconds.value
_ATTR_RETRY_COUNT = OpAttributeName.RetryCount.value
_ATTR_SCOPE_NAME = OpAttributeName.ScopeName.value
_ATTR_SERVICE = OpAttributeName.Service.value
_ATTR_SYSTEM_NAME = OpAttributeName.SystemName.value


class CppOpAttributeName(Enum):
    ClusterName = 'cluster_name'
    ClusterUUID = 'cluster_uuid'
    RetryCount = 'retries'


# CppOpAttributeName cached values
_CPP_ATTR_CLUSTER_NAME = CppOpAttributeName.ClusterName.value
_CPP_ATTR_CLUSTER_UUID = CppOpAttributeName.ClusterUUID.value
_CPP_ATTR_RETRY_COUNT = CppOpAttributeName.RetryCount.value


class DispatchAttributeName(Enum):
    LocalId = 'couchbase.local_id'
    NetworkTransport = 'network.transport'
    OperationId = 'couchbase.operation_id'
    PeerAddress = 'network.peer.address'
    PeerPort = 'network.peer.port'
    ServerAddress = 'server.address'
    ServerDuration = 'couchbase.server_duration'
    ServerPort = 'server.port'


# DispatchAttributeName cached values
_DISP_LOCAL_ID = DispatchAttributeName.LocalId.value
_DISP_OPERATION_ID = DispatchAttributeName.OperationId.value
_DISP_PEER_ADDRESS = DispatchAttributeName.PeerAddress.value
_DISP_PEER_PORT = DispatchAttributeName.PeerPort.value
_DISP_SERVER_ADDRESS = DispatchAttributeName.ServerAddress.value
_DISP_SERVER_DURATION = DispatchAttributeName.ServerDuration.value
_DISP_SERVER_PORT = DispatchAttributeName.ServerPort.value


class ExceptionName(Enum):
    AmbiguousTimeoutException = 'AmbiguousTimeout'
    AnalyticsLinkExistsException = 'LinkExists'
    AnalyticsLinkNotFoundException = 'LinkNotFound'
    AuthenticationException = 'AuthenticationFailure'
    BucketAlreadyExistsException = 'BucketExists'
    BucketDoesNotExistException = 'BucketNotFound'
    BucketNotFlushableException = 'BucketNotFlushable'
    BucketNotFoundException = 'BucketNotFound'
    CASMismatchException = 'CasMismatch'
    CasMismatchException = 'CasMismatch'
    CollectionAlreadyExistsException = 'CollectionExists'
    CollectionNotFoundException = 'CollectionNotFound'
    CryptoException = 'Crypto'
    CryptoKeyNotFoundException = 'CryptoKeyNotFound'
    DatasetAlreadyExistsException = 'DatasetExists'
    DatasetNotFoundException = 'DatasetNotFound'
    DataverseAlreadyExistsException = 'DataverseExists'
    DataverseNotFoundException = 'DataverseNotFound'
    DecrypterAlreadyExistsException = 'DecrypterExists'
    DecrypterNotFoundException = 'DecrypterNotFound'
    DecryptionFailureException = 'DecryptionFailure'
    DeltaInvalidException = 'DeltaInvalid'
    DesignDocumentNotFoundException = 'DesignDocumentNotFound'
    DocumentExistsException = 'DocumentExists'
    DocumentLockedException = 'DocumentLocked'
    DocumentNotFoundException = 'DocumentNotFound'
    DocumentNotFoundOnReplicaException = 'DocumentNotFoundOnReplica'
    DocumentNotJsonException = 'DocumentNotJson'
    DocumentNotLockedException = 'DocumentNotLocked'
    DocumentUnretrievableException = 'DocumentUnretrievable'
    DurabilityImpossibleException = 'DurabilityImpossible'
    DurabilityInvalidLevelException = 'DurabilityLevelNotAvailable'
    DurabilitySyncWriteAmbiguousException = 'DurabilityAmbiguous'
    DurabilitySyncWriteInProgressException = 'DurableWriteInProgress'
    EncrypterAlreadyExistsException = 'EncrypterExists'
    EncrypterNotFoundException = 'EncrypterNotFound'
    EncryptionFailureException = 'EncryptionFailure'
    EventingFunctionAlreadyDeployedException = 'EventingFunctionDeployed'
    EventingFunctionCollectionNotFoundException = 'CollectionNotFound'
    EventingFunctionCompilationFailureException = 'EventingFunctionCompilationFailure'
    EventingFunctionIdenticalKeyspaceException = 'EventingFunctionIdenticalKeyspace'
    EventingFunctionNotBootstrappedException = 'EventingFunctionNotBootstrapped'
    EventingFunctionNotDeployedException = 'EventingFunctionNotDeployed'
    EventingFunctionNotFoundException = 'EventingFunctionNotFound'
    EventingFunctionNotUnDeployedException = 'EventingFunctionDeployed'
    FeatureNotFoundException = 'FeatureNotAvailable'
    FeatureUnavailableException = 'FeatureNotAvailable'
    GroupNotFoundException = 'GroupNotFound'
    InternalServerFailureException = 'InternalServerFailure'
    InvalidArgumentException = 'InvalidArgument'
    InvalidCipherTextException = 'InvalidCiphertext'
    InvalidCryptoKeyException = 'InvalidCryptoKey'
    InvalidIndexException = 'InvalidIndex'
    InvalidValueException = 'InvalidValue'
    KeyspaceNotFoundException = 'KeyspaceNotFound'
    NumberTooBigException = 'NumberTooBig'
    ParsingFailedException = 'ParsingFailure'
    PathExistsException = 'PathExists'
    PathInvalidException = 'PathInvalid'
    PathMismatchException = 'PathMismatch'
    PathNotFoundException = 'PathNotFound'
    PathTooBigException = 'PathTooBig'
    PathTooDeepException = 'PathTooDeep'
    QueryIndexAlreadyExistsException = 'IndexExists'
    QueryIndexNotFoundException = 'IndexNotFound'
    QuotaLimitedException = 'QuotaLimited'
    RateLimitedException = 'RateLimited'
    ReplicaIndexCurrentlyUnavailableException = 'ReplicaIndexCurrentlyUnavailable'
    ReplicaIndexOutOfBoundsException = 'ReplicaIndexOutOfBounds'
    RequestCanceledException = 'RequestCanceled'
    ScopeAlreadyExistsException = 'ScopeExists'
    ScopeNotFoundException = 'ScopeNotFound'
    SearchIndexNotFoundException = 'IndexNotFound'
    ServiceUnavailableException = 'ServiceNotAvailable'
    SubdocCantInsertValueException = 'ValueInvalid'
    SubdocPathMismatchException = 'PathMismatch'
    TemporaryFailException = 'TemporaryFailure'
    TimeoutException = 'Timeout'
    UnAmbiguousTimeoutException = 'UnambiguousTimeout'
    UserNotFoundException = 'UserNotFound'
    ValueFormatException = 'ValueFormat'
    ValueTooDeepException = 'ValueTooDeep'

    @staticmethod
    def from_exception(exception: Exception) -> ExceptionName | None:
        try:
            return ExceptionName[exception.__class__.__name__]
        except KeyError:
            return None
