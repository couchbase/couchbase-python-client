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

from dataclasses import dataclass
from enum import Enum
from typing import (Any,
                    Callable,
                    Mapping,
                    Optional,
                    Protocol,
                    Union,
                    runtime_checkable)

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.operation_types import (AnalyticsMgmtOperationType,
                                             BucketMgmtOperationType,
                                             CollectionMgmtOperationType,
                                             DatastructureOperationType,
                                             EventingFunctionMgmtOperationType,
                                             KeyValueMultiOperationType,
                                             KeyValueOperationType,
                                             MgmtOperationType,
                                             QueryIndexMgmtOperationType,
                                             SearchIndexMgmtOperationType,
                                             StreamingOperationType,
                                             UserMgmtOperationType,
                                             ViewIndexMgmtOperationType)


@runtime_checkable
class LegacySpanProtocol(Protocol):

    def set_attribute(self, name, value: Any) -> None: ...

    def finish(self) -> None: ...


@runtime_checkable
class LegacyTracerProtocol(Protocol):

    def start_span(self,
                   name: str,
                   parent: Optional[LegacySpanProtocol] = None,
                   start_time: Optional[int] = None) -> LegacySpanProtocol: ...


@runtime_checkable
class RequestSpanProtocol(Protocol):

    @property
    def name(self) -> str: ...

    def set_attribute(self, key: str, value: Any) -> None: ...

    def set_attributes(self, attributes: Mapping[str, Any]) -> None: ...

    def add_event(self, name: str, value: Any) -> None: ...

    def set_status(self, status: Any) -> None: ...

    def end(self, timestamp: Optional[int]) -> None: ...


@runtime_checkable
class RequestTracerProtocol(Protocol):

    def request_span(self,
                     name: str,
                     parent_span: Optional[RequestSpanProtocol] = None,
                     start_time: Optional[int] = None
                     ) -> RequestSpanProtocol:
        ...


SpanProtocol = Union[LegacySpanProtocol, RequestSpanProtocol]
TracerProtocol = Union[LegacyTracerProtocol, RequestTracerProtocol]


@runtime_checkable
class ValueRecorderProtocol(Protocol):

    def record_value(self, value: int) -> None: ...


@runtime_checkable
class MeterProtocol(Protocol):

    def value_recorder(self, name: str, tags: Mapping[str, str]) -> ValueRecorderProtocol: ...


OpType = Union[AnalyticsMgmtOperationType,
               BucketMgmtOperationType,
               CollectionMgmtOperationType,
               DatastructureOperationType,
               EventingFunctionMgmtOperationType,
               KeyValueMultiOperationType,
               KeyValueOperationType,
               MgmtOperationType,
               QueryIndexMgmtOperationType,
               SearchIndexMgmtOperationType,
               StreamingOperationType,
               UserMgmtOperationType,
               ViewIndexMgmtOperationType]


@dataclass
class WrappedTracer:
    tracer: TracerProtocol
    is_legacy: bool


@dataclass
class ObservabilityInstruments:
    tracer: WrappedTracer
    meter: MeterProtocol
    get_cluster_labels_fn: Optional[Callable[[], Mapping[str, str]]] = None


class ServiceType(Enum):
    Analytics = 'analytics'
    Eventing = 'eventing'
    KeyValue = 'kv'
    Management = 'management'
    Query = 'query'
    Search = 'search'
    Transactions = 'transactions'
    Views = 'views'

    def is_http_service_type(self) -> bool:
        return (self.is_streaming_service_type()
                or self is ServiceType.Eventing
                or self is ServiceType.Management)

    def is_key_value_service_type(self) -> bool:
        return self is ServiceType.KeyValue

    def is_streaming_service_type(self) -> bool:
        return (self is ServiceType.Analytics
                or self is ServiceType.Query
                or self is ServiceType.Search
                or self is ServiceType.Views)

    def is_txn_service_type(self) -> bool:
        return self is ServiceType.Transactions

    @classmethod
    def from_str(cls, service_type: str) -> ServiceType:
        return ServiceType(service_type)

    @classmethod
    def from_op_type(cls, opt_type: OpType) -> ServiceType:  # noqa: C901
        if isinstance(opt_type, (KeyValueMultiOperationType, KeyValueOperationType, DatastructureOperationType)):
            return ServiceType.KeyValue
        elif isinstance(opt_type, StreamingOperationType):
            if opt_type == StreamingOperationType.AnalyticsQuery:
                return ServiceType.Analytics
            elif opt_type == StreamingOperationType.Query:
                return ServiceType.Query
            elif opt_type == StreamingOperationType.SearchQuery:
                return ServiceType.Search
            elif opt_type == StreamingOperationType.ViewQuery:
                return ServiceType.Views
        elif isinstance(opt_type, AnalyticsMgmtOperationType):
            return ServiceType.Analytics
        elif isinstance(opt_type, EventingFunctionMgmtOperationType):
            return ServiceType.Eventing
        # MgmtOperationType is a special case for Python-only composite operations
        elif isinstance(opt_type, MgmtOperationType):
            if opt_type is MgmtOperationType.ViewIndexPublish:
                return ServiceType.Views
            return ServiceType.Query
        elif isinstance(opt_type, QueryIndexMgmtOperationType):
            return ServiceType.Query
        elif isinstance(opt_type, SearchIndexMgmtOperationType):
            return ServiceType.Search
        elif isinstance(opt_type, ViewIndexMgmtOperationType):
            return ServiceType.Views
        elif isinstance(opt_type, (BucketMgmtOperationType,
                                   CollectionMgmtOperationType,
                                   UserMgmtOperationType)):
            return ServiceType.Management

        raise InvalidArgumentException(f'Unsupported operation type {opt_type}')


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


class CppOpAttributeName(Enum):
    ClusterName = 'cluster_name'
    ClusterUUID = 'cluster_uuid'
    RetryCount = 'retries'


class DispatchAttributeName(Enum):
    LocalId = 'couchbase.local_id'
    NetworkTransport = 'network.transport'
    OperationId = 'couchbase.operation_id'
    PeerAddress = 'network.peer.address'
    PeerPort = 'network.peer.port'
    ServerAddress = 'server.address'
    ServerDuration = 'couchbase.server_duration'
    ServerPort = 'server.port'


class OpName(Enum):
    Append = 'append'
    Decrement = 'decrement'
    Exists = 'exists'
    Get = 'get'
    GetAllReplicas = 'get_all_replicas'
    GetAndLock = 'get_and_lock'
    GetAndTouch = 'get_and_touch'
    GetAnyReplica = 'get_any_replica'
    GetReplica = 'get_replica'  # this is only for the C++ core replica ops
    GetProjected = 'get_projected'
    Increment = 'increment'
    Insert = 'insert'
    LookupIn = 'lookup_in'
    LookupInAllReplicas = 'lookup_in_all_replicas'
    LookupInAnyReplica = 'lookup_in_any_replica'
    LookupInReplica = 'lookup_in_replica'  # this is only for the C++ core replica ops
    MutateIn = 'mutate_in'
    Prepend = 'prepend'
    Remove = 'remove'
    Replace = 'replace'
    Touch = 'touch'
    Unlock = 'unlock'
    Upsert = 'upsert'
    # multi ops
    AppendMulti = 'append_multi'
    DecrementMulti = 'decrement_multi'
    ExistsMulti = 'exists_multi'
    GetMulti = 'get_multi'
    GetAllReplicasMulti = 'get_all_replicas_multi'
    GetAndLockMulti = 'get_and_lock_multi'
    GetAndTouchMulti = 'get_and_touch_multi'
    GetAnyReplicaMulti = 'get_any_replica_multi'
    IncrementMulti = 'increment_multi'
    InsertMulti = 'insert_multi'
    PrependMulti = 'prepend_multi'
    RemoveMulti = 'remove_multi'
    ReplaceMulti = 'replace_multi'
    TouchMulti = 'touch_multi'
    UnlockMulti = 'unlock_multi'
    UpsertMulti = 'upsert_multi'
    # datastructures
    ListAppend = 'list_append'
    ListClear = 'list_clear'
    ListGetAll = 'list_get_all'
    ListGetAt = 'list_get_at'
    ListIndexOf = 'list_index_of'
    ListPrepend = 'list_prepend'
    ListRemoveAt = 'list_remove_at'
    ListSetAt = 'list_set_at'
    ListSize = 'list_size'
    MapAdd = 'map_add'
    MapClear = 'map_clear'
    MapExists = 'map_exists'
    MapGet = 'map_get'
    MapGetAll = 'map_get_all'
    MapItems = 'map_items'
    MapKeys = 'map_keys'
    MapRemove = 'map_remove'
    MapSize = 'map_size'
    MapValues = 'map_values'
    QueueClear = 'queue_clear'
    QueuePop = 'queue_pop'
    QueuePush = 'queue_push'
    QueueSize = 'queue_size'
    SetAdd = 'set_add'
    SetClear = 'set_clear'
    SetContains = 'set_contains'
    SetRemove = 'set_remove'
    SetSize = 'set_size'
    SetValues = 'set_values'
    # streaming
    AnalyticsQuery = 'analytics_query'
    Query = 'query'
    SearchQuery = 'search_query'
    ViewQuery = 'view_query'
    # analytics mgmt
    AnalyticsDatasetCreate = 'manager_analytics_create_dataset'
    AnalyticsDatasetDrop = 'manager_analytics_drop_dataset'
    AnalyticsDatasetGetAll = 'manager_analytics_get_all_datasets'
    AnalyticsDataverseCreate = 'manager_analytics_create_dataverse'
    AnalyticsDataverseDrop = 'manager_analytics_drop_dataverse'
    AnalyticsGetPendingMutations = 'manager_analytics_get_pending_mutations'
    AnalyticsIndexCreate = 'manager_analytics_create_index'
    AnalyticsIndexDrop = 'manager_analytics_drop_index'
    AnalyticsIndexGetAll = 'manager_analytics_get_all_indexes'
    AnalyticsLinkConnect = 'manager_analytics_connectlink'
    AnalyticsLinkCreate = 'manager_analytics_create_link'
    AnalyticsLinkDisconnect = 'manager_analytics_disconnect_link'
    AnalyticsLinkDrop = 'manager_analytics_drop_link'
    AnalyticsLinkGetAll = 'manager_analytics_get_all_links'
    AnalyticsLinkReplace = 'manager_analytics_replace_link'
    # bucket mgmt
    BucketCreate = 'manager_buckets_create_bucket'
    BucketDescribe = 'manager_buckets_describe_bucket'
    BucketDrop = 'manager_buckets_drop_bucket'
    BucketFlush = 'manager_buckets_flush_bucket'
    BucketGet = 'manager_buckets_get_bucket'
    BucketGetAll = 'manager_buckets_get_all_buckets'
    BucketUpdate = 'manager_buckets_update_bucket'
    # collection mgmt
    CollectionCreate = 'manager_collections_create_collection'
    CollectionsManifestGet = 'manager_collections_get_collections_manifest'
    CollectionDrop = 'manager_collections_drop_collection'
    CollectionUpdate = 'manager_collections_update_collection'
    ScopeCreate = 'manager_collections_create_scope'
    ScopeDrop = 'manager_collections_drop_scope'
    ScopeGetAll = 'manager_collections_get_all_scopes'
    # eventing mgmt
    EventingDeployFunction = 'manager_eventing_deploy_function'
    EventingDropFunction = 'manager_eventing_drop_function'
    EventingGetAllFunctions = 'manager_eventing_get_all_functions'
    EventingGetFunction = 'manager_eventing_get_function'
    EventingGetStatus = 'manager_eventing_functions_status'
    EventingPauseFunction = 'manager_eventing_pause_function'
    EventingResumeFunction = 'manager_eventing_resume_function'
    EventingUndeployFunction = 'manager_eventing_undeploy_function'
    EventingUpsertFunction = 'manager_eventing_upsert_function'
    # query index mgmt
    QueryIndexBuild = 'manager_query_build_indexes'
    QueryIndexBuildDeferred = 'manager_query_build_deferred_indexes'
    QueryIndexCreate = 'manager_query_create_index'
    QueryIndexDrop = 'manager_query_drop_index'
    QueryIndexGetAll = 'manager_query_get_all_indexes'
    QueryIndexGetAllDeferred = 'manager_query_get_all_deferred_indexes'
    QueryIndexWatchIndexes = 'manager_query_watch_indexes'
    # search index mgmt
    SearchGetStats = 'manager_search_get_stats'
    SearchIndexAllowQuerying = 'manager_search_allow_querying'
    SearchIndexAnalyzeDocument = 'manager_search_analyze_document'
    SearchIndexDisallowQuerying = 'manager_search_disallow_querying'
    SearchIndexDrop = 'manager_search_drop_index'
    SearchIndexFreezePlan = 'manager_search_freeze_plan'
    SearchIndexGet = 'manager_search_get_index'
    SearchIndexGetAll = 'manager_search_get_all_indexes'
    SearchIndexGetDocumentsCount = 'manager_search_get_indexed_documents_count'
    SearchIndexGetStats = 'manager_search_get_index_stats'
    SearchIndexPauseIngest = 'manager_search_pause_ingest'
    SearchIndexResumeIngest = 'manager_search_resume_ingest'
    SearchIndexUnfreezePlan = 'manager_search_unfreeze_plan'
    SearchIndexUpsert = 'manager_search_upsert_index'
    # user mgmt
    ChangePassword = 'manager_users_change_password'
    GroupDrop = 'manager_users_drop_group'
    GroupGet = 'manager_users_get_group'
    GroupGetAll = 'manager_users_get_all_groups'
    GroupUpsert = 'manager_users_upsert_group'
    RoleGetAll = 'manager_users_get_all_roles'
    UserDrop = 'manager_users_drop_user'
    UserGet = 'manager_users_get_user'
    UserGetAll = 'manager_users_get_all_users'
    UserUpsert = 'manager_users_upsert_user'
    # view index mgmt
    ViewIndexDrop = 'manager_views_drop_design_document'
    ViewIndexGet = 'manager_views_get_design_document'
    ViewIndexGetAll = 'manager_views_get_all_design_documents'
    ViewIndexPublish = 'manager_views_publish_design_document'
    ViewIndexUpsert = 'manager_views_upsert_design_document'

    def is_multi_op(self) -> bool:
        return self.name.endswith('Multi')

    def is_streaming_op(self) -> bool:
        return (self is OpName.AnalyticsQuery
                or self is OpName.Query
                or self is OpName.SearchQuery
                or self is OpName.ViewQuery)

    @classmethod
    def from_op_type(cls, op_type: OpType, toggle: Optional[bool] = None) -> OpName:  # noqa: C901
        if isinstance(op_type, (DatastructureOperationType,
                                KeyValueMultiOperationType,
                                KeyValueOperationType,
                                StreamingOperationType)):
            return OpName(op_type.value)
        elif isinstance(op_type, AnalyticsMgmtOperationType):
            if (op_type is AnalyticsMgmtOperationType.AnalyticsLinkCreateAzureBlobExternalLink
                or op_type is AnalyticsMgmtOperationType.AnalyticsLinkCreateCouchbaseRemoteLink
                    or op_type is AnalyticsMgmtOperationType.AnalyticsLinkCreateS3ExternalLink):
                return OpName.AnalyticsLinkCreate
            elif (op_type is AnalyticsMgmtOperationType.AnalyticsLinkReplaceAzureBlobExternalLink
                  or op_type is AnalyticsMgmtOperationType.AnalyticsLinkReplaceCouchbaseRemoteLink
                  or op_type is AnalyticsMgmtOperationType.AnalyticsLinkReplaceS3ExternalLink):
                return OpName.AnalyticsLinkReplace
            return OpName[op_type.name]
        elif isinstance(op_type, SearchIndexMgmtOperationType):
            if op_type is SearchIndexMgmtOperationType.SearchIndexControlIngest:
                if toggle is None:
                    msg = "Toggle value must be provided for SearchIndexControlIngest operation type"
                    raise InvalidArgumentException(message=msg)
                elif toggle:
                    return OpName.SearchIndexPauseIngest
                else:
                    return OpName.SearchIndexResumeIngest
            if op_type is SearchIndexMgmtOperationType.SearchIndexControlPlanFreeze:
                if toggle is None:
                    msg = "Toggle value must be provided for SearchIndexControlPlanFreeze operation type"
                    raise InvalidArgumentException(message=msg)
                elif toggle:
                    return OpName.SearchIndexFreezePlan
                else:
                    return OpName.SearchIndexUnfreezePlan
            if op_type is SearchIndexMgmtOperationType.SearchIndexControlQuery:
                if toggle is None:
                    msg = "Toggle value must be provided for SearchIndexControlQuery operation type"
                    raise InvalidArgumentException(message=msg)
                elif toggle:
                    return OpName.SearchIndexAllowQuerying
                else:
                    return OpName.SearchIndexDisallowQuerying

            return OpName[op_type.name]
        elif isinstance(op_type, (BucketMgmtOperationType,
                                  CollectionMgmtOperationType,
                                  EventingFunctionMgmtOperationType,
                                  MgmtOperationType,
                                  QueryIndexMgmtOperationType,
                                  UserMgmtOperationType,
                                  ViewIndexMgmtOperationType)):
            return OpName[op_type.name]

        raise InvalidArgumentException(f'Unsupported operation type {op_type}')


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
    def from_exception(exception: Exception) -> Optional[ExceptionName]:
        try:
            return ExceptionName[exception.__class__.__name__]
        except KeyError:
            return None
