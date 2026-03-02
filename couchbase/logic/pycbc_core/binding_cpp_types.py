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

from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Set,
                    Tuple,
                    TypedDict,
                    Union)

# ==========================================================================================
# C++ Core Types
# ==========================================================================================


class CppDocumentId(TypedDict, total=False):
    bucket: Optional[str]
    scope: Optional[str]
    collection: Optional[str]
    key: Optional[str]


class CppMutationToken(TypedDict, total=False):
    partition_id: Optional[int]
    partition_uuid: Optional[int]
    sequence_number: Optional[int]
    bucket_name: Optional[str]


class CppSubdocField(TypedDict, total=False):
    path: Optional[str]
    value: Optional[bytes]
    index: Optional[int]
    exists: Optional[bool]
    opcode: Optional[int]
    status: Optional[int]


class CppSubdocSpec(TypedDict, total=False):
    opcode: Optional[int]
    path: Optional[str]
    value: Optional[bytes]
    flags: Optional[int]
    index: Optional[int]


class CppQueryContext(TypedDict, total=False):
    bucket_name: Optional[str]
    scope_name: Optional[str]


class CppTimeoutDefaults(TypedDict):
    bootstrap_timeout: int
    dispatch_timeout: int
    resolve_timeout: int
    connect_timeout: int
    key_value_timeout: int
    key_value_durable_timeout: int
    key_value_scan_timeout: int
    view_timeout: int
    query_timeout: int
    analytics_timeout: int
    search_timeout: int
    management_timeout: int
    eventing_timeout: int
    dns_srv_timeout: int
    tcp_keep_alive_interval: int
    config_poll_interval: int
    config_poll_floor: int
    config_idle_redial_timeout: int
    idle_http_connection_timeout: int
    app_telemetry_ping_interval: int
    app_telemetry_ping_timeout: int
    app_telemetry_backoff_interval: int


class CppWrapperSdkSpan(TypedDict, total=False):
    attributes: Optional[Dict[str, Union[str, int]]]
    children: Optional[List[CppWrapperSdkChildSpan]]


class CppWrapperSdkChildSpan(TypedDict, total=False):
    name: str
    start: int
    end: int
    attributes: Optional[Dict[str, Union[str, int]]]
    children: Optional[List[CppWrapperSdkChildSpan]]


# ==========================================================================================
# Connection Management Operations
# ==========================================================================================

class CppConnectRequest(TypedDict, total=False):
    connstr: str
    auth: Dict[str, Any]
    options: Dict[str, Any]
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class CppCloseRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class CppOpenBucketRequest(TypedDict, total=False):
    bucket_name: str
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class CppCloseBucketRequest(TypedDict, total=False):
    bucket_name: str
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class CppUpdateCredentialsRequest(TypedDict):
    auth: Dict[str, Any]

# ==========================================================================================
# Diagnostic Operations
# ==========================================================================================


class CppDiagnosticsRequest(TypedDict, total=False):
    report_id: Optional[str]
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class CppPingRequest(TypedDict, total=False):
    services: Set[str]
    bucket_name: Optional[str]
    report_id: Optional[str]
    timetout: Optional[int]
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]

# ==========================================================================================
# Key-Value Range Scan Operations
# ==========================================================================================


class CppRangeScanRequest(TypedDict, total=False):
    bucket: str
    scope: str
    collection: str
    scan_type: int
    scan_config: Union[CppRangeScan, CppRangeScanPrefixScan, CppRangeScanSamplingScan]
    orchestrator_options: Optional[CppRangeScanOrchestratorOptions]

# ====================================================================================================
# AUTOGENERATED SECTION START - DO NOT EDIT MANUALLY
# Generated-On: 2026-03-21 17:13:31
# Content-Hash: 2ffc6fc7fb28c7b7f37bfb0824d31e85
# ====================================================================================================
# ==========================================================================================
# C++ Core Types
# ==========================================================================================


class CppAnalyticsAzureBlobExternalLink(TypedDict, total=False):
    link_name: str
    dataverse: str
    connection_string: Optional[str]
    account_name: Optional[str]
    account_key: Optional[str]
    shared_access_signature: Optional[str]
    blob_endpoint: Optional[str]
    endpoint_suffix: Optional[str]


class CppAnalyticsCouchbaseRemoteLink(TypedDict, total=False):
    link_name: str
    dataverse: str
    hostname: str
    encryption: CppAnalyticsCouchbaseRemoteLinkEncryptionSettings
    username: Optional[str]
    password: Optional[str]


class CppAnalyticsCouchbaseRemoteLinkEncryptionSettings(TypedDict, total=False):
    level: str
    certificate: Optional[str]
    client_certificate: Optional[str]
    client_key: Optional[str]


class CppAnalyticsDataset(TypedDict, total=False):
    name: str
    dataverse_name: str
    link_name: str
    bucket_name: str


class CppAnalyticsIndex(TypedDict, total=False):
    name: str
    dataverse_name: str
    dataset_name: str
    is_primary: bool


class CppAnalyticsLinkConnectProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsLinkCreateProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsLinkDisconnectProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsLinkDropProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsLinkGetAllProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsLinkReplaceProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsManagementProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsMetaData(TypedDict, total=False):
    request_id: str
    client_context_id: str
    status: str
    metrics: CppAnalyticsMetrics
    errors: List[CppAnalyticsProblem]
    warnings: List[CppAnalyticsProblem]
    signature: Optional[str]


class CppAnalyticsMetrics(TypedDict, total=False):
    elapsed_time: int
    execution_time: int
    result_count: int
    result_size: int
    error_count: int
    processed_objects: int
    warning_count: int


class CppAnalyticsProblem(TypedDict, total=False):
    code: int
    message: str


class CppAnalyticsS3ExternalLink(TypedDict, total=False):
    link_name: str
    dataverse: str
    access_key_id: str
    secret_access_key: str
    region: str
    session_token: Optional[str]
    service_endpoint: Optional[str]


class CppBucketInfo(TypedDict, total=False):
    name: str
    uuid: str
    number_of_nodes: int
    number_of_replicas: int
    bucket_capabilities: List[str]
    server_groups: Dict[str, CppServerGroup]
    storage_backend: str
    # config_json


class CppBucketSettingNode(TypedDict, total=False):
    hostname: str
    status: str
    version: str
    services: List[str]
    ports: Dict[str, int]


class CppBucketSettings(TypedDict, total=False):
    name: str
    uuid: str
    ram_quota_mb: int
    bucket_type: str
    compression_mode: str
    eviction_policy: str
    conflict_resolution_type: str
    storage_backend: str
    capabilities: List[str]
    nodes: List[CppBucketSettingNode]
    max_expiry: Optional[int]
    minimum_durability_level: Optional[int]
    num_replicas: Optional[int]
    replica_indexes: Optional[bool]
    flush_enabled: Optional[bool]
    history_retention_collection_default: Optional[bool]
    history_retention_bytes: Optional[int]
    history_retention_duration: Optional[int]
    num_vbuckets: Optional[int]


class CppClusterInfo(TypedDict, total=False):
    nodes: List[CppClusterInfoNode]
    buckets: List[CppClusterInfoBucket]
    services: Set[str]


class CppClusterInfoBucket(TypedDict, total=False):
    uuid: str
    name: str


class CppClusterInfoNode(TypedDict, total=False):
    uuid: str
    otp_node: str
    status: str
    hostname: str
    os: str
    version: str
    services: List[str]


class CppCollectionsManifest(TypedDict, total=False):
    id: List[int]
    uid: int
    scopes: List[CppCollectionsManifestScope]


class CppCollectionsManifestCollection(TypedDict, total=False):
    uid: int
    name: str
    max_expiry: int
    history: Optional[bool]


class CppCollectionsManifestScope(TypedDict, total=False):
    uid: int
    name: str
    collections: List[CppCollectionsManifestCollection]


class CppDiagnosticsResult(TypedDict, total=False):
    id: str
    sdk: str
    services: Dict[str, List[CppEndpointDiagInfo]]
    version: int


class CppEndpointDiagInfo(TypedDict, total=False):
    type: str
    id: str
    remote: str
    local: str
    state: str
    last_activity: Optional[int]
    bucket: Optional[str]
    details: Optional[str]


class CppEndpointPingInfo(TypedDict, total=False):
    type: str
    id: str
    latency: int
    remote: str
    local: str
    state: str
    bucket: Optional[str]
    error: Optional[str]


class CppEventingFunction(TypedDict, total=False):
    name: str
    code: str
    metadata_keyspace: CppEventingFunctionKeyspace
    source_keyspace: CppEventingFunctionKeyspace
    bucket_bindings: List[CppEventingFunctionBucketBinding]
    url_bindings: List[CppEventingFunctionUrlBinding]
    constant_bindings: List[CppEventingFunctionConstantBinding]
    settings: CppEventingFunctionSettings
    # internal
    version: Optional[str]
    enforce_schema: Optional[bool]
    handler_uuid: Optional[int]
    function_instance_id: Optional[str]


class CppEventingFunctionBucketBinding(TypedDict, total=False):
    alias: str
    name: CppEventingFunctionKeyspace
    access: str


class CppEventingFunctionConstantBinding(TypedDict, total=False):
    alias: str
    literal: str


class CppEventingFunctionKeyspace(TypedDict, total=False):
    bucket: str
    scope: Optional[str]
    collection: Optional[str]


class CppEventingFunctionSettings(TypedDict, total=False):
    handler_headers: List[str]
    handler_footers: List[str]
    cpp_worker_count: Optional[int]
    dcp_stream_boundary: Optional[str]
    description: Optional[str]
    deployment_status: Optional[str]
    processing_status: Optional[str]
    log_level: Optional[str]
    language_compatibility: Optional[str]
    execution_timeout: Optional[int]
    lcb_inst_capacity: Optional[int]
    lcb_retry_count: Optional[int]
    lcb_timeout: Optional[int]
    query_consistency: Optional[str]
    num_timer_partitions: Optional[int]
    sock_batch_size: Optional[int]
    tick_duration: Optional[int]
    timer_context_size: Optional[int]
    user_prefix: Optional[str]
    bucket_cache_size: Optional[int]
    bucket_cache_age: Optional[int]
    curl_max_allowed_resp_size: Optional[int]
    query_prepare_all: Optional[bool]
    worker_count: Optional[int]
    enable_app_log_rotation: Optional[bool]
    app_log_dir: Optional[str]
    app_log_max_size: Optional[int]
    app_log_max_files: Optional[int]
    checkpoint_interval: Optional[int]


class CppEventingFunctionState(TypedDict, total=False):
    name: str
    status: str
    num_bootstrapping_nodes: int
    num_deployed_nodes: int
    deployment_status: str
    processing_status: str
    # internal
    redeploy_required: Optional[bool]


class CppEventingFunctionUrlAuthBasic(TypedDict, total=False):
    username: str
    password: str


class CppEventingFunctionUrlAuthBearer(TypedDict, total=False):
    key: str


class CppEventingFunctionUrlAuthDigest(TypedDict, total=False):
    username: str
    password: str


class CppEventingFunctionUrlBinding(TypedDict, total=False):
    alias: str
    hostname: str
    allow_cookies: bool
    validate_ssl_certificate: bool
    auth: Union[CppEventingFunctionUrlNoAuth, CppEventingFunctionUrlAuthBasic,
                CppEventingFunctionUrlAuthDigest, CppEventingFunctionUrlAuthBearer]


class CppEventingFunctionUrlNoAuth(TypedDict, total=False):
    pass


class CppEventingProblem(TypedDict, total=False):
    code: int
    name: str
    description: str


class CppEventingStatus(TypedDict, total=False):
    num_eventing_nodes: int
    functions: List[CppEventingFunctionState]


class CppGetAllReplicasResponseEntry(TypedDict, total=False):
    value: bytes
    cas: int
    flags: int
    replica: bool


class CppLookupInAllReplicasResponseEntry(TypedDict, total=False):
    fields: List[CppLookupInEntry]
    cas: int
    deleted: bool
    is_replica: bool


class CppLookupInAnyReplicaResponseEntry(TypedDict, total=False):
    path: str
    value: bytes
    original_index: int
    exists: bool
    opcode: int
    status: int
    ec: int


class CppLookupInEntry(TypedDict, total=False):
    path: str
    value: bytes
    original_index: int
    exists: bool
    opcode: int
    status: int
    ec: int


class CppLookupInResponseEntry(TypedDict, total=False):
    path: str
    value: bytes
    original_index: int
    exists: bool
    opcode: int
    status: int
    ec: int


class CppMutateInResponseEntry(TypedDict, total=False):
    path: str
    value: bytes
    original_index: int
    opcode: int
    status: int
    ec: int


class CppPingResult(TypedDict, total=False):
    id: str
    sdk: str
    services: Dict[str, List[CppEndpointPingInfo]]
    version: int


class CppQueryIndex(TypedDict, total=False):
    is_primary: bool
    name: str
    state: str
    type: str
    index_key: List[str]
    bucket_name: str
    partition: Optional[str]
    condition: Optional[str]
    scope_name: Optional[str]
    collection_name: Optional[str]


class CppQueryIndexBuildDeferredProblem(TypedDict, total=False):
    code: int
    message: str


class CppQueryIndexBuildProblem(TypedDict, total=False):
    code: int
    message: str


class CppQueryIndexCreateProblem(TypedDict, total=False):
    code: int
    message: str


class CppQueryIndexDropProblem(TypedDict, total=False):
    code: int
    message: str


class CppQueryMetaData(TypedDict, total=False):
    request_id: str
    client_context_id: str
    status: str
    metrics: Optional[CppQueryMetrics]
    signature: Optional[str]
    profile: Optional[str]
    warnings: Optional[List[CppQueryProblem]]
    errors: Optional[List[CppQueryProblem]]


class CppQueryMetrics(TypedDict, total=False):
    elapsed_time: int
    execution_time: int
    result_count: int
    result_size: int
    sort_count: int
    mutation_count: int
    error_count: int
    warning_count: int


class CppQueryProblem(TypedDict, total=False):
    code: int
    message: str
    reason: Optional[int]
    retry: Optional[bool]


class CppRangeScan(TypedDict, total=False):
    from_: Optional[CppRangeScanScanTerm]
    to: Optional[CppRangeScanScanTerm]


class CppRangeScanCancelOptions(TypedDict, total=False):
    timeout: int
    # retry_strategy
    # internal


class CppRangeScanContinueOptions(TypedDict, total=False):
    batch_item_limit: int
    batch_byte_limit: int
    timeout: int
    batch_time_limit: int
    # retry_strategy
    # internal


class CppRangeScanContinueResult(TypedDict, total=False):
    more: bool
    complete: bool
    ids_only: bool


class CppRangeScanCreateOptions(TypedDict, total=False):
    scope_name: str
    collection_name: str
    timeout: int
    collection_id: int
    ids_only: bool
    # retry_strategy
    # internal
    scan_type: Optional[Union[CppRangeScan, CppRangeScanPrefixScan, CppRangeScanSamplingScan]]
    snapshot_requirements: Optional[CppRangeSnapshotRequirements]


class CppRangeScanCreateResult(TypedDict, total=False):
    scan_uuid: bytes
    ids_only: bool


class CppRangeScanItem(TypedDict, total=False):
    key: str
    body: Optional[CppRangeScanItemBody]


class CppRangeScanItemBody(TypedDict, total=False):
    flags: int
    expiry: int
    cas: int
    sequence_number: int
    datatype: int
    value: bytes


class CppRangeScanMutationState(TypedDict, total=False):
    tokens: List[CppMutationToken]


class CppRangeScanOrchestratorOptions(TypedDict, total=False):
    ids_only: bool
    batch_item_limit: int
    batch_byte_limit: int
    concurrency: int
    # retry_strategy
    timeout: int
    consistent_with: Optional[CppRangeScanMutationState]


class CppRangeScanPrefixScan(TypedDict, total=False):
    prefix: str


class CppRangeScanSamplingScan(TypedDict, total=False):
    limit: int
    seed: Optional[int]


class CppRangeScanScanTerm(TypedDict, total=False):
    term: str
    exclusive: bool


class CppRangeSnapshotRequirements(TypedDict, total=False):
    vbucket_uuid: int
    sequence_number: int
    sequence_number_exists: bool


class CppRbacGroup(TypedDict, total=False):
    name: str
    roles: List[CppRbacRole]
    description: Optional[str]
    ldap_group_reference: Optional[str]


class CppRbacOrigin(TypedDict, total=False):
    type: str
    name: Optional[str]


class CppRbacRole(TypedDict, total=False):
    name: str
    bucket: Optional[str]
    scope: Optional[str]
    collection: Optional[str]


class CppRbacRoleAndDescription(TypedDict, total=False):
    display_name: str
    description: str
    name: str
    bucket: Optional[str]
    scope: Optional[str]
    collection: Optional[str]


class CppRbacRoleAndOrigins(TypedDict, total=False):
    origins: List[CppRbacOrigin]
    name: str
    bucket: Optional[str]
    scope: Optional[str]
    collection: Optional[str]


class CppRbacUser(TypedDict, total=False):
    username: str
    groups: Set[str]
    roles: List[CppRbacRole]
    display_name: Optional[str]
    password: Optional[str]


class CppRbacUserAndMetadata(TypedDict, total=False):
    domain: str
    effective_roles: List[CppRbacRoleAndOrigins]
    external_groups: Set[str]
    username: str
    groups: Set[str]
    roles: List[CppRbacRole]
    password_changed: Optional[str]
    display_name: Optional[str]
    password: Optional[str]


class CppSearchDateRangeFacet(TypedDict, total=False):
    name: str
    count: int
    start: Optional[str]
    end: Optional[str]


class CppSearchFacet(TypedDict, total=False):
    name: str
    field: str
    total: int
    missing: int
    other: int
    terms: List[CppSearchTermFacet]
    date_ranges: List[CppSearchDateRangeFacet]
    numeric_ranges: List[CppSearchNumericRangeFacet]


class CppSearchIndex(TypedDict, total=False):
    uuid: str
    name: str
    type: str
    params_json: str
    source_uuid: str
    source_name: str
    source_type: str
    source_params_json: str
    plan_params_json: str


class CppSearchLocation(TypedDict, total=False):
    field: str
    term: str
    position: int
    start_offset: int
    end_offset: int
    array_positions: Optional[List[int]]


class CppSearchMetaData(TypedDict, total=False):
    client_context_id: str
    metrics: CppSearchMetrics
    errors: Dict[str, str]


class CppSearchMetrics(TypedDict, total=False):
    took: int
    total_rows: int
    max_score: float
    success_partition_count: int
    error_partition_count: int


class CppSearchNumericRangeFacet(TypedDict, total=False):
    name: str
    count: int
    min: Optional[Union[int, float]]
    max: Optional[Union[int, float]]


class CppSearchRow(TypedDict, total=False):
    index: str
    id: str
    score: float
    locations: List[CppSearchLocation]
    fragments: Dict[str, List[str]]
    fields: str
    explanation: str


class CppSearchTermFacet(TypedDict, total=False):
    term: str
    count: int


class CppServerGroup(TypedDict, total=False):
    name: str
    nodes: List[CppServerNode]


class CppServerNode(TypedDict, total=False):
    server_group_name: str
    server_index: int
    default_network: CppServerNodeAddress
    external_network: CppServerNodeAddress
    active_vbuckets: Set[int]
    replica_vbuckets: Set[int]


class CppServerNodeAddress(TypedDict, total=False):
    hostname: str
    kv_plain: int
    kv_tls: int


class CppSubdocCommand(TypedDict, total=False):
    opcode: int
    path: str
    value: bytes
    flags: int
    original_index: int


class CppViewDesignDocument(TypedDict, total=False):
    name: str
    ns: str
    views: Dict[str, CppViewDesignDocumentView]
    rev: Optional[str]


class CppViewDesignDocumentView(TypedDict, total=False):
    name: str
    map: Optional[str]
    reduce: Optional[str]


class CppViewMetadata(TypedDict, total=False):
    total_rows: Optional[int]
    debug_info: Optional[str]


class CppViewProblem(TypedDict, total=False):
    code: str
    message: str


class CppViewRow(TypedDict, total=False):
    key: str
    value: str
    id: Optional[str]


# ==========================================================================================
# Key-Value Operations
# ==========================================================================================

class CppAppendRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    cas: int
    durability_level: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAppendResponse(TypedDict, total=False):
    # ctx
    cas: int
    token: CppMutationToken


class CppAppendWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    cas: int
    # retries
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAppendWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppDecrementRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    expiry: int
    delta: int
    durability_level: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    initial_value: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppDecrementResponse(TypedDict, total=False):
    # ctx
    content: int
    cas: int
    token: CppMutationToken


class CppDecrementWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    expiry: int
    delta: int
    # retries
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    initial_value: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppDecrementWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppExistsRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppExistsResponse(TypedDict, total=False):
    # ctx
    deleted: bool
    cas: int
    flags: int
    expiry: int
    sequence_number: int
    datatype: int
    document_exists: bool


class CppGetRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGetResponse(TypedDict, total=False):
    # ctx
    value: bytes
    cas: int
    flags: int


class CppGetAllReplicasRequest(TypedDict, total=False):
    id: CppDocumentId
    read_preference: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGetAndLockRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    lock_time: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGetAndLockResponse(TypedDict, total=False):
    # ctx
    value: bytes
    cas: int
    flags: int


class CppGetAndTouchRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    expiry: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGetAndTouchResponse(TypedDict, total=False):
    # ctx
    value: bytes
    cas: int
    flags: int


class CppGetAnyReplicaRequest(TypedDict, total=False):
    id: CppDocumentId
    read_preference: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGetAnyReplicaResponse(TypedDict, total=False):
    # ctx
    value: bytes
    cas: int
    flags: int
    replica: bool


class CppGetProjectedRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    projections: List[str]
    with_expiry: bool
    effective_projections: List[str]
    preserve_array_indexes: bool
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGetProjectedResponse(TypedDict, total=False):
    # ctx
    value: bytes
    cas: int
    flags: int
    expiry: Optional[int]


class CppIncrementRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    expiry: int
    delta: int
    durability_level: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    initial_value: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppIncrementResponse(TypedDict, total=False):
    # ctx
    content: int
    cas: int
    token: CppMutationToken


class CppIncrementWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    expiry: int
    delta: int
    # retries
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    initial_value: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppIncrementWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppInsertRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    flags: int
    expiry: int
    durability_level: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppInsertResponse(TypedDict, total=False):
    # ctx
    cas: int
    token: CppMutationToken


class CppInsertWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    flags: int
    expiry: int
    # retries
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppInsertWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppLookupInRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    access_deleted: bool
    specs: List[CppSubdocCommand]
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppLookupInResponse(TypedDict, total=False):
    # ctx
    cas: int
    fields: List[CppLookupInResponseEntry]
    deleted: bool


class CppLookupInAllReplicasRequest(TypedDict, total=False):
    id: CppDocumentId
    specs: List[CppSubdocCommand]
    read_preference: int
    access_deleted: bool
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppLookupInAnyReplicaRequest(TypedDict, total=False):
    id: CppDocumentId
    specs: List[CppSubdocCommand]
    read_preference: int
    access_deleted: bool
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppLookupInAnyReplicaResponse(TypedDict, total=False):
    # ctx
    cas: int
    fields: List[CppLookupInAnyReplicaResponseEntry]
    deleted: bool
    is_replica: bool


class CppMutateInRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    cas: int
    access_deleted: bool
    create_as_deleted: bool
    revive_document: bool
    store_semantics: int
    specs: List[CppSubdocCommand]
    durability_level: int
    # retries
    preserve_expiry: bool
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    expiry: Optional[int]
    flags: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppMutateInResponse(TypedDict, total=False):
    # ctx
    cas: int
    token: CppMutationToken
    fields: List[CppMutateInResponseEntry]
    deleted: bool


class CppMutateInWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    cas: int
    access_deleted: bool
    create_as_deleted: bool
    revive_document: bool
    store_semantics: int
    specs: List[CppSubdocCommand]
    # retries
    preserve_expiry: bool
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    expiry: Optional[int]
    flags: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppMutateInWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppPrependRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    cas: int
    durability_level: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppPrependResponse(TypedDict, total=False):
    # ctx
    cas: int
    token: CppMutationToken


class CppPrependWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    cas: int
    # retries
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppPrependWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppRemoveRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    cas: int
    durability_level: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppRemoveResponse(TypedDict, total=False):
    # ctx
    cas: int
    token: CppMutationToken


class CppRemoveWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    cas: int
    # retries
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppRemoveWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppReplaceRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    flags: int
    expiry: int
    cas: int
    durability_level: int
    # retries
    preserve_expiry: bool
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppReplaceResponse(TypedDict, total=False):
    # ctx
    cas: int
    token: CppMutationToken


class CppReplaceWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    flags: int
    expiry: int
    cas: int
    # retries
    preserve_expiry: bool
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppReplaceWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


class CppTouchRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    expiry: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppTouchResponse(TypedDict, total=False):
    # ctx
    cas: int


class CppUnlockRequest(TypedDict, total=False):
    id: CppDocumentId
    # partition
    # opaque
    cas: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppUnlockResponse(TypedDict, total=False):
    # ctx
    cas: int


class CppUpsertRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    flags: int
    expiry: int
    durability_level: int
    # retries
    preserve_expiry: bool
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppUpsertResponse(TypedDict, total=False):
    # ctx
    cas: int
    token: CppMutationToken


class CppUpsertWithLegacyDurabilityRequest(TypedDict, total=False):
    id: CppDocumentId
    value: bytes
    # partition
    # opaque
    flags: int
    expiry: int
    # retries
    preserve_expiry: bool
    persist_to: int
    replicate_to: int
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppUpsertWithLegacyDurabilityResponse(TypedDict, total=False):
    pass


# ==========================================================================================
# Key-Value Multi Operations
# ==========================================================================================

class CppAppendMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    cas: int
    durability_level: int
    # retries
    timeout: Optional[int]


class CppAppendMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppAppendMultiOptions
    per_key_args: Dict[str, CppAppendMultiOptions]


class CppAppendWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    cas: int
    # retries
    persist_to: int
    replicate_to: int
    timeout: Optional[int]


class CppAppendWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppAppendWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppAppendWithLegacyDurabilityMultiOptions]


class CppDecrementMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    delta: int
    durability_level: int
    # retries
    initial_value: Optional[int]
    timeout: Optional[int]


class CppDecrementMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppDecrementMultiOptions
    per_key_args: Dict[str, CppDecrementMultiOptions]


class CppDecrementWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    delta: int
    # retries
    persist_to: int
    replicate_to: int
    initial_value: Optional[int]
    timeout: Optional[int]


class CppDecrementWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppDecrementWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppDecrementWithLegacyDurabilityMultiOptions]


class CppExistsMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    # retries
    timeout: Optional[int]


class CppExistsMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppExistsMultiOptions
    per_key_args: Dict[str, CppExistsMultiOptions]


class CppGetMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    # retries
    timeout: Optional[int]


class CppGetMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppGetMultiOptions
    per_key_args: Dict[str, CppGetMultiOptions]


class CppGetAllReplicasMultiOptions(TypedDict, total=False):
    read_preference: int
    timeout: Optional[int]


class CppGetAllReplicasMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppGetAllReplicasMultiOptions
    per_key_args: Dict[str, CppGetAllReplicasMultiOptions]


class CppGetAndLockMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    lock_time: int
    # retries
    timeout: Optional[int]


class CppGetAndLockMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppGetAndLockMultiOptions
    per_key_args: Dict[str, CppGetAndLockMultiOptions]


class CppGetAndTouchMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    # retries
    timeout: Optional[int]


class CppGetAndTouchMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppGetAndTouchMultiOptions
    per_key_args: Dict[str, CppGetAndTouchMultiOptions]


class CppGetAnyReplicaMultiOptions(TypedDict, total=False):
    read_preference: int
    timeout: Optional[int]


class CppGetAnyReplicaMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppGetAnyReplicaMultiOptions
    per_key_args: Dict[str, CppGetAnyReplicaMultiOptions]


class CppIncrementMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    delta: int
    durability_level: int
    # retries
    initial_value: Optional[int]
    timeout: Optional[int]


class CppIncrementMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppIncrementMultiOptions
    per_key_args: Dict[str, CppIncrementMultiOptions]


class CppIncrementWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    delta: int
    # retries
    persist_to: int
    replicate_to: int
    initial_value: Optional[int]
    timeout: Optional[int]


class CppIncrementWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppIncrementWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppIncrementWithLegacyDurabilityMultiOptions]


class CppInsertMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    durability_level: int
    # retries
    timeout: Optional[int]


class CppInsertMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppInsertMultiOptions
    per_key_args: Dict[str, CppInsertMultiOptions]


class CppInsertWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    # retries
    persist_to: int
    replicate_to: int
    timeout: Optional[int]


class CppInsertWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppInsertWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppInsertWithLegacyDurabilityMultiOptions]


class CppPrependMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    cas: int
    durability_level: int
    # retries
    timeout: Optional[int]


class CppPrependMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppPrependMultiOptions
    per_key_args: Dict[str, CppPrependMultiOptions]


class CppPrependWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    cas: int
    # retries
    persist_to: int
    replicate_to: int
    timeout: Optional[int]


class CppPrependWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppPrependWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppPrependWithLegacyDurabilityMultiOptions]


class CppRemoveMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    cas: int
    durability_level: int
    # retries
    timeout: Optional[int]


class CppRemoveMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppRemoveMultiOptions
    per_key_args: Dict[str, CppRemoveMultiOptions]


class CppRemoveWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    cas: int
    # retries
    persist_to: int
    replicate_to: int
    timeout: Optional[int]


class CppRemoveWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppRemoveWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppRemoveWithLegacyDurabilityMultiOptions]


class CppReplaceMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    cas: int
    durability_level: int
    # retries
    preserve_expiry: bool
    timeout: Optional[int]


class CppReplaceMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppReplaceMultiOptions
    per_key_args: Dict[str, CppReplaceMultiOptions]


class CppReplaceWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    cas: int
    # retries
    preserve_expiry: bool
    persist_to: int
    replicate_to: int
    timeout: Optional[int]


class CppReplaceWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppReplaceWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppReplaceWithLegacyDurabilityMultiOptions]


class CppTouchMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    # retries
    timeout: Optional[int]


class CppTouchMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppTouchMultiOptions
    per_key_args: Dict[str, CppTouchMultiOptions]


class CppUnlockMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    cas: int
    # retries
    timeout: Optional[int]


class CppUnlockMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: List[str]
    op_args: CppUnlockMultiOptions
    per_key_args: Dict[str, CppUnlockMultiOptions]


class CppUpsertMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    durability_level: int
    # retries
    preserve_expiry: bool
    timeout: Optional[int]


class CppUpsertMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppUpsertMultiOptions
    per_key_args: Dict[str, CppUpsertMultiOptions]


class CppUpsertWithLegacyDurabilityMultiOptions(TypedDict, total=False):
    # partition
    # opaque
    expiry: int
    # retries
    preserve_expiry: bool
    persist_to: int
    replicate_to: int
    timeout: Optional[int]


class CppUpsertWithLegacyDurabilityMultiRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    doc_list: Tuple[str, Tuple[bytes, int]]
    op_args: CppUpsertWithLegacyDurabilityMultiOptions
    per_key_args: Dict[str, CppUpsertWithLegacyDurabilityMultiOptions]


# ==========================================================================================
# Streaming Operations
# ==========================================================================================

class CppAnalyticsRequest(TypedDict, total=False):
    statement: str
    readonly: bool
    priority: bool
    raw: Dict[str, bytes]
    positional_parameters: List[bytes]
    named_parameters: Dict[str, bytes]
    # row_callback
    # body_str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scan_consistency: Optional[str]
    scope_name: Optional[str]
    scope_qualifier: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppDocumentViewRequest(TypedDict, total=False):
    bucket_name: str
    document_name: str
    view_name: str
    ns: str
    keys: List[str]
    debug: bool
    raw: Dict[str, str]
    query_string: List[str]
    # row_callback
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    consistency: Optional[str]
    end_key: Optional[str]
    end_key_doc_id: Optional[str]
    errback: Optional[Callable[..., None]]
    full_set: Optional[bool]
    group: Optional[bool]
    group_level: Optional[int]
    inclusive_end: Optional[bool]
    key: Optional[str]
    limit: Optional[int]
    on_error: Optional[str]
    order: Optional[str]
    parent_span: Optional[Any]
    reduce: Optional[bool]
    skip: Optional[int]
    start_key: Optional[str]
    start_key_doc_id: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppQueryRequest(TypedDict, total=False):
    statement: str
    adhoc: bool
    metrics: bool
    readonly: bool
    flex_index: bool
    preserve_expiry: bool
    mutation_state: List[CppMutationToken]
    raw: Dict[str, bytes]
    positional_parameters: List[bytes]
    named_parameters: Dict[str, bytes]
    # row_callback
    # send_to_node
    # ctx
    # extract_encoded_plan
    # body_str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    max_parallelism: Optional[int]
    parent_span: Optional[Any]
    pipeline_batch: Optional[int]
    pipeline_cap: Optional[int]
    profile: Optional[str]
    query_context: Optional[str]
    scan_cap: Optional[int]
    scan_consistency: Optional[str]
    scan_wait: Optional[int]
    timeout: Optional[int]
    use_replica: Optional[bool]
    wrapper_span_name: Optional[str]


class CppSearchRequest(TypedDict, total=False):
    index_name: str
    query: bytes
    disable_scoring: bool
    include_locations: bool
    highlight_fields: List[str]
    fields: List[str]
    collections: List[str]
    mutation_state: List[CppMutationToken]
    sort_specs: List[str]
    facets: Dict[str, str]
    raw: Dict[str, bytes]
    # row_callback
    # body_str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    explain: Optional[bool]
    highlight_style: Optional[str]
    limit: Optional[int]
    log_request: Optional[bool]
    log_response: Optional[bool]
    parent_span: Optional[Any]
    scan_consistency: Optional[str]
    scope_name: Optional[str]
    show_request: Optional[bool]
    skip: Optional[int]
    timeout: Optional[int]
    vector_query_combination: Optional[str]
    vector_search: Optional[bytes]
    wrapper_span_name: Optional[str]


# ==========================================================================================
# Analytics Management Operations
# ==========================================================================================

class CppAnalyticsDatasetCreateRequest(TypedDict, total=False):
    dataverse_name: str
    dataset_name: str
    bucket_name: str
    ignore_if_exists: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    condition: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsDatasetCreateResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsDatasetDropRequest(TypedDict, total=False):
    dataverse_name: str
    dataset_name: str
    ignore_if_does_not_exist: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsDatasetDropResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsDatasetGetAllRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsDatasetGetAllResponse(TypedDict, total=False):
    # ctx
    status: str
    datasets: List[CppAnalyticsDataset]
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsDataverseCreateRequest(TypedDict, total=False):
    dataverse_name: str
    ignore_if_exists: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsDataverseCreateResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsDataverseDropRequest(TypedDict, total=False):
    dataverse_name: str
    ignore_if_does_not_exist: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsDataverseDropResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsGetPendingMutationsRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsGetPendingMutationsResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsManagementProblem]
    stats: Dict[str, int]


class CppAnalyticsIndexCreateRequest(TypedDict, total=False):
    dataverse_name: str
    dataset_name: str
    index_name: str
    fields: Dict[str, str]
    ignore_if_exists: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsIndexCreateResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsIndexDropRequest(TypedDict, total=False):
    dataverse_name: str
    dataset_name: str
    index_name: str
    ignore_if_does_not_exist: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsIndexDropResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsIndexGetAllRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsIndexGetAllResponse(TypedDict, total=False):
    # ctx
    status: str
    indexes: List[CppAnalyticsIndex]
    errors: List[CppAnalyticsManagementProblem]


class CppAnalyticsLinkConnectRequest(TypedDict, total=False):
    dataverse_name: str
    link_name: str
    force: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkConnectResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsLinkConnectProblem]


class CppAnalyticsLinkCreateAzureBlobExternalLinkRequest(TypedDict, total=False):
    link: CppAnalyticsAzureBlobExternalLink
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkCreateResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsLinkCreateProblem]


class CppAnalyticsLinkCreateCouchbaseRemoteLinkRequest(TypedDict, total=False):
    link: CppAnalyticsCouchbaseRemoteLink
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkCreateS3ExternalLinkRequest(TypedDict, total=False):
    link: CppAnalyticsS3ExternalLink
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkDisconnectRequest(TypedDict, total=False):
    dataverse_name: str
    link_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkDisconnectResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsLinkDisconnectProblem]


class CppAnalyticsLinkDropRequest(TypedDict, total=False):
    link_name: str
    dataverse_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkDropResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsLinkDropProblem]


class CppAnalyticsLinkGetAllRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    dataverse_name: Optional[str]
    errback: Optional[Callable[..., None]]
    link_name: Optional[str]
    link_type: Optional[str]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkGetAllResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsLinkGetAllProblem]
    couchbase: List[CppAnalyticsCouchbaseRemoteLink]
    s3: List[CppAnalyticsS3ExternalLink]
    azure_blob: List[CppAnalyticsAzureBlobExternalLink]


class CppAnalyticsLinkReplaceAzureBlobExternalLinkRequest(TypedDict, total=False):
    link: CppAnalyticsAzureBlobExternalLink
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkReplaceResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppAnalyticsLinkReplaceProblem]


class CppAnalyticsLinkReplaceCouchbaseRemoteLinkRequest(TypedDict, total=False):
    link: CppAnalyticsCouchbaseRemoteLink
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppAnalyticsLinkReplaceS3ExternalLinkRequest(TypedDict, total=False):
    link: CppAnalyticsS3ExternalLink
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


# ==========================================================================================
# Bucket Management Operations
# ==========================================================================================

class CppBucketCreateRequest(TypedDict, total=False):
    bucket: CppBucketSettings
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppBucketCreateResponse(TypedDict, total=False):
    # ctx
    error_message: str


class CppBucketDescribeRequest(TypedDict, total=False):
    name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppBucketDescribeResponse(TypedDict, total=False):
    # ctx
    info: CppBucketInfo


class CppBucketDropRequest(TypedDict, total=False):
    name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppBucketDropResponse(TypedDict, total=False):
    pass


class CppBucketFlushRequest(TypedDict, total=False):
    name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppBucketFlushResponse(TypedDict, total=False):
    pass


class CppBucketGetRequest(TypedDict, total=False):
    name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppBucketGetResponse(TypedDict, total=False):
    # ctx
    bucket: CppBucketSettings


class CppBucketGetAllRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppBucketGetAllResponse(TypedDict, total=False):
    # ctx
    buckets: List[CppBucketSettings]


class CppBucketUpdateRequest(TypedDict, total=False):
    bucket: CppBucketSettings
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppBucketUpdateResponse(TypedDict, total=False):
    # ctx
    bucket: CppBucketSettings
    error_message: str


# ==========================================================================================
# Cluster Management Operations
# ==========================================================================================

class CppClusterDescribeRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppClusterDescribeResponse(TypedDict, total=False):
    # ctx
    info: CppClusterInfo


class CppClusterDeveloperPreviewEnableRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppClusterDeveloperPreviewEnableResponse(TypedDict, total=False):
    pass


# ==========================================================================================
# Collection Management Operations
# ==========================================================================================

class CppCollectionCreateRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    history: Optional[bool]
    max_expiry: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppCollectionCreateResponse(TypedDict, total=False):
    # ctx
    uid: int


class CppCollectionDropRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppCollectionDropResponse(TypedDict, total=False):
    # ctx
    uid: int


class CppCollectionUpdateRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    history: Optional[bool]
    max_expiry: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppCollectionUpdateResponse(TypedDict, total=False):
    # ctx
    uid: int


class CppCollectionsManifestGetRequest(TypedDict, total=False):
    id: CppDocumentId
    partition: int
    opaque: int
    # retries
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppCollectionsManifestGetResponse(TypedDict, total=False):
    # ctx
    manifest: CppCollectionsManifest


class CppScopeCreateRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppScopeCreateResponse(TypedDict, total=False):
    # ctx
    uid: int


class CppScopeDropRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppScopeDropResponse(TypedDict, total=False):
    # ctx
    uid: int


class CppScopeGetAllRequest(TypedDict, total=False):
    bucket_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppScopeGetAllResponse(TypedDict, total=False):
    # ctx
    manifest: CppCollectionsManifest


# ==========================================================================================
# EventingFunction Management Operations
# ==========================================================================================

class CppEventingDeployFunctionRequest(TypedDict, total=False):
    name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingDeployFunctionResponse(TypedDict, total=False):
    # ctx
    error: Optional[CppEventingProblem]


class CppEventingDropFunctionRequest(TypedDict, total=False):
    name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingDropFunctionResponse(TypedDict, total=False):
    # ctx
    error: Optional[CppEventingProblem]


class CppEventingGetAllFunctionsRequest(TypedDict, total=False):
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingGetAllFunctionsResponse(TypedDict, total=False):
    # ctx
    functions: List[CppEventingFunction]
    error: Optional[CppEventingProblem]


class CppEventingGetFunctionRequest(TypedDict, total=False):
    name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingGetFunctionResponse(TypedDict, total=False):
    # ctx
    function: CppEventingFunction
    error: Optional[CppEventingProblem]


class CppEventingGetStatusRequest(TypedDict, total=False):
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingGetStatusResponse(TypedDict, total=False):
    # ctx
    status: CppEventingStatus
    error: Optional[CppEventingProblem]


class CppEventingPauseFunctionRequest(TypedDict, total=False):
    name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingPauseFunctionResponse(TypedDict, total=False):
    # ctx
    error: Optional[CppEventingProblem]


class CppEventingResumeFunctionRequest(TypedDict, total=False):
    name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingResumeFunctionResponse(TypedDict, total=False):
    # ctx
    error: Optional[CppEventingProblem]


class CppEventingUndeployFunctionRequest(TypedDict, total=False):
    name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingUndeployFunctionResponse(TypedDict, total=False):
    # ctx
    error: Optional[CppEventingProblem]


class CppEventingUpsertFunctionRequest(TypedDict, total=False):
    function: CppEventingFunction
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppEventingUpsertFunctionResponse(TypedDict, total=False):
    # ctx
    error: Optional[CppEventingProblem]


# ==========================================================================================
# QueryIndex Management Operations
# ==========================================================================================

class CppQueryIndexBuildRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    query_ctx: CppQueryContext
    index_names: List[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppQueryIndexBuildResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppQueryIndexBuildProblem]


class CppQueryIndexBuildDeferredRequest(TypedDict, total=False):
    bucket_name: str
    query_ctx: CppQueryContext
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    collection_name: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppQueryIndexBuildDeferredResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppQueryIndexBuildDeferredProblem]


class CppQueryIndexCreateRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    index_name: str
    keys: List[str]
    query_ctx: CppQueryContext
    is_primary: bool
    ignore_if_exists: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    condition: Optional[str]
    deferred: Optional[bool]
    errback: Optional[Callable[..., None]]
    num_replicas: Optional[int]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppQueryIndexCreateResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppQueryIndexCreateProblem]


class CppQueryIndexDropRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    index_name: str
    query_ctx: CppQueryContext
    is_primary: bool
    ignore_if_does_not_exist: bool
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppQueryIndexDropResponse(TypedDict, total=False):
    # ctx
    status: str
    errors: List[CppQueryIndexDropProblem]


class CppQueryIndexGetAllRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    query_ctx: CppQueryContext
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppQueryIndexGetAllResponse(TypedDict, total=False):
    # ctx
    status: str
    indexes: List[CppQueryIndex]


class CppQueryIndexGetAllDeferredRequest(TypedDict, total=False):
    bucket_name: str
    scope_name: str
    collection_name: str
    query_ctx: CppQueryContext
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppQueryIndexGetAllDeferredResponse(TypedDict, total=False):
    # ctx
    status: str
    index_names: List[str]


# ==========================================================================================
# SearchIndex Management Operations
# ==========================================================================================

class CppSearchGetStatsRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchGetStatsResponse(TypedDict, total=False):
    # ctx
    stats: str


class CppSearchIndexAnalyzeDocumentRequest(TypedDict, total=False):
    index_name: str
    encoded_document: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexAnalyzeDocumentResponse(TypedDict, total=False):
    # ctx
    status: str
    error: str
    analysis: str


class CppSearchIndexControlIngestRequest(TypedDict, total=False):
    index_name: str
    pause: bool
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexControlIngestResponse(TypedDict, total=False):
    # ctx
    status: str
    error: str


class CppSearchIndexControlPlanFreezeRequest(TypedDict, total=False):
    index_name: str
    freeze: bool
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexControlPlanFreezeResponse(TypedDict, total=False):
    # ctx
    status: str
    error: str


class CppSearchIndexControlQueryRequest(TypedDict, total=False):
    index_name: str
    allow: bool
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexControlQueryResponse(TypedDict, total=False):
    # ctx
    status: str
    error: str


class CppSearchIndexDropRequest(TypedDict, total=False):
    index_name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexDropResponse(TypedDict, total=False):
    # ctx
    status: str
    error: str


class CppSearchIndexGetRequest(TypedDict, total=False):
    index_name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexGetResponse(TypedDict, total=False):
    # ctx
    status: str
    index: CppSearchIndex
    error: str


class CppSearchIndexGetAllRequest(TypedDict, total=False):
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexGetAllResponse(TypedDict, total=False):
    # ctx
    status: str
    impl_version: str
    indexes: List[CppSearchIndex]


class CppSearchIndexGetDocumentsCountRequest(TypedDict, total=False):
    index_name: str
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexGetDocumentsCountResponse(TypedDict, total=False):
    # ctx
    status: str
    count: int
    error: str


class CppSearchIndexGetStatsRequest(TypedDict, total=False):
    index_name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexGetStatsResponse(TypedDict, total=False):
    # ctx
    status: str
    error: str
    stats: str


class CppSearchIndexUpsertRequest(TypedDict, total=False):
    index: CppSearchIndex
    bucket_name: Optional[str]
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    scope_name: Optional[str]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppSearchIndexUpsertResponse(TypedDict, total=False):
    # ctx
    status: str
    name: str
    uuid: str
    error: str


# ==========================================================================================
# User Management Operations
# ==========================================================================================

class CppChangePasswordRequest(TypedDict, total=False):
    newPassword: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppChangePasswordResponse(TypedDict, total=False):
    pass


class CppGroupDropRequest(TypedDict, total=False):
    name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGroupDropResponse(TypedDict, total=False):
    pass


class CppGroupGetRequest(TypedDict, total=False):
    name: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGroupGetResponse(TypedDict, total=False):
    # ctx
    group: CppRbacGroup


class CppGroupGetAllRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGroupGetAllResponse(TypedDict, total=False):
    # ctx
    groups: List[CppRbacGroup]


class CppGroupUpsertRequest(TypedDict, total=False):
    group: CppRbacGroup
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppGroupUpsertResponse(TypedDict, total=False):
    # ctx
    errors: List[str]


class CppRoleGetAllRequest(TypedDict, total=False):
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppRoleGetAllResponse(TypedDict, total=False):
    # ctx
    roles: List[CppRbacRoleAndDescription]


class CppUserDropRequest(TypedDict, total=False):
    username: str
    domain: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppUserDropResponse(TypedDict, total=False):
    pass


class CppUserGetRequest(TypedDict, total=False):
    username: str
    domain: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppUserGetResponse(TypedDict, total=False):
    # ctx
    user: CppRbacUserAndMetadata


class CppUserGetAllRequest(TypedDict, total=False):
    domain: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppUserGetAllResponse(TypedDict, total=False):
    # ctx
    users: List[CppRbacUserAndMetadata]


class CppUserUpsertRequest(TypedDict, total=False):
    domain: str
    user: CppRbacUser
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppUserUpsertResponse(TypedDict, total=False):
    # ctx
    errors: List[str]


# ==========================================================================================
# ViewIndex Management Operations
# ==========================================================================================

class CppViewIndexDropRequest(TypedDict, total=False):
    bucket_name: str
    document_name: str
    ns: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppViewIndexDropResponse(TypedDict, total=False):
    pass


class CppViewIndexGetRequest(TypedDict, total=False):
    bucket_name: str
    document_name: str
    ns: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppViewIndexGetResponse(TypedDict, total=False):
    # ctx
    document: CppViewDesignDocument


class CppViewIndexGetAllRequest(TypedDict, total=False):
    bucket_name: str
    ns: str
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppViewIndexGetAllResponse(TypedDict, total=False):
    # ctx
    design_documents: List[CppViewDesignDocument]


class CppViewIndexUpsertRequest(TypedDict, total=False):
    bucket_name: str
    document: CppViewDesignDocument
    callback: Optional[Callable[..., None]]
    client_context_id: Optional[str]
    errback: Optional[Callable[..., None]]
    parent_span: Optional[Any]
    timeout: Optional[int]
    wrapper_span_name: Optional[str]


class CppViewIndexUpsertResponse(TypedDict, total=False):
    pass

# ====================================================================================================
# AUTOGENERATED SECTION END - DO NOT EDIT MANUALLY
# Generated-On: 2026-03-21 17:13:31
# Content-Hash: 2ffc6fc7fb28c7b7f37bfb0824d31e85
# ====================================================================================================


__all__ = [
    name for name, obj in vars().items()
    if isinstance(obj, type)
    and obj.__module__ == __name__
    and not name.startswith("_")
]
