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

import sys
from typing import (Any,
                    Callable,
                    Dict,
                    Generic,
                    List,
                    Optional,
                    Tuple,
                    TypeVar,
                    Union)

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

import couchbase.logic.pycbc_core.binding_cpp_types as CppTypes
from couchbase.logic.pycbc_core.pycbc_core_types import (CreateNewAttemptContextRequest,
                                                         CreateTransactionContextRequest,
                                                         CreateTransactionsRequest,
                                                         DestroyTransactionsRequest,
                                                         HdrPercentileReport,
                                                         ParsedTransactionsQueryOptions,
                                                         TransactionCommitRequest,
                                                         TransactionContextCapsuleType,
                                                         TransactionGetMultiOpRequest,
                                                         TransactionOpRequest,
                                                         TransactionQueryOpRequest,
                                                         TransactionRollbackRequest,
                                                         TransactionsCapsuleType)

T = TypeVar('T')

# Module-level constants
FMT_BYTES: int
FMT_JSON: int
FMT_UTF8: int
FMT_PICKLE: int
FMT_COMMON_MASK: int
FMT_LEGACY_MASK: int
CXXCBC_METADATA: str


def shutdown_logger() -> None:
    """Shutdown the pycbc_core logger."""
    ...

class pycbc_result(Generic[T]):

    raw_result: T
    core_span: Optional[Any]
    start_time: Optional[int]
    end_time: Optional[int]

    def __init__(self) -> None: ...


class pycbc_streamed_result(Generic[T]):

    core_span: Optional[Any]
    start_time: Optional[int]
    end_time: Optional[int]

    def __iter__(self) -> pycbc_streamed_result[T]: ...

    def __next__(self) -> Union[T, pycbc_exception, None]: ...


class pycbc_scan_iterator(Generic[T]):

    def __iter__(self) -> pycbc_scan_iterator[T]: ...

    def __next__(self) -> Union[T, pycbc_exception]: ...


class pycbc_exception:

    core_span: Optional[Any]
    start_time: Optional[int]
    end_time: Optional[int]

    def err(self) -> int: ...

    def err_category(self) -> str: ...

    def strerror(self) -> str: ...

    def error_context(self) -> Dict[str, Any]: ...


class pycbc_logger:
    ...


# ==========================================================================================
# Transactions Core Types
# ==========================================================================================

class transaction_config:
    def __init__(self,
                 *,
                 durability_level: Optional[int] = None,
                 cleanup_window: Optional[int] = None,
                 timeout: Optional[int] = None,
                 cleanup_lost_attempts: Optional[bool] = None,
                 cleanup_client_attempts: Optional[bool] = None,
                 metadata_bucket: Optional[str] = None,
                 metadata_collection: Optional[str] = None,
                 metadata_scope: Optional[str] = None,
                 scan_consistency: Optional[str] = None,
                 ) -> None:
        ...

    def to_dict(self) -> Dict[str, Any]: ...

class transaction_options:
    def __init__(self,
                 *,
                 durability_level: Optional[int] = None,
                 timeout: Optional[int] = None,
                 scan_consistency: Optional[str] = None,
                 metadata_collection: Optional[Dict[str, str]] = None,
                 ) -> None:
        ...

    def to_dict(self) -> Dict[str, Any]: ...

    def __str__(self) -> str: ...

class transaction_query_options:
    def __init__(self, query_args: ParsedTransactionsQueryOptions) -> None:
        ...

    def to_dict(self) -> Dict[str, Any]: ...

class transaction_get_result:
    def __init__(self) -> None: ...

    def __str__(self) -> str: ...

    def get(self, field_name: str, default: Any = ...) -> Any: ...

class transaction_get_multi_result:

    @property
    def content(self): List[Tuple[Any, int]]

    def __init__(self) -> None: ...


# ==========================================================================================
# Transactions Operations
# ==========================================================================================

def create_new_attempt_context(**kwargs: Unpack[CreateNewAttemptContextRequest]) -> None:
    ...

def create_transactions(**kwargs: Unpack[CreateTransactionsRequest]) -> TransactionsCapsuleType:
    ...

def create_transaction_context(**kwargs: Unpack[CreateTransactionContextRequest]) -> TransactionContextCapsuleType:
    ...

def destroy_transactions(**kwargs: Unpack[DestroyTransactionsRequest]) -> None:
    ...

def transaction_commit(**kwargs: Unpack[TransactionCommitRequest]) -> None:
    ...

def transaction_get_multi_op(**kwargs: Unpack[TransactionGetMultiOpRequest]) -> Any:
    ...

def transaction_op(**kwargs: Unpack[TransactionOpRequest]) -> Any:
    ...

def transaction_query_op(**kwargs: Unpack[TransactionQueryOpRequest]) -> Any:
    ...

def transaction_rollback(**kwargs: Unpack[TransactionRollbackRequest]) -> None:
    ...

# ==========================================================================================
# HDR Histogram type
# ==========================================================================================

class pycbc_hdr_histogram:
    """HDR (High Dynamic Range) Histogram for recording and analyzing value distributions."""

    def __init__(self,
                 lowest_discernible_value: int,
                 highest_trackable_value: int,
                 significant_figures: int) -> None:
        """Initialize HDR histogram.

        Args:
            lowest_discernible_value: Smallest distinguishable value (>= 1)
            highest_trackable_value: Largest trackable value
            significant_figures: Precision level (1-5)

        Raises:
            ValueError: If parameters are invalid
            MemoryError: If allocation fails
        """
        ...

    def close(self) -> None:
        """Close and free the histogram."""
        ...

    def record_value(self, value: int) -> None:
        """Record a value atomically.

        Args:
            value: The value to record
        """
        ...

    def value_at_percentile(self, percentile: float) -> int:
        """Get value at given percentile.

        Args:
            percentile: Percentile (0.0-100.0)

        Returns:
            Value at the percentile
        """
        ...

    def get_percentiles_and_reset(self, percentiles: List[float]) -> HdrPercentileReport:
        """Get multiple percentiles and reset histogram.

        Args:
            percentiles: List of percentiles to query

        Returns:
            Dict with 'total_count' and 'percentiles' list
        """
        ...

    def reset(self) -> None:
        """Reset histogram to zero."""
        ...

# ==========================================================================================
# pycbc KV Request
# ==========================================================================================

class pycbc_kv_request:
    opcode: int
    bucket: str
    scope: str
    collection: str
    key: str
    access_deleted: Optional[bool]
    cas: Optional[int]
    create_as_deleted: Optional[bool]
    delta: Optional[int]
    durability_level: Optional[int]
    effective_projections: Optional[List[str]]
    expiry: Optional[int]
    flags: Optional[int]
    initial_value: Optional[int]
    lock_time: Optional[int]
    persist_to: Optional[int]
    preserve_array_indexes: Optional[bool]
    preserve_expiry: Optional[bool]
    projections: Optional[List[str]]
    read_preference: Optional[int]
    replicate_to: Optional[int]
    revive_document: Optional[bool]
    specs: Optional[List[CppTypes.CppSubdocCommand]]
    store_semantics: Optional[int]
    timeout: Optional[int]
    value: Optional[bytes]
    with_expiry: Optional[bool]
    parent_span: Optional[Any]
    wrapper_span_name: Optional[str]
    callback: Optional[Callable]
    errback: Optional[Callable]
    with_metrics: Optional[bool]

    def __init__(self, opcode: int) -> None: ...

# ==========================================================================================
# pycbc Connection
# ==========================================================================================

class pycbc_connection:

    @property
    def connected(self) -> bool:
        ...

    def __init__(self, num_io_threads: int = 1) -> None:
        ...

    # ==========================================================================================
    # Connection Operations
    # ==========================================================================================

    def pycbc_connect(self, **kwargs: Unpack[CppTypes.CppConnectRequest]) -> None:
        ...

    def pycbc_close(self, **kwargs: Unpack[CppTypes.CppCloseRequest]) -> None:
        ...

    def pycbc_open_bucket(self, **kwargs: Unpack[CppTypes.CppOpenBucketRequest]) -> None:
        ...

    def pycbc_close_bucket(self, **kwargs: Unpack[CppTypes.CppCloseBucketRequest]) -> None:
        ...

    def pycbc_update_credentials(self, **kwargs: Unpack[CppTypes.CppUpdateCredentialsRequest]) -> None:
        ...

    @staticmethod
    def pycbc_get_default_timeouts() -> CppTypes.CppTimeoutDefaults:
        ...

    # ==========================================================================================
    # Diagnostic Operations
    # ==========================================================================================

    def pycbc_diagnostics(self, **kwargs: Unpack[CppTypes.CppDiagnosticsRequest]) -> pycbc_result[CppTypes.CppDiagnosticsResult]:
        ...

    def pycbc_ping(self, **kwargs: Unpack[CppTypes.CppPingRequest]) -> pycbc_result[CppTypes.CppDiagnosticsResult]:
        ...

    def pycbc_get_connection_info(self) -> Dict[str, Any]:
        ...

    # ==========================================================================================
    # Key-Value Range Scan Operations
    # ==========================================================================================

    def pycbc_kv_range_scan(self, **kwargs: Unpack[CppTypes.CppRangeScanRequest]) -> pycbc_scan_iterator[Any]:
        ...

    # ==========================================================================================
    # Observability Operations
    # ==========================================================================================

    def pycbc_get_cluster_labels(self) -> Dict[str, str]:
        ...

# ====================================================================================================
# AUTOGENERATED SECTION START - DO NOT EDIT MANUALLY
# Generated-On: 2026-04-25 14:10:17
# Content-Hash: dd9ed2155f49c923d294bbac55effba3
# ====================================================================================================
    # ==========================================================================================
    # Key-Value Operations
    # ==========================================================================================

    def pycbc_append(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppAppendResponse]:
        ...

    def pycbc_append_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppAppendResponse]:
        ...

    def pycbc_decrement(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppDecrementResponse]:
        ...

    def pycbc_decrement_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppDecrementResponse]:
        ...

    def pycbc_exists(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppExistsResponse]:
        ...

    def pycbc_get(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppGetResponse]:
        ...

    def pycbc_get_all_replicas(self, request: pycbc_kv_request) -> pycbc_streamed_result[Any]:
        ...

    def pycbc_get_and_lock(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppGetAndLockResponse]:
        ...

    def pycbc_get_and_touch(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppGetAndTouchResponse]:
        ...

    def pycbc_get_any_replica(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppGetAnyReplicaResponse]:
        ...

    def pycbc_get_projected(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppGetProjectedResponse]:
        ...

    def pycbc_increment(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppIncrementResponse]:
        ...

    def pycbc_increment_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppIncrementResponse]:
        ...

    def pycbc_insert(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppInsertResponse]:
        ...

    def pycbc_insert_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppInsertResponse]:
        ...

    def pycbc_lookup_in(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppLookupInResponse]:
        ...

    def pycbc_lookup_in_all_replicas(self, request: pycbc_kv_request) -> pycbc_streamed_result[Any]:
        ...

    def pycbc_lookup_in_any_replica(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppLookupInAnyReplicaResponse]:
        ...

    def pycbc_mutate_in(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppMutateInResponse]:
        ...

    def pycbc_mutate_in_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppMutateInResponse]:
        ...

    def pycbc_prepend(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppPrependResponse]:
        ...

    def pycbc_prepend_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppPrependResponse]:
        ...

    def pycbc_remove(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppRemoveResponse]:
        ...

    def pycbc_remove_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppRemoveResponse]:
        ...

    def pycbc_replace(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppReplaceResponse]:
        ...

    def pycbc_replace_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppReplaceResponse]:
        ...

    def pycbc_touch(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppTouchResponse]:
        ...

    def pycbc_unlock(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppUnlockResponse]:
        ...

    def pycbc_upsert(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppUpsertResponse]:
        ...

    def pycbc_upsert_with_legacy_durability(self, request: pycbc_kv_request) -> pycbc_result[CppTypes.CppUpsertResponse]:
        ...

    # ==========================================================================================
    # Key-Value Multi Operations
    # ==========================================================================================

    def pycbc_append_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppAppendResponse]]:
        ...

    def pycbc_append_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppAppendResponse]]:
        ...

    def pycbc_decrement_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppDecrementResponse]]:
        ...

    def pycbc_decrement_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppDecrementResponse]]:
        ...

    def pycbc_exists_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppExistsResponse]]:
        ...

    def pycbc_get_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppGetResponse]]:
        ...

    def pycbc_get_all_replicas_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, pycbc_streamed_result[Any]]]:
        ...

    def pycbc_get_and_lock_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppGetAndLockResponse]]:
        ...

    def pycbc_get_and_touch_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppGetAndTouchResponse]]:
        ...

    def pycbc_get_any_replica_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppGetAnyReplicaResponse]]:
        ...

    def pycbc_increment_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppIncrementResponse]]:
        ...

    def pycbc_increment_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppIncrementResponse]]:
        ...

    def pycbc_insert_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppInsertResponse]]:
        ...

    def pycbc_insert_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppInsertResponse]]:
        ...

    def pycbc_prepend_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppPrependResponse]]:
        ...

    def pycbc_prepend_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppPrependResponse]]:
        ...

    def pycbc_remove_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppRemoveResponse]]:
        ...

    def pycbc_remove_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppRemoveResponse]]:
        ...

    def pycbc_replace_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppReplaceResponse]]:
        ...

    def pycbc_replace_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppReplaceResponse]]:
        ...

    def pycbc_touch_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppTouchResponse]]:
        ...

    def pycbc_unlock_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppUnlockResponse]]:
        ...

    def pycbc_upsert_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppUpsertResponse]]:
        ...

    def pycbc_upsert_with_legacy_durability_multi(
        self, request_list: List[pycbc_kv_request]
    ) -> pycbc_result[Dict[str, CppTypes.CppUpsertResponse]]:
        ...

    # ==========================================================================================
    # Streaming Operations
    # ==========================================================================================

    def pycbc_analytics_query(self, **kwargs: Unpack[CppTypes.CppAnalyticsRequest]) -> pycbc_streamed_result[Any]:
        ...

    def pycbc_query(self, **kwargs: Unpack[CppTypes.CppQueryRequest]) -> pycbc_streamed_result[Any]:
        ...

    def pycbc_search_query(self, **kwargs: Unpack[CppTypes.CppSearchRequest]) -> pycbc_streamed_result[Any]:
        ...

    def pycbc_view_query(self, **kwargs: Unpack[CppTypes.CppDocumentViewRequest]) -> pycbc_streamed_result[Any]:
        ...

    # ==========================================================================================
    # Analytics Management Operations
    # ==========================================================================================

    def pycbc_analytics_dataset_create(self, **kwargs: Unpack[CppTypes.CppAnalyticsDatasetCreateRequest]) -> pycbc_result[CppTypes.CppAnalyticsDatasetCreateResponse]:
        ...

    def pycbc_analytics_dataset_drop(self, **kwargs: Unpack[CppTypes.CppAnalyticsDatasetDropRequest]) -> pycbc_result[CppTypes.CppAnalyticsDatasetDropResponse]:
        ...

    def pycbc_analytics_dataset_get_all(self, **kwargs: Unpack[CppTypes.CppAnalyticsDatasetGetAllRequest]) -> pycbc_result[CppTypes.CppAnalyticsDatasetGetAllResponse]:
        ...

    def pycbc_analytics_dataverse_create(self, **kwargs: Unpack[CppTypes.CppAnalyticsDataverseCreateRequest]) -> pycbc_result[CppTypes.CppAnalyticsDataverseCreateResponse]:
        ...

    def pycbc_analytics_dataverse_drop(self, **kwargs: Unpack[CppTypes.CppAnalyticsDataverseDropRequest]) -> pycbc_result[CppTypes.CppAnalyticsDataverseDropResponse]:
        ...

    def pycbc_analytics_get_pending_mutations(self, **kwargs: Unpack[CppTypes.CppAnalyticsGetPendingMutationsRequest]) -> pycbc_result[CppTypes.CppAnalyticsGetPendingMutationsResponse]:
        ...

    def pycbc_analytics_index_create(self, **kwargs: Unpack[CppTypes.CppAnalyticsIndexCreateRequest]) -> pycbc_result[CppTypes.CppAnalyticsIndexCreateResponse]:
        ...

    def pycbc_analytics_index_drop(self, **kwargs: Unpack[CppTypes.CppAnalyticsIndexDropRequest]) -> pycbc_result[CppTypes.CppAnalyticsIndexDropResponse]:
        ...

    def pycbc_analytics_index_get_all(self, **kwargs: Unpack[CppTypes.CppAnalyticsIndexGetAllRequest]) -> pycbc_result[CppTypes.CppAnalyticsIndexGetAllResponse]:
        ...

    def pycbc_analytics_link_connect(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkConnectRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkConnectResponse]:
        ...

    def pycbc_analytics_link_create_azure_blob_external_link(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkCreateAzureBlobExternalLinkRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkCreateResponse]:
        ...

    def pycbc_analytics_link_create_couchbase_remote_link(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkCreateCouchbaseRemoteLinkRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkCreateResponse]:
        ...

    def pycbc_analytics_link_create_s3_external_link(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkCreateS3ExternalLinkRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkCreateResponse]:
        ...

    def pycbc_analytics_link_disconnect(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkDisconnectRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkDisconnectResponse]:
        ...

    def pycbc_analytics_link_drop(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkDropRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkDropResponse]:
        ...

    def pycbc_analytics_link_get_all(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkGetAllRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkGetAllResponse]:
        ...

    def pycbc_analytics_link_replace_azure_blob_external_link(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkReplaceAzureBlobExternalLinkRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkReplaceResponse]:
        ...

    def pycbc_analytics_link_replace_couchbase_remote_link(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkReplaceCouchbaseRemoteLinkRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkReplaceResponse]:
        ...

    def pycbc_analytics_link_replace_s3_external_link(self, **kwargs: Unpack[CppTypes.CppAnalyticsLinkReplaceS3ExternalLinkRequest]) -> pycbc_result[CppTypes.CppAnalyticsLinkReplaceResponse]:
        ...


    # ==========================================================================================
    # Bucket Management Operations
    # ==========================================================================================

    def pycbc_bucket_create(self, **kwargs: Unpack[CppTypes.CppBucketCreateRequest]) -> pycbc_result[CppTypes.CppBucketCreateResponse]:
        ...

    def pycbc_bucket_describe(self, **kwargs: Unpack[CppTypes.CppBucketDescribeRequest]) -> pycbc_result[CppTypes.CppBucketDescribeResponse]:
        ...

    def pycbc_bucket_drop(self, **kwargs: Unpack[CppTypes.CppBucketDropRequest]) -> pycbc_result[CppTypes.CppBucketDropResponse]:
        ...

    def pycbc_bucket_flush(self, **kwargs: Unpack[CppTypes.CppBucketFlushRequest]) -> pycbc_result[CppTypes.CppBucketFlushResponse]:
        ...

    def pycbc_bucket_get(self, **kwargs: Unpack[CppTypes.CppBucketGetRequest]) -> pycbc_result[CppTypes.CppBucketGetResponse]:
        ...

    def pycbc_bucket_get_all(self, **kwargs: Unpack[CppTypes.CppBucketGetAllRequest]) -> pycbc_result[CppTypes.CppBucketGetAllResponse]:
        ...

    def pycbc_bucket_update(self, **kwargs: Unpack[CppTypes.CppBucketUpdateRequest]) -> pycbc_result[CppTypes.CppBucketUpdateResponse]:
        ...


    # ==========================================================================================
    # Cluster Management Operations
    # ==========================================================================================

    def pycbc_cluster_describe(self, **kwargs: Unpack[CppTypes.CppClusterDescribeRequest]) -> pycbc_result[CppTypes.CppClusterDescribeResponse]:
        ...

    def pycbc_cluster_developer_preview_enable(self, **kwargs: Unpack[CppTypes.CppClusterDeveloperPreviewEnableRequest]) -> pycbc_result[CppTypes.CppClusterDeveloperPreviewEnableResponse]:
        ...


    # ==========================================================================================
    # Collection Management Operations
    # ==========================================================================================

    def pycbc_collection_create(self, **kwargs: Unpack[CppTypes.CppCollectionCreateRequest]) -> pycbc_result[CppTypes.CppCollectionCreateResponse]:
        ...

    def pycbc_collection_drop(self, **kwargs: Unpack[CppTypes.CppCollectionDropRequest]) -> pycbc_result[CppTypes.CppCollectionDropResponse]:
        ...

    def pycbc_collection_update(self, **kwargs: Unpack[CppTypes.CppCollectionUpdateRequest]) -> pycbc_result[CppTypes.CppCollectionUpdateResponse]:
        ...

    def pycbc_collections_manifest_get(self, **kwargs: Unpack[CppTypes.CppCollectionsManifestGetRequest]) -> pycbc_result[CppTypes.CppCollectionsManifestGetResponse]:
        ...

    def pycbc_scope_create(self, **kwargs: Unpack[CppTypes.CppScopeCreateRequest]) -> pycbc_result[CppTypes.CppScopeCreateResponse]:
        ...

    def pycbc_scope_drop(self, **kwargs: Unpack[CppTypes.CppScopeDropRequest]) -> pycbc_result[CppTypes.CppScopeDropResponse]:
        ...

    def pycbc_scope_get_all(self, **kwargs: Unpack[CppTypes.CppScopeGetAllRequest]) -> pycbc_result[CppTypes.CppScopeGetAllResponse]:
        ...


    # ==========================================================================================
    # EventingFunction Management Operations
    # ==========================================================================================

    def pycbc_eventing_deploy_function(self, **kwargs: Unpack[CppTypes.CppEventingDeployFunctionRequest]) -> pycbc_result[CppTypes.CppEventingDeployFunctionResponse]:
        ...

    def pycbc_eventing_drop_function(self, **kwargs: Unpack[CppTypes.CppEventingDropFunctionRequest]) -> pycbc_result[CppTypes.CppEventingDropFunctionResponse]:
        ...

    def pycbc_eventing_get_all_functions(self, **kwargs: Unpack[CppTypes.CppEventingGetAllFunctionsRequest]) -> pycbc_result[CppTypes.CppEventingGetAllFunctionsResponse]:
        ...

    def pycbc_eventing_get_function(self, **kwargs: Unpack[CppTypes.CppEventingGetFunctionRequest]) -> pycbc_result[CppTypes.CppEventingGetFunctionResponse]:
        ...

    def pycbc_eventing_get_status(self, **kwargs: Unpack[CppTypes.CppEventingGetStatusRequest]) -> pycbc_result[CppTypes.CppEventingGetStatusResponse]:
        ...

    def pycbc_eventing_pause_function(self, **kwargs: Unpack[CppTypes.CppEventingPauseFunctionRequest]) -> pycbc_result[CppTypes.CppEventingPauseFunctionResponse]:
        ...

    def pycbc_eventing_resume_function(self, **kwargs: Unpack[CppTypes.CppEventingResumeFunctionRequest]) -> pycbc_result[CppTypes.CppEventingResumeFunctionResponse]:
        ...

    def pycbc_eventing_undeploy_function(self, **kwargs: Unpack[CppTypes.CppEventingUndeployFunctionRequest]) -> pycbc_result[CppTypes.CppEventingUndeployFunctionResponse]:
        ...

    def pycbc_eventing_upsert_function(self, **kwargs: Unpack[CppTypes.CppEventingUpsertFunctionRequest]) -> pycbc_result[CppTypes.CppEventingUpsertFunctionResponse]:
        ...


    # ==========================================================================================
    # QueryIndex Management Operations
    # ==========================================================================================

    def pycbc_query_index_build(self, **kwargs: Unpack[CppTypes.CppQueryIndexBuildRequest]) -> pycbc_result[CppTypes.CppQueryIndexBuildResponse]:
        ...

    def pycbc_query_index_build_deferred(self, **kwargs: Unpack[CppTypes.CppQueryIndexBuildDeferredRequest]) -> pycbc_result[CppTypes.CppQueryIndexBuildDeferredResponse]:
        ...

    def pycbc_query_index_create(self, **kwargs: Unpack[CppTypes.CppQueryIndexCreateRequest]) -> pycbc_result[CppTypes.CppQueryIndexCreateResponse]:
        ...

    def pycbc_query_index_drop(self, **kwargs: Unpack[CppTypes.CppQueryIndexDropRequest]) -> pycbc_result[CppTypes.CppQueryIndexDropResponse]:
        ...

    def pycbc_query_index_get_all(self, **kwargs: Unpack[CppTypes.CppQueryIndexGetAllRequest]) -> pycbc_result[CppTypes.CppQueryIndexGetAllResponse]:
        ...

    def pycbc_query_index_get_all_deferred(self, **kwargs: Unpack[CppTypes.CppQueryIndexGetAllDeferredRequest]) -> pycbc_result[CppTypes.CppQueryIndexGetAllDeferredResponse]:
        ...


    # ==========================================================================================
    # SearchIndex Management Operations
    # ==========================================================================================

    def pycbc_search_get_stats(self, **kwargs: Unpack[CppTypes.CppSearchGetStatsRequest]) -> pycbc_result[CppTypes.CppSearchGetStatsResponse]:
        ...

    def pycbc_search_index_analyze_document(self, **kwargs: Unpack[CppTypes.CppSearchIndexAnalyzeDocumentRequest]) -> pycbc_result[CppTypes.CppSearchIndexAnalyzeDocumentResponse]:
        ...

    def pycbc_search_index_control_ingest(self, **kwargs: Unpack[CppTypes.CppSearchIndexControlIngestRequest]) -> pycbc_result[CppTypes.CppSearchIndexControlIngestResponse]:
        ...

    def pycbc_search_index_control_plan_freeze(self, **kwargs: Unpack[CppTypes.CppSearchIndexControlPlanFreezeRequest]) -> pycbc_result[CppTypes.CppSearchIndexControlPlanFreezeResponse]:
        ...

    def pycbc_search_index_control_query(self, **kwargs: Unpack[CppTypes.CppSearchIndexControlQueryRequest]) -> pycbc_result[CppTypes.CppSearchIndexControlQueryResponse]:
        ...

    def pycbc_search_index_drop(self, **kwargs: Unpack[CppTypes.CppSearchIndexDropRequest]) -> pycbc_result[CppTypes.CppSearchIndexDropResponse]:
        ...

    def pycbc_search_index_get(self, **kwargs: Unpack[CppTypes.CppSearchIndexGetRequest]) -> pycbc_result[CppTypes.CppSearchIndexGetResponse]:
        ...

    def pycbc_search_index_get_all(self, **kwargs: Unpack[CppTypes.CppSearchIndexGetAllRequest]) -> pycbc_result[CppTypes.CppSearchIndexGetAllResponse]:
        ...

    def pycbc_search_index_get_documents_count(self, **kwargs: Unpack[CppTypes.CppSearchIndexGetDocumentsCountRequest]) -> pycbc_result[CppTypes.CppSearchIndexGetDocumentsCountResponse]:
        ...

    def pycbc_search_index_get_stats(self, **kwargs: Unpack[CppTypes.CppSearchIndexGetStatsRequest]) -> pycbc_result[CppTypes.CppSearchIndexGetStatsResponse]:
        ...

    def pycbc_search_index_upsert(self, **kwargs: Unpack[CppTypes.CppSearchIndexUpsertRequest]) -> pycbc_result[CppTypes.CppSearchIndexUpsertResponse]:
        ...


    # ==========================================================================================
    # User Management Operations
    # ==========================================================================================

    def pycbc_change_password(self, **kwargs: Unpack[CppTypes.CppChangePasswordRequest]) -> pycbc_result[CppTypes.CppChangePasswordResponse]:
        ...

    def pycbc_group_drop(self, **kwargs: Unpack[CppTypes.CppGroupDropRequest]) -> pycbc_result[CppTypes.CppGroupDropResponse]:
        ...

    def pycbc_group_get(self, **kwargs: Unpack[CppTypes.CppGroupGetRequest]) -> pycbc_result[CppTypes.CppGroupGetResponse]:
        ...

    def pycbc_group_get_all(self, **kwargs: Unpack[CppTypes.CppGroupGetAllRequest]) -> pycbc_result[CppTypes.CppGroupGetAllResponse]:
        ...

    def pycbc_group_upsert(self, **kwargs: Unpack[CppTypes.CppGroupUpsertRequest]) -> pycbc_result[CppTypes.CppGroupUpsertResponse]:
        ...

    def pycbc_role_get_all(self, **kwargs: Unpack[CppTypes.CppRoleGetAllRequest]) -> pycbc_result[CppTypes.CppRoleGetAllResponse]:
        ...

    def pycbc_user_drop(self, **kwargs: Unpack[CppTypes.CppUserDropRequest]) -> pycbc_result[CppTypes.CppUserDropResponse]:
        ...

    def pycbc_user_get(self, **kwargs: Unpack[CppTypes.CppUserGetRequest]) -> pycbc_result[CppTypes.CppUserGetResponse]:
        ...

    def pycbc_user_get_all(self, **kwargs: Unpack[CppTypes.CppUserGetAllRequest]) -> pycbc_result[CppTypes.CppUserGetAllResponse]:
        ...

    def pycbc_user_upsert(self, **kwargs: Unpack[CppTypes.CppUserUpsertRequest]) -> pycbc_result[CppTypes.CppUserUpsertResponse]:
        ...


    # ==========================================================================================
    # ViewIndex Management Operations
    # ==========================================================================================

    def pycbc_view_index_drop(self, **kwargs: Unpack[CppTypes.CppViewIndexDropRequest]) -> pycbc_result[CppTypes.CppViewIndexDropResponse]:
        ...

    def pycbc_view_index_get(self, **kwargs: Unpack[CppTypes.CppViewIndexGetRequest]) -> pycbc_result[CppTypes.CppViewIndexGetResponse]:
        ...

    def pycbc_view_index_get_all(self, **kwargs: Unpack[CppTypes.CppViewIndexGetAllRequest]) -> pycbc_result[CppTypes.CppViewIndexGetAllResponse]:
        ...

    def pycbc_view_index_upsert(self, **kwargs: Unpack[CppTypes.CppViewIndexUpsertRequest]) -> pycbc_result[CppTypes.CppViewIndexUpsertResponse]:
        ...

# ====================================================================================================
# AUTOGENERATED SECTION END - DO NOT EDIT MANUALLY
# Generated-On: 2026-04-25 14:10:17
# Content-Hash: dd9ed2155f49c923d294bbac55effba3
# ====================================================================================================
