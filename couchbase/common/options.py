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

__all__ = [
    'AcceptableInts',
    'AnalyticsOptions',
    'AnalyticsOptionsKwargs',
    'AppendMultiOptions',
    'AppendMultiOptionsKwargs',
    'AppendOptions',
    'AppendOptionsKwargs',
    'ClusterMetricsOptions',
    'ClusterMetricsOptionsKwargs',
    'ClusterOptions',
    'ClusterOptionsKwargs',
    'ClusterOrphanReportingOptions',
    'ClusterOrphanReportingOptionsKwargs',
    'ClusterTimeoutOptions',
    'ClusterTimeoutOptionsKwargs',
    'ClusterTracingOptions',
    'ClusterTracingOptionsKwargs',
    'ConstrainedInt',
    'DecrementMultiOptions',
    'DecrementMultiOptionsKwargs',
    'DecrementOptions',
    'DecrementOptionsKwargs',
    'DeltaValue',
    'DiagnosticsOptions',
    'DiagnosticsOptionsKwargs',
    'ExistsMultiOptions',
    'ExistsMultiOptionsKwargs',
    'ExistsOptions',
    'ExistsOptionsKwargs',
    'GetAllReplicasMultiOptions',
    'GetAllReplicasMultiOptionsKwargs',
    'GetAllReplicasOptions',
    'GetAllReplicasOptionsKwargs',
    'GetAndLockMultiOptions',
    'GetAndLockMultiOptionsKwargs',
    'GetAndLockOptions',
    'GetAndLockOptionsKwargs',
    'GetAndTouchOptions',
    'GetAndTouchOptionsKwargs',
    'GetAnyReplicaMultiOptions',
    'GetAnyReplicaMultiOptionsKwargs',
    'GetAnyReplicaOptions',
    'GetAnyReplicaOptionsKwargs',
    'GetMultiOptions',
    'GetMultiOptionsKwargs',
    'GetOptions',
    'GetOptionsKwargs',
    'IncrementMultiOptions',
    'IncrementMultiOptionsKwargs',
    'IncrementOptions',
    'IncrementOptionsKwargs',
    'InsertMultiOptions',
    'InsertMultiOptionsKwargs',
    'InsertOptions',
    'InsertOptionsKwargs',
    'LockMultiOptions',
    'LookupInAllReplicasOptions',
    'LookupInAllReplicasOptionsKwargs',
    'LookupInAnyReplicaOptions',
    'LookupInAnyReplicaOptionsKwargs',
    'LookupInOptions',
    'LookupInOptionsKwargs',
    'MutateInOptions',
    'MutateInOptionsKwargs',
    'MutationMultiOptions',
    'MutationOptions',
    'NoValueMultiOptions',
    'PingOptions',
    'PingOptionsKwargs',
    'PrependMultiOptions',
    'PrependMultiOptionsKwargs',
    'PrependOptions',
    'PrependOptionsKwargs',
    'QueryOptions',
    'QueryOptionsKwargs',
    'RemoveMultiOptions',
    'RemoveMultiOptionsKwargs',
    'RemoveOptions',
    'RemoveOptionsKwargs',
    'ReplaceMultiOptions',
    'ReplaceMultiOptionsKwargs',
    'ReplaceOptions',
    'ReplaceOptionsKwargs',
    'ScanOptions',
    'ScanOptionsKwargs',
    'SearchOptions',
    'SearchOptionsKwargs',
    'SignedInt64',
    'TouchMultiOptions',
    'TouchMultiOptionsKwargs',
    'TouchOptions',
    'TouchOptionsKwargs',
    'TransactionConfig',
    'TransactionConfigKwargs',
    'TransactionGetMultiOptions',
    'TransactionGetMultiOptionsKwargs',
    'TransactionGetMultiReplicasFromPreferredServerGroupOptions',
    'TransactionGetMultiReplicasFromPreferredServerGroupOptionsKwargs',
    'TransactionGetOptions',
    'TransactionGetOptionsKwargs',
    'TransactionGetReplicaFromPreferredServerGroupOptions',
    'TransactionGetReplicaFromPreferredServerGroupOptionsKwargs',
    'TransactionInsertOptions',
    'TransactionInsertOptionsKwargs',
    'TransactionOptions',
    'TransactionOptionsKwargs',
    'TransactionQueryOptions',
    'TransactionQueryOptionsKwargs',
    'TransactionReplaceOptions',
    'TransactionReplaceOptionsKwargs',
    'UnlockMultiOptions',
    'UnlockMultiOptionsKwargs',
    'UnlockOptions',
    'UnlockOptionsKwargs',
    'UnsignedInt32',
    'UnsignedInt64',
    'UpsertMultiOptions',
    'UpsertMultiOptionsKwargs',
    'UpsertOptions',
    'UpsertOptionsKwargs',
    'VectorSearchOptions',
    'VectorSearchOptionsKwargs',
    'ViewOptions',
    'ViewOptionsKwargs',
    'WaitUntilReadyOptions',
    'WaitUntilReadyOptionsKwargs',
]

from ctypes import c_int64, c_uint64
from typing import (TYPE_CHECKING,
                    Any,
                    SupportsInt,
                    TypedDict)

from couchbase.common._option_args import cluster_keys, transaction_args
from couchbase.common._supportability import Supportability
from couchbase.common.config_profiles import CONFIG_PROFILES, KnownConfigProfiles
from couchbase.common.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from datetime import timedelta

    from typing_extensions import Unpack

    from couchbase.common._metrics import CouchbaseMeter
    from couchbase.common._observability import TracerProtocol
    from couchbase.common._utils import JSONType, ScopeKeyspace
    from couchbase.common.analytics import AnalyticsScanConsistency
    from couchbase.common.auth import Authenticator
    from couchbase.common.diagnostics import ClusterState, ServiceType
    from couchbase.common.durability import DurabilityType, ServerDurability
    from couchbase.common.management.views import DesignDocumentNamespace
    from couchbase.common.mutation_state import MutationState
    from couchbase.common.n1ql import QueryProfile, QueryScanConsistency
    from couchbase.common.observability.tracing import RequestSpan
    from couchbase.common.option_enums import (Compression,
                                               IpProtocol,
                                               LockMode,
                                               OptionValidationMode,
                                               TLSVerifyMode)
    from couchbase.common.replica_reads import ReadPreference
    from couchbase.common.search import (Facet,
                                         HighlightStyle,
                                         SearchScanConsistency,
                                         Sort)
    from couchbase.common.search_scoring import SearchScoring
    from couchbase.common.serializer import Serializer
    from couchbase.common.subdocument import StoreSemantics
    from couchbase.common.transcoder import Transcoder
    from couchbase.common.vector_search import VectorQueryCombination
    from couchbase.common.views import (ViewErrorMode,
                                        ViewOrdering,
                                        ViewScanConsistency)
    from couchbase.transactions import (TransactionGetMultiMode,
                                        TransactionGetMultiReplicasFromPreferredServerGroupMode,
                                        TransactionKeyspace)


# ==========================================================================================
# Analytics
# ==========================================================================================


class AnalyticsOptionsKwargs(TypedDict, total=False):
    client_context_id: str | None
    raw: dict[str, JSONType] | None
    metrics: bool | None
    named_parameters: dict[str, JSONType] | None
    positional_parameters: Iterable[JSONType] | None
    read_only: bool | None
    timeout: timedelta | None
    scan_consistency: AnalyticsScanConsistency | None
    priority: bool | None
    query_context: str | None
    serializer: Serializer | None
    parent_span: RequestSpan | None


class AnalyticsOptions(dict):
    """Available options for an analytics query.

    .. warning::
        Importing options from ``couchbase.analytics`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        client_context_id (str, optional): The returned client context id for this query. Defaults to a generated UUID.
        raw (dict[str, JSONType], optional): Specifies any additional parameters which should be passed to the query
            engine when executing the query.
        metrics (bool, optional): Specifies whether metrics should be captured as part of the execution of the query.
            Defaults to True.
        named_parameters (dict[str, JSONType], optional): Named values to be used for the placeholders within the query.
        positional_parameters (Iterable[JSONType], optional): Positional values to be used for the placeholders within
            the query.
        read_only (bool, optional): Specifies that this query should be executed in read-only mode, disabling the
            ability for the query to make any changes to the data. Defaults to False.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.query_timeout`.
        scan_consistency (:class:`~couchbase.analytics.AnalyticsScanConsistency`, optional): Specifies the consistency
            requirements when executing the query. Defaults to `NotBounded`.
        priority (bool, optional): Indicates whether this analytics query should be executed with a specific priority
            level. Defaults to False.
        query_context (str, optional): Specifies the context within which this query should be executed. This can be
            scoped to a scope or a collection within the dataset.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[AnalyticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Binary
# ==========================================================================================


class AppendOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    durability: DurabilityType | None
    cas: int | None
    parent_span: RequestSpan | None


class AppendOptions(dict):
    """Available options for a binary append operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[AppendOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class DecrementOptionsKwargs(TypedDict, total=False):
    delta: DeltaValue | None
    initial: SignedInt64 | None
    timeout: timedelta | None
    expiry: timedelta | None
    durability: DurabilityType | None
    parent_span: RequestSpan | None


class DecrementOptions(dict):
    """Available options for a key-value decrement operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        delta (:class:`~couchbase.options.DeltaValue`, optional): The amount to decrement the key. Defaults to 1.
        initial (:class:`~couchbase.options.SignedInt64`, optional): The initial value to use for the document if it
            does not already exist. Setting it to a negative value means that the document will not be created if it
            does not exist. Defaults to 0.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DecrementOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)


class IncrementOptionsKwargs(TypedDict, total=False):
    delta: DeltaValue | None
    initial: SignedInt64 | None
    timeout: timedelta | None
    expiry: timedelta | None
    durability: DurabilityType | None
    parent_span: RequestSpan | None


class IncrementOptions(dict):
    """Available options for a binary increment operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        delta (:class:`~couchbase.options.DeltaValue`, optional): The amount to increment the key. Defaults to 1.
        initial (:class:`~couchbase.options.SignedInt64`, optional): The initial value to use for the document if it
            does not already exist. Setting it to a negative value means that the document will not be created if it
            does not exist. Defaults to 0.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[IncrementOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)


class PrependOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    durability: DurabilityType | None
    cas: int | None
    parent_span: RequestSpan | None


class PrependOptions(dict):
    """Available options for a binary prepend operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PrependOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Binary Multi
# ==========================================================================================


class AppendMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, AppendOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    durability: DurabilityType | None
    cas: int | None
    parent_span: RequestSpan | None


class AppendMultiOptions(dict):
    """Available options for a binary multi-append operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all append operations handled with this multi-append operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.AppendOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.AppendOptions`], optional): Specify
            :class:`~couchbase.options.AppendOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[AppendMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "durability",
            "cas",
            "parent_span",
        ]


class DecrementMultiOptionsKwargs(TypedDict, total=False):
    delta: DeltaValue | None
    initial: SignedInt64 | None
    per_key_options: dict[str, DecrementOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    durability: DurabilityType | None
    parent_span: RequestSpan | None


class DecrementMultiOptions(dict):
    """Available options for a binary multi-decrement operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all decrement operations handled with this multi-decrement
    operation). Use *per_key_options* to set specific :class:`~couchbase.options.DecrementOptions` for specific keys.

    Args:
        delta (:class:`~couchbase.options.DeltaValue`, optional): The amount to decrement the key. Defaults to 1.
        initial (:class:`~couchbase.options.SignedInt64`, optional): The initial value to use for the document if it
            does not already exist. Setting it to a negative value means that the document will not be created if it
            does not exist. Defaults to 0.
        per_key_options (dict[str, :class:`~couchbase.options.DecrementOptions`], optional): Specify
            :class:`~couchbase.options.DecrementOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DecrementMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "delta",
            "initial",
            "per_key_options",
            "return_exceptions",
            "timeout",
            "durability",
            "parent_span",
        ]


class IncrementMultiOptionsKwargs(TypedDict, total=False):
    delta: DeltaValue | None
    initial: SignedInt64 | None
    per_key_options: dict[str, IncrementOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    durability: DurabilityType | None
    parent_span: RequestSpan | None


class IncrementMultiOptions(dict):
    """Available options for a binary multi-increment operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all increment operations handled with this multi-increment
    operation). Use *per_key_options* to set specific :class:`~couchbase.options.IncrementOptions` for specific keys.

    Args:
        delta (:class:`~couchbase.options.DeltaValue`, optional): The amount to increment the key. Defaults to 1.
        initial (:class:`~couchbase.options.SignedInt64`, optional): The initial value to use for the document if it
            does not already exist. Setting it to a negative value means that the document will not be created if it
            does not exist. Defaults to 0.
        per_key_options (dict[str, :class:`~couchbase.options.IncrementOptions`], optional): Specify
            :class:`~couchbase.options.IncrementOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[IncrementMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "delta",
            "initial",
            "per_key_options",
            "return_exceptions",
            "timeout",
            "durability",
            "parent_span",
        ]


class PrependMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, PrependOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    durability: DurabilityType | None
    cas: int | None
    parent_span: RequestSpan | None


class PrependMultiOptions(dict):
    """Available options for a binary multi-prepend operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all prepend operations handled with this multi-prepend operation).
    Use *per_key_options* to set specific :class:`~couchbase.options.PrependOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.PrependOptions`], optional): Specify
            :class:`~couchbase.options.PrependOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PrependMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "durability",
            "cas",
            "parent_span",
        ]


# ==========================================================================================
# Cluster
# ==========================================================================================


class ClusterMetricsOptionsKwargs(TypedDict, total=False):
    enable_metrics: bool | None
    emit_interval: timedelta | None


class ClusterMetricsOptions(dict):
    """Available metrics options to set when creating a cluster.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        enable_metrics (bool, optional): Set to False to disable default logging meter (enables no-op meter). Defaults
            to True (enabled).
        emit_interval (timedelta, optional): Interval to flush the metrics operations queue.
    """

    def __init__(self, **kwargs: Unpack[ClusterMetricsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        stored = {
            "emit_interval": "metrics_emit_interval",
            "enable_metrics": "metrics_enable_metrics",
        }
        provided = {stored.get(k, k): v for k, v in provided.items()}
        super().__init__(**provided)

    @staticmethod
    def get_allowed_option_keys(use_transform_keys: bool | None = False) -> list[str]:
        if use_transform_keys is True:
            Supportability.method_deprecated("get_allowed_option_keys(use_transform_keys=True)",
                                             "get_allowed_option_keys()",
                                             "What a key is sent as is the protocol's, so it answers with the "
                                             "same keys.")
        return cluster_keys("ClusterMetricsOptions")


class ClusterOptionsKwargs(TypedDict, total=False):
    option_validation: OptionValidationMode | str | None
    trust_store_path: str | None
    timeout_options: ClusterTimeoutOptions | None
    orphan_reporting_options: ClusterOrphanReportingOptions | None
    metrics_options: ClusterMetricsOptions | None
    tracing_options: ClusterTracingOptions | None
    enable_tls: bool | None
    enable_mutation_tokens: bool | None
    enable_tcp_keep_alive: bool | None
    ip_protocol: IpProtocol | str | None
    enable_dns_srv: bool | None
    show_queries: bool | None
    enable_unordered_execution: bool | None
    enable_clustermap_notification: bool | None
    enable_compression: bool | None
    enable_tracing: bool | None
    enable_metrics: bool | None
    enable_orphan_reporting: bool | None
    network: str | None
    tls_verify: TLSVerifyMode | str | None
    serializer: Serializer | None
    transcoder: Transcoder | None
    tcp_keep_alive_interval: timedelta | None
    config_poll_interval: timedelta | None
    config_poll_floor: timedelta | None
    max_http_connections: int | None
    user_agent_extra: str | None
    logging_meter_emit_interval: timedelta | None
    transaction_config: TransactionConfig | None
    log_redaction: bool | None
    compression: Compression | None
    compression_min_size: int | None
    compression_min_ratio: float | None
    lockmode: LockMode | None
    tracer: TracerProtocol | None
    meter: CouchbaseMeter | None
    dns_nameserver: str | None
    dns_port: int | None
    disable_mozilla_ca_certificates: bool | None
    dump_configuration: bool | None
    preferred_server_group: str | None
    enable_app_telemetry: bool | None
    app_telemetry_endpoint: str | None
    app_telemetry_backoff: timedelta | None
    app_telemetry_ping_interval: timedelta | None
    app_telemetry_ping_timeout: timedelta | None
    allow_enterprise_analytics: bool | None
    enable_lazy_connections: bool | None


class ClusterOptions(dict):
    """Available options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Cluster options enable the configuration of various global cluster settings. Some options can be set globally for
    the cluster, but overridden for specific operations (i.e. ClusterTimeoutOptions)

    Args:
        authenticator (:class:`~couchbase.auth.Authenticator`): An authenticator instance
        option_validation (:class:`~couchbase.options.OptionValidationMode` | str, optional): Specifies what the SDK
            does with an option key a class does not carry: ``ignore``, ``warn`` or ``strict``. Overrides
            ``PYCBC_OPTION_VALIDATION`` for this cluster. A value that is none of the three raises
            :class:`~couchbase.exceptions.InvalidArgumentException`. Defaults to warn, or PYCBC_OPTION_VALIDATION where
            that is set.
        trust_store_path (str, optional): Path to the trust store. May also be given on the authenticator, which wins.
        timeout_options (:class:`~couchbase.options.ClusterTimeoutOptions`, optional): Timeout options for various SDK
            operations. See :class:`~couchbase.options.ClusterTimeoutOptions` for details.
        orphan_reporting_options (:class:`~couchbase.options.ClusterOrphanReportingOptions`, optional): Options for SDK
            orphaned response reporting. See :class:`~couchbase.options.ClusterOrphanReportingOptions` for details.
        metrics_options (:class:`~couchbase.options.ClusterMetricsOptions`, optional):
        tracing_options (:class:`~couchbase.options.ClusterTracingOptions`, optional): Tracing options for SDK tracing
            bevavior. See :class:`~couchbase.options.ClusterTracingOptions` for details. These are ignored if an
            external tracer is specified.
        enable_tls (bool, optional): Set to True to enable tls. Defaults to False (disabled).
        enable_mutation_tokens (bool, optional): Set to False to disable mutation tokens in mutation results. Defaults
            to True (enabled).
        enable_tcp_keep_alive (bool, optional): Set to False to disable tcp keep alive. Defaults to True (enabled).
        ip_protocol (:class:`~couchbase.options.IpProtocol` | str, optional): Set IP protocol. Defaults to
            IpProtocol.Any.
        enable_dns_srv (bool, optional): Set to False to disable DNS SRV. Defaults to True (enabled).
        show_queries (bool, optional): Set to True to enabled showing queries. Defaults to False (disabled).
        enable_unordered_execution (bool, optional): Set to False to disable unordered query execution. Defaults to True
            (enabled).
        enable_clustermap_notification (bool, optional): Set to False to disable cluster map notification. Defaults to
            True (enabled).
        enable_compression (bool, optional): Set to False to disable compression. Defaults to True (enabled).
        enable_tracing (bool, optional): Set to False to disable default threshold log tracing (enables no-op tracer).
            Defaults to True (enabled).
        enable_metrics (bool, optional): Set to False to disable default logging meter (enables no-op meter). Defaults
            to True (enabled).
        enable_orphan_reporting (bool, optional): Set to False to disable orphaned response reporting. Defaults to True
            (enabled).
        network (str, optional): Set network resolution method. Can be set to 'default' (if the client is running on the
            same network as the server) or 'external' (if the client is running on a different network). Defaults to
            'auto'.
        tls_verify (:class:`~couchbase.options.TLSVerifyMode` | str, optional): Set tls verify mode. Defaults to
            TLSVerifyMode.PEER.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Global serializer to translate JSON to Python
            objects. Used for any operation that does not set its own. Defaults to
            :class:`~couchbase.serializer.DefaultJsonSerializer`.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Global transcoder to use for key-value
            operations. Used for any operation that does not set its own. Defaults to
            :class:`~couchbase.transcoder.JSONTranscoder`.
        tcp_keep_alive_interval (timedelta, optional): TCP keep-alive interval. Defaults to 60 seconds.
        config_poll_interval (timedelta, optional): Config polling floor interval. Defaults to 2.5 seconds.
        config_poll_floor (timedelta, optional): Config polling floor interval. Defaults to 50 milliseconds.
        max_http_connections (int, optional): Maximum number of HTTP connections. Defaults to 0 (unlimited).
        user_agent_extra (str, optional): Appended to the user agent the SDK sends, after the Python version.
        logging_meter_emit_interval (timedelta, optional): Interval to flush the metrics operations queue.

            .. deprecated:: 4.6.0
                Use :class:`~couchbase.options.ClusterMetricsOptions` instead.
        transaction_config (:class:`~couchbase.options.TransactionConfig`, optional): Global configuration for
            transactions. Defaults to a default :class:`~couchbase.options.TransactionConfig`.
        log_redaction (bool, optional): Set to True to enable log redaction.
        compression (:class:`~couchbase.options.Compression`, optional): Set compression mode.
        compression_min_size (int, optional): Set compression min size.
        compression_min_ratio (float, optional): Set compression min size.
        lockmode (:class:`~couchbase.options.LockMode`, optional): Set LockMode mode.

            .. deprecated:: 4.1.7
                It has no effect.
        tracer (:class:`~couchbase.common._observability.TracerProtocol`, optional): Set an external tracer. Defaults to
            None, enabling the `threshold_logging_tracer`. Note when this is set, all tracing_options (see
            :class:`~couchbase.options.ClusterTracingOptions`) and then `enable_tracing` option are ignored.
        meter (:class:`~couchbase.metrics.CouchbaseMeter`, optional): Set an external meter. Defaults to None, enabling
            the `logging_meter`.   Note when this is set, the `logging_meter_emit_interval` option is ignored.
        dns_nameserver (str, optional): **VOLATILE** This API is subject to change at any time. Set to configure custom
            DNS nameserver. Defaults to the system resolver.
        dns_port (int, optional): **VOLATILE** This API is subject to change at any time. Set to configure custom DNS
            port. Defaults to 53.
        disable_mozilla_ca_certificates (bool, optional): Set to True to disable loading Mozilla's list of CA
            certificates for TLS verification. Defaults to False (the bundled Mozilla roots are trusted).
        dump_configuration (bool, optional): Set to True to dump every new configuration when TRACE level logging.
            Defaults to False (disabled).
        preferred_server_group (str, optional): Specifies the preferred server group to be used for replica reads with
            'selected server group' read preference.
        enable_app_telemetry (bool, optional): Specifies if application telemetry feature should be enabled. Defaults to
            True (enabled).
        app_telemetry_endpoint (str, optional): Specifies an endpoint to override the application metrics endpoint
            discovered during configuration. Defaults to the endpoint discovered during configuration.
        app_telemetry_backoff (timedelta, optional): Specifies the time to wait before attempting a websocket
            reconnection. Defaults to 1 hour.
        app_telemetry_ping_interval (timedelta, optional): Specifies the time to wait between sending consecutive
            websocket PING commands to the server. Defaults to 30 seconds.
        app_telemetry_ping_timeout (timedelta, optional): Specifies the time allowed for the server to respond to
            websocket PING command. Defaults to 2 seconds.
        allow_enterprise_analytics (bool, optional): Set to True to allow analytics queries against an Enterprise
            Analytics cluster, which this SDK otherwise refuses with FeatureUnavailableException. Defaults to False
            (disabled).
        enable_lazy_connections (bool, optional): Set to True to enable the C++ core to lazily establish bucket
            connections. Defaults to False (disabled).
    """

    def __init__(self, authenticator: Authenticator, **kwargs: Unpack[ClusterOptionsKwargs]) -> None:
        provided: dict[str, Any] = dict(kwargs)
        if authenticator:
            provided["authenticator"] = authenticator
        for nested in (
            "timeout_options",
            "tracing_options",
            "metrics_options",
            "orphan_reporting_options",
        ):
            for key, value in (provided.pop(nested, None) or {}).items():
                if key not in provided:
                    provided[key] = value
        provided = {k: v for k, v in provided.items() if v is not None}
        super().__init__(**provided)

    @staticmethod
    def get_valid_options() -> dict[str, Any]:
        Supportability.method_deprecated("ClusterOptions.get_valid_options",
                                         message="What a key is sent as is the protocol's, so each key is "
                                                 "answered under its own name and unconverted.")
        return {key: (key, None) for key in cluster_keys("ClusterOptions")}

    def apply_profile(self, profile_name: KnownConfigProfiles | str) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name ([:class:`~couchbase.options.KnownConfigProfiles`, str]):  The name of the profile to apply
                toward ClusterOptions.
            authenticator (:class:`~couchbase.auth.PasswordAuthenticator` | :class:`~couchbase.auth.CertificateAuthenticator`):
                An authenticator instance.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        prof_name = profile_name.value if isinstance(profile_name, KnownConfigProfiles) else profile_name
        CONFIG_PROFILES.apply_profile(prof_name, self)

    @classmethod
    def create_options_with_profile(cls,
                                    authenticator: Authenticator,
                                    profile_name: KnownConfigProfiles | str
                                    ) -> ClusterOptions:
        """
        **VOLATILE** This API is subject to change at any time.

        Create a ClusterOptions instance and apply the provided ConfigProfile options.

        Args:
            authenticator (:class:`~couchbase.auth.PasswordAuthenticator` | :class:`~couchbase.auth.CertificateAuthenticator`):
                An authenticator instance.
            profile_name ([:class:`~couchbase.options.KnownConfigProfiles`, str]):  The name of the profile to apply
                toward ClusterOptions.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        opts = cls(authenticator)
        prof_name = profile_name.value if isinstance(profile_name, KnownConfigProfiles) else profile_name
        CONFIG_PROFILES.apply_profile(prof_name, opts)
        return opts


class ClusterOrphanReportingOptionsKwargs(TypedDict, total=False):
    enable_orphan_reporting: bool | None
    sample_size: int | None
    emit_interval: timedelta | None


class ClusterOrphanReportingOptions(dict):
    """Available orphan reporting options to set when creating a cluster.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        enable_orphan_reporting (bool, optional): Set to False to disable orphaned response reporting. Defaults to True
            (enabled).
        sample_size (int, optional): Size of orphan operations queue.
        emit_interval (timedelta, optional): Interval to flush the orphaned operations queue.
    """

    def __init__(self, **kwargs: Unpack[ClusterOrphanReportingOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        stored = {
            "emit_interval": "orphan_emit_interval",
            "enable_orphan_reporting": "orphan_enable_orphan_reporting",
            "sample_size": "orphan_sample_size",
        }
        provided = {stored.get(k, k): v for k, v in provided.items()}
        super().__init__(**provided)

    @staticmethod
    def get_allowed_option_keys(use_transform_keys: bool | None = False) -> list[str]:
        if use_transform_keys is True:
            Supportability.method_deprecated("get_allowed_option_keys(use_transform_keys=True)",
                                             "get_allowed_option_keys()",
                                             "What a key is sent as is the protocol's, so it answers with the "
                                             "same keys.")
        return cluster_keys("ClusterOrphanReportingOptions")


class ClusterTimeoutOptionsKwargs(TypedDict, total=False):
    bootstrap_timeout: timedelta | None
    resolve_timeout: timedelta | None
    connect_timeout: timedelta | None
    kv_timeout: timedelta | None
    kv_durable_timeout: timedelta | None
    views_timeout: timedelta | None
    query_timeout: timedelta | None
    analytics_timeout: timedelta | None
    search_timeout: timedelta | None
    management_timeout: timedelta | None
    dns_srv_timeout: timedelta | None
    idle_http_connection_timeout: timedelta | None
    config_idle_redial_timeout: timedelta | None
    config_total_timeout: timedelta | None


class ClusterTimeoutOptions(dict):
    """Available timeout options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    These will set the default timeouts for operations for the cluster. Some operations allow the timeout to be
    overridden on a per operation basis.

    Args:
        bootstrap_timeout (timedelta, optional): Overall bootstrap timeout. Defaults to 10 seconds.
        resolve_timeout (timedelta, optional): Time to resolve hostnames. Defaults to 2 seconds.
        connect_timeout (timedelta, optional): connect timeout. Defaults to 2 seconds.
        kv_timeout (timedelta, optional): KV operations timeout. Defaults to 2.5 seconds.
        kv_durable_timeout (timedelta, optional): KV durability operations timeout. Defaults to 10 seconds.
        views_timeout (timedelta, optional): views operations timeout. Defaults to 75 seconds.
        query_timeout (timedelta, optional): query operations timeout. Defaults to 75 seconds.
        analytics_timeout (timedelta, optional): analytics operations timeout. Defaults to 75 seconds.
        search_timeout (timedelta, optional): search operations timeout. Defaults to 75 seconds.
        management_timeout (timedelta, optional): management operations timeout. Defaults to 75 seconds.
        dns_srv_timeout (timedelta, optional): Time to make DNS-SRV query. Defaults to 500 milliseconds.
        idle_http_connection_timeout (timedelta, optional): Idle HTTP connection timeout. Defaults to 1 second.
        config_idle_redial_timeout (timedelta, optional): Idle redial timeout. Defaults to 5 minutes.
        config_total_timeout (timedelta, optional): Complete bootstrap timeout.

            .. deprecated:: 4.0.0
                It has no effect.
    """

    def __init__(self, **kwargs: Unpack[ClusterTimeoutOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @staticmethod
    def get_allowed_option_keys(use_transform_keys: bool | None = False) -> list[str]:
        if use_transform_keys is True:
            Supportability.method_deprecated("get_allowed_option_keys(use_transform_keys=True)",
                                             "get_allowed_option_keys()",
                                             "What a key is sent as is the protocol's, so it answers with the "
                                             "same keys.")
        return cluster_keys("ClusterTimeoutOptions")


class ClusterTracingOptionsKwargs(TypedDict, total=False):
    enable_tracing: bool | None
    tracing_threshold_kv: timedelta | None
    tracing_threshold_view: timedelta | None
    tracing_threshold_query: timedelta | None
    tracing_threshold_search: timedelta | None
    tracing_threshold_analytics: timedelta | None
    tracing_threshold_eventing: timedelta | None
    tracing_threshold_management: timedelta | None
    tracing_threshold_queue_size: int | None
    tracing_threshold_queue_flush_interval: timedelta | None
    tracing_orphaned_queue_size: int | None
    tracing_orphaned_queue_flush_interval: timedelta | None


class ClusterTracingOptions(dict):
    """Available tracing options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        enable_tracing (bool, optional): Set to False to disable default threshold log tracing (enables no-op tracer).
            Defaults to True (enabled).
        tracing_threshold_kv (timedelta, optional): KV operations threshold. Defaults to 500 milliseconds.
        tracing_threshold_view (timedelta, optional): Views operations threshold. Defaults to 1 second.
        tracing_threshold_query (timedelta, optional): Query operations threshold. Defaults to 1 second.
        tracing_threshold_search (timedelta, optional): Search operations threshold.. Defaults to 1 second.
        tracing_threshold_analytics (timedelta, optional): Analytics operations threshold. Defaults to 1 second.
        tracing_threshold_eventing (timedelta, optional): Eventing operations threshold. Defaults to 1 second.
        tracing_threshold_management (timedelta, optional): Management operations threshold. Defaults to 1 second.
        tracing_threshold_queue_size (int, optional): Size of tracing operations queue. Defaults to 10.
        tracing_threshold_queue_flush_interval (timedelta, optional): Interval to flush tracing operations queue.
            Defaults to 10 seconds.
        tracing_orphaned_queue_size (int, optional): Size of the orphaned response queue.

            .. deprecated:: 4.6.0
                Use :class:`~couchbase.options.ClusterOrphanReportingOptions` instead.
        tracing_orphaned_queue_flush_interval (timedelta, optional): Interval to flush the orphaned response queue.

            .. deprecated:: 4.6.0
                Use :class:`~couchbase.options.ClusterOrphanReportingOptions` instead.
    """

    def __init__(self, **kwargs: Unpack[ClusterTracingOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        stored = {
            "enable_tracing": "tracing_enable_tracing",
        }
        provided = {stored.get(k, k): v for k, v in provided.items()}
        super().__init__(**provided)

    @staticmethod
    def get_allowed_option_keys(use_transform_keys: bool | None = False) -> list[str]:
        if use_transform_keys is True:
            Supportability.method_deprecated("get_allowed_option_keys(use_transform_keys=True)",
                                             "get_allowed_option_keys()",
                                             "What a key is sent as is the protocol's, so it answers with the "
                                             "same keys.")
        return cluster_keys("ClusterTracingOptions")


# ==========================================================================================
# Diagnostics
# ==========================================================================================


class DiagnosticsOptionsKwargs(TypedDict, total=False):
    report_id: str | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class DiagnosticsOptions(dict):
    """Available options for a diagnostics operation.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        report_id (str, optional): A unique identifier for the report generated by this operation.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[DiagnosticsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class PingOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    report_id: str | None
    service_types: Iterable[ServiceType] | Iterable[str] | None
    parent_span: RequestSpan | None


class PingOptions(dict):
    """Available options for a ping operation.

    .. warning::
        Importing options from ``couchbase.bucket`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        report_id (str, optional): A unique identifier for the report generated by this operation.
        service_types (Iterable[:class:`~couchbase.diagnostics.ServiceType`] | Iterable[str], optional): The services
            which should be pinged. Either every element is a :class:`~couchbase.diagnostics.ServiceType` or every
            element is its ``.value`` string; the two forms are equivalent and are not intended to be mixed within one
            iterable.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[PingOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class WaitUntilReadyOptionsKwargs(TypedDict, total=False):
    desired_state: ClusterState | None
    service_types: Iterable[ServiceType] | Iterable[str] | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class WaitUntilReadyOptions(dict):
    """Available options for a wait until ready operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        desired_state (:class:`~couchbase.diagnostics.ClusterState`, optional): The desired state to wait for in order
            to determine the cluster or bucket is ready. Defaults to `Online`.
        service_types (Iterable[:class:`~couchbase.diagnostics.ServiceType`] | Iterable[str], optional): The services
            which should be pinged. Either every element is a :class:`~couchbase.diagnostics.ServiceType` or every
            element is its ``.value`` string; the two forms are equivalent and are not intended to be mixed within one
            iterable.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[WaitUntilReadyOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Key-Value
# ==========================================================================================


class ExistsOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class ExistsOptions(dict):
    """Available options for a key-value exists operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ExistsOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAllReplicasOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    transcoder: Transcoder | None
    read_preference: ReadPreference | None
    parent_span: RequestSpan | None


class GetAllReplicasOptions(dict):
    """Available options for a key-value get-all-replicas operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        read_preference (:class:`~couchbase.replica_reads.ReadPreference`, optional): Specifies how the replica nodes
            will be selected. Defaults to no preference.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAllReplicasOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAndLockOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    transcoder: Transcoder | None
    parent_span: RequestSpan | None


class GetAndLockOptions(dict):
    """Available options for a key-value get and lock operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAndLockOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAndTouchOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    transcoder: Transcoder | None
    parent_span: RequestSpan | None


class GetAndTouchOptions(dict):
    """Available options for a key-value get and touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAndTouchOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetAnyReplicaOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    transcoder: Transcoder | None
    read_preference: ReadPreference | None
    parent_span: RequestSpan | None


class GetAnyReplicaOptions(dict):
    """Available options for a key-value get-any-replica operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        read_preference (:class:`~couchbase.replica_reads.ReadPreference`, optional): Specifies how the replica nodes
            will be selected. Defaults to no preference.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetAnyReplicaOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class GetOptionsKwargs(TypedDict, total=False):
    project: Iterable[str] | None
    with_expiry: bool | None
    timeout: timedelta | None
    transcoder: Transcoder | None
    parent_span: RequestSpan | None


class GetOptions(dict):
    """Available options for a key-value get operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        project (Iterable[str], optional): Specifies a list of fields within the document which should be fetched. This
            allows for easy retrieval of select fields without incurring the overhead of fetching the whole document.
        with_expiry (bool, optional): Indicates that the expiry of the document should be fetched alongside the data
            itself. Defaults to False.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[GetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def project(self) -> Iterable[str]:
        return self.get("project", [])

    @property
    def with_expiry(self) -> bool:
        return self.get("with_expiry", False)


class InsertOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    expiry: timedelta | None
    durability: DurabilityType | None
    transcoder: Transcoder | None
    parent_span: RequestSpan | None


class InsertOptions(dict):
    """Available options for a key-value insert operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[InsertOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)


class RemoveOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    cas: int | None
    durability: DurabilityType | None
    parent_span: RequestSpan | None


class RemoveOptions(dict):
    """Available options for a key-value remove operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[RemoveOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class ReplaceOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    expiry: timedelta | None
    cas: int | None
    preserve_expiry: bool | None
    durability: DurabilityType | None
    transcoder: Transcoder | None
    parent_span: RequestSpan | None


class ReplaceOptions(dict):
    """Available options for a key-value replace operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
            Defaults to False.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ReplaceOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)


class ScanOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    ids_only: bool | None
    consistent_with: MutationState | None
    batch_byte_limit: int | None
    batch_item_limit: int | None
    batch_time_limit: timedelta | None
    transcoder: Transcoder | None
    concurrency: int | None
    parent_span: RequestSpan | None


class ScanOptions(dict):
    """Available options for a key-value scan operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        ids_only (bool, optional): Specifies that scan should only return document ids. Defaults to False.
        consistent_with (:class:`~couchbase.mutation_state.MutationState`, optional): Specifies a
            :class:`~couchbase.mutation_state.MutationState` which the operation should be consistent with.
        batch_byte_limit (int, optional): The limit applied to the number of bytes returned from the server for each
            partition batch. Defaults to 15000.
        batch_item_limit (int, optional): The limit applied to the number of items returned from the server for each
            partition batch. Defaults to 50.
        batch_time_limit (timedelta, optional): Not applied: the server takes no time limit for a scan batch.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        concurrency (int, optional): The upper bound on the number of vbuckets that should be scanned in parallel.
            Defaults to 1.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): Not applied: a scan is not
            traced.
    """

    def __init__(self, **kwargs: Unpack[ScanOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class TouchOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class TouchOptions(dict):
    """Available options for a key-value touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[TouchOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UnlockOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UnlockOptions(dict):
    """Available options for a key-value unlock operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UnlockOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class UpsertOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    expiry: timedelta | None
    preserve_expiry: bool | None
    durability: DurabilityType | None
    transcoder: Transcoder | None
    parent_span: RequestSpan | None


class UpsertOptions(dict):
    """Available options for a key-value upsert operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
            Defaults to False.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UpsertOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)


# ==========================================================================================
# Key-Value Multi
# ==========================================================================================


class ExistsMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, ExistsOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class ExistsMultiOptions(dict):
    """Available options for a key-value multi-exists operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all exists operations handled with this multi-exists operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.ExistsOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.ExistsOptions`], optional): Specify
            :class:`~couchbase.options.ExistsOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[ExistsMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
        ]


class GetAllReplicasMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, GetAllReplicasOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    transcoder: Transcoder | None
    read_preference: ReadPreference | None


class GetAllReplicasMultiOptions(dict):
    """Available options for a key-value multi-get-all-replicas operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all get operations handled with this multi-get-all-replicas
    operation). Use *per_key_options* to set specific :class:`~couchbase.options.GetAllReplicasOptions` for specific
    keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.GetAllReplicasOptions`], optional): Specify
            :class:`~couchbase.options.GetAllReplicasOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        read_preference (:class:`~couchbase.replica_reads.ReadPreference`, optional): Specifies how the replica nodes
            will be selected. Defaults to no preference.
    """

    def __init__(self, **kwargs: Unpack[GetAllReplicasMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "transcoder",
            "read_preference",
        ]


class GetAndLockMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, GetAndLockOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    transcoder: Transcoder | None


class GetAndLockMultiOptions(dict):
    """Available options for a key-value multi-lock operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all lock operations handled with this multi-lock operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.GetAndLockOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.GetAndLockOptions`], optional): Specify
            :class:`~couchbase.options.GetAndLockOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[GetAndLockMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "transcoder",
            "lock_time",
        ]


class GetAnyReplicaMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, GetAnyReplicaOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    transcoder: Transcoder | None
    read_preference: ReadPreference | None


class GetAnyReplicaMultiOptions(dict):
    """Available options for a key-value multi-get-any-replica operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all get operations handled with this multi-get-any-replica
    operation). Use *per_key_options* to set specific :class:`~couchbase.options.GetAnyReplicaOptions` for specific
    keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.GetAnyReplicaOptions`], optional): Specify
            :class:`~couchbase.options.GetAnyReplicaOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
        read_preference (:class:`~couchbase.replica_reads.ReadPreference`, optional): Specifies how the replica nodes
            will be selected. Defaults to no preference.
    """

    def __init__(self, **kwargs: Unpack[GetAnyReplicaMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "transcoder",
            "read_preference",
        ]


class GetMultiOptionsKwargs(TypedDict, total=False):
    project: Iterable[str] | None
    with_expiry: bool | None
    per_key_options: dict[str, GetOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    transcoder: Transcoder | None


class GetMultiOptions(dict):
    """Available options for a key-value multi-get operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all get operations handled with this multi-get operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.GetOptions` for specific keys.

    Args:
        project (Iterable[str], optional): Not supported by a multi get, here or in *per_key_options*. Setting it raises
            :class:`~couchbase.exceptions.InvalidArgumentException`.
        with_expiry (bool, optional): Not supported by a multi get, here or in *per_key_options*.

            .. deprecated:: 4.7.0
                A multi get does not fetch the expiry, so it has no effect.
        per_key_options (dict[str, :class:`~couchbase.options.GetOptions`], optional): Specify
            :class:`~couchbase.options.GetOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[GetMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def project(self) -> Iterable[str]:
        return self.get("project", [])

    @property
    def with_expiry(self) -> bool:
        return self.get("with_expiry", False)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "project",
            "with_expiry",
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "transcoder",
        ]


class InsertMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, InsertOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    expiry: timedelta | None
    durability: DurabilityType | None
    transcoder: Transcoder | None


class InsertMultiOptions(dict):
    """Available options for a key-value multi-insert operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all insert operations handled with this multi-insert operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.InsertOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.InsertOptions`], optional): Specify
            :class:`~couchbase.options.InsertOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[InsertMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "expiry",
            "durability",
            "transcoder",
        ]


class RemoveMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, RemoveOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    cas: int | None
    durability: DurabilityType | None
    transcoder: Transcoder | None


class RemoveMultiOptions(dict):
    """Available options for a key-value multi-remove operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all remove operations handled with this multi-remove operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.RemoveOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.RemoveOptions`], optional): Specify
            :class:`~couchbase.options.RemoveOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): A remove carries no value to transcode.

            .. deprecated:: 4.7.0
                It has no effect.
    """

    def __init__(self, **kwargs: Unpack[RemoveMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "cas",
            "durability",
            "transcoder",
        ]


class ReplaceMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, ReplaceOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    expiry: timedelta | None
    cas: int | None
    preserve_expiry: bool | None
    durability: DurabilityType | None
    transcoder: Transcoder | None


class ReplaceMultiOptions(dict):
    """Available options for a key-value multi-replace operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all replace operations handled with this multi-replace operation).
    Use *per_key_options* to set specific :class:`~couchbase.options.ReplaceOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.ReplaceOptions`], optional): Specify
            :class:`~couchbase.options.ReplaceOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
            Defaults to False.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[ReplaceMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "expiry",
            "cas",
            "preserve_expiry",
            "durability",
            "transcoder",
        ]


class TouchMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, TouchOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class TouchMultiOptions(dict):
    """Available options for a key-value multi-touch operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all touch operations handled with this multi-touch operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.TouchOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.TouchOptions`], optional): Specify
            :class:`~couchbase.options.TouchOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[TouchMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "expiry",
        ]


class UnlockMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, UnlockOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None


class UnlockMultiOptions(dict):
    """Available options for a key-value multi-unlock operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all unlock operations handled with this multi-unlock operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.UnlockOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.UnlockOptions`], optional): Specify
            :class:`~couchbase.options.UnlockOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[UnlockMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
        ]


class UpsertMultiOptionsKwargs(TypedDict, total=False):
    per_key_options: dict[str, UpsertOptions] | None
    return_exceptions: bool | None
    timeout: timedelta | None
    parent_span: RequestSpan | None
    expiry: timedelta | None
    preserve_expiry: bool | None
    durability: DurabilityType | None
    transcoder: Transcoder | None


class UpsertMultiOptions(dict):
    """Available options for a key-value multi-upsert operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Options can be set at a global level (i.e. for all upsert operations handled with this multi-upsert operation). Use
    *per_key_options* to set specific :class:`~couchbase.options.UpsertOptions` for specific keys.

    Args:
        per_key_options (dict[str, :class:`~couchbase.options.UpsertOptions`], optional): Specify
            :class:`~couchbase.options.UpsertOptions` per key.
        return_exceptions (bool, optional): If False, raise an Exception when encountered. If True return the Exception
            without raising. Defaults to True.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
            Defaults to False.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[UpsertMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)

    @classmethod
    def get_valid_keys(cls) -> list[str]:
        return [
            "per_key_options",
            "return_exceptions",
            "timeout",
            "parent_span",
            "expiry",
            "preserve_expiry",
            "durability",
            "transcoder",
        ]


# ==========================================================================================
# Query
# ==========================================================================================


class QueryOptionsKwargs(TypedDict, total=False):
    client_context_id: str | None
    raw: dict[str, JSONType] | None
    adhoc: bool | None
    max_parallelism: int | None
    pipeline_batch: int | None
    pipeline_cap: int | None
    profile: QueryProfile | None
    scan_cap: int | None
    scan_wait: timedelta | None
    metrics: bool | None
    named_parameters: dict[str, JSONType] | None
    positional_parameters: Iterable[JSONType] | None
    read_only: bool | None
    timeout: timedelta | None
    scan_consistency: QueryScanConsistency | None
    query_context: str | None
    flex_index: bool | None
    preserve_expiry: bool | None
    use_replica: bool | None
    consistent_with: MutationState | None
    send_to_node: str | None
    parent_span: RequestSpan | None
    serializer: Serializer | None


class QueryOptions(dict):
    """Available options for a N1QL (SQL++) query.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        client_context_id (str, optional): The returned client context id for this query. Defaults to a generated UUID.
        raw (dict[str, JSONType], optional): Specifies any additional parameters which should be passed to the query
            engine when executing the query.
        adhoc (bool, optional): Specifies whether this is an ad-hoc query, or if it should be prepared for faster
            execution in the future. Defaults to True.
        max_parallelism (int, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option.
        pipeline_batch (int, optional): This is an advanced option, see the query service reference for more information
            on the proper use and tuning of this option.
        pipeline_cap (int, optional): This is an advanced option, see the query service reference for more information
            on the proper use and tuning of this option.
        profile (:class:`~couchbase.n1ql.QueryProfile`, optional): Specifies the level of profiling that should be used
            for the query. Defaults to `Off`.
        scan_cap (int, optional): This is an advanced option, see the query service reference for more information on
            the proper use and tuning of this option.
        scan_wait (timedelta, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option.
        metrics (bool, optional): Specifies whether metrics should be captured as part of the execution of the query.
            Defaults to False.
        named_parameters (dict[str, JSONType], optional): Named values to be used for the placeholders within the query.
        positional_parameters (Iterable[JSONType], optional): Positional values to be used for the placeholders within
            the query.
        read_only (bool, optional): Specifies that this query should be executed in read-only mode, disabling the
            ability for the query to make any changes to the data. Defaults to False.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.query_timeout`.
        scan_consistency (:class:`~couchbase.n1ql.QueryScanConsistency`, optional): Specifies the consistency
            requirements when executing the query. Defaults to `NotBounded`.
        query_context (str, optional): Specifies the context within which this query should be executed. This can be
            scoped to a scope or a collection within the dataset.
        flex_index (bool, optional): Specifies whether flex-indexes should be enabled. Allowing the use of full-text
            search from the query service. Defaults to False.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
            Defaults to False.
        use_replica (bool, optional): Specifies that the query engine should use replica nodes for KV fetches if the
            active node is down.
        consistent_with (:class:`~couchbase.mutation_state.MutationState`, optional): Specifies a
            :class:`~couchbase.mutation_state.MutationState` which the operation should be consistent with.
        send_to_node (str, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
    """

    def __init__(self, **kwargs: Unpack[QueryOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Search
# ==========================================================================================


class SearchOptionsKwargs(TypedDict, total=False):
    client_context_id: str | None
    raw: dict[str, JSONType] | None
    limit: int | None
    skip: int | None
    timeout: timedelta | None
    explain: bool | None
    fields: list[str] | None
    highlight_style: HighlightStyle | None
    highlight_fields: list[str] | None
    scan_consistency: SearchScanConsistency | None
    consistent_with: MutationState | None
    facets: dict[str, Facet] | None
    sort: Sequence[str] | Sequence[Sort] | None
    disable_scoring: bool | None
    scoring: SearchScoring | None
    scope_name: str | None
    collections: list[str] | None
    include_locations: bool | None
    serializer: Serializer | None
    show_request: bool | None
    log_request: bool | None
    log_response: bool | None
    parent_span: RequestSpan | None


class SearchOptions(dict):
    """Available options for a search (FTS) query.

    .. warning::
        Importing options from ``couchbase.search`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        client_context_id (str, optional): The returned client context id for this query. Defaults to a generated UUID.
        raw (dict[str, JSONType], optional): Specifies any additional parameters which should be passed to the query
            engine when executing the query.
        limit (int, optional): Specifies the limit to the number of results that should be returned.
        skip (int, optional): Specifies the number of results to skip from the index before returning results.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.search_timeout`.
        explain (bool, optional): Configures whether the result should contain the execution plan for the search query.
            Defaults to False.
        fields (list[str], optional): Specifies the list of fields which should be searched.
        highlight_style (:class:`~couchbase.search.HighlightStyle`, optional): Specifies the mode used for highlighting.
        highlight_fields (list[str], optional): Specifies the list of fields that should be highlighted.
        scan_consistency (:class:`~couchbase.search.SearchScanConsistency`, optional): Specifies the consistency
            requirements when executing the query. Defaults to `NotBounded`.
        consistent_with (:class:`~couchbase.mutation_state.MutationState`, optional): Specifies a
            :class:`~couchbase.mutation_state.MutationState` which the operation should be consistent with.
        facets (dict[str, :class:`~couchbase.search.Facet`], optional): Specifies any facets that should be included in
            the search query.
        sort (Sequence[str] | Sequence[:class:`~couchbase.search.Sort`], optional): Specifies a list of fields or search
            :class:`~couchbase.search.Sort`'s to use when sorting the result sets.
        disable_scoring (bool, optional): Specifies that scoring should be disabled. This improves performance but makes
            it impossible to sort based on how well a particular result scored. Defaults to False.

            .. deprecated:: 4.7.0
                Use ``scoring`` with :class:`~couchbase.search_scoring.ScoringNone` instead.
        scoring (:class:`~couchbase.search_scoring.SearchScoring`, optional): Controls result scoring and how a hybrid
            request combines FTS and vector results into one ranked list. Cannot be combined with
            ``disable_scoring=True``.
        scope_name (str, optional): Specifies the scope which should be searched as part of the search query.

            .. deprecated:: 4.7.0
                Use ``Scope.search()`` instead.
        collections (list[str], optional): Specifies the collections which should be searched as part of the search
            query.
        include_locations (bool, optional): If set to True, will include the locations in the search result. Defaults to
            False.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
        show_request (bool, optional): Specifies if the search response should contain the request for the search query.
        log_request (bool, optional): **UNCOMMITTED** Specifies if search request body should appear the log. Defaults
            to False.
        log_response (bool, optional): **UNCOMMITTED** Specifies if search response should appear in the log. Defaults
            to False.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
    """

    def __init__(self, **kwargs: Unpack[SearchOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class VectorSearchOptionsKwargs(TypedDict, total=False):
    vector_query_combination: VectorQueryCombination | None


class VectorSearchOptions(dict):
    """Available options for a FTS vector search.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        vector_query_combination (:class:`~couchbase.vector_search.VectorQueryCombination`, optional): Specifies logical
            operation to use with multiple vector queries.
    """

    def __init__(self, **kwargs: Unpack[VectorSearchOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Subdocument
# ==========================================================================================


class LookupInAllReplicasOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None
    serializer: Serializer | None
    read_preference: ReadPreference | None


class LookupInAllReplicasOptions(dict):
    """Available options for a subdocument lookup-in-all-replicas operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
        read_preference (:class:`~couchbase.replica_reads.ReadPreference`, optional): Specifies how the replica nodes
            will be selected. Defaults to no preference.
    """

    def __init__(self, **kwargs: Unpack[LookupInAllReplicasOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class LookupInAnyReplicaOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    parent_span: RequestSpan | None
    serializer: Serializer | None
    read_preference: ReadPreference | None


class LookupInAnyReplicaOptions(dict):
    """Available options for a subdocument lookup-in-any-replica operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
        read_preference (:class:`~couchbase.replica_reads.ReadPreference`, optional): Specifies how the replica nodes
            will be selected. Defaults to no preference.
    """

    def __init__(self, **kwargs: Unpack[LookupInAnyReplicaOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class LookupInOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    access_deleted: bool | None
    parent_span: RequestSpan | None
    serializer: Serializer | None


class LookupInOptions(dict):
    """Available options for a subdocument lookup-in operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        access_deleted (bool, optional):
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
    """

    def __init__(self, **kwargs: Unpack[LookupInOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class MutateInOptionsKwargs(TypedDict, total=False):
    timeout: timedelta | None
    expiry: timedelta | None
    cas: int | None
    durability: DurabilityType | None
    store_semantics: StoreSemantics | None
    access_deleted: bool | None
    preserve_expiry: bool | None
    parent_span: RequestSpan | None
    serializer: Serializer | None


class MutateInOptions(dict):
    """Available options for a subdocument mutate-in operation.

    .. warning::
        Importing options from ``couchbase.subdocument`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
        expiry (timedelta, optional): Specifies the expiry time for this document. Cannot be combined with
            `preserve_expiry`.
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from this
            value, indicating that the document has changed.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability for this
            operation.
        store_semantics (:class:`~couchbase.subdocument.StoreSemantics`, optional): Specifies the store semantics to use
            for this operation.
        access_deleted (bool, optional):
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
            Defaults to False.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
    """

    def __init__(self, **kwargs: Unpack[MutateInOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)

    @property
    def expiry(self) -> timedelta | None:
        return self.get("expiry", None)


# ==========================================================================================
# Transactions
# ==========================================================================================


class TransactionConfigKwargs(TypedDict, total=False):
    expiration_time: timedelta | None
    metadata_collection: TransactionKeyspace | None
    durability: ServerDurability | None
    cleanup_window: timedelta | None
    kv_timeout: timedelta | None
    cleanup_lost_attempts: bool | None
    cleanup_client_attempts: bool | None
    scan_consistency: QueryScanConsistency | None
    timeout: timedelta | None


class TransactionConfig(dict):
    """Args:
        expiration_time (timedelta, optional): Maximum amount of time a transaction can take before rolling back.

            .. deprecated:: 4.1.10
                Use ``timeout`` instead.
        metadata_collection (:class:`~couchbase.transactions.TransactionKeyspace`, optional):
        durability (:class:`~couchbase.durability.ServerDurability`, optional): Specifies the level of durability for
            this operation.
        cleanup_window (timedelta, optional): The query metadata is cleaned up over a the cleanup_window. Longer windows
            mean less background activity, shorter intervals will clean things faster.
        kv_timeout (timedelta, optional): KV operations timeout.

            .. deprecated:: 4.1.10
                It has no effect. Transactions use the cluster's ``kv_durable_timeout``.
        cleanup_lost_attempts (bool, optional):
        cleanup_client_attempts (bool, optional):
        scan_consistency (:class:`~couchbase.n1ql.QueryScanConsistency`, optional): Specifies the consistency
            requirements when executing the query. Defaults to `NotBounded`.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
    """

    def __init__(self, **kwargs: Unpack[TransactionConfigKwargs]) -> None:
        provided: dict[str, Any] = dict(kwargs)
        super().__init__(transaction_args("TransactionConfig", provided))


class TransactionGetMultiOptionsKwargs(TypedDict, total=False):
    mode: TransactionGetMultiMode | None


class TransactionGetMultiOptions(dict):
    """Available options for transaction get_multi operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        mode (:class:`~couchbase.transactions.TransactionGetMultiMode`, optional): Specifies the mode to use for this
            specific operation. Defaults to `PrioritiseLatency`.
    """

    def __init__(self, **kwargs: Unpack[TransactionGetMultiOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class TransactionGetMultiReplicasFromPreferredServerGroupOptionsKwargs(TypedDict, total=False):
    mode: TransactionGetMultiReplicasFromPreferredServerGroupMode | None


class TransactionGetMultiReplicasFromPreferredServerGroupOptions(dict):
    """Available options for transaction get_multi_replicas_from_preferred_server_group operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        mode (:class:`~couchbase.transactions.TransactionGetMultiReplicasFromPreferredServerGroupMode`, optional):
            Specifies the mode to use for this specific operation. Defaults to `PrioritiseLatency`.
    """

    def __init__(self, **kwargs: Unpack[TransactionGetMultiReplicasFromPreferredServerGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class TransactionGetOptionsKwargs(TypedDict, total=False):
    transcoder: Transcoder | None


class TransactionGetOptions(dict):
    """Available options for transaction get operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[TransactionGetOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class TransactionGetReplicaFromPreferredServerGroupOptionsKwargs(TypedDict, total=False):
    transcoder: Transcoder | None


class TransactionGetReplicaFromPreferredServerGroupOptions(dict):
    """Available options for transaction get_replica_from_preferred_server_group operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[TransactionGetReplicaFromPreferredServerGroupOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class TransactionInsertOptionsKwargs(TypedDict, total=False):
    transcoder: Transcoder | None


class TransactionInsertOptions(dict):
    """Available options for transaction insert operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[TransactionInsertOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


class TransactionOptionsKwargs(TypedDict, total=False):
    expiration_time: timedelta | None
    metadata_collection: TransactionKeyspace | None
    durability: ServerDurability | None
    kv_timeout: timedelta | None
    scan_consistency: QueryScanConsistency | None
    timeout: timedelta | None


class TransactionOptions(dict):
    """Args:
        expiration_time (timedelta, optional): Maximum amount of time a transaction can take before rolling back.

            .. deprecated:: 4.1.10
                Use ``timeout`` instead.
        metadata_collection (:class:`~couchbase.transactions.TransactionKeyspace`, optional):
        durability (:class:`~couchbase.durability.ServerDurability`, optional): Specifies the level of durability for
            this operation.
        kv_timeout (timedelta, optional): KV operations timeout.

            .. deprecated:: 4.1.10
                It has no effect. Transactions use the cluster's ``kv_durable_timeout``.
        scan_consistency (:class:`~couchbase.n1ql.QueryScanConsistency`, optional): Specifies the consistency
            requirements when executing the query. Defaults to `NotBounded`.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.kv_timeout`.
    """

    def __init__(self, **kwargs: Unpack[TransactionOptionsKwargs]) -> None:
        provided: dict[str, Any] = dict(kwargs)
        super().__init__(transaction_args("TransactionOptions", provided))


class TransactionQueryOptionsKwargs(TypedDict, total=False):
    client_context_id: str | None
    raw: dict[str, JSONType] | None
    adhoc: bool | None
    max_parallelism: int | None
    pipeline_batch: int | None
    pipeline_cap: int | None
    profile: QueryProfile | None
    scan_cap: int | None
    scan_wait: timedelta | None
    metrics: bool | None
    named_parameters: dict[str, JSONType] | None
    positional_parameters: Iterable[JSONType] | None
    read_only: bool | None
    scan_consistency: QueryScanConsistency | None
    scope: ScopeKeyspace | None


class TransactionQueryOptions(dict):
    """Args:
        client_context_id (str, optional): The returned client context id for this query. Defaults to a generated UUID.
        raw (dict[str, JSONType], optional): Specifies any additional parameters which should be passed to the query
            engine when executing the query.
        adhoc (bool, optional): Specifies whether this is an ad-hoc query, or if it should be prepared for faster
            execution in the future. Defaults to True.
        max_parallelism (int, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option.
        pipeline_batch (int, optional): This is an advanced option, see the query service reference for more information
            on the proper use and tuning of this option.
        pipeline_cap (int, optional): This is an advanced option, see the query service reference for more information
            on the proper use and tuning of this option.
        profile (:class:`~couchbase.n1ql.QueryProfile`, optional): Specifies the level of profiling that should be used
            for the query. Defaults to `Off`.
        scan_cap (int, optional): This is an advanced option, see the query service reference for more information on
            the proper use and tuning of this option.
        scan_wait (timedelta, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option.
        metrics (bool, optional): Specifies whether metrics should be captured as part of the execution of the query.
            Defaults to True.
        named_parameters (dict[str, JSONType], optional): Named values to be used for the placeholders within the query.
        positional_parameters (Iterable[JSONType], optional): Positional values to be used for the placeholders within
            the query.
        read_only (bool, optional): Specifies that this query should be executed in read-only mode, disabling the
            ability for the query to make any changes to the data. Defaults to False.
        scan_consistency (:class:`~couchbase.n1ql.QueryScanConsistency`, optional): Specifies the consistency
            requirements when executing the query. Defaults to `NotBounded`.
        scope (ScopeKeyspace, optional): Specify the scope of the query.
    """

    def __init__(self, **kwargs: Unpack[TransactionQueryOptionsKwargs]) -> None:
        provided: dict[str, Any] = dict(kwargs)
        super().__init__(transaction_args("TransactionQueryOptions", provided))

    def split_scope_qualifier(self, remove_backticks: bool | None = True) -> tuple[str, str] | None:
        """The bucket and scope a `scope` was given for, or nothing if none was.

        Read from the scope's own two names rather than from a qualifier string assembled out
        of them, which is one parse and one format fewer.
        """
        scope = self.get('scope', None)
        if scope is None:
            return None
        bucket, name = scope.bucket_name, scope.name
        if remove_backticks:
            return bucket.replace('`', ''), name.replace('`', '')
        return bucket, name


class TransactionReplaceOptionsKwargs(TypedDict, total=False):
    transcoder: Transcoder | None


class TransactionReplaceOptions(dict):
    """Available options for transaction replace operation.

    .. note::
        All options should be imported from ``couchbase.options``.

    Args:
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder to use for
            this specific operation. Defaults to :class:`~couchbase.transcoder.JSONTranscoder`.
    """

    def __init__(self, **kwargs: Unpack[TransactionReplaceOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


# ==========================================================================================
# Value Types
# ==========================================================================================


class ConstrainedInt:
    """An integer a subclass checks against the range it states.

    .. note::
        Import it from ``couchbase.options``.

    Args:
        value (:class:`~couchbase.options.AcceptableInts`): The value to initialise this with.
    """

    def __init__(self, value: AcceptableInts) -> None:
        self.value = type(self).verify_value(value)

    @classmethod
    def min(cls) -> int:
        raise NotImplementedError()

    @classmethod
    def max(cls) -> int:
        raise NotImplementedError()

    @classmethod
    def verify_value(cls, value: AcceptableInts) -> int:
        held = getattr(value, "value", value)
        if not isinstance(held, int) or not (cls.min() <= held <= cls.max()):
            raise InvalidArgumentException(
                "An integer between {} and {} inclusive is required.".format(cls.min(), cls.max()))
        return held

    @classmethod
    def is_valid(cls, value: AcceptableInts) -> bool:
        return isinstance(value, cls)

    def __int__(self) -> int:
        return self.value

    def __index__(self) -> int:
        return self.value

    def __neg__(self) -> int:
        return -self.value

    def __add__(self, other: SupportsInt) -> ConstrainedInt:
        return type(self)(self.value + int(other))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, (int, ConstrainedInt)) and self.value == int(other)

    def __gt__(self, other: SupportsInt) -> bool:
        return self.value > int(other)

    def __ge__(self, other: SupportsInt) -> bool:
        return self.value >= int(other)

    def __lt__(self, other: SupportsInt) -> bool:
        return self.value < int(other)

    def __le__(self, other: SupportsInt) -> bool:
        return self.value <= int(other)

    def __hash__(self) -> int:
        return hash(self.value)

    def __str__(self) -> str:
        return "{cls_name} with value {value}".format(cls_name=type(self).__name__, value=self.value)

    def __repr__(self) -> str:
        return str(self)


class DeltaValue(ConstrainedInt):
    """An integer between 0 and 0x7FFFFFFFFFFFFFFF inclusive. Used as an argument for :meth:`Collection.increment` and
    :meth:`Collection.decrement`.

    .. warning::
        Importing this class from ``couchbase.collection`` is deprecated.
        Import it from ``couchbase.options``.

    Args:
        value (:class:`~couchbase.options.AcceptableInts`): The value to initialise this with.
    """

    def __init__(self, value: AcceptableInts) -> None:
        super().__init__(value)

    @classmethod
    def min(cls) -> int:
        return 0

    @classmethod
    def max(cls) -> int:
        return 0x7FFFFFFFFFFFFFFF


class SignedInt64(ConstrainedInt):
    """An integer between -0x8000000000000000 and 0x7FFFFFFFFFFFFFFF inclusive.

    .. note::
        Import it from ``couchbase.options``.

    Args:
        value (:class:`~couchbase.options.AcceptableInts`): The value to initialise this with.
    """

    def __init__(self, value: AcceptableInts) -> None:
        super().__init__(value)

    @classmethod
    def min(cls) -> int:
        return -0x8000000000000000

    @classmethod
    def max(cls) -> int:
        return 0x7FFFFFFFFFFFFFFF


@Supportability.class_deprecated("an int")
class UnsignedInt32(ConstrainedInt):
    """An integer between 0x00000000 and 0x80000000 inclusive.

    .. note::
        Import it from ``couchbase.options``.

    .. deprecated:: 4.7.0
        Use an int instead.

    Args:
        value (:class:`~couchbase.options.AcceptableInts`): The value to initialise this with.
    """

    def __init__(self, value: AcceptableInts) -> None:
        super().__init__(value)

    @classmethod
    def min(cls) -> int:
        return 0x00000000

    @classmethod
    def max(cls) -> int:
        return 0x80000000


class UnsignedInt64(ConstrainedInt):
    """An integer between 0x0000000000000000 and 0x8000000000000000 inclusive.

    .. note::
        Import it from ``couchbase.options``.

    Args:
        value (:class:`~couchbase.options.AcceptableInts`): The value to initialise this with.
    """

    def __init__(self, value: AcceptableInts) -> None:
        super().__init__(value)

    @classmethod
    def min(cls) -> int:
        return 0x0000000000000000

    @classmethod
    def max(cls) -> int:
        return 0x8000000000000000


# ==========================================================================================
# Views
# ==========================================================================================


class ViewOptionsKwargs(TypedDict, total=False):
    client_context_id: str | None
    raw: dict[str, str] | None
    limit: int | None
    skip: int | None
    timeout: timedelta | None
    scan_consistency: ViewScanConsistency | None
    startkey: JSONType | None
    endkey: JSONType | None
    startkey_docid: str | None
    endkey_docid: str | None
    inclusive_end: bool | None
    group: bool | None
    group_level: int | None
    key: JSONType | None
    keys: Sequence[JSONType] | None
    order: ViewOrdering | None
    reduce: bool | None
    on_error: ViewErrorMode | None
    debug: bool | None
    namespace: DesignDocumentNamespace | None
    query_string: list[str] | None
    full_set: bool | None
    parent_span: RequestSpan | None
    serializer: Serializer | None


class ViewOptions(dict):
    """Available options for a view query.

    .. warning::
        Importing options from ``couchbase.bucket`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        client_context_id (str, optional): The returned client context id for this query. Defaults to a generated UUID.
        raw (dict[str, str], optional): Specifies any additional parameters which should be passed to the query engine
            when executing the query.
        limit (int, optional): Specifies the limit to the number of results that should be returned.
        skip (int, optional): Specifies the number of results to skip from the index before returning results.
        timeout (timedelta, optional): The timeout for this operation. Defaults to
            :attr:`~couchbase.options.ClusterTimeoutOptions.views_timeout`.
        scan_consistency (:class:`~couchbase.views.ViewScanConsistency`, optional): Specifies the consistency
            requirements when executing the query. Defaults to `NotBounded`.
        startkey (JSONType, optional): Specifies the first key that should be included in the results.
        endkey (JSONType, optional): Specifies the last key that should be included in the results.
        startkey_docid (str, optional): Specifies the first document ID that should be included in the results.
        endkey_docid (str, optional): Specifies the last document ID that should be included in the results.
        inclusive_end (bool, optional): Specifies whether the end key should be considered inclusive or exclusive.
            Defaults to True.
        group (bool, optional): Specifies whether the results should be grouped together.
        group_level (int, optional): Specifies the level to which results should be group.
        key (JSONType, optional): Specifies a specific key which should be fetched from the index.
        keys (Sequence[JSONType], optional): Specifies a list of keys which should be fetched from the index.
        order (:class:`~couchbase.views.ViewOrdering`, optional): Specifies the ordering that should be used when
            returning results.
        reduce (bool, optional): Specifies whether reduction should be performed as part of the view query.
        on_error (:class:`~couchbase.views.ViewErrorMode`, optional): Specifies the error-handling behaviour that should
            be used when an error occurs.
        debug (bool, optional):
        namespace (:class:`~couchbase.management.views.DesignDocumentNamespace`, optional): Specifies the namespace for
            the design document. Defaults to ``Development``.
        query_string (list[str], optional):
        full_set (bool, optional): Specifies whether the query should force the entire set of document in the index to
            be included in the result. Defaults to False.
        parent_span (:class:`~couchbase.observability.tracing.RequestSpan`, optional): The parent span for this
            operation.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer to use for
            this specific operation. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
    """

    def __init__(self, **kwargs: Unpack[ViewOptionsKwargs]) -> None:
        provided = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**provided)


AcceptableInts = c_int64 | c_uint64 | int
LockMultiOptions = GetAndLockMultiOptions
MutationMultiOptions = InsertMultiOptions | UpsertMultiOptions | ReplaceMultiOptions
MutationOptions = (InsertOptions | UpsertOptions | ReplaceOptions | RemoveOptions | MutateInOptions | AppendOptions |
                   PrependOptions | IncrementOptions | DecrementOptions)
NoValueMultiOptions = (GetMultiOptions | ExistsMultiOptions | RemoveMultiOptions | TouchMultiOptions | LockMultiOptions
                       | UnlockMultiOptions)
