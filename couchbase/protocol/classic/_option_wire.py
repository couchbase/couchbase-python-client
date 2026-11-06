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

"""What each option is sent to the classic binding as: the key it reads, and what converts the value.

A record is one option's wire wherever the option is carried, and a class that sends it
differently holds a `replace` of the record.  `WIRES` is keyed the way
`couchbase/common/_option_spec.py` keys `BLOCKS`, by the spelling the class receives, and names
only what is not sent as received: a key a class carries and its entry leaves out reaches the
binding under that spelling and unconverted, and a class with nothing to convert has no entry.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from couchbase.common._durability import DurabilityParser
from couchbase.common._transforms import (as_str,
                                          csv_or_list,
                                          enum_value,
                                          ip_protocol_as_str,
                                          json_encoded_list,
                                          json_encoded_values,
                                          mutation_tokens_object,
                                          passthrough,
                                          timedelta_as_milliseconds,
                                          timedelta_as_seconds,
                                          timedelta_as_timestamp,
                                          tls_verify_as_str,
                                          transaction_durability,
                                          transaction_duration,
                                          transaction_scan_consistency,
                                          validate_bool,
                                          validate_int,
                                          validate_str)

DROP = None


@dataclass(frozen=True)
class Wire:
    """One key as the binding reads it.

    `name` is the key the binding reads: empty for the spelling the key is stored under, `DROP`
    for a key that is accepted and sent nowhere.  `transform` is `None` only on a class a request
    builder maps, where the schema names a helper of the builder's rather than a function of
    the value.
    """

    name: str | None = ''
    transform: Callable[[Any], Any] | None = passthrough

    def name_for(self, stored: str) -> str | None:
        return stored if self.name == '' else self.name


@dataclass(frozen=True)
class WireBlock:
    """One options class's keys that are not sent as received.

    `by_builder` says a request builder maps the class and not `to_wire`, so its rows describe
    the map rather than drive it.
    """

    keys: dict[str, Wire]
    by_builder: bool = False


# ==========================================================================================
# _common
# ==========================================================================================
TIMEOUT = Wire(transform=timedelta_as_milliseconds)

# ==========================================================================================
# cluster
# ==========================================================================================
ALLOWED_SASL_MECHANISMS = Wire(transform=csv_or_list)
ALLOW_ENTERPRISE_ANALYTICS = Wire(transform=validate_bool)
ANALYTICS_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
APP_TELEMETRY_BACKOFF = Wire(transform=timedelta_as_milliseconds)
APP_TELEMETRY_ENDPOINT = Wire(transform=validate_str)
APP_TELEMETRY_PING_INTERVAL = Wire(transform=timedelta_as_milliseconds)
APP_TELEMETRY_PING_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
BOOTSTRAP_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
CERT_PATH = Wire(transform=validate_str)
COMPRESSION = Wire(name=DROP)
COMPRESSION_MIN_RATIO = Wire(name=DROP)
COMPRESSION_MIN_SIZE = Wire(name=DROP)
CONFIG_IDLE_REDIAL_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
CONFIG_POLL_FLOOR = Wire(transform=timedelta_as_milliseconds)
CONFIG_POLL_INTERVAL = Wire(transform=timedelta_as_milliseconds)
CONFIG_TOTAL_TIMEOUT = Wire(name=DROP)
CONNECT_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
DISABLE_MOZILLA_CA_CERTIFICATES = Wire(transform=validate_bool)
DNS_NAMESERVER = Wire(transform=validate_str)
DNS_PORT = Wire(transform=validate_int)
DNS_SRV_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
DUMP_CONFIGURATION = Wire(transform=validate_bool)
EMIT_INTERVAL = Wire(name='metrics_emit_interval', transform=timedelta_as_milliseconds)
ENABLE_APP_TELEMETRY = Wire(transform=validate_bool)
ENABLE_CLUSTERMAP_NOTIFICATION = Wire(transform=validate_bool)
ENABLE_COMPRESSION = Wire(transform=validate_bool)
ENABLE_DNS_SRV = Wire(transform=validate_bool)
ENABLE_LAZY_CONNECTIONS = Wire(transform=validate_bool)
ENABLE_METRICS = Wire(transform=validate_bool)
ENABLE_MUTATION_TOKENS = Wire(transform=validate_bool)
ENABLE_ORPHAN_REPORTING = Wire(transform=validate_bool)
ENABLE_TCP_KEEP_ALIVE = Wire(transform=validate_bool)
ENABLE_TLS = Wire(transform=validate_bool)
ENABLE_TRACING = Wire(transform=validate_bool)
ENABLE_UNORDERED_EXECUTION = Wire(transform=validate_bool)
IDLE_HTTP_CONNECTION_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
IP_PROTOCOL = Wire(name='use_ip_protocol', transform=ip_protocol_as_str)
KV_DURABLE_TIMEOUT = Wire(name='key_value_durable_timeout', transform=timedelta_as_milliseconds)
KV_TIMEOUT = Wire(name='key_value_timeout', transform=timedelta_as_milliseconds)
LOCKMODE = Wire(name=DROP)
LOGGING_METER_EMIT_INTERVAL = Wire(name='cluster_metrics_emit_interval', transform=timedelta_as_milliseconds)
LOG_REDACTION = Wire(name=DROP)
MANAGEMENT_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
MAX_HTTP_CONNECTIONS = Wire(transform=validate_int)
METRICS_OPTIONS = Wire(name=DROP)
NETWORK = Wire(transform=validate_str)
NUM_IO_THREADS = Wire(transform=validate_int)
OPTION_VALIDATION = Wire(transform=validate_str)
ORPHAN_REPORTING_OPTIONS = Wire(name=DROP)
PREFERRED_SERVER_GROUP = Wire(transform=validate_str)
QUERY_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
REPORT_ID = Wire(transform=as_str)
RESOLVE_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
SAMPLE_SIZE = Wire(name='orphan_sample_size', transform=validate_int)
SEARCH_TIMEOUT = Wire(transform=timedelta_as_milliseconds)
SHOW_QUERIES = Wire(transform=validate_bool)
TCP_KEEP_ALIVE_INTERVAL = Wire(transform=timedelta_as_milliseconds)
TIMEOUT_OPTIONS = Wire(name=DROP)
TLS_VERIFY = Wire(transform=tls_verify_as_str)
TRACING_OPTIONS = Wire(name=DROP)
TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL = Wire(name='orphan_emit_interval', transform=timedelta_as_milliseconds)
TRACING_ORPHANED_QUEUE_SIZE = Wire(name='orphan_sample_size', transform=validate_int)
TRACING_THRESHOLD_ANALYTICS = Wire(name='analytics_threshold', transform=timedelta_as_milliseconds)
TRACING_THRESHOLD_EVENTING = Wire(name='eventing_threshold', transform=timedelta_as_milliseconds)
TRACING_THRESHOLD_KV = Wire(name='key_value_threshold', transform=timedelta_as_milliseconds)
TRACING_THRESHOLD_MANAGEMENT = Wire(name='management_threshold', transform=timedelta_as_milliseconds)
TRACING_THRESHOLD_QUERY = Wire(name='query_threshold', transform=timedelta_as_milliseconds)
TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL = Wire(name='threshold_emit_interval', transform=timedelta_as_milliseconds)
TRACING_THRESHOLD_QUEUE_SIZE = Wire(name='threshold_sample_size', transform=validate_int)
TRACING_THRESHOLD_SEARCH = Wire(name='search_threshold', transform=timedelta_as_milliseconds)
TRACING_THRESHOLD_VIEW = Wire(name='view_threshold', transform=timedelta_as_milliseconds)
TRUST_STORE_PATH = Wire(transform=validate_str)
USER_AGENT_EXTRA = Wire(transform=validate_str)
VIEWS_TIMEOUT = Wire(name='view_timeout', transform=timedelta_as_milliseconds)

# ==========================================================================================
# kv
# ==========================================================================================
BATCH_BYTE_LIMIT = Wire(transform=validate_int)
BATCH_ITEM_LIMIT = Wire(transform=validate_int)
CAS = Wire(transform=validate_int)
CONCURRENCY = Wire(transform=validate_int)
CONSISTENT_WITH = Wire(transform=mutation_tokens_object)
DURABILITY = Wire(transform=DurabilityParser.parse_durability)
EXPIRY = Wire(transform=timedelta_as_timestamp)
LOCK_TIME = Wire(transform=timedelta_as_seconds)
READ_PREFERENCE = Wire(transform=enum_value)
RETURN_EXCEPTIONS = Wire(transform=validate_bool)

# ==========================================================================================
# streaming
# ==========================================================================================
ENDKEY = Wire(name='end_key', transform=None)
ENDKEY_DOCID = Wire(name='end_key_doc_id')
HIGHLIGHT_STYLE = Wire(transform=enum_value)
KEY = Wire(transform=None)
KEYS = Wire(transform=None)
NAMESPACE = Wire(name='ns', transform=None)
ON_ERROR = Wire(transform=None)
ORDER = Wire(transform=None)
PROFILE = Wire(transform=enum_value)
RAW = Wire(transform=json_encoded_values)
READ_ONLY = Wire(name='readonly')
SCAN_CONSISTENCY = Wire(transform=enum_value)
SCAN_WAIT = Wire(transform=timedelta_as_milliseconds)
SCORING = Wire(transform=None)
STARTKEY = Wire(name='start_key', transform=None)
STARTKEY_DOCID = Wire(name='start_key_doc_id')
STREAMING_SCOPE_NAME = Wire(name=DROP)
VECTOR_QUERY_COMBINATION = Wire(transform=enum_value)

# ==========================================================================================
# txns
# ==========================================================================================
EXPIRATION_TIME = Wire(name=DROP)

WIRES: dict[str, WireBlock] = {
    # ==========================================================================================
    # cluster
    # ==========================================================================================
    'ClusterMetricsOptions': WireBlock({
        'enable_metrics': replace(ENABLE_METRICS, name='enable_metrics'),
        'emit_interval': EMIT_INTERVAL,
    }),
    'ClusterOptions': WireBlock({
        'option_validation': OPTION_VALIDATION,
        'allowed_sasl_mechanisms': ALLOWED_SASL_MECHANISMS,
        'cert_path': CERT_PATH,
        'num_io_threads': NUM_IO_THREADS,
        'trust_store_path': TRUST_STORE_PATH,
        'timeout_options': TIMEOUT_OPTIONS,
        'orphan_reporting_options': ORPHAN_REPORTING_OPTIONS,
        'metrics_options': METRICS_OPTIONS,
        'tracing_options': TRACING_OPTIONS,
        'enable_tls': ENABLE_TLS,
        'enable_mutation_tokens': ENABLE_MUTATION_TOKENS,
        'enable_tcp_keep_alive': ENABLE_TCP_KEEP_ALIVE,
        'ip_protocol': IP_PROTOCOL,
        'enable_dns_srv': ENABLE_DNS_SRV,
        'show_queries': SHOW_QUERIES,
        'enable_unordered_execution': ENABLE_UNORDERED_EXECUTION,
        'enable_clustermap_notification': ENABLE_CLUSTERMAP_NOTIFICATION,
        'enable_compression': ENABLE_COMPRESSION,
        'enable_tracing': replace(ENABLE_TRACING, name='cluster_enable_tracing'),
        'enable_metrics': replace(ENABLE_METRICS, name='cluster_enable_metrics'),
        'enable_orphan_reporting': replace(ENABLE_ORPHAN_REPORTING, name='cluster_enable_orphan_reporting'),
        'network': NETWORK,
        'tls_verify': TLS_VERIFY,
        'tcp_keep_alive_interval': TCP_KEEP_ALIVE_INTERVAL,
        'config_poll_interval': CONFIG_POLL_INTERVAL,
        'config_poll_floor': CONFIG_POLL_FLOOR,
        'max_http_connections': MAX_HTTP_CONNECTIONS,
        'user_agent_extra': USER_AGENT_EXTRA,
        'logging_meter_emit_interval': LOGGING_METER_EMIT_INTERVAL,
        'log_redaction': LOG_REDACTION,
        'compression': COMPRESSION,
        'compression_min_size': COMPRESSION_MIN_SIZE,
        'compression_min_ratio': COMPRESSION_MIN_RATIO,
        'lockmode': LOCKMODE,
        'dns_nameserver': DNS_NAMESERVER,
        'dns_port': DNS_PORT,
        'disable_mozilla_ca_certificates': DISABLE_MOZILLA_CA_CERTIFICATES,
        'dump_configuration': DUMP_CONFIGURATION,
        'preferred_server_group': PREFERRED_SERVER_GROUP,
        'enable_app_telemetry': ENABLE_APP_TELEMETRY,
        'app_telemetry_endpoint': APP_TELEMETRY_ENDPOINT,
        'app_telemetry_backoff': APP_TELEMETRY_BACKOFF,
        'app_telemetry_ping_interval': APP_TELEMETRY_PING_INTERVAL,
        'app_telemetry_ping_timeout': APP_TELEMETRY_PING_TIMEOUT,
        'allow_enterprise_analytics': ALLOW_ENTERPRISE_ANALYTICS,
        'enable_lazy_connections': ENABLE_LAZY_CONNECTIONS,
        'bootstrap_timeout': BOOTSTRAP_TIMEOUT,
        'resolve_timeout': RESOLVE_TIMEOUT,
        'connect_timeout': CONNECT_TIMEOUT,
        'kv_timeout': KV_TIMEOUT,
        'kv_durable_timeout': KV_DURABLE_TIMEOUT,
        'views_timeout': VIEWS_TIMEOUT,
        'query_timeout': QUERY_TIMEOUT,
        'analytics_timeout': ANALYTICS_TIMEOUT,
        'search_timeout': SEARCH_TIMEOUT,
        'management_timeout': MANAGEMENT_TIMEOUT,
        'dns_srv_timeout': DNS_SRV_TIMEOUT,
        'idle_http_connection_timeout': IDLE_HTTP_CONNECTION_TIMEOUT,
        'config_idle_redial_timeout': CONFIG_IDLE_REDIAL_TIMEOUT,
        'config_total_timeout': CONFIG_TOTAL_TIMEOUT,
        'tracing_enable_tracing': replace(ENABLE_TRACING, name='enable_tracing'),
        'tracing_threshold_kv': TRACING_THRESHOLD_KV,
        'tracing_threshold_view': TRACING_THRESHOLD_VIEW,
        'tracing_threshold_query': TRACING_THRESHOLD_QUERY,
        'tracing_threshold_search': TRACING_THRESHOLD_SEARCH,
        'tracing_threshold_analytics': TRACING_THRESHOLD_ANALYTICS,
        'tracing_threshold_eventing': TRACING_THRESHOLD_EVENTING,
        'tracing_threshold_management': TRACING_THRESHOLD_MANAGEMENT,
        'tracing_threshold_queue_size': TRACING_THRESHOLD_QUEUE_SIZE,
        'tracing_threshold_queue_flush_interval': TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL,
        'tracing_orphaned_queue_size': TRACING_ORPHANED_QUEUE_SIZE,
        'tracing_orphaned_queue_flush_interval': TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL,
        'metrics_enable_metrics': replace(ENABLE_METRICS, name='enable_metrics'),
        'metrics_emit_interval': EMIT_INTERVAL,
        'orphan_enable_orphan_reporting': replace(ENABLE_ORPHAN_REPORTING, name='enable_orphan_reporting'),
        'orphan_sample_size': SAMPLE_SIZE,
        'orphan_emit_interval': replace(EMIT_INTERVAL, name=''),
    }),
    'ClusterOrphanReportingOptions': WireBlock({
        'enable_orphan_reporting': replace(ENABLE_ORPHAN_REPORTING, name='enable_orphan_reporting'),
        'sample_size': SAMPLE_SIZE,
        'emit_interval': replace(EMIT_INTERVAL, name=''),
    }),
    'ClusterTimeoutOptions': WireBlock({
        'bootstrap_timeout': BOOTSTRAP_TIMEOUT,
        'resolve_timeout': RESOLVE_TIMEOUT,
        'connect_timeout': CONNECT_TIMEOUT,
        'kv_timeout': KV_TIMEOUT,
        'kv_durable_timeout': KV_DURABLE_TIMEOUT,
        'views_timeout': VIEWS_TIMEOUT,
        'query_timeout': QUERY_TIMEOUT,
        'analytics_timeout': ANALYTICS_TIMEOUT,
        'search_timeout': SEARCH_TIMEOUT,
        'management_timeout': MANAGEMENT_TIMEOUT,
        'dns_srv_timeout': DNS_SRV_TIMEOUT,
        'idle_http_connection_timeout': IDLE_HTTP_CONNECTION_TIMEOUT,
        'config_idle_redial_timeout': CONFIG_IDLE_REDIAL_TIMEOUT,
        'config_total_timeout': CONFIG_TOTAL_TIMEOUT,
    }),
    'ClusterTracingOptions': WireBlock({
        'enable_tracing': replace(ENABLE_TRACING, name='enable_tracing'),
        'tracing_threshold_kv': TRACING_THRESHOLD_KV,
        'tracing_threshold_view': TRACING_THRESHOLD_VIEW,
        'tracing_threshold_query': TRACING_THRESHOLD_QUERY,
        'tracing_threshold_search': TRACING_THRESHOLD_SEARCH,
        'tracing_threshold_analytics': TRACING_THRESHOLD_ANALYTICS,
        'tracing_threshold_eventing': TRACING_THRESHOLD_EVENTING,
        'tracing_threshold_management': TRACING_THRESHOLD_MANAGEMENT,
        'tracing_threshold_queue_size': TRACING_THRESHOLD_QUEUE_SIZE,
        'tracing_threshold_queue_flush_interval': TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL,
        'tracing_orphaned_queue_size': TRACING_ORPHANED_QUEUE_SIZE,
        'tracing_orphaned_queue_flush_interval': TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL,
    }),
    'DiagnosticsOptions': WireBlock({
        'report_id': REPORT_ID,
        'timeout': TIMEOUT,
        'parent_span': Wire(name=DROP),
    }),
    'PingOptions': WireBlock({
        'timeout': TIMEOUT,
        'report_id': REPORT_ID,
        'parent_span': Wire(name=DROP),
    }),
    'WaitUntilReadyOptions': WireBlock({
        'timeout': TIMEOUT,
    }),

    # ==========================================================================================
    # kv
    # ==========================================================================================
    'AppendMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'durability': DURABILITY,
        'cas': CAS,
    }),
    'AppendOptions': WireBlock({
        'timeout': TIMEOUT,
        'durability': DURABILITY,
        'cas': CAS,
    }),
    'DecrementMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'durability': DURABILITY,
    }),
    'DecrementOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'durability': DURABILITY,
    }),
    'ExistsMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
    }),
    'ExistsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllReplicasMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'read_preference': READ_PREFERENCE,
    }),
    'GetAllReplicasOptions': WireBlock({
        'timeout': TIMEOUT,
        'read_preference': READ_PREFERENCE,
    }),
    'GetAndLockMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'lock_time': LOCK_TIME,
    }),
    'GetAndLockOptions': WireBlock({
        'timeout': TIMEOUT,
        'lock_time': LOCK_TIME,
    }),
    'GetAndTouchOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
    }),
    'GetAnyReplicaMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'read_preference': READ_PREFERENCE,
    }),
    'GetAnyReplicaOptions': WireBlock({
        'timeout': TIMEOUT,
        'read_preference': READ_PREFERENCE,
    }),
    'GetMultiOptions': WireBlock({
        'with_expiry': Wire(name=DROP),
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
    }),
    'GetOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'IncrementMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'durability': DURABILITY,
    }),
    'IncrementOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'durability': DURABILITY,
    }),
    'InsertMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'durability': DURABILITY,
    }),
    'InsertOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'durability': DURABILITY,
    }),
    'LookupInAllReplicasOptions': WireBlock({
        'timeout': TIMEOUT,
        'read_preference': READ_PREFERENCE,
    }),
    'LookupInAnyReplicaOptions': WireBlock({
        'timeout': TIMEOUT,
        'read_preference': READ_PREFERENCE,
    }),
    'LookupInOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'MutateInOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'cas': CAS,
        'durability': DURABILITY,
    }),
    'PrependMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'durability': DURABILITY,
        'cas': CAS,
    }),
    'PrependOptions': WireBlock({
        'timeout': TIMEOUT,
        'durability': DURABILITY,
        'cas': CAS,
    }),
    'RemoveMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'cas': CAS,
        'durability': DURABILITY,
        'transcoder': Wire(name=DROP),
    }),
    'RemoveOptions': WireBlock({
        'timeout': TIMEOUT,
        'cas': CAS,
        'durability': DURABILITY,
    }),
    'ReplaceMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'cas': CAS,
        'preserve_expiry': Wire(transform=validate_bool),
        'durability': DURABILITY,
    }),
    'ReplaceOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'cas': CAS,
        'durability': DURABILITY,
    }),
    'ScanOptions': WireBlock({
        'timeout': TIMEOUT,
        'consistent_with': CONSISTENT_WITH,
        'batch_byte_limit': BATCH_BYTE_LIMIT,
        'batch_item_limit': BATCH_ITEM_LIMIT,
        'batch_time_limit': Wire(name=DROP),
        'concurrency': CONCURRENCY,
        'parent_span': Wire(name=DROP),
    }),
    'TouchMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
    }),
    'TouchOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
    }),
    'UnlockMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
    }),
    'UnlockOptions': WireBlock({
        'timeout': TIMEOUT,
        'cas': CAS,
    }),
    'UpsertMultiOptions': WireBlock({
        'return_exceptions': RETURN_EXCEPTIONS,
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'preserve_expiry': Wire(transform=validate_bool),
        'durability': DURABILITY,
    }),
    'UpsertOptions': WireBlock({
        'timeout': TIMEOUT,
        'expiry': EXPIRY,
        'durability': DURABILITY,
    }),

    # ==========================================================================================
    # streaming
    # ==========================================================================================
    'AnalyticsOptions': WireBlock({
        'raw': RAW,
        'read_only': READ_ONLY,
        'timeout': TIMEOUT,
        'scan_consistency': SCAN_CONSISTENCY,
        'query_context': Wire(name='scope_qualifier'),
    }, by_builder=True),
    'QueryOptions': WireBlock({
        'raw': RAW,
        'profile': PROFILE,
        'scan_wait': SCAN_WAIT,
        'read_only': READ_ONLY,
        'timeout': TIMEOUT,
        'scan_consistency': SCAN_CONSISTENCY,
        'consistent_with': replace(CONSISTENT_WITH, name='mutation_state', transform=None),
    }, by_builder=True),
    'SearchOptions': WireBlock({
        'raw': RAW,
        'timeout': TIMEOUT,
        'highlight_style': HIGHLIGHT_STYLE,
        'scan_consistency': SCAN_CONSISTENCY,
        'consistent_with': replace(CONSISTENT_WITH, name='mutation_state', transform=None),
        'scoring': SCORING,
        'scope_name': STREAMING_SCOPE_NAME,
    }, by_builder=True),
    'VectorSearchOptions': WireBlock({
        'vector_query_combination': VECTOR_QUERY_COMBINATION,
    }, by_builder=True),
    'ViewOptions': WireBlock({
        'timeout': TIMEOUT,
        'scan_consistency': replace(SCAN_CONSISTENCY, name='consistency', transform=None),
        'startkey': STARTKEY,
        'endkey': ENDKEY,
        'startkey_docid': STARTKEY_DOCID,
        'endkey_docid': ENDKEY_DOCID,
        'key': KEY,
        'keys': KEYS,
        'order': ORDER,
        'on_error': ON_ERROR,
        'namespace': NAMESPACE,
    }, by_builder=True),

    # ==========================================================================================
    # mgmt
    # ==========================================================================================
    'AllowQueryingSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'AnalyzeDocumentSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'BucketDescribeOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'BuildDeferredQueryIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'ChangePasswordOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'ConnectLinkOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateAnalyticsIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateBucketOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateCollectionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateDatasetOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateDataverseOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateLinkAnalyticsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreatePrimaryQueryIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateQueryIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'CreateScopeOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DeployFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DisallowQueryingSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DisconnectLinkOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropAnalyticsIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropBucketOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropCollectionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropDatasetOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropDataverseOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropDesignDocumentOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropGroupOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropLinkAnalyticsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropPrimaryQueryIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropQueryIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropScopeOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'DropUserOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'EventingFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'FlushBucketOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'FreezePlanSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'FunctionsStatusOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllAnalyticsIndexesOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllBucketOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllDatasetOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllDesignDocumentsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllGroupsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllQueryIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllScopesOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllSearchIndexStatsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllSearchIndexesOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetAllUsersOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetBucketOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetDesignDocumentOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetGroupOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetLinksAnalyticsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetPendingMutationsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetRolesOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetSearchIndexStatsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetSearchIndexedDocumentsCountOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'GetUserOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'PauseFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'PauseIngestSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'PublishDesignDocumentOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'ReplaceLinkAnalyticsOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'ResumeFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'ResumeIngestSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UndeployFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UnfreezePlanSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UpdateBucketOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UpdateCollectionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UpsertDesignDocumentOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UpsertFunctionOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UpsertGroupOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UpsertSearchIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'UpsertUserOptions': WireBlock({
        'timeout': TIMEOUT,
    }),
    'WatchQueryIndexOptions': WireBlock({
        'timeout': TIMEOUT,
    }),

    # ==========================================================================================
    # txns
    # ==========================================================================================
    'TransactionConfig': WireBlock({
        'expiration_time': EXPIRATION_TIME,
        'durability': replace(DURABILITY, name='durability_level', transform=transaction_durability),
        'cleanup_window': Wire(transform=transaction_duration),
        'kv_timeout': replace(KV_TIMEOUT, name=DROP),
        'scan_consistency': replace(SCAN_CONSISTENCY, transform=transaction_scan_consistency),
        'timeout': replace(TIMEOUT, transform=transaction_duration),
    }),
    'TransactionOptions': WireBlock({
        'expiration_time': EXPIRATION_TIME,
        'durability': replace(DURABILITY, name='durability_level', transform=transaction_durability),
        'kv_timeout': replace(KV_TIMEOUT, name=DROP),
        'scan_consistency': replace(SCAN_CONSISTENCY, transform=transaction_scan_consistency),
        'timeout': replace(TIMEOUT, transform=transaction_duration),
    }),
    'TransactionQueryOptions': WireBlock({
        'raw': RAW,
        'profile': PROFILE,
        'scan_wait': SCAN_WAIT,
        'named_parameters': Wire(transform=json_encoded_values),
        'positional_parameters': Wire(transform=json_encoded_list),
        'read_only': READ_ONLY,
        'scan_consistency': replace(SCAN_CONSISTENCY, transform=transaction_scan_consistency),
    }),
}
