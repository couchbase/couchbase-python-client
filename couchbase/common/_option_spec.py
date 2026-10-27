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
"""What holds of an option key for every protocol: one record per option, one entry per class.

A record answers for its key wherever the key is carried, and a class that carries it differently
holds a `replace` of the record, so a difference between two classes is one line that says what
differs.  An entry is keyed by the spelling the class receives, which for a class that flattens
others into its own keywords is the spelling they store under.

`BLOCKS` is what the runtime reads: `check_options` judges a caller's keywords against an entry,
and each protocol maps the keywords an entry accepts to what it sends, from its own table.
`SUPERSEDED` is the one question an entry cannot answer, which is what a key the class does not
carry was replaced by, so it indexes the records that say.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class OptionSpec:
    """One key as a block receives it.

    `stored` is the spelling a nested block keeps the key under, which is the spelling the block
    flattening it receives.  `deprecated` is the warning a key the caller wrote is reported
    with, the same words as its docstring directive, and `into` is the key its value goes to
    unless the caller wrote that key too.  `duration` says the value is a `timedelta`, which a
    connection string can only spell as text.
    """

    key: str
    suggest: bool = True
    inert: frozenset[str] = frozenset()
    superseded_by: str | None = None
    stored: str = ''
    deprecated: str = ''
    into: str | None = None
    duration: bool = False


@dataclass(frozen=True)
class BlockSpec:
    """One options class: every key it receives, and what it does with a key it does not name.

    `refuses_unknown` drops a key the class does not name instead of passing it on.
    `free_keywords` makes an unnamed keyword input rather than a mistake.  `retired` is the keys
    carrying a deprecation, so a block with none is answered by one read per call rather than
    one per key.
    """

    keys: dict[str, OptionSpec]
    refuses_unknown: bool = False
    free_keywords: bool = False
    retired: tuple[str, ...] = ()


def block(*specs: OptionSpec, flattened: tuple[OptionSpec, ...] = (), **flags: Any) -> BlockSpec:
    keys = {spec.key: spec for spec in specs}
    keys.update((spec.stored or spec.key, spec) for spec in flattened)
    return BlockSpec(keys=keys, retired=tuple(key for key, spec in keys.items() if spec.deprecated), **flags)


# ==========================================================================================
# _common
# ==========================================================================================
PARENT_SPAN = OptionSpec('parent_span')
SPAN = OptionSpec('span', suggest=False, superseded_by='parent_span')
TIMEOUT = OptionSpec('timeout', duration=True)

# ==========================================================================================
# cluster
# ==========================================================================================
ALLOWED_SASL_MECHANISMS = OptionSpec('allowed_sasl_mechanisms', suggest=False)
ALLOW_ENTERPRISE_ANALYTICS = OptionSpec('allow_enterprise_analytics')
ANALYTICS_TIMEOUT = OptionSpec('analytics_timeout', duration=True)
APP_TELEMETRY_BACKOFF = OptionSpec('app_telemetry_backoff', duration=True)
APP_TELEMETRY_ENDPOINT = OptionSpec('app_telemetry_endpoint')
APP_TELEMETRY_PING_INTERVAL = OptionSpec('app_telemetry_ping_interval', duration=True)
APP_TELEMETRY_PING_TIMEOUT = OptionSpec('app_telemetry_ping_timeout', duration=True)
AUTHENTICATOR = OptionSpec('authenticator')
BOOTSTRAP_TIMEOUT = OptionSpec('bootstrap_timeout', duration=True)
CERT_PATH = OptionSpec('cert_path', suggest=False)
COMPRESSION = OptionSpec('compression', inert=frozenset({'classic'}))
COMPRESSION_MIN_RATIO = OptionSpec('compression_min_ratio', inert=frozenset({'classic'}))
COMPRESSION_MIN_SIZE = OptionSpec('compression_min_size', inert=frozenset({'classic'}))
CONFIG_IDLE_REDIAL_TIMEOUT = OptionSpec('config_idle_redial_timeout', duration=True)
CONFIG_POLL_FLOOR = OptionSpec('config_poll_floor', duration=True)
CONFIG_POLL_INTERVAL = OptionSpec('config_poll_interval', duration=True)
CONFIG_TOTAL_TIMEOUT = OptionSpec('config_total_timeout', inert=frozenset({'classic'}), deprecated='It has no effect.',
                                  duration=True)
CONNECT_TIMEOUT = OptionSpec('connect_timeout', duration=True)
DESIRED_STATE = OptionSpec('desired_state')
DISABLE_MOZILLA_CA_CERTIFICATES = OptionSpec('disable_mozilla_ca_certificates')
DNS_NAMESERVER = OptionSpec('dns_nameserver')
DNS_PORT = OptionSpec('dns_port')
DNS_SRV_TIMEOUT = OptionSpec('dns_srv_timeout', duration=True)
DUMP_CONFIGURATION = OptionSpec('dump_configuration')
EMIT_INTERVAL = OptionSpec('emit_interval', duration=True)
ENABLE_APP_TELEMETRY = OptionSpec('enable_app_telemetry')
ENABLE_CLUSTERMAP_NOTIFICATION = OptionSpec('enable_clustermap_notification')
ENABLE_COMPRESSION = OptionSpec('enable_compression')
ENABLE_DNS_SRV = OptionSpec('enable_dns_srv')
ENABLE_LAZY_CONNECTIONS = OptionSpec('enable_lazy_connections')
ENABLE_METRICS = OptionSpec('enable_metrics')
ENABLE_MUTATION_TOKENS = OptionSpec('enable_mutation_tokens')
ENABLE_ORPHAN_REPORTING = OptionSpec('enable_orphan_reporting')
ENABLE_TCP_KEEP_ALIVE = OptionSpec('enable_tcp_keep_alive')
ENABLE_TLS = OptionSpec('enable_tls')
ENABLE_TRACING = OptionSpec('enable_tracing')
ENABLE_UNORDERED_EXECUTION = OptionSpec('enable_unordered_execution')
IDLE_HTTP_CONNECTION_TIMEOUT = OptionSpec('idle_http_connection_timeout', duration=True)
IP_PROTOCOL = OptionSpec('ip_protocol')
KV_DURABLE_TIMEOUT = OptionSpec('kv_durable_timeout', duration=True)
KV_TIMEOUT = OptionSpec('kv_timeout', duration=True)
LOCKMODE = OptionSpec('lockmode', inert=frozenset({'classic'}), deprecated='It has no effect.')
LOGGING_METER_EMIT_INTERVAL = OptionSpec('logging_meter_emit_interval', deprecated='Use ClusterMetricsOptions instead.',
                                         duration=True)
LOG_REDACTION = OptionSpec('log_redaction', inert=frozenset({'classic'}))
MANAGEMENT_TIMEOUT = OptionSpec('management_timeout', duration=True)
MAX_HTTP_CONNECTIONS = OptionSpec('max_http_connections')
METER = OptionSpec('meter')
METRICS_OPTIONS = OptionSpec('metrics_options')
NETWORK = OptionSpec('network')
NUM_IO_THREADS = OptionSpec('num_io_threads', suggest=False)
OPTION_VALIDATION = OptionSpec('option_validation')
ORPHAN_REPORTING_OPTIONS = OptionSpec('orphan_reporting_options')
PREFERRED_SERVER_GROUP = OptionSpec('preferred_server_group')
QUERY_TIMEOUT = OptionSpec('query_timeout', duration=True)
REPORT_ID = OptionSpec('report_id')
RESOLVE_TIMEOUT = OptionSpec('resolve_timeout', duration=True)
SAMPLE_SIZE = OptionSpec('sample_size')
SEARCH_TIMEOUT = OptionSpec('search_timeout', duration=True)
SERVICE_TYPES = OptionSpec('service_types')
SHOW_QUERIES = OptionSpec('show_queries')
TCP_KEEP_ALIVE_INTERVAL = OptionSpec('tcp_keep_alive_interval', duration=True)
TIMEOUT_OPTIONS = OptionSpec('timeout_options')
TLS_VERIFY = OptionSpec('tls_verify')
TRACER = OptionSpec('tracer')
TRACING_OPTIONS = OptionSpec('tracing_options')
TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL = OptionSpec('tracing_orphaned_queue_flush_interval',
                                                   deprecated='Use ClusterOrphanReportingOptions instead.',
                                                   duration=True)
TRACING_ORPHANED_QUEUE_SIZE = OptionSpec('tracing_orphaned_queue_size',
                                         deprecated='Use ClusterOrphanReportingOptions instead.')
TRACING_THRESHOLD_ANALYTICS = OptionSpec('tracing_threshold_analytics', duration=True)
TRACING_THRESHOLD_EVENTING = OptionSpec('tracing_threshold_eventing', duration=True)
TRACING_THRESHOLD_KV = OptionSpec('tracing_threshold_kv', duration=True)
TRACING_THRESHOLD_MANAGEMENT = OptionSpec('tracing_threshold_management', duration=True)
TRACING_THRESHOLD_QUERY = OptionSpec('tracing_threshold_query', duration=True)
TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL = OptionSpec('tracing_threshold_queue_flush_interval', duration=True)
TRACING_THRESHOLD_QUEUE_SIZE = OptionSpec('tracing_threshold_queue_size')
TRACING_THRESHOLD_SEARCH = OptionSpec('tracing_threshold_search', duration=True)
TRACING_THRESHOLD_VIEW = OptionSpec('tracing_threshold_view', duration=True)
TRANSACTION_CONFIG = OptionSpec('transaction_config')
TRUST_STORE_PATH = OptionSpec('trust_store_path')
USER_AGENT_EXTRA = OptionSpec('user_agent_extra')
VIEWS_TIMEOUT = OptionSpec('views_timeout', duration=True)

# ==========================================================================================
# kv
# ==========================================================================================
ACCESS_DELETED = OptionSpec('access_deleted')
BATCH_BYTE_LIMIT = OptionSpec('batch_byte_limit')
BATCH_ITEM_LIMIT = OptionSpec('batch_item_limit')
BATCH_TIME_LIMIT = OptionSpec('batch_time_limit', duration=True)
CAS = OptionSpec('cas')
CONCURRENCY = OptionSpec('concurrency')
CONSISTENT_WITH = OptionSpec('consistent_with')
DELTA = OptionSpec('delta')
DURABILITY = OptionSpec('durability')
EXPIRY = OptionSpec('expiry', duration=True)
IDS_ONLY = OptionSpec('ids_only')
INITIAL = OptionSpec('initial')
LOCK_TIME = OptionSpec('lock_time', suggest=False, duration=True)
PER_KEY_OPTIONS = OptionSpec('per_key_options')
PRESERVE_EXPIRY = OptionSpec('preserve_expiry')
PROJECT = OptionSpec('project')
READ_PREFERENCE = OptionSpec('read_preference')
RETURN_EXCEPTIONS = OptionSpec('return_exceptions')
STORE_SEMANTICS = OptionSpec('store_semantics')
TRANSCODER = OptionSpec('transcoder')
WITH_EXPIRY = OptionSpec('with_expiry')

# ==========================================================================================
# streaming
# ==========================================================================================
ADHOC = OptionSpec('adhoc')
CLIENT_CONTEXT_ID = OptionSpec('client_context_id')
COLLECTIONS = OptionSpec('collections')
DEBUG = OptionSpec('debug')
DISABLE_SCORING = OptionSpec('disable_scoring', deprecated='Use scoring with ScoringNone instead.')
ENDKEY = OptionSpec('endkey')
ENDKEY_DOCID = OptionSpec('endkey_docid')
EXPLAIN = OptionSpec('explain')
FACETS = OptionSpec('facets')
FIELDS = OptionSpec('fields')
FLEX_INDEX = OptionSpec('flex_index')
FULL_SET = OptionSpec('full_set')
GROUP = OptionSpec('group')
GROUP_LEVEL = OptionSpec('group_level')
HIGHLIGHT_FIELDS = OptionSpec('highlight_fields')
HIGHLIGHT_STYLE = OptionSpec('highlight_style')
INCLUDE_LOCATIONS = OptionSpec('include_locations')
INCLUSIVE_END = OptionSpec('inclusive_end')
KEY = OptionSpec('key')
KEYS = OptionSpec('keys')
LIMIT = OptionSpec('limit')
LOG_REQUEST = OptionSpec('log_request')
LOG_RESPONSE = OptionSpec('log_response')
MAX_PARALLELISM = OptionSpec('max_parallelism')
METRICS = OptionSpec('metrics')
NAMED_PARAMETERS = OptionSpec('named_parameters')
NAMESPACE = OptionSpec('namespace')
ON_ERROR = OptionSpec('on_error')
ORDER = OptionSpec('order')
PIPELINE_BATCH = OptionSpec('pipeline_batch')
PIPELINE_CAP = OptionSpec('pipeline_cap')
POSITIONAL_PARAMETERS = OptionSpec('positional_parameters')
PRIORITY = OptionSpec('priority')
PROFILE = OptionSpec('profile')
QUERY_CONTEXT = OptionSpec('query_context')
QUERY_STRING = OptionSpec('query_string')
RAW = OptionSpec('raw')
READ_ONLY = OptionSpec('read_only')
REDUCE = OptionSpec('reduce')
SCAN_CAP = OptionSpec('scan_cap')
SCAN_CONSISTENCY = OptionSpec('scan_consistency')
SCAN_WAIT = OptionSpec('scan_wait', duration=True)
SCOPE = OptionSpec('scope')
SCORING = OptionSpec('scoring')
SEND_TO_NODE = OptionSpec('send_to_node')
SERIALIZER = OptionSpec('serializer')
SHOW_REQUEST = OptionSpec('show_request')
SKIP = OptionSpec('skip')
SORT = OptionSpec('sort')
STARTKEY = OptionSpec('startkey')
STARTKEY_DOCID = OptionSpec('startkey_docid')
STREAMING_SCOPE_NAME = OptionSpec('scope_name', deprecated='Use Scope.search() instead.')
USE_REPLICA = OptionSpec('use_replica')
VECTOR_QUERY_COMBINATION = OptionSpec('vector_query_combination')

# ==========================================================================================
# mgmt
# ==========================================================================================
BUCKET_TYPE = OptionSpec('bucket_type')
COLLECTION_NAME = OptionSpec('collection_name', deprecated='Use CollectionQueryIndexManager instead.')
COMPRESSION_MODE = OptionSpec('compression_mode')
CONDITION = OptionSpec('condition')
CONFLICT_RESOLUTION_TYPE = OptionSpec('conflict_resolution_type')
DATAVERSE_NAME = OptionSpec('dataverse_name')
DEFERRED = OptionSpec('deferred')
DOMAIN_NAME = OptionSpec('domain_name')
EVICTION_POLICY = OptionSpec('eviction_policy')
FLUSH_ENABLED = OptionSpec('flush_enabled')
FORCE = OptionSpec('force')
HISTORY_RETENTION_BYTES = OptionSpec('history_retention_bytes')
HISTORY_RETENTION_COLLECTION_DEFAULT = OptionSpec('history_retention_collection_default')
HISTORY_RETENTION_DURATION = OptionSpec('history_retention_duration', duration=True)
IGNORE_IF_EXISTS = OptionSpec('ignore_if_exists')
IGNORE_IF_NOT_EXISTS = OptionSpec('ignore_if_not_exists')
INDEX_NAME = OptionSpec('index_name')
LINK_NAME = OptionSpec('link_name')
LINK_TYPE = OptionSpec('link_type')
MAX_EXPIRY = OptionSpec('max_expiry')
MAX_TTL = OptionSpec('max_ttl')
MGMT_SCOPE_NAME = OptionSpec('scope_name', deprecated='Use CollectionQueryIndexManager instead.')
MINIMUM_DURABILITY_LEVEL = OptionSpec('minimum_durability_level')
NAME = OptionSpec('name')
NUM_REPLICAS = OptionSpec('num_replicas')
NUM_VBUCKETS = OptionSpec('num_vbuckets')
RAM_QUOTA_MB = OptionSpec('ram_quota_mb')
REPLICA_INDEX = OptionSpec('replica_index')
STORAGE_BACKEND = OptionSpec('storage_backend')
WATCH_PRIMARY = OptionSpec('watch_primary')

# ==========================================================================================
# txns
# ==========================================================================================
CLEANUP_CLIENT_ATTEMPTS = OptionSpec('cleanup_client_attempts')
CLEANUP_LOST_ATTEMPTS = OptionSpec('cleanup_lost_attempts')
CLEANUP_WINDOW = OptionSpec('cleanup_window', duration=True)
EXPIRATION_TIME = OptionSpec('expiration_time', deprecated='Use timeout instead.', into='timeout', duration=True)
METADATA_COLLECTION = OptionSpec('metadata_collection')
MODE = OptionSpec('mode')

SUPERSEDED: dict[str, OptionSpec] = {
    'span': SPAN,
}

BLOCKS: dict[str, BlockSpec] = {
    # ==========================================================================================
    # cluster
    # ==========================================================================================
    'ClusterMetricsOptions': block(
        replace(ENABLE_METRICS, stored='metrics_enable_metrics'),
        replace(EMIT_INTERVAL, stored='metrics_emit_interval'),
        refuses_unknown=True,
    ),
    'ClusterOptions': block(
        AUTHENTICATOR, OPTION_VALIDATION, ALLOWED_SASL_MECHANISMS, CERT_PATH, NUM_IO_THREADS,
        replace(SPAN, inert=frozenset({'classic'})), TRUST_STORE_PATH, TIMEOUT_OPTIONS,
        ORPHAN_REPORTING_OPTIONS, METRICS_OPTIONS, TRACING_OPTIONS, ENABLE_TLS, ENABLE_MUTATION_TOKENS,
        ENABLE_TCP_KEEP_ALIVE, IP_PROTOCOL, ENABLE_DNS_SRV, SHOW_QUERIES, ENABLE_UNORDERED_EXECUTION,
        ENABLE_CLUSTERMAP_NOTIFICATION, ENABLE_COMPRESSION, ENABLE_TRACING, ENABLE_METRICS,
        ENABLE_ORPHAN_REPORTING, NETWORK, TLS_VERIFY, SERIALIZER, TRANSCODER, TCP_KEEP_ALIVE_INTERVAL,
        CONFIG_POLL_INTERVAL, CONFIG_POLL_FLOOR, MAX_HTTP_CONNECTIONS, USER_AGENT_EXTRA,
        LOGGING_METER_EMIT_INTERVAL, TRANSACTION_CONFIG, LOG_REDACTION, COMPRESSION, COMPRESSION_MIN_SIZE,
        COMPRESSION_MIN_RATIO, LOCKMODE, TRACER, METER, DNS_NAMESERVER, DNS_PORT,
        DISABLE_MOZILLA_CA_CERTIFICATES, DUMP_CONFIGURATION, PREFERRED_SERVER_GROUP, ENABLE_APP_TELEMETRY,
        APP_TELEMETRY_ENDPOINT, APP_TELEMETRY_BACKOFF, APP_TELEMETRY_PING_INTERVAL, APP_TELEMETRY_PING_TIMEOUT,
        ALLOW_ENTERPRISE_ANALYTICS, ENABLE_LAZY_CONNECTIONS,
        flattened=(
            BOOTSTRAP_TIMEOUT, RESOLVE_TIMEOUT, CONNECT_TIMEOUT, KV_TIMEOUT, KV_DURABLE_TIMEOUT, VIEWS_TIMEOUT,
            QUERY_TIMEOUT, ANALYTICS_TIMEOUT, SEARCH_TIMEOUT, MANAGEMENT_TIMEOUT, DNS_SRV_TIMEOUT,
            IDLE_HTTP_CONNECTION_TIMEOUT, CONFIG_IDLE_REDIAL_TIMEOUT, CONFIG_TOTAL_TIMEOUT,
            replace(ENABLE_TRACING, stored='tracing_enable_tracing'), TRACING_THRESHOLD_KV,
            TRACING_THRESHOLD_VIEW, TRACING_THRESHOLD_QUERY, TRACING_THRESHOLD_SEARCH,
            TRACING_THRESHOLD_ANALYTICS, TRACING_THRESHOLD_EVENTING, TRACING_THRESHOLD_MANAGEMENT,
            TRACING_THRESHOLD_QUEUE_SIZE, TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL, TRACING_ORPHANED_QUEUE_SIZE,
            TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL, replace(ENABLE_METRICS, stored='metrics_enable_metrics'),
            replace(EMIT_INTERVAL, stored='metrics_emit_interval'),
            replace(ENABLE_ORPHAN_REPORTING, stored='orphan_enable_orphan_reporting'),
            replace(SAMPLE_SIZE, stored='orphan_sample_size'),
            replace(EMIT_INTERVAL, stored='orphan_emit_interval'),
        ),
        refuses_unknown=True,
    ),
    'ClusterOrphanReportingOptions': block(
        replace(ENABLE_ORPHAN_REPORTING, stored='orphan_enable_orphan_reporting'),
        replace(SAMPLE_SIZE, stored='orphan_sample_size'),
        replace(EMIT_INTERVAL, stored='orphan_emit_interval'),
        refuses_unknown=True,
    ),
    'ClusterTimeoutOptions': block(
        BOOTSTRAP_TIMEOUT, RESOLVE_TIMEOUT, CONNECT_TIMEOUT, KV_TIMEOUT, KV_DURABLE_TIMEOUT, VIEWS_TIMEOUT,
        QUERY_TIMEOUT, ANALYTICS_TIMEOUT, SEARCH_TIMEOUT, MANAGEMENT_TIMEOUT, DNS_SRV_TIMEOUT,
        IDLE_HTTP_CONNECTION_TIMEOUT, CONFIG_IDLE_REDIAL_TIMEOUT, CONFIG_TOTAL_TIMEOUT,
        refuses_unknown=True,
    ),
    'ClusterTracingOptions': block(
        replace(ENABLE_TRACING, stored='tracing_enable_tracing'), TRACING_THRESHOLD_KV, TRACING_THRESHOLD_VIEW,
        TRACING_THRESHOLD_QUERY, TRACING_THRESHOLD_SEARCH, TRACING_THRESHOLD_ANALYTICS,
        TRACING_THRESHOLD_EVENTING, TRACING_THRESHOLD_MANAGEMENT, TRACING_THRESHOLD_QUEUE_SIZE,
        TRACING_THRESHOLD_QUEUE_FLUSH_INTERVAL, TRACING_ORPHANED_QUEUE_SIZE,
        TRACING_ORPHANED_QUEUE_FLUSH_INTERVAL,
        refuses_unknown=True,
    ),
    'DiagnosticsOptions': block(
        REPORT_ID, TIMEOUT, replace(PARENT_SPAN, inert=frozenset({'classic'})),
    ),
    'PingOptions': block(
        TIMEOUT, REPORT_ID, SERVICE_TYPES, replace(PARENT_SPAN, inert=frozenset({'classic'})),
    ),
    'WaitUntilReadyOptions': block(
        DESIRED_STATE, SERVICE_TYPES, TIMEOUT, PARENT_SPAN,
    ),

    # ==========================================================================================
    # kv
    # ==========================================================================================
    'AppendMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, DURABILITY, CAS, PARENT_SPAN,
        refuses_unknown=True,
    ),
    'AppendOptions': block(
        TIMEOUT, DURABILITY, CAS, PARENT_SPAN, SPAN,
    ),
    'DecrementMultiOptions': block(
        DELTA, INITIAL, PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, DURABILITY, PARENT_SPAN,
        refuses_unknown=True,
    ),
    'DecrementOptions': block(
        DELTA, INITIAL, TIMEOUT, EXPIRY, DURABILITY, PARENT_SPAN, SPAN,
    ),
    'ExistsMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN,
        refuses_unknown=True,
    ),
    'ExistsOptions': block(
        TIMEOUT, PARENT_SPAN, SPAN,
    ),
    'GetAllReplicasMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, TRANSCODER, READ_PREFERENCE,
        refuses_unknown=True,
    ),
    'GetAllReplicasOptions': block(
        TIMEOUT, TRANSCODER, READ_PREFERENCE, PARENT_SPAN, SPAN,
    ),
    'GetAndLockMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, TRANSCODER, LOCK_TIME,
        refuses_unknown=True,
    ),
    'GetAndLockOptions': block(
        TIMEOUT, TRANSCODER, PARENT_SPAN, SPAN, LOCK_TIME,
    ),
    'GetAndTouchOptions': block(
        TIMEOUT, TRANSCODER, PARENT_SPAN, SPAN, replace(EXPIRY, suggest=False),
    ),
    'GetAnyReplicaMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, TRANSCODER, READ_PREFERENCE,
        refuses_unknown=True,
    ),
    'GetAnyReplicaOptions': block(
        TIMEOUT, TRANSCODER, READ_PREFERENCE, PARENT_SPAN, SPAN,
    ),
    'GetMultiOptions': block(
        PROJECT,
        replace(WITH_EXPIRY, inert=frozenset({'classic'}),
                deprecated='A multi get does not fetch the expiry, so it has no effect.'),
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, TRANSCODER,
        refuses_unknown=True,
    ),
    'GetOptions': block(
        PROJECT, WITH_EXPIRY, TIMEOUT, TRANSCODER, PARENT_SPAN, SPAN,
    ),
    'IncrementMultiOptions': block(
        DELTA, INITIAL, PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, DURABILITY, PARENT_SPAN,
        refuses_unknown=True,
    ),
    'IncrementOptions': block(
        DELTA, INITIAL, TIMEOUT, EXPIRY, DURABILITY, PARENT_SPAN, SPAN,
    ),
    'InsertMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, EXPIRY, DURABILITY, TRANSCODER,
        refuses_unknown=True,
    ),
    'InsertOptions': block(
        TIMEOUT, EXPIRY, DURABILITY, TRANSCODER, PARENT_SPAN, SPAN,
    ),
    'LookupInAllReplicasOptions': block(
        TIMEOUT, PARENT_SPAN, SPAN, SERIALIZER, READ_PREFERENCE,
    ),
    'LookupInAnyReplicaOptions': block(
        TIMEOUT, PARENT_SPAN, SPAN, SERIALIZER, READ_PREFERENCE,
    ),
    'LookupInOptions': block(
        TIMEOUT, ACCESS_DELETED, PARENT_SPAN, SPAN, SERIALIZER,
    ),
    'MutateInOptions': block(
        TIMEOUT, EXPIRY, CAS, DURABILITY, STORE_SEMANTICS, ACCESS_DELETED, PRESERVE_EXPIRY, PARENT_SPAN, SPAN,
        SERIALIZER,
    ),
    'PrependMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, DURABILITY, CAS, PARENT_SPAN,
        refuses_unknown=True,
    ),
    'PrependOptions': block(
        TIMEOUT, DURABILITY, CAS, PARENT_SPAN, SPAN,
    ),
    'RemoveMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, CAS, DURABILITY,
        replace(TRANSCODER, inert=frozenset({'classic'}), deprecated='It has no effect.'),
        refuses_unknown=True,
    ),
    'RemoveOptions': block(
        TIMEOUT, CAS, DURABILITY, PARENT_SPAN, SPAN,
    ),
    'ReplaceMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, EXPIRY, CAS, PRESERVE_EXPIRY, DURABILITY,
        TRANSCODER,
        refuses_unknown=True,
    ),
    'ReplaceOptions': block(
        TIMEOUT, EXPIRY, CAS, PRESERVE_EXPIRY, DURABILITY, TRANSCODER, PARENT_SPAN, SPAN,
    ),
    'ScanOptions': block(
        TIMEOUT, IDS_ONLY, CONSISTENT_WITH, BATCH_BYTE_LIMIT, BATCH_ITEM_LIMIT,
        replace(BATCH_TIME_LIMIT, inert=frozenset({'classic'})), TRANSCODER, CONCURRENCY,
        replace(PARENT_SPAN, inert=frozenset({'classic'})),
    ),
    'TouchMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, replace(EXPIRY, suggest=False),
        refuses_unknown=True,
    ),
    'TouchOptions': block(
        TIMEOUT, PARENT_SPAN, SPAN, replace(EXPIRY, suggest=False),
    ),
    'UnlockMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN,
        refuses_unknown=True,
    ),
    'UnlockOptions': block(
        TIMEOUT, PARENT_SPAN, SPAN, replace(CAS, suggest=False),
    ),
    'UpsertMultiOptions': block(
        PER_KEY_OPTIONS, RETURN_EXCEPTIONS, TIMEOUT, PARENT_SPAN, EXPIRY, PRESERVE_EXPIRY, DURABILITY,
        TRANSCODER,
        refuses_unknown=True,
    ),
    'UpsertOptions': block(
        TIMEOUT, EXPIRY, PRESERVE_EXPIRY, DURABILITY, TRANSCODER, PARENT_SPAN, SPAN,
    ),

    # ==========================================================================================
    # streaming
    # ==========================================================================================
    'AnalyticsOptions': block(
        CLIENT_CONTEXT_ID, RAW, METRICS, NAMED_PARAMETERS, POSITIONAL_PARAMETERS, READ_ONLY, TIMEOUT,
        SCAN_CONSISTENCY, PRIORITY, QUERY_CONTEXT, SERIALIZER, PARENT_SPAN, SPAN,
        free_keywords=True,
    ),
    'QueryOptions': block(
        CLIENT_CONTEXT_ID, RAW, ADHOC, MAX_PARALLELISM, PIPELINE_BATCH, PIPELINE_CAP, PROFILE, SCAN_CAP,
        SCAN_WAIT, METRICS, NAMED_PARAMETERS, POSITIONAL_PARAMETERS, READ_ONLY, TIMEOUT, SCAN_CONSISTENCY,
        QUERY_CONTEXT, FLEX_INDEX, PRESERVE_EXPIRY, USE_REPLICA, CONSISTENT_WITH, SEND_TO_NODE, PARENT_SPAN,
        SPAN, SERIALIZER,
        free_keywords=True,
    ),
    'SearchOptions': block(
        CLIENT_CONTEXT_ID, RAW, LIMIT, SKIP, TIMEOUT, EXPLAIN, FIELDS, HIGHLIGHT_STYLE, HIGHLIGHT_FIELDS,
        SCAN_CONSISTENCY, CONSISTENT_WITH, FACETS, SORT, DISABLE_SCORING, SCORING, STREAMING_SCOPE_NAME,
        COLLECTIONS, INCLUDE_LOCATIONS, SERIALIZER, SHOW_REQUEST, LOG_REQUEST, LOG_RESPONSE, PARENT_SPAN, SPAN,
    ),
    'VectorSearchOptions': block(
        VECTOR_QUERY_COMBINATION,
    ),
    'ViewOptions': block(
        CLIENT_CONTEXT_ID, RAW, LIMIT, SKIP, TIMEOUT, SCAN_CONSISTENCY, STARTKEY, ENDKEY, STARTKEY_DOCID,
        ENDKEY_DOCID, INCLUSIVE_END, GROUP, GROUP_LEVEL, KEY, KEYS, ORDER, REDUCE, ON_ERROR, DEBUG, NAMESPACE,
        QUERY_STRING, FULL_SET, PARENT_SPAN, SPAN, SERIALIZER,
    ),

    # ==========================================================================================
    # mgmt
    # ==========================================================================================
    'AllowQueryingSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'AnalyzeDocumentSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'BucketDescribeOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'BucketSettings': block(
        BUCKET_TYPE, COMPRESSION_MODE, EVICTION_POLICY, FLUSH_ENABLED, HISTORY_RETENTION_BYTES,
        HISTORY_RETENTION_COLLECTION_DEFAULT, HISTORY_RETENTION_DURATION, MAX_EXPIRY, MAX_TTL,
        MINIMUM_DURABILITY_LEVEL, NUM_VBUCKETS, RAM_QUOTA_MB, REPLICA_INDEX, STORAGE_BACKEND, NAME,
        NUM_REPLICAS,
    ),
    'BuildDeferredQueryIndexOptions': block(
        TIMEOUT, MGMT_SCOPE_NAME, COLLECTION_NAME, PARENT_SPAN,
    ),
    'ChangePasswordOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'ConnectLinkOptions': block(
        TIMEOUT, DATAVERSE_NAME, LINK_NAME, FORCE, PARENT_SPAN,
    ),
    'CreateAnalyticsIndexOptions': block(
        TIMEOUT, IGNORE_IF_EXISTS, DATAVERSE_NAME, PARENT_SPAN,
    ),
    'CreateBucketOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'CreateBucketSettings': block(
        BUCKET_TYPE, COMPRESSION_MODE, EVICTION_POLICY, FLUSH_ENABLED, HISTORY_RETENTION_BYTES,
        HISTORY_RETENTION_COLLECTION_DEFAULT, HISTORY_RETENTION_DURATION, MAX_EXPIRY, MAX_TTL,
        MINIMUM_DURABILITY_LEVEL, NUM_VBUCKETS, RAM_QUOTA_MB, REPLICA_INDEX, STORAGE_BACKEND, NAME,
        CONFLICT_RESOLUTION_TYPE, NUM_REPLICAS,
    ),
    'CreateCollectionOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'CreateDatasetOptions': block(
        TIMEOUT, IGNORE_IF_EXISTS, CONDITION, DATAVERSE_NAME, PARENT_SPAN,
    ),
    'CreateDataverseOptions': block(
        TIMEOUT, IGNORE_IF_EXISTS, PARENT_SPAN,
    ),
    'CreateLinkAnalyticsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'CreatePrimaryQueryIndexOptions': block(
        INDEX_NAME, TIMEOUT, IGNORE_IF_EXISTS, NUM_REPLICAS, DEFERRED, CONDITION, MGMT_SCOPE_NAME,
        COLLECTION_NAME, PARENT_SPAN,
    ),
    'CreateQueryIndexOptions': block(
        TIMEOUT, IGNORE_IF_EXISTS, NUM_REPLICAS, DEFERRED, CONDITION, MGMT_SCOPE_NAME, COLLECTION_NAME,
        PARENT_SPAN,
    ),
    'CreateScopeOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'DeployFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'DisallowQueryingSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'DisconnectLinkOptions': block(
        TIMEOUT, DATAVERSE_NAME, LINK_NAME, PARENT_SPAN,
    ),
    'DropAnalyticsIndexOptions': block(
        TIMEOUT, IGNORE_IF_NOT_EXISTS, DATAVERSE_NAME, PARENT_SPAN,
    ),
    'DropBucketOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'DropCollectionOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'DropDatasetOptions': block(
        TIMEOUT, IGNORE_IF_NOT_EXISTS, DATAVERSE_NAME, PARENT_SPAN,
    ),
    'DropDataverseOptions': block(
        TIMEOUT, IGNORE_IF_NOT_EXISTS, PARENT_SPAN,
    ),
    'DropDesignDocumentOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'DropFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'DropGroupOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'DropLinkAnalyticsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'DropPrimaryQueryIndexOptions': block(
        INDEX_NAME, IGNORE_IF_NOT_EXISTS, TIMEOUT, MGMT_SCOPE_NAME, COLLECTION_NAME, PARENT_SPAN,
    ),
    'DropQueryIndexOptions': block(
        IGNORE_IF_NOT_EXISTS, TIMEOUT, MGMT_SCOPE_NAME, COLLECTION_NAME, PARENT_SPAN,
    ),
    'DropScopeOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'DropSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'DropUserOptions': block(
        DOMAIN_NAME, TIMEOUT, PARENT_SPAN,
    ),
    'EventingFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'FlushBucketOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'FreezePlanSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'FunctionsStatusOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllAnalyticsIndexesOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllBucketOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'GetAllDatasetOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllDesignDocumentsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllGroupsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllQueryIndexOptions': block(
        TIMEOUT, MGMT_SCOPE_NAME, COLLECTION_NAME, PARENT_SPAN,
    ),
    'GetAllScopesOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'GetAllSearchIndexStatsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllSearchIndexesOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetAllUsersOptions': block(
        DOMAIN_NAME, TIMEOUT, PARENT_SPAN,
    ),
    'GetBucketOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'GetDesignDocumentOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetGroupOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetLinksAnalyticsOptions': block(
        TIMEOUT, DATAVERSE_NAME, NAME, LINK_TYPE, PARENT_SPAN,
    ),
    'GetPendingMutationsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetRolesOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetSearchIndexStatsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetSearchIndexedDocumentsCountOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'GetUserOptions': block(
        DOMAIN_NAME, TIMEOUT, PARENT_SPAN,
    ),
    'PauseFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'PauseIngestSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'PublishDesignDocumentOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'ReplaceLinkAnalyticsOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'ResumeFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'ResumeIngestSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'UndeployFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'UnfreezePlanSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'UpdateBucketOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'UpdateCollectionOptions': block(
        PARENT_SPAN, TIMEOUT,
    ),
    'UpsertDesignDocumentOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'UpsertFunctionOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'UpsertGroupOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'UpsertSearchIndexOptions': block(
        TIMEOUT, PARENT_SPAN,
    ),
    'UpsertUserOptions': block(
        DOMAIN_NAME, TIMEOUT, PARENT_SPAN,
    ),
    'WatchQueryIndexOptions': block(
        WATCH_PRIMARY, TIMEOUT, MGMT_SCOPE_NAME, COLLECTION_NAME, PARENT_SPAN,
    ),

    # ==========================================================================================
    # txns
    # ==========================================================================================
    'TransactionConfig': block(
        EXPIRATION_TIME, METADATA_COLLECTION, DURABILITY, CLEANUP_WINDOW,
        replace(KV_TIMEOUT, inert=frozenset({'classic'}),
                deprecated="It has no effect. Transactions use the cluster's kv_durable_timeout."),
        CLEANUP_LOST_ATTEMPTS, CLEANUP_CLIENT_ATTEMPTS, SCAN_CONSISTENCY, TIMEOUT,
        refuses_unknown=True,
    ),
    'TransactionGetMultiOptions': block(
        MODE,
    ),
    'TransactionGetMultiReplicasFromPreferredServerGroupOptions': block(
        MODE,
    ),
    'TransactionGetOptions': block(
        TRANSCODER,
    ),
    'TransactionGetReplicaFromPreferredServerGroupOptions': block(
        TRANSCODER,
    ),
    'TransactionInsertOptions': block(
        TRANSCODER,
    ),
    'TransactionOptions': block(
        EXPIRATION_TIME, METADATA_COLLECTION, DURABILITY,
        replace(KV_TIMEOUT, inert=frozenset({'classic'}),
                deprecated="It has no effect. Transactions use the cluster's kv_durable_timeout."),
        SCAN_CONSISTENCY, TIMEOUT,
        refuses_unknown=True,
    ),
    'TransactionQueryOptions': block(
        CLIENT_CONTEXT_ID, RAW, ADHOC, MAX_PARALLELISM, PIPELINE_BATCH, PIPELINE_CAP, PROFILE, SCAN_CAP,
        SCAN_WAIT, METRICS, NAMED_PARAMETERS, POSITIONAL_PARAMETERS, READ_ONLY, SCAN_CONSISTENCY, SCOPE,
        refuses_unknown=True,
    ),
    'TransactionReplaceOptions': block(
        TRANSCODER,
    ),
}
