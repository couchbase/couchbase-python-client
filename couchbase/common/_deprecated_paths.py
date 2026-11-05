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

"""Each deprecated import path: the module it serves the name from, and what to do instead.

Rendered from the compat entries in ``tools/public_api.yaml`` by
``python tools/check_public_api.py --write``; do not edit.
"""

PATHS: dict[str, dict[str, tuple[str, str]]] = {
    'acouchbase.analytics': {
        'AnalyticsQuery': ('couchbase.protocol.classic._analytics', 'Nothing replaces it: internal to the SDK.'),
    },
    'acouchbase.n1ql': {
        'N1QLQuery': ('couchbase.protocol.classic._n1ql', 'Nothing replaces it: internal to the SDK.'),
    },
    'acouchbase.options': {
        'forward_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
        'get_valid_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
        'get_valid_multi_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
    },
    'acouchbase.search': {
        'SearchQueryBuilder': ('couchbase.protocol.classic._search', 'Nothing replaces it: internal to the SDK.'),
    },
    'acouchbase.subdocument': {
        'ArrayValues': ('couchbase.common.subdocument', 'Nothing replaces it: internal to the SDK.'),
        'SubDocOp': ('couchbase.common.subdocument', 'Nothing replaces it: internal to the SDK.'),
    },
    'acouchbase.views': {
        'ViewQuery': ('couchbase.protocol.classic._views', 'Nothing replaces it: internal to the SDK.'),
    },
    'couchbase.analytics': {
        'AnalyticsOptions': ('couchbase.options', 'Import AnalyticsOptions from couchbase.options instead.'),
        'AnalyticsQuery': ('couchbase.protocol.classic._analytics', 'Nothing replaces it: internal to the SDK.'),
    },
    'couchbase.bucket': {
        'PingOptions': ('couchbase.options', 'Import PingOptions from couchbase.options instead.'),
        'ViewErrorMode': ('couchbase.views', 'Import ViewErrorMode from couchbase.views instead.'),
        'ViewOptions': ('couchbase.options', 'Import ViewOptions from couchbase.options instead.'),
        'ViewOrdering': ('couchbase.views', 'Import ViewOrdering from couchbase.views instead.'),
        'ViewScanConsistency': ('couchbase.views', 'Import ViewScanConsistency from couchbase.views instead.'),
    },
    'couchbase.cluster': {
        'ClusterOptions': ('couchbase.options', 'Import ClusterOptions from couchbase.options instead.'),
        'ClusterTimeoutOptions': ('couchbase.options', 'Import ClusterTimeoutOptions from couchbase.options instead.'),
        'ClusterTracingOptions': ('couchbase.options', 'Import ClusterTracingOptions from couchbase.options instead.'),
        'Compression': ('couchbase.options', 'Import Compression from couchbase.options instead.'),
        'DiagnosticsOptions': ('couchbase.options', 'Import DiagnosticsOptions from couchbase.options instead.'),
        'QueryOptions': ('couchbase.options', 'Import QueryOptions from couchbase.options instead.'),
        'QueryProfile': ('couchbase.n1ql', 'Import QueryProfile from couchbase.n1ql instead.'),
        'QueryScanConsistency': ('couchbase.n1ql', 'Import QueryScanConsistency from couchbase.n1ql instead.'),
    },
    'couchbase.collection': {
        'AppendOptions': ('couchbase.options', 'Import AppendOptions from couchbase.options instead.'),
        'CBCollection': ('couchbase._legacy', 'Import Collection from couchbase.collection instead.'),
        'ConstrainedInt': ('couchbase.options', 'Import ConstrainedInt from couchbase.options instead.'),
        'DecrementOptions': ('couchbase.options', 'Import DecrementOptions from couchbase.options instead.'),
        'DeltaValue': ('couchbase.options', 'Import DeltaValue from couchbase.options instead.'),
        'ExistsOptions': ('couchbase.options', 'Import ExistsOptions from couchbase.options instead.'),
        'GetAllReplicasOptions': ('couchbase.options', 'Import GetAllReplicasOptions from couchbase.options instead.'),
        'GetAndLockOptions': ('couchbase.options', 'Import GetAndLockOptions from couchbase.options instead.'),
        'GetAndTouchOptions': ('couchbase.options', 'Import GetAndTouchOptions from couchbase.options instead.'),
        'GetAnyReplicaOptions': ('couchbase.options', 'Import GetAnyReplicaOptions from couchbase.options instead.'),
        'GetOptions': ('couchbase.options', 'Import GetOptions from couchbase.options instead.'),
        'IncrementOptions': ('couchbase.options', 'Import IncrementOptions from couchbase.options instead.'),
        'InsertOptions': ('couchbase.options', 'Import InsertOptions from couchbase.options instead.'),
        'LookupInOptions': ('couchbase.options', 'Import LookupInOptions from couchbase.options instead.'),
        'PrependOptions': ('couchbase.options', 'Import PrependOptions from couchbase.options instead.'),
        'RemoveOptions': ('couchbase.options', 'Import RemoveOptions from couchbase.options instead.'),
        'ReplaceOptions': ('couchbase.options', 'Import ReplaceOptions from couchbase.options instead.'),
        'Scope': ('couchbase.scope', 'Import Scope from couchbase.scope instead.'),
        'SignedInt64': ('couchbase.options', 'Import SignedInt64 from couchbase.options instead.'),
        'TouchOptions': ('couchbase.options', 'Import TouchOptions from couchbase.options instead.'),
        'UnlockOptions': ('couchbase.options', 'Import UnlockOptions from couchbase.options instead.'),
        'UpsertOptions': ('couchbase.options', 'Import UpsertOptions from couchbase.options instead.'),
    },
    'couchbase.metrics': {
        'CouchbaseMeter': ('couchbase.common._metrics', 'Import Meter from couchbase.observability.metrics instead.'),
        'CouchbaseValueRecorder': (
            'couchbase.common._metrics',
            'Import ValueRecorder from couchbase.observability.metrics instead.',
        ),
    },
    'couchbase.mutation_state': {
        'MutationToken': ('couchbase.result', 'Import MutationToken from couchbase.result instead.'),
    },
    'couchbase.n1ql': {
        'N1QLQuery': ('couchbase.protocol.classic._n1ql', 'Nothing replaces it: internal to the SDK.'),
    },
    'couchbase.options': {
        'DefaultJsonSerializer': (
            'couchbase.serializer',
            'Import DefaultJsonSerializer from couchbase.serializer instead.',
        ),
        'DurabilityParser': ('couchbase.common._durability', 'Nothing replaces it: internal to the SDK.'),
        'InvalidArgumentException': (
            'couchbase.exceptions',
            'Import InvalidArgumentException from couchbase.exceptions instead.',
        ),
        'Supportability': ('couchbase.common._supportability', 'Nothing replaces it: internal to the SDK.'),
        'forward_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
        'get_valid_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
        'get_valid_multi_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
        'timedelta_as_milliseconds': ('couchbase.common._transforms', 'Nothing replaces it: internal to the SDK.'),
        'timedelta_as_timestamp': ('couchbase.common._transforms', 'Nothing replaces it: internal to the SDK.'),
        'validate_int': ('couchbase.common._transforms', 'Nothing replaces it: internal to the SDK.'),
    },
    'couchbase.result': {
        'HttpResult': ('couchbase._legacy', 'Nothing replaces it: nothing in the SDK returns one.'),
    },
    'couchbase.search': {
        'NoChildrenException': (
            'couchbase.exceptions',
            'Import NoChildrenException from couchbase.exceptions instead.',
        ),
        'SearchOptions': ('couchbase.options', 'Import SearchOptions from couchbase.options instead.'),
        'SearchQueryBuilder': ('couchbase.protocol.classic._search', 'Nothing replaces it: internal to the SDK.'),
    },
    'couchbase.subdocument': {
        'ArrayValues': ('couchbase.common.subdocument', 'Nothing replaces it: internal to the SDK.'),
        'MutateInOptions': ('couchbase.options', 'Import MutateInOptions from couchbase.options instead.'),
        'SubDocOp': ('couchbase.common.subdocument', 'Nothing replaces it: internal to the SDK.'),
    },
    'couchbase.tracing': {
        'CouchbaseSpan': (
            'couchbase.common._tracing',
            'Import RequestSpan from couchbase.observability.tracing instead.',
        ),
        'CouchbaseTracer': (
            'couchbase.common._tracing',
            'Import RequestTracer from couchbase.observability.tracing instead.',
        ),
    },
    'couchbase.transcoder': {
        'DefaultJsonSerializer': (
            'couchbase.serializer',
            'Import DefaultJsonSerializer from couchbase.serializer instead.',
        ),
        'FMT_BYTES': ('couchbase.constants', 'Import FMT_BYTES from couchbase.constants instead.'),
        'FMT_COMMON_MASK': ('couchbase.constants', 'Import FMT_COMMON_MASK from couchbase.constants instead.'),
        'FMT_JSON': ('couchbase.constants', 'Import FMT_JSON from couchbase.constants instead.'),
        'FMT_LEGACY_MASK': ('couchbase.constants', 'Import FMT_LEGACY_MASK from couchbase.constants instead.'),
        'FMT_PICKLE': ('couchbase.constants', 'Import FMT_PICKLE from couchbase.constants instead.'),
        'FMT_UTF8': ('couchbase.constants', 'Import FMT_UTF8 from couchbase.constants instead.'),
    },
    'couchbase.views': {
        'ViewQuery': ('couchbase.protocol.classic._views', 'Nothing replaces it: internal to the SDK.'),
    },
    'txcouchbase.options': {
        'forward_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
        'get_valid_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
        'get_valid_multi_args': ('couchbase.protocol.classic._wire', 'Nothing replaces it: internal to the SDK.'),
    },
    'txcouchbase.subdocument': {
        'ArrayValues': ('couchbase.common.subdocument', 'Nothing replaces it: internal to the SDK.'),
        'SubDocOp': ('couchbase.common.subdocument', 'Nothing replaces it: internal to the SDK.'),
    },
}
