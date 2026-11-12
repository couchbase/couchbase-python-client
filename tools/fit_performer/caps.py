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

from performer import caps_pb2 as performer_caps_pb
from sdk import caps_pb2 as sdk_caps_pb

PERFORMER_CAPS = [
    performer_caps_pb.KV_SUPPORT_1,
    performer_caps_pb.CLUSTER_CONFIG_CERT,
    performer_caps_pb.CLUSTER_CONFIG_INSECURE,
    performer_caps_pb.OBSERVABILITY_1,
]

SDK_CAPS = [
    sdk_caps_pb.SDK_KV,
    sdk_caps_pb.SDK_QUERY_INDEX_MANAGEMENT,
    sdk_caps_pb.SDK_SEARCH,
    sdk_caps_pb.SDK_SCOPE_SEARCH,
    sdk_caps_pb.SDK_SEARCH_INDEX_MANAGEMENT,
    sdk_caps_pb.SDK_QUERY,
    sdk_caps_pb.SDK_LOOKUP_IN,
    sdk_caps_pb.SDK_BUCKET_MANAGEMENT,
    sdk_caps_pb.SDK_COLLECTION_QUERY_INDEX_MANAGEMENT,
    sdk_caps_pb.SDK_KV_RANGE_SCAN,
    sdk_caps_pb.SDK_QUERY_READ_FROM_REPLICA,
    sdk_caps_pb.SDK_LOOKUP_IN_REPLICAS,
    sdk_caps_pb.SDK_MANAGEMENT_HISTORY_RETENTION,
    sdk_caps_pb.SDK_COLLECTION_MANAGEMENT,
    sdk_caps_pb.SDK_DOCUMENT_NOT_LOCKED,
    sdk_caps_pb.SDK_VECTOR_SEARCH,
    sdk_caps_pb.SDK_SCOPE_SEARCH_INDEX_MANAGEMENT,
    sdk_caps_pb.SDK_INDEX_MANAGEMENT_RFC_REVISION_25,
    sdk_caps_pb.SDK_SEARCH_RFC_REVISION_11,
    sdk_caps_pb.SDK_VECTOR_SEARCH_BASE64,
    sdk_caps_pb.SDK_ZONE_AWARE_READ_FROM_REPLICA,
    sdk_caps_pb.SDK_GET_REPLICA,
    sdk_caps_pb.SDK_APP_TELEMETRY,
    sdk_caps_pb.SDK_BUCKET_SETTINGS_NUM_VBUCKETS,
    sdk_caps_pb.SDK_PREFILTER_VECTOR_SEARCH,
    sdk_caps_pb.SUPPORTS_AUTHENTICATOR,
    sdk_caps_pb.SDK_SET_AUTHENTICATOR,
    sdk_caps_pb.SDK_OBSERVABILITY_RFC_REV_24,
    sdk_caps_pb.SDK_OBSERVABILITY_CLUSTER_LABELS,
    sdk_caps_pb.SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS,
    sdk_caps_pb.SDK_STABLE_OTEL_SEMANTIC_CONVENTIONS_EMITTED_BY_DEFAULT,
    sdk_caps_pb.SDK_JWT,
    sdk_caps_pb.SDK_QUERY_2120,
    sdk_caps_pb.SDK_SEARCH_SCORE_FUSION,
    sdk_caps_pb.SDK_EVENTING_FUNCTION_MANAGER,
    sdk_caps_pb.SDK_EVENTING_RESUMING_FUNCTION_STATUS,
    sdk_caps_pb.SDK_SCOPE_EVENTING_FUNCTION_MANAGER,
]
