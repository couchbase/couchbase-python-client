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

"""What the SDK's operation names mean, for the observability suites.

`OpName` is a flat enum of every operation the SDK labels a metric or a span with.  Both
validators need to sort those names into groups, and they need to agree: the metrics suites and
the tracing suites assert on the same `service` attribute, produced by the same SDK code, so a
table that lived in one of them and was copied into the other would be a drift waiting to
happen.  It sits here instead, imported by both.
"""

from __future__ import annotations

from couchbase.logic.observability import OpName, ServiceType

# Operations that encode a value on the way out, and so produce an encoding span.
ENCODING_OPS = frozenset({OpName.Insert, OpName.MutateIn, OpName.Replace, OpName.Upsert})

# The datastructure operations.  Each is a facade over one or more key-value sub-operations,
# and gets a span of its own with those sub-operations underneath it.
DS_OPS = frozenset({
    OpName.ListAppend,
    OpName.ListClear,
    OpName.ListGetAll,
    OpName.ListGetAt,
    OpName.ListIndexOf,
    OpName.ListPrepend,
    OpName.ListRemoveAt,
    OpName.ListSetAt,
    OpName.ListSize,
    OpName.MapAdd,
    OpName.MapClear,
    OpName.MapExists,
    OpName.MapGet,
    OpName.MapGetAll,
    OpName.MapItems,
    OpName.MapKeys,
    OpName.MapRemove,
    OpName.MapSize,
    OpName.MapValues,
    OpName.QueueClear,
    OpName.QueuePop,
    OpName.QueuePush,
    OpName.QueueSize,
    OpName.SetAdd,
    OpName.SetClear,
    OpName.SetContains,
    OpName.SetRemove,
    OpName.SetSize,
    OpName.SetValues,
})

# The datastructure operations that take something out.  Their `mutate_in` carries a remove
# spec, which encodes nothing, so the sub-operation has no encoding span where every other
# datastructure mutation has one.
DS_REMOVE_OPS = frozenset({OpName.ListRemoveAt, OpName.MapRemove, OpName.QueuePop, OpName.SetRemove})

# Which service an http operation is labelled with.  A query is a query; a *manager* operation
# is labelled with the service it manages, and the bucket, collection and user managers all
# answer to the management service.  Derived here rather than restated by each of the sixty-odd
# management tests, and asserted against what the SDK actually recorded.
_SERVICE_BY_OP = {
    OpName.AnalyticsQuery: ServiceType.Analytics,
    OpName.Query: ServiceType.Query,
    OpName.SearchQuery: ServiceType.Search,
    OpName.ViewQuery: ServiceType.Views,
}

_SERVICE_BY_MANAGER_PREFIX = {
    'manager_analytics': ServiceType.Analytics,
    'manager_buckets': ServiceType.Management,
    'manager_collections': ServiceType.Management,
    'manager_eventing': ServiceType.Eventing,
    'manager_query': ServiceType.Query,
    'manager_search': ServiceType.Search,
    'manager_users': ServiceType.Management,
    'manager_views': ServiceType.Views,
}


def service_for(op_name: OpName) -> ServiceType:
    service = _SERVICE_BY_OP.get(op_name)
    if service is not None:
        return service
    for prefix, prefixed_service in _SERVICE_BY_MANAGER_PREFIX.items():
        if op_name.value.startswith(prefix):
            return prefixed_service
    raise AssertionError(
        f'{op_name.value!r} is not an http operation this validator knows a service for; '
        'add it to the tables in tests/observability_ops.py'
    )
