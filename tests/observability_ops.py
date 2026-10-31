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

`Op` is a flat enum of every operation the SDK performs, and the validators name the operation
under test by its member.  Both validators need to sort those members into groups, and they need
to agree: the metrics suites and the tracing suites assert on the same `service` attribute,
produced by the same SDK code, so a table that lived in one of them and was copied into the
other would be a drift waiting to happen.  It sits here instead, imported by both.
"""

from __future__ import annotations

from couchbase.common._operations import Op, ServiceType

# Operations that encode a value on the way out, and so produce an encoding span.
ENCODING_OPS = frozenset({Op.Insert, Op.MutateIn, Op.Replace, Op.Upsert})

# The datastructure operations.  Each is a facade over one or more key-value sub-operations,
# and gets a span of its own with those sub-operations underneath it.
DS_OPS = frozenset({
    Op.ListAppend,
    Op.ListClear,
    Op.ListGetAll,
    Op.ListGetAt,
    Op.ListIndexOf,
    Op.ListPrepend,
    Op.ListRemoveAt,
    Op.ListSetAt,
    Op.ListSize,
    Op.MapAdd,
    Op.MapClear,
    Op.MapExists,
    Op.MapGet,
    Op.MapGetAll,
    Op.MapItems,
    Op.MapKeys,
    Op.MapRemove,
    Op.MapSize,
    Op.MapValues,
    Op.QueueClear,
    Op.QueuePop,
    Op.QueuePush,
    Op.QueueSize,
    Op.SetAdd,
    Op.SetClear,
    Op.SetContains,
    Op.SetRemove,
    Op.SetSize,
    Op.SetValues,
})

# The datastructure operations that take something out.  Their `mutate_in` carries a remove
# spec, which encodes nothing, so the sub-operation has no encoding span where every other
# datastructure mutation has one.
DS_REMOVE_OPS = frozenset({Op.ListRemoveAt, Op.MapRemove, Op.QueuePop, Op.SetRemove})

# Which service an http operation is labelled with.  A query is a query; a *manager* operation
# is labelled with the service it manages, and the bucket, collection and user managers all
# answer to the management service.  Derived here rather than restated by each of the sixty-odd
# management tests, and asserted against what the SDK actually recorded.
_SERVICE_BY_OP = {
    Op.AnalyticsQuery: ServiceType.Analytics,
    Op.Query: ServiceType.Query,
    Op.SearchQuery: ServiceType.Search,
    Op.ViewQuery: ServiceType.Views,
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


def span_name(op: Op | str) -> str:
    """The name a span carries: the operation's, or as given for a span the core names itself."""
    if isinstance(op, str):
        return op
    assert op.span_name is not None, f'{op} is never traced or metered'
    return op.span_name


def service_for(op_name: Op) -> ServiceType:
    service = _SERVICE_BY_OP.get(op_name)
    if service is not None:
        return service
    for prefix, prefixed_service in _SERVICE_BY_MANAGER_PREFIX.items():
        if span_name(op_name).startswith(prefix):
            return prefixed_service
    raise AssertionError(
        f'{span_name(op_name)!r} is not an http operation this validator knows a service for; '
        'add it to the tables in tests/observability_ops.py'
    )
