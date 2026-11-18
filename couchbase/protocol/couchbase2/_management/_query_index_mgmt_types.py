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

"""The requests a query index manager's builder returns, and the indexes the gateway's listing becomes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Generic,
                    TypeVar)

from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.exceptions import CouchbaseException
from couchbase.common.management.queries import QueryIndex
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler

M = TypeVar('M')

_TYPES = {query_pb2.INDEX_TYPE_VIEW: 'view', query_pb2.INDEX_TYPE_GSI: 'gsi'}

_STATES = {
    query_pb2.INDEX_STATE_DEFERRED: 'deferred',
    query_pb2.INDEX_STATE_BUILDING: 'building',
    query_pb2.INDEX_STATE_PENDING: 'pending',
    query_pb2.INDEX_STATE_ONLINE: 'online',
    query_pb2.INDEX_STATE_OFFLINE: 'offline',
    query_pb2.INDEX_STATE_ABRIDGED: 'abridged',
    query_pb2.INDEX_STATE_SCHEDULED: 'scheduled',
}


class QueryIndexMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.COUCHBASE2):
    BuildDeferredQueryIndex = 'BuildDeferredQueryIndexOptions'
    CreatePrimaryQueryIndex = 'CreatePrimaryQueryIndexOptions'
    CreateQueryIndex = 'CreateQueryIndexOptions'
    DropPrimaryQueryIndex = 'DropPrimaryQueryIndexOptions'
    DropQueryIndex = 'DropQueryIndexOptions'
    GetAllQueryIndex = 'GetAllQueryIndexOptions'
    WatchQueryIndex = 'WatchQueryIndexOptions'


@dataclass(frozen=True)
class QueryIndexMgmtRequest(Generic[M]):
    message: M
    timeout: float
    obs_handler: ObservableRequestHandler | None


@dataclass(frozen=True)
class WatchIndexesRequest:
    """One wait for each watched index, in turn, and how long the watch waits in all."""
    waits: list[query_pb2.WaitForIndexOnlineRequest]
    timeout: float


def still_deferred(exc: CouchbaseException) -> bool:
    """Whether `exc` is the gateway refusing to wait on an index because it has not been built.

    The SDK has no exception for it, so it arrives as a plain `CouchbaseException` naming the violation.
    """
    return exc.error_context._get_base().get('precondition_violation') == 'NOT_BUILDING'


def query_indexes(response: query_pb2.GetAllIndexesResponse) -> list[QueryIndex]:
    return [_query_index(index) for index in response.indexes]


def _query_index(index: query_pb2.GetAllIndexesResponse.Index) -> QueryIndex:
    """The gateway reports no namespace or datastore.

    A scope or collection name has no presence, so an empty one is an index on the bucket itself.
    The condition and partition do, so an index without one is told apart from an empty one.
    """
    scope_name = index.scope_name or None
    collection_name = index.collection_name or None
    return QueryIndex(index.name,
                      index.is_primary,
                      _TYPES.get(index.type, 'unknown'),
                      _STATES.get(index.state, 'unknown'),
                      None,
                      None,
                      collection_name or index.bucket_name,
                      list(index.fields),
                      index.condition if index.HasField('condition') else None,
                      index.bucket_name,
                      scope_name,
                      collection_name,
                      index.partition if index.HasField('partition') else None)
