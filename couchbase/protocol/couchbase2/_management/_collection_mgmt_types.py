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

"""The requests a collection manager's builder returns, and the scopes the gateway's listing becomes.

The gateway and the SDK spell a collection's maximum expiry differently.  The SDK's zero is the
bucket's own maximum and its -1 is never to expire; the gateway leaves the field out for the
bucket's maximum and reads zero as never.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Generic,
                    TypeVar)

from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.management.collections import CollectionSpec, ScopeSpec
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.couchbase2._management._mgmt_errors import unavailable_when

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler
    from couchbase.protocol.couchbase2._generated.admin.collection.v1 import collection_pb2

M = TypeVar('M')


class CollectionMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.COUCHBASE2):
    CreateCollection = 'CreateCollectionOptions'
    CreateScope = 'CreateScopeOptions'
    DropCollection = 'DropCollectionOptions'
    DropScope = 'DropScopeOptions'
    GetAllScopes = 'GetAllScopesOptions'
    UpdateCollection = 'UpdateCollectionOptions'


@dataclass(frozen=True)
class CollectionMgmtRequest(Generic[M]):
    message: M
    timeout: float
    obs_handler: ObservableRequestHandler | None


@contextmanager
def history_unavailable() -> Iterator[None]:
    """A collection's history asked of a bucket that keeps none, such as a couchstore one, is unavailable."""
    with unavailable_when('not allowed on this type of bucket'):
        yield


def scope_specs(response: collection_pb2.ListCollectionsResponse) -> list[ScopeSpec]:
    return [ScopeSpec(s.name, [_collection_spec(c, s.name) for c in s.collections]) for s in response.scopes]


def _collection_spec(collection: collection_pb2.ListCollectionsResponse.Collection,
                     scope_name: str) -> CollectionSpec:
    if not collection.HasField('max_expiry_secs'):
        max_expiry = timedelta(0)
    elif collection.max_expiry_secs == 0:
        max_expiry = timedelta(seconds=-1)
    else:
        max_expiry = timedelta(seconds=collection.max_expiry_secs)
    history = collection.history_retention_enabled if collection.HasField('history_retention_enabled') else None
    return CollectionSpec(collection.name, scope_name, max_expiry, history=history)
