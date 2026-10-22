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

from typing import TYPE_CHECKING

from couchbase.common._capabilities import Capability

if TYPE_CHECKING:
    from acouchbase.protocol.classic._transactions_impl import AsyncAttemptContextImpl
    from couchbase.common._interfaces._collection import SupportsLegacyDatastructures
    from couchbase.common._interfaces._transactions import (AsyncSupportsTransactionsPreferredServerGroup,
                                                            AsyncSupportsTransactionsQuery,
                                                            SupportsTransactionsPreferredServerGroup,
                                                            SupportsTransactionsQuery)
    from couchbase.protocol.classic._collection_impl import CollectionImpl
    from couchbase.protocol.classic._transactions_impl import AttemptContextImpl

CAPABILITIES = frozenset({
    Capability.TRANSACTIONS_QUERY,
    Capability.TRANSACTIONS_PREFERRED_SERVER_GROUP,
    Capability.LEGACY_DATASTRUCTURES,
})

if TYPE_CHECKING:
    # mypy checks each declared capability against the impls here; nothing else assigns an impl to one.
    _sync: AttemptContextImpl
    _aio: AsyncAttemptContextImpl
    _query: SupportsTransactionsQuery = _sync
    _server_group: SupportsTransactionsPreferredServerGroup = _sync
    _aio_query: AsyncSupportsTransactionsQuery = _aio
    _aio_server_group: AsyncSupportsTransactionsPreferredServerGroup = _aio
    _collection: CollectionImpl
    _legacy_datastructures: SupportsLegacyDatastructures = _collection
