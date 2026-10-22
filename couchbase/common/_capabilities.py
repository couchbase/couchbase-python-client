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

"""The features a protocol may or may not provide, and the one call a facade makes to reach one.

Each protocol declares its set in its own `_capabilities.py`.  A facade reaches a capability method
through `require()`, which raises `FeatureUnavailableException` when the impl's protocol did not
declare it and otherwise returns the impl typed as that capability's interface.  Adding a capability
adds a member here, its interfaces in `couchbase.common._interfaces`, and one overload per tree.
"""

from __future__ import annotations

from enum import Enum
from typing import (TYPE_CHECKING,
                    Literal,
                    overload)

from couchbase.common.exceptions import FeatureUnavailableException

if TYPE_CHECKING:
    from couchbase.common._interfaces._collection import CollectionInterface, SupportsLegacyDatastructures
    from couchbase.common._interfaces._transactions import (AsyncAttemptContextInterface,
                                                            AsyncSupportsTransactionsPreferredServerGroup,
                                                            AsyncSupportsTransactionsQuery,
                                                            AttemptContextInterface,
                                                            SupportsTransactionsPreferredServerGroup,
                                                            SupportsTransactionsQuery)


class Capability(Enum):
    TRANSACTIONS_QUERY = 'transactions_query'
    TRANSACTIONS_PREFERRED_SERVER_GROUP = 'transactions_preferred_server_group'
    LEGACY_DATASTRUCTURES = 'legacy_datastructures'


@overload
def require(impl: CollectionInterface,
            capability: Literal[Capability.LEGACY_DATASTRUCTURES]) -> SupportsLegacyDatastructures: ...


@overload
def require(impl: AttemptContextInterface,
            capability: Literal[Capability.TRANSACTIONS_QUERY]) -> SupportsTransactionsQuery: ...


@overload
def require(impl: AttemptContextInterface,
            capability: Literal[Capability.TRANSACTIONS_PREFERRED_SERVER_GROUP]
            ) -> SupportsTransactionsPreferredServerGroup: ...


@overload
def require(impl: AsyncAttemptContextInterface,
            capability: Literal[Capability.TRANSACTIONS_QUERY]) -> AsyncSupportsTransactionsQuery: ...


@overload
def require(impl: AsyncAttemptContextInterface,
            capability: Literal[Capability.TRANSACTIONS_PREFERRED_SERVER_GROUP]
            ) -> AsyncSupportsTransactionsPreferredServerGroup: ...


def require(impl: CollectionInterface | AttemptContextInterface | AsyncAttemptContextInterface,
            capability: Capability) -> object:
    if capability not in impl.capabilities:
        raise FeatureUnavailableException(f'{capability.value} is not available on this connection.')
    return impl
