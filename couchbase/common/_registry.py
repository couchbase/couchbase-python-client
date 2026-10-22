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

"""Which cluster impl a cluster facade constructs, chosen by protocol.

A facade never names a protocol's impl.  A cluster facade names an `ImplKey` declared here and asks
`resolve()` for the class the protocol registered against it, and every impl below the cluster is built
by the impl above it, so adding a protocol is one registration, in that protocol's own package, and no
facade changes.  A registration names modules rather than importing them, so resolving an impl imports
only that impl's module, and only once a cluster facade is constructed.
"""

from __future__ import annotations

from importlib import import_module
from typing import (TYPE_CHECKING,
                    Generic,
                    TypeVar,
                    cast)

from couchbase.common.exceptions import FeatureUnavailableException
from couchbase.common.protocol import CouchbaseProtocol

if TYPE_CHECKING:
    from couchbase.common._interfaces._cluster import (AsyncClusterInterface,
                                                       ClusterInterface,
                                                       TxClusterInterface)

T = TypeVar('T')


class ImplKey(Generic[T]):
    """**INTERNAL**

    Names one impl role; its type parameter is the interface the class `resolve()` returns provides.
    """

    __slots__ = ('name',)

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f'ImplKey({self.name!r})'


CLUSTER: ImplKey[ClusterInterface] = ImplKey('Cluster')
ASYNC_CLUSTER: ImplKey[AsyncClusterInterface] = ImplKey('AsyncCluster')
TX_CLUSTER: ImplKey[TxClusterInterface] = ImplKey('TxCluster')

# Every protocol's impls raise `RuntimeError` with this text for an operation on a closed cluster.
CLOSED_CLUSTER_MSG = 'Cannot perform operations on a closed cluster. Create a new cluster instance to reconnect.'

# Each protocol's registration module holds `IMPLS`, mapping an `ImplKey`'s name to `module:Class`.
_REGISTRATIONS: dict[CouchbaseProtocol, str] = {
    CouchbaseProtocol.CLASSIC: 'couchbase.protocol.classic._registry',
}


def registered_protocols() -> frozenset[CouchbaseProtocol]:
    """**INTERNAL**"""
    return frozenset(_REGISTRATIONS)


def resolve(key: ImplKey[T], protocol: CouchbaseProtocol) -> type[T]:
    """**INTERNAL**

    Raises:
        :class:`~couchbase.exceptions.FeatureUnavailableException`: If `protocol` registered nothing,
            or registered nothing against `key`.
    """
    registration = _REGISTRATIONS.get(protocol)
    if registration is None:
        raise FeatureUnavailableException(f'The {protocol.name.lower()} protocol is not available.')
    impls: dict[str, str] = import_module(registration).IMPLS
    target = impls.get(key.name)
    if target is None:
        raise FeatureUnavailableException(f'{key.name} is not available over the {protocol.name.lower()} protocol.')
    module, _, name = target.partition(':')
    return cast('type[T]', getattr(import_module(module), name))
