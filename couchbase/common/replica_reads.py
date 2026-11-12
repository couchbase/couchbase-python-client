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

"""The read preference for replica reads, and the strategy that selects the replica ``get_replica`` reads."""

from __future__ import annotations

__all__ = [
    'GetReplicaStrategy',
    'GetReplicaStrategyFromIndex',
    'ReadPreference',
    'ReplicaIndex',
]

from enum import IntEnum

from couchbase.common.exceptions import InvalidArgumentException


class ReadPreference(IntEnum):
    """Which replicas a replica read may read from.

    NO_PREFERENCE reads from any replica. SELECTED_SERVER_GROUP reads only from nodes in the server group the
    ``preferred_server_group`` cluster option names.
    """

    NO_PREFERENCE = 0
    SELECTED_SERVER_GROUP = 1


class ReplicaIndex(IntEnum):
    """The position of a replica in a document's replica chain."""

    FIRST = 0
    SECOND = 1
    THIRD = 2


class GetReplicaStrategy:
    """Selects the replica that ``get_replica`` reads from.

    Create one with :meth:`from_index`.
    """

    @staticmethod
    def from_index(replica_index: ReplicaIndex, wrap: bool = False) -> GetReplicaStrategyFromIndex:
        """Read from the replica at ``replica_index``.

        Args:
            replica_index (:class:`ReplicaIndex`): The replica to read from.
            wrap (bool, optional): When the replica at ``replica_index`` cannot be read, because the bucket has
                fewer replicas or that replica is currently unavailable, read the next one that can be read
                instead, wrapping around the bucket's replicas. Defaults to False.

        Returns:
            :class:`GetReplicaStrategyFromIndex`: The strategy.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If ``replica_index`` is not a
                :class:`ReplicaIndex`, or ``wrap`` is not a bool.
        """
        return GetReplicaStrategyFromIndex(replica_index, wrap)


class GetReplicaStrategyFromIndex(GetReplicaStrategy):
    """Reads from the replica at a given index.

    Created by :meth:`GetReplicaStrategy.from_index`.
    """

    def __init__(self, replica_index: ReplicaIndex, wrap: bool) -> None:
        # A plain int is refused: it leaves open whether the first replica is 0 or 1.
        if not isinstance(replica_index, ReplicaIndex):
            raise InvalidArgumentException(message='replica_index must be a ReplicaIndex.')
        if not isinstance(wrap, bool):
            raise InvalidArgumentException(message='wrap must be a bool.')
        self._replica_index = replica_index
        self._wrap = wrap

    @property
    def replica_index(self) -> ReplicaIndex:
        """The replica to read from."""
        return self._replica_index

    @property
    def wrap(self) -> bool:
        """Whether to read the next readable replica when this one cannot be read."""
        return self._wrap

    def __repr__(self) -> str:
        return f'{type(self).__name__}(replica_index=ReplicaIndex.{self._replica_index.name}, wrap={self._wrap})'
