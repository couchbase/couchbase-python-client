#  Copyright 2016-2022. Couchbase, Inc.
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

__all__ = [
    'ClientDurability',
    'Durability',
    'DurabilityLevel',
    'DurabilityType',
    'PersistTo',
    'PersistToExtended',
    'ReplicateTo',
    'ServerDurability',
]

from enum import IntEnum

from couchbase.common._supportability import DURABILITY_LEVEL_READ, Supportability
from couchbase.common._utils import fields_repr


class ReplicateTo(IntEnum):
    """
    Specify the number of nodes to wait for replication.
    """
    NONE = 0    # Do not apply replication requirements.
    ONE = 1     # Wait for replication to atleast one replica nodes
    TWO = 2     # Wait for replication to atleast two replica nodes
    THREE = 3   # Wait for replication to all three replica nodes.


class PersistTo(IntEnum):
    """
    Specify the number of nodes to wait for persistance.

    Use `~couchbase.durability.PersistToExtended` for extended functionality.  The
    `~couchbase.durability.PersistToExtended` is the preferred enum for to use for the 4.x SDK.
    """
    NONE = 0    # Do not apply persistance requirements.
    ONE = 2     # cxx: Wait for persistence to at least one node.
    TWO = 3     # cxx: Wait for persistence to at least two nodes.
    THREE = 4   # cxx: Wait for persistence to at least three nodes.


class PersistToExtended(IntEnum):
    """
    Extends the `~couchbase.durability.PersistTo` enum to match the cxx client options.  This is the
    preferred Enum, wihle the `~couchbase.durability.PersistTo` enum is around for 3.x compatibility.
    """
    NONE = 0    # Do not apply persistance requirements.
    ACTIVE = 1  # Wait for persistence to active node.
    ONE = 2     # Wait for persistence to at least one node.
    TWO = 3     # Wait for persistence to at least two nodes.
    THREE = 4   # Wait for persistence to at least three nodes.
    # This is maximum possible persistence requirement, that includes active and all three replica nodes.
    FOUR = 5    # Wait for persistence to at least all nodes.


class Durability(IntEnum):
    """Synchronous Durability Level

    .. deprecated:: 4.0.0
        Use :class:`DurabilityLevel` instead.
    """
    NONE = 0
    MAJORITY = 1
    MAJORITY_AND_PERSIST_TO_ACTIVE = 2
    PERSIST_TO_MAJORITY = 3


class DurabilityLevel(IntEnum):
    NONE = 0
    MAJORITY = 1
    MAJORITY_AND_PERSIST_TO_ACTIVE = 2
    PERSIST_TO_MAJORITY = 3

    @classmethod
    def to_server_str(cls, value: int) -> str:
        if value == cls.MAJORITY_AND_PERSIST_TO_ACTIVE:
            return 'majority_and_persist_to_active'
        elif value == cls.NONE:
            return 'none'
        elif value == cls.MAJORITY:
            return 'majority'
        elif value == cls.PERSIST_TO_MAJORITY:
            return 'persist_to_majority'
        else:
            return 'none'

    @classmethod
    def from_server_str(cls, value: str) -> DurabilityLevel:
        """
        .. deprecated:: 4.7.0
            The SDK returns a :class:`DurabilityLevel` member wherever it reads a durability level from the server.
        """
        Supportability.method_deprecated('from_server_str', message=DURABILITY_LEVEL_READ)
        if value == 'majority_and_persist_to_active':
            return cls.MAJORITY_AND_PERSIST_TO_ACTIVE
        elif value == 'none':
            return cls.NONE
        elif value == 'majority':
            return cls.MAJORITY
        elif value == 'persist_to_majority':
            return cls.PERSIST_TO_MAJORITY
        else:
            return cls.NONE


class ClientDurability:

    def __init__(self,
                 replicate_to: ReplicateTo = ReplicateTo.NONE,
                 persist_to: PersistTo | PersistToExtended = PersistTo.NONE
                 ) -> None:
        """
        Client Durability

        Args:
            persist_to: If set, wait for the item to be removed from the storage of at least these many nodes
            replicate_to: If set, wait for the item to be removed from the cache of at least these many nodes (excluding
                the master)
        """
        self._replicate_to = replicate_to
        self._persist_to = persist_to

    @property
    def replicate_to(self) -> ReplicateTo:
        return self._replicate_to

    @property
    def persist_to(self) -> PersistTo | PersistToExtended:
        return self._persist_to

    def __repr__(self) -> str:
        return fields_repr(self, replicate_to=self._replicate_to, persist_to=self._persist_to)


class ServerDurability:

    def __init__(self, level: DurabilityLevel) -> None:
        """
        Server-based Durability (Synchronous Replication)

        Args:
            level (Durability): durability level
        """
        self._level = level

    @property
    def level(self) -> DurabilityLevel:
        return self._level

    def __repr__(self) -> str:
        return fields_repr(self, level=self._level)


DurabilityType = ClientDurability | ServerDurability
