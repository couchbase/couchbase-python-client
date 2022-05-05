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

from enum import IntEnum
from typing import (Dict,
                    Optional,
                    TypeVar,
                    Union)


class ReplicateTo(IntEnum):
    NONE = 0
    ONE = 1
    TWO = 2
    THREE = 3


class PersistTo(IntEnum):
    NONE = 0
    ONE = 1
    TWO = 2
    THREE = 3


class Durability(IntEnum):
    """Synchronous Durability Level

    **DEPRECATED** Use `DurabilityLevel`
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

    # def to_server_str(self):
    #     if self.name == 'MAJORITY_AND_PERSIST_TO_ACTIVE':
    #         return 'majorityAndPersistActive'
    #     elif self.name == 'NONE':
    #         return 'none'
    #     elif self.name == 'MAJORITY':
    #         return 'majority'
    #     elif self.name == 'PERSIST_TO_MAJORITY':
    #         return 'persistToMajority'
    #     else:
    #         return 'none'

    @classmethod
    def to_server_str(cls, value):
        if value == cls.MAJORITY_AND_PERSIST_TO_ACTIVE:
            return 'majorityAndPersistActive'
        elif value == cls.NONE:
            return 'none'
        elif value == cls.MAJORITY:
            return 'majority'
        elif value == cls.PERSIST_TO_MAJORITY:
            return 'persistToMajority'
        else:
            return 'none'

    @classmethod
    def from_server_str(cls, value):
        if value == 'majorityAndPersistActive':
            return cls.MAJORITY_AND_PERSIST_TO_ACTIVE
        elif value == 'none':
            return cls.NONE
        elif value == 'majority':
            return cls.MAJORITY
        elif value == 'persistToMajority':
            return cls.PERSIST_TO_MAJORITY
        else:
            return cls.NONE


class ClientDurability:

    def __init__(self,
                 replicate_to=ReplicateTo.NONE,  # type: ReplicateTo
                 persist_to=PersistTo.NONE  # type: PersistTo
                 ):
        # type: (...) -> None
        """
        Client Durability

        :param persist_to: If set, wait for the item to be removed
            from the storage of at least these many nodes

        :param replicate_to: If set, wait for the item to be removed
            from the cache of at least these many nodes
            (excluding the master)
        """
        self._replicate_to = replicate_to
        self._persist_to = persist_to

    @property
    def replicate_to(self) -> ReplicateTo:
        return self._replicate_to

    @property
    def persist_to(self) -> PersistTo:
        return self._persist_to


class ServerDurability:

    def __init__(self,  # type: ServerDurability
                 level,  # type: DurabilityLevel
                 ):
        # type: (...) -> None
        """
        Server-based Durability (Synchronous Replication)

        :param Durability level: durability level
        """
        self._level = level

    @property
    def level(self) -> DurabilityLevel:
        return self._level


DurabilityType = TypeVar('DurabilityType', bound=Union[ClientDurability, ServerDurability])


class DurabilityParser:
    @staticmethod
    def parse_durability(durability  # type: DurabilityType
                         ) -> Optional[Union[int, Dict[str, int]]]:
        if isinstance(durability, ClientDurability):
            return {
                durability.replicate_to.name: durability.replicate_to.value,
                durability.persist_to.name: durability.persist_to.value
            }

        if isinstance(durability, ServerDurability):
            return durability.level.value

        return None
