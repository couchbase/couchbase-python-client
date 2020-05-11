from functools import wraps
from typing import *

from couchbase_core.supportability import internal

from .options import Cardinal, OptionBlock, OptionBlockTimeOut
from couchbase_core.durability import Durability
from datetime import timedelta

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

ReplicateTo = Cardinal
PersistTo = Cardinal


T = TypeVar('T', bound=OptionBlock)


class DurabilityTypeBase(dict):
    def __init__(self, content):
        super(DurabilityTypeBase,self).__init__(**content)


class DurabilityType(dict):
    @internal
    def __init__(self,  # type: DurabilityType
                 content  # type: Dict[str, Any]
                 ):
        # type: (...) -> None
        """
        Durability configuration options

        :param content: dictionary passed up from subclasses
        """
        super(DurabilityType, self).__init__(content)


class ClientDurability(DurabilityType):
    Storage = TypedDict('Storage', {'replicate_to': ReplicateTo, 'persist_to': PersistTo}, total=True)

    def __init__(self,  # type: T
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
        super(ClientDurability, self).__init__(ClientDurability.Storage(replicate_to=replicate_to, persist_to=persist_to))


class ServerDurability(DurabilityType):
    Storage = TypedDict('Storage', {'level': Durability}, total=True)

    def __init__(self,  # type: ServerDurability
                 level,  # type: Durability
                 ):
        # type: (...) -> None
        """
        Server-based Durability (Synchronous Replication)

        :param Durability level: durability level
        """
        super(ServerDurability, self).__init__(ServerDurability.Storage(level=level))


class ClientDurableOptionBlock(OptionBlockTimeOut):
    def __init__(self,  # type: ClientDurableOptionBlock
                 timeout=None,       # type: timedelta
                 durability=None     # type: ClientDurability
                 ):
        # type: (...) -> None
        """
        Options for operations with client-type durability

        :param durability: Client durability settings
        :param timeout: Timeout for operation
        """
        super(ClientDurableOptionBlock, self).__init__(durability=durability, timeout=timeout)


class ServerDurableOptionBlock(OptionBlockTimeOut):
    def __init__(self,               # type: ServerDurableOptionBlock
                 timeout=None,       # type: timedelta
                 durability=None     # type: ServerDurability
                 ):
        # type: (...) -> None
        """
        Options for operations with server-type durability

        :param durability: Server durability settings
        :param timeout: Timeout for operation
        """
        super(ServerDurableOptionBlock, self).__init__(durability=durability, timeout=timeout)


class DurabilityOptionBlock(OptionBlockTimeOut):
    def __init__(self,      # type: DurabilityOptionBlock
                 timeout=None,       # type: timedelta
                 durability=None,    # type: DurabilityType
                 expiry=None,        # type: timedelta
                 **kwargs):
        # type: (...) -> None
        """
        Options for operations with any type of durability

        :param durability: Durability settings
        :param expiry: When any mutation should expire
        :param timeout: Timeout for operation
        """
        super(DurabilityOptionBlock, self).__init__(durability=durability, expiry=expiry, timeout=timeout, **kwargs)

    @property
    def expiry(self):
        return self.get('expiry', None)
