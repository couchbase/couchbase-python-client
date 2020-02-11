from typing import *
import couchbase.options
from .options import Cardinal, OptionBlock, OptionBlockTimeOut
from couchbase_core.durability import Durability
from couchbase_core._pyport import TypedDict, with_metaclass
from couchbase_core import ABCMeta
from datetime import timedelta

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

ReplicateTo=Cardinal
PersistTo=Cardinal


T = TypeVar('T', bound=OptionBlock)


class DurabilityTypeBase(dict):
    def __init__(self, content):
        super(DurabilityTypeBase,self).__init__(**content)


class DurabilityType(dict):
    def __init__(self, content):
        super(DurabilityType,self).__init__(content)


class ClientDurability(DurabilityType):
    Storage = TypedDict('Storage', {'replicate_to': ReplicateTo, 'persist_to': PersistTo}, total=True)

    def __init__(self,  # type: T
                 replicate_to,  # type: ReplicateTo
                 persist_to  # type: PersistTo
                 ):
        # type: (...) -> None
        """
        Client Durability

        :param int persist_to: If set, wait for the item to be removed
        from the storage of at least these many nodes
        :param int replicate_to: If set, wait for the item to be removed
        from the cache of at least these many nodes
        (excluding the master)
        """
        super(ClientDurability,self).__init__(ClientDurability.Storage(replicate_to=replicate_to, persist_to=persist_to))


class ServerDurability(DurabilityType):
    Storage = TypedDict('Storage', {'level': Durability}, total=True)

    def __init__(self,  # type: T
                 level,  # type: Durability
                 ):
        # type: (...) -> None
        """
        Server-based Durability (Synchronous Replication)

        :param Durability level: durability level
        """
        super(ServerDurability,self).__init__(ServerDurability.Storage(level=level))

class ClientDurableOptionBlock(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,       # type: timedelta
                 durability=None     # type: ClientDurability
                ):
        pass
    def __init__(self,
                 **kwargs
                ):
        super(ClientDurableOptionBlock, self).__init__(**kwargs)

class ServerDurableOptionBlock(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,       # type: timedelta
                 durability=None     # type: ServerDurability
                ):
        pass
    def __init__(self,
                 **kwargs
                ):
        super(ServerDurableOptionBlock, self).__init__(**kwargs)

class DurabilityOptionBlock(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,       # type: timedelta
                 durability=None     # type: DurabilityType
                 ):
        pass
    def __init__(self,
                 **kwargs
                 ):
        super(DurabilityOptionBlock, self).__init__(**kwargs)

