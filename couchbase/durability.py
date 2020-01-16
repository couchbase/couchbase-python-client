from typing import *
import couchbase.options
from .options import Cardinal, OptionBlock, OptionBlockBase
from couchbase_core.durability import Durability
from couchbase_core._pyport import TypedDict, with_metaclass
from couchbase_core import ABCMeta
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


class DurabilityType(DurabilityTypeBase):
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


ClientDurableOptionBlock = TypedDict("ClientDurableOptionBlock", {'durability': ClientDurability}, total=False)
ServerDurableOptionBlock = TypedDict("ServerDurableOptionBlock", {'durability': ServerDurability}, total=False)
DurabilityOptionBlock = TypedDict("DurabilityOptionBlock", {'durability': DurabilityType}, total=False)
