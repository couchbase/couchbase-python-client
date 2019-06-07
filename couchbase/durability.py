from enum import Enum, IntEnum

import couchbase_core._libcouchbase as _LCB
from typing import *
import couchbase.options
from .options import Cardinal, OptionBlock


class Durability(IntEnum):
    MAJORITY_AND_PERSIST_ACTIVE = _LCB.LCB_DURABILITYLEVEL_MAJORITY_AND_PERSIST_ON_MASTER
    NONE = _LCB.LCB_DURABILITYLEVEL_NONE
    MAJORITY = _LCB.LCB_DURABILITYLEVEL_MAJORITY
    PERSIST_TO_MAJORITY = _LCB.LCB_DURABILITYLEVEL_PERSIST_TO_MAJORITY


class ReplicateTo(Cardinal):
    Value = couchbase.options.Value


class PersistTo(Cardinal):
    Value = couchbase.options.Value


T = TypeVar('T', bound=OptionBlock)


class ClientDurableOption(object):
    def dur_client(self,  # type: T
                   replicate_to,  # type: ReplicateTo
                   persist_to,  # type: PersistTo
                   ):
        # type: (...)->T.ClientDurable
        self['replicate_to'] = replicate_to
        self['persist_to'] = persist_to
        return self


class ServerDurableOption(object):
    def dur_server(self,  # type: T
                   level,  # type: Durability
                   ):
        # type: (...)->T.ServerDurable
        self['durability_level'] = level
        return self
