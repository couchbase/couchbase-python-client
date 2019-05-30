from enum import Enum, IntEnum

import couchbase_core._libcouchbase as _LCB
from typing import *

from .options import Cardinal, OptionBlock


class ReplicateTo(Cardinal):
    pass


class PersistToOne(Cardinal.ONE):
    def __init__(self, *args, **kwargs):
        super(PersistTo.ONE, self).__init__(*args, **kwargs)


class PersistTo(Cardinal):
    ONE = PersistToOne


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

