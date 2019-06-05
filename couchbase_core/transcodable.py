from abc import abstractmethod

from . import JSON
from typing import *
T = TypeVar('T', bound='couchbase_core.Transcodable')


class Transcodable(object):
    @abstractmethod
    def encode_canonical(self):
        # type: (...)->JSON
        pass

    @classmethod
    def decode_canonical(cls,  # type: Type[T]
                         input  # type: JSON
                         ):
        # type: (...)->T
        """IMPORTANT: this is class method, override it with @classmethod!"""
        pass
