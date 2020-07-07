import copy
from functools import wraps
from typing import *

import couchbase.exceptions
import ctypes
from couchbase_core import abstractmethod, ABCMeta, operation_mode
from couchbase_core._pyport import with_metaclass
from datetime import timedelta
from enum import IntEnum
try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict
from enum import IntEnum
from couchbase_core._libcouchbase import LOCKMODE_EXC, LOCKMODE_NONE, LOCKMODE_WAIT

OptionBlockBase = dict


T = TypeVar('T', bound=OptionBlockBase)


class OptionBlock(OptionBlockBase):
    def __init__(self,
                 *args,  # type: Any
                 **kwargs  # type: Any
                 ):
        # type: (...) -> None
        """
        This is a wrapper for a set of options for a Couchbase command. It can be passed
        into the command in the 'options' parameter and overriden by parameters of the
        same name via the following **kwargs.

        :param args:
        :param kwargs: parameters to pass in to the OptionBlock
        """
        super(OptionBlock, self).__init__(**{k: v for k, v in kwargs.items() if v is not None})
        self._args = args

    @classmethod
    def _wrap_docs(cls,  # type: Type[T]
                   **doc_params  # type: Any
                   ):
        # type: (...) -> Type[T]
        class DocWrapper(cls):
            @wraps(cls.__init__)
            def __init__(self,  # type: DocWrapper
                         *args,  # type: Any
                         **kwargs  # type: Any
                         ):
                # type: (...) -> None
                try:
                    super(DocWrapper, self).__init__(self, *args, **kwargs)
                except Exception as e:
                    raise

            operation_mode.operate_on_doc(__init__, lambda x: cls.__init__.__doc__.format(**doc_params))
        return DocWrapper


class OptionBlockTimeOut(OptionBlock):
    def __init__(self,  # type: OptionBlockTimeOut
                 timeout=None,  # type: timedelta
                 **kwargs  # type: Any
                 ):
        # type: (...) -> None
        """
        An OptionBlock with a timeout

        :param timeout: Timeout for an operation
        """
        super(OptionBlockTimeOut, self).__init__(timeout=timeout, **kwargs)

    def timeout(self,  # type: T
                duration  # type: timedelta
                ):
        # type: (...) -> T
        self['timeout'] = duration
        return self


class Cardinal(IntEnum):
    ONE = 1
    TWO = 2
    THREE = 3
    NONE = 0


OptionBlockDeriv = TypeVar('OptionBlockDeriv', bound=OptionBlock)


class Forwarder(with_metaclass(ABCMeta)):
    def forward_args(self, arg_vars,  # type: Optional[Dict[str,Any]]
                     *options  # type: OptionBlockDeriv
                     ):
        # type: (...) -> OptionBlockDeriv[str,Any]
        arg_vars = copy.copy(arg_vars) if arg_vars else {}
        temp_options = copy.copy(options[0]) if (options and options[0]) else OptionBlock()
        kwargs = arg_vars.pop('kwargs', {})
        temp_options.update(kwargs)
        temp_options.update(arg_vars)

        end_options = {}
        for k, v in temp_options.items():
            map_item = self.arg_mapping().get(k, None)
            if not (map_item is None):
                for out_k, out_f in map_item.items():
                    converted = out_f(v)
                    if converted is not None:
                        end_options[out_k] = converted
            else:
                end_options[k] = v
        return end_options

    @abstractmethod
    def arg_mapping(self):
        pass


def timedelta_as_timestamp(duration  # type: timedelta
                           ):
    # type: (...)->int
    if not isinstance(duration,timedelta):
        raise couchbase.exceptions.InvalidArgumentException("Expected timedelta instead of {}".format(duration))
    return int(duration.total_seconds() if duration else 0)


def timedelta_as_microseconds(duration  # type: timedelta
                           ):
    # type: (...)->int
    if not isinstance(duration,timedelta):
        raise couchbase.exceptions.InvalidArgumentException("Expected timedelta instead of {}".format(duration))
    return int(duration.total_seconds()*1e6 if duration else 0)


class DefaultForwarder(Forwarder):
    def arg_mapping(self):
        return {'spec': {'specs': lambda x: x}, 'id': {},
                'timeout': {'timeout': timedelta_as_microseconds},
                'expiry': {'ttl': timedelta_as_timestamp},
                'self': {},
                'options': {},
                'durability': {'durability_level': lambda durability: getattr(durability.get('level', None),'value', None),
                               "replicate_to": lambda client_dur: client_dur.get('replicate_to', None),
                               "persist_to": lambda client_dur: client_dur.get('persist_to', None)}}


forward_args = DefaultForwarder().forward_args

AcceptableInts = Union['ConstrainedValue', ctypes.c_int64, ctypes.c_uint64, int]


class ConstrainedInt(object):
    def __init__(self,value):
        """
        A signed integer between cls.min() and cls.max() inclusive

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        self.value = type(self).verified_value(value)

    @classmethod
    def verified_value(cls, item  # type: AcceptableInts
                        ):
        # type: (...) -> int
        value = getattr(item, 'value', item)
        if not isinstance(value, int) or not (cls.min()<=value<=cls.max()):
            raise couchbase.exceptions.InvalidArgumentException("Integer in range {} and {} inclusiverequired".format(cls.min(), cls.max()))
        return value

    @classmethod
    def verified(cls,
                 item  # type: AcceptableInts
                 ):
        if isinstance(item, cls):
            return item
        raise couchbase.exceptions.InvalidArgumentException("Argument is not {}".format(cls))

    def __neg__(self):
        return -self.value

    def __int__(self):
        return self.value

    def __add__(self, other):
        if not (self.min() <= (self.value + int(other)) <= self.max()):
            raise couchbase.exceptions.ArgumentError("{} + {} would be out of range {}-{}".format(self.value, other, self.min(), self.min()))

    @classmethod
    def max(cls):
        raise NotImplementedError()

    @classmethod
    def min(cls):
        raise NotImplementedError()

    def __str__(self):
        return "{cls_name} with value {value}".format(cls_name=type(self), value=self.value)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value


class SignedInt64(ConstrainedInt):
    def __init__(self, value):
        """
        A signed integer between -0x8000000000000000 and +0x7FFFFFFFFFFFFFFF inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super(SignedInt64,self).__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return -0x8000000000000000


class UnsignedInt32(ConstrainedInt):
    def __init__(self, value):
        """
        An unsigned integer between 0x00000000 and +0x80000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super(UnsignedInt32, self).__init__(value)

    @classmethod
    def max(cls):
        return 0x00000000

    @classmethod
    def min(cls):
        return 0x80000000


class UnsignedInt64(ConstrainedInt):
    def __init__(self, value):
        """
        An unsigned integer between 0x0000000000000000 and +0x8000000000000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super(UnsignedInt64, self).__init__(value)

    @classmethod
    def min(cls):
        return 0x0000000000000000

    @classmethod
    def max(cls):
        return 0x8000000000000000


AcceptableUnsignedInt32 = Union[UnsignedInt32, ctypes.c_uint32]


class LockMode(IntEnum):
    WAIT = LOCKMODE_WAIT
    EXC = LOCKMODE_EXC
    NONE = LOCKMODE_NONE
