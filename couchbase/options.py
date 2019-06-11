import time
import copy
from typing import *


class FiniteDuration(object):
    def __init__(self, seconds  # type: Union[float,int]
                 ):
        self.value = seconds

    @staticmethod
    def time():
        return FiniteDuration(time.time())

    def __float__(self):
        return float(self.value)

    def __int__(self):
        return self.value

    def __add__(self, other):
        result = copy.deepcopy(self)
        result.value += other.value
        return result

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return self.value > other.value


class Duration(float):
    def __init__(self, seconds  # type: Union[float,int]
                 ):
        # type: (...)->None
        super(Duration, self).__init__(seconds)


class Seconds(FiniteDuration):
    def __init__(self,
                 seconds  # type: Union[float,int]
                 ):
        # type: (...)->None
        super(Seconds, self).__init__(seconds)


class Durations:
    def __init__(self):
        pass

    @staticmethod
    def minutes(minutes  # type: int
                ):
        return Seconds(minutes * 60)

    @staticmethod
    def days(days  # type: int
             ):
        return Durations.minutes(days * 24 * 60)

    @staticmethod
    def seconds(seconds):
        return Seconds(seconds)


class OptionBlock(dict):
    def __init__(self, *args, **kwargs):
        # type: (*Any, **Any) -> None
        super(OptionBlock, self).__init__(**kwargs)


T = TypeVar('T', bound=OptionBlock)


class OptionBlockTimeOut(OptionBlock):
    def __init__(self, *args, **kwargs):
        # type: (*Any, **Any) -> None
        super(OptionBlockTimeOut, self).__init__(**kwargs)

    def timeout(self,  # type: T
                duration):
        # type: (...)->T
        self['ttl'] = duration.__int__()
        return self


class Value(object):
    def __init__(self,
                 value,  # type: Union[str,bytes,SupportsInt]
                 **kwargs  # type: Any
                 ):
        self.value = value
        self.kwargs = kwargs

    def __int__(self):
        return self.value

    def __float__(self):
        return self.value


class Cardinal(OptionBlock):
    class ONE(Value):
        def __init__(self, **kwargs):
            super(Cardinal.ONE, self).__init__(1, **kwargs)

    TWO = Value(2)
    THREE = Value(3)
    NONE = Value(0)


OptionBlockDeriv = TypeVar('OptionBlockDeriv', bound=OptionBlock)


def forward_args(arg_vars,  # type: Optional[Dict[str,Any]]
                 *options  # type: Tuple[OptionBlockDeriv,...]
                 ):
    # type: (...)->OptionBlockDeriv[str,Any]
    arg_vars = copy.copy(arg_vars) if arg_vars else {}
    temp_options = copy.copy(options[0]) if (options and options[0]) else OptionBlock()
    kwargs = arg_vars.pop('kwargs', {})
    temp_options.update(kwargs)
    temp_options.update(arg_vars)
    arg_mapping = {'spec': {'specs': lambda x: x}, 'id': {},
                   'replicate_to': {"replicate_to":int},
                   'persist_to': {"persist_to":int},
                   'timeout' : {'ttl': int},
                   'expiration': {'ttl': int}, 'self': {}, 'options': {}}
    end_options = {}
    for k, v in temp_options.items():
        map_item = arg_mapping.get(k, None)
        if not (map_item is None):
            for out_k, out_f in map_item.items():
                end_options[out_k] = out_f(v)
        else:
            end_options[k] = v
    return end_options


