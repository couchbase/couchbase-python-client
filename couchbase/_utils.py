import re
from datetime import timedelta
from enum import Enum
from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Tuple,
                    Type,
                    TypeVar,
                    Union)
from urllib.parse import quote

from couchbase.exceptions import InvalidArgumentException
from couchbase.pycbc_core import exception


class DurationParser:

    _BASE_UNIT = 1
    _SECOND = _BASE_UNIT * 1000 * 1000 * 1000 * 1000

    _UNIT_CONVERSION = {
        "ns": 1,
        "us": 1 * 1000,
        "µs": 1 * 1000 * 1000,  # micro sign: unicode - 0xB5
        "μs": 1 * 1000 * 1000,  # greek small letter mu: unicode - 0x3BC
        "ms": 1 * 1000 * 1000 * 1000,
        "s": 1 * 1000 * 1000 * 1000 * 1000,
        "m": _SECOND * 60,
        "h": _SECOND * 60 * 60,
        "d": _SECOND * 60 * 60 * 24,
        "w": _SECOND * 60 * 60 * 24 * 7,
        "mm": _SECOND * 60 * 60 * 24 * 30,
        "y": _SECOND * 60 * 60 * 24 * 365,
    }

    @classmethod
    def from_str(cls, duration  # type: str
                 ) -> timedelta:
        """Parse GoLang duration string to a timedelta"""

        if duration in ("0", "+0", "-0"):
            return timedelta()

        pattern = re.compile(r'([\d\.]+)([a-zµμ]+)')
        total = 0
        sign = -1 if duration[0] == '-' else 1
        matches = pattern.findall(duration)

        if not len(matches):
            raise ValueError("Invalid duration {}".format(duration))

        for (value, unit) in matches:
            if unit not in cls._UNIT_CONVERSION:
                raise TypeError(
                    "Unknown unit {} in duration {}".format(unit, duration))
            try:
                total += float(value) * cls._UNIT_CONVERSION[unit]
            except Exception:
                raise ValueError(
                    "Invalid value {} in duration {}".format(value, duration))

        microseconds = total / cls._UNIT_CONVERSION["µs"]
        return timedelta(microseconds=sign * microseconds)


JSONType = Union[str, int, float, bool,
                 None, Dict[str, Any], List[Any]]

PyCapsuleType = TypeVar('PyCapsuleType')


def is_null_or_empty(
    value  # type: str
) -> bool:
    return not (value and not value.isspace())


def to_form_str(params  # type: Dict[str, Any]
                ):
    encoded_params = []
    for k, v in params.items():
        encoded_params.append('{0}={1}'.format(quote(k), quote(str(v))))

    return '&'.join(encoded_params)


def identity(x  # type: Any
             ) -> Any:
    return x


def timedelta_as_microseconds(
    duration,  # type: timedelta
) -> int:
    if duration and not isinstance(duration, timedelta):
        raise InvalidArgumentException(
            "Expected timedelta instead of {}".format(duration)
        )
    return int(duration.total_seconds() * 1e6 if duration else 0)


class Identity:
    def __init__(self, type_  # type: Callable
                 ):
        self._type = type_

    def __call__(self, x  # type: Any
                 ) -> Any:
        if not isinstance(x, self._type):
            exc = InvalidArgumentException.pycbc_create_exception(
                exception(),
                "Argument must be of type {} but got {}".format(
                    self._type, x))
            raise exc
        return x


class EnumToStr:
    def __init__(self, type_,  # type: Enum
                 conversion_fn=None  # type: Callable
                 ):
        self._type = type_
        self._conversion_fn = conversion_fn

    def __call__(self, value  # type:  Enum
                 ) -> str:
        # TODO:  maybe?
        # if isinstance(value, str) and value in map(lambda x: x.value, self._type):
        #     warn("Using deprecated string parameter {}".format(value))
        #     return value
        if not isinstance(value, self._type):
            exc = InvalidArgumentException.pycbc_create_exception(
                exception(),
                "Argument must be of type {} but got {}".format(
                    self._type, value))
            raise exc
        if self._conversion_fn:
            return self._conversion_fn(value)
        return value.value


class StrToEnum:
    def __init__(self, type_,  # type: Enum
                 conversion_fn=None  # type: Callable
                 ):
        self._type = type_
        self._conversion_fn = conversion_fn

    def __call__(self, value  # type: str
                 ) -> Enum:

        if self._conversion_fn:
            return self._conversion_fn(value)
        return self._type(value)


NumberType = TypeVar('NumberType', bound=Union[float, int])


class SecondsToTimeDelta:
    def __init__(self, type_  # type: timedelta
                 ):
        self._type = type_

    def __call__(self, value  # type: NumberType
                 ) -> timedelta:
        try:
            return self._type(seconds=value)
        except (OverflowError, ValueError):
            exc = InvalidArgumentException.pycbc_create_exception(
                exception(), "Invalid duration arg: {}".format(value))
            raise exc


class TimeDeltaToSeconds:
    def __init__(self, type_  # type: Union[float,int]
                 ):
        self._type = type_

    def __call__(self, td  # type: Union[timedelta, float, int]
                 ) -> Type[NumberType]:
        if isinstance(td, (float, int)):
            return self._type(td)
        return self._type(td.total_seconds())

# class TimeDeltaToSeconds:
#     def __init__(self, dest_type: Type[NumberType]):
#         super(
#             Timedelta,
#             self).__init__(
#             TimedeltaToSeconds(dest_type),
#             _seconds_to_timedelta)


class ParamTransform:

    def __init__(self, key=None,  # type: str
                 transform=Identity(object)  # type: Callable
                 ) -> None:
        self._key = key
        self._transform = transform

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, value):
        self._key = value

    @property
    def transform(self):
        return self._transform


class UnidirectionalTransform:

    def __init__(self, key,  # type: str
                 to_dest,  # type: ParamTransform
                 ):
        self._key = key
        self._to_dest = to_dest


class TransformComponents:

    def __init__(self,
                 to_dest,  # type: ParamTransform
                 from_dest,  # type: ParamTransform
                 default=None  # type: Any
                 ):
        self._to_dest = to_dest
        self._from_dest = from_dest
        self._default = default

    @property
    def default(self):
        return self._default

    def to_dest_components(self) -> Tuple[str, Callable]:
        return self._to_dest.key, self._to_dest.transform

    def from_dest_components(self) -> Tuple[str, Callable]:
        return self._from_dest.key, self._from_dest.transform


class BidirectionalTransform:

    def __init__(self, key,  # type: str
                 to_dest,  # type: ParamTransform
                 from_dest,  # type: ParamTransform
                 default=None  # type: Any
                 ):
        self._key = key
        self._to_dest = to_dest
        if self._to_dest.key is None:
            self._to_dest.key = self._key
        self._from_dest = from_dest
        if self._from_dest.key is None:
            self._from_dest.key = self._key
        self._default = default

    def transform_as_dict(self):
        return {self._key: TransformComponents(self._to_dest, self._from_dest, self._default)}


class BidirectionalMapping:

    def __init__(self, transforms  # type: List[BidirectionalTransform]
                 ):
        self._transforms = transforms
        self._mapping = {}
        for t in self._transforms:
            self._mapping.update(t.transform_as_dict())

    @staticmethod
    def convert_to_dest(mapping,  # type: Dict[str, Any]
                        raw_info  # type: Dict[str, Any]
                        ) -> Dict[str, Any]:
        converted = {}
        for k, v in raw_info.items():
            param_transform = mapping.get(k, TransformComponents(
                ParamTransform(k), ParamTransform(k)))
            try:
                key, transform = param_transform.to_dest_components()
                if not key:
                    key = k
                converted[key] = transform(v)
            except InvalidArgumentException as e:
                print("problem w/ key: {}".format(k))
                raise e
                # raise InvalidArgumentException(
                #     "Problem processing argument {}".format(
                #         k))
        return converted

    @staticmethod
    def convert_from_dest(mapping,  # type: Dict[str, Any]
                          raw_info  # type: Dict[str, Any]
                          ) -> Dict[str, Any]:
        converted = {}
        for k, param_transform in mapping.items():
            key, transform = param_transform.from_dest_components()
            if key not in raw_info:
                exc = InvalidArgumentException.pycbc_create_exception(
                    exception(), "Unable to find {} in destination data".format(key))
                raise exc
            try:
                converted[k] = transform(raw_info[key])
            except InvalidArgumentException as e:
                print("problem w/ key: {}".format(k))
                raise e
                # raise InvalidArgumentException(
                #     "Problem processing argument {}".format(
                #         k))
        return converted

    def transform_to_dest(self,
                          data  # type: Dict[str, Any]
                          ) -> Dict[str, Any]:

        # set the defaults
        for k in self._mapping.keys():
            if k not in data.keys() and self._mapping[k].default is not None:
                data[k] = self._mapping[k].default
        return self.convert_to_dest(self._mapping, data)

    def transform_from_dest(self, data):
        # set the defaults
        # for k in self._mapping.keys():
        #     if k not in data.keys() and self._mapping[k].default is not None:
        #         data[k] = self._mapping[k].default
        return self.convert_from_dest(self._mapping, data)
