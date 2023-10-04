#  Copyright 2016-2023. Couchbase, Inc.
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

from datetime import timedelta
from enum import Enum
from inspect import (Parameter,
                     Signature,
                     signature)
from time import time
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
            message="Expected timedelta instead of {}".format(duration)
        )
    return int(duration.total_seconds() * 1e6 if duration else 0)


def to_microseconds(
    timeout  # type: Union[timedelta, float, int]
) -> int:
    if timeout and not isinstance(timeout, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Excepted timeout to be of type "
                                                f"Union[timedelta, float, int] instead of {timeout}"))
    if not timeout:
        total_us = 0
    elif isinstance(timeout, timedelta):
        total_us = int(timeout.total_seconds() * 1e6)
    else:
        total_us = int(timeout * 1e6)

    return total_us


THIRTY_DAYS_IN_SECONDS = 30 * 24 * 60 * 60


def timedelta_as_timestamp(
    duration,  # type: timedelta
) -> int:
    if not isinstance(duration, timedelta):
        raise InvalidArgumentException(f'Expected timedelta instead of {duration}')

    # PYCBC-1177 remove deprecated heuristic from PYCBC-948:
    seconds = int(duration.total_seconds())
    if seconds < 0:
        raise InvalidArgumentException(f'Expected expiry seconds of zero (for no expiry) or greater, got {seconds}.')

    if seconds < THIRTY_DAYS_IN_SECONDS:
        return seconds

    return seconds + int(time())


def validate_int(value  # type: int
                 ) -> int:
    if not isinstance(value, int):
        raise InvalidArgumentException(message='Expected value to be of type int.')
    return value


def validate_bool(value  # type: bool
                  ) -> int:
    if not isinstance(value, bool):
        raise InvalidArgumentException(message='Expected value to be of type bool.')
    return value


def validate_str(value  # type: str
                 ) -> int:
    if not isinstance(value, str):
        raise InvalidArgumentException(message='Expected value to be of type str.')
    return value


class Identity:
    def __init__(self,
                 type_,  # type: Callable
                 optional=False  # type: bool
                 ):
        self._type = type_
        self._optional = optional

    def __call__(self, x  # type: Any
                 ) -> Any:
        if self._optional and x is None:
            return x
        if not isinstance(x, self._type):
            exc = InvalidArgumentException.pycbc_create_exception(
                exception(),
                "Argument must be of type {} but got {}".format(
                    self._type, x))
            raise exc
        return x


class EnumToStr:
    def __init__(self, type_,  # type: Type[Enum]
                 conversion_fn=None  # type: Callable
                 ):
        self._type = type_
        self._conversion_fn = conversion_fn

    def __call__(self, value  # type:  Enum
                 ) -> str:
        # TODO:  maybe?
        if isinstance(value, str) and value in map(lambda x: x.value, self._type):
            # TODO: use warning?
            # warn("Using deprecated string parameter {}".format(value))
            return value
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
    def __init__(self, type_,  # type: Type[Enum]
                 conversion_fn=None,  # type: Callable
                 optional=False  # type: bool
                 ):
        self._type = type_
        self._conversion_fn = conversion_fn
        self._optional = optional

    def __call__(self, value  # type: str
                 ) -> Enum:

        if self._optional and value is None:
            return value
        if self._conversion_fn:
            return self._conversion_fn(value)
        return self._type(value)


NumberType = TypeVar('NumberType', bound=Union[float, int])


class SecondsToTimeDelta:
    def __init__(self, type_  # type: Type[timedelta]
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
    def __init__(self, type_  # type: Union[Type[int], Type[float]]
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
                raise e
        return converted

    @staticmethod
    def convert_from_dest(mapping,  # type: Dict[str, Any]
                          raw_info  # type: Dict[str, Any]
                          ) -> Dict[str, Any]:
        converted = {}
        for k, param_transform in mapping.items():
            key, transform = param_transform.from_dest_components()
            if key not in raw_info:
                continue
            try:
                converted[k] = transform(raw_info[key])
            except InvalidArgumentException as e:
                raise e
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
        return self.convert_from_dest(self._mapping, data)


class OverloadType(Enum):
    DEFAULT = 0
    SECONDARY = 1


class Overload:
    def __init__(self, name):
        self._name = name
        self._sig_map = {}
        self._default_fn = None

    def register(self, fn):
        sig = signature(fn)
        if sig in self._sig_map:
            raise TypeError(f'Already registered {self._name} with signature {sig}')
        self._sig_map[sig] = fn

    def register_default(self, fn):
        if self._default_fn is not None:
            raise TypeError(f'Default function already registered for {self._name}')
        self._default_fn = fn

    def __call__(self, *args, **kwargs):
        for sig, fn in self._sig_map.items():
            params = sig.parameters
            try:
                bound_args = sig.bind(*args, **kwargs)
            except TypeError:
                continue
            if all(params[arg_name].annotation is Signature.empty or isinstance(arg, params[arg_name].annotation)
                   for arg_name, arg in bound_args.arguments.items()
                   if params[arg_name].kind not in [Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD]):
                return fn(*args, **kwargs)

        # None of the functions in the signature map can be called, call the default one if it exists
        if self._default_fn is None:
            raise TypeError(f'Unable to find appropriate registered overload for {self._name}')
        return self._default_fn(*args, **kwargs)
