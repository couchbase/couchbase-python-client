from typing import *
import enum
import datetime
import warnings

from couchbase.exceptions import InvalidArgumentException

Src = TypeVar('Src')
Dest = TypeVar('Dest')


Functor = TypeVar('Functor', bound=Callable[[Src],Dest])
SrcToDest = TypeVar('SrcToDest', bound=Callable[[Src], Dest])
DestToSrc = TypeVar('DestToSrc', bound=Callable[[Dest], Src])


class Bijection(Generic[Src, Dest, SrcToDest, DestToSrc]):
    def __init__(
            self,
            src_to_dest,  # type:  SrcToDest
            dest_to_src=None,  # type: DestToSrc
            parent=None  # type: Bijection[Dest,Src]
    ):
        # type: (...) -> None
        """
        Bijective mapping for JSON serialisation/deserialisation

        :param src_to_dest: callable to convert Src type to Dest
        :param dest_to_src: callable to convert Dest type to Src
        :param parent: interanl use only - used to construct the inverse
        """
        self._src_to_dest=src_to_dest
        if parent:
            self._inverse = parent
        else:
            self._inverse = Bijection(dest_to_src, parent=self)

    def __neg__(self):
        # type: (...) -> Bijection[Dest,Src]
        """
        Generate the inverse of this bijection (Dest to Src)

        :return: the inverse of this bijection
        """
        return self._inverse

    def __call__(self,
                 src  # type: Src
                 ):
        # type: (...) -> Dest
        """
        Return the Src to Dest transform on src

        :param src: source to be transformed
        :return: transformed data as type Dest
        """
        return self._src_to_dest(src)


def identity(input: Src) -> Src:
    return input


class Identity(Bijection[Src,Src, identity, identity]):
    def __init__(self, type: Type[Src]):
        self._type = type
        super(Identity, self).__init__(self, self)

    def __call__(self, x: Src) -> Src:
        if not isinstance(x, self._type):
            raise InvalidArgumentException("Argument must be of type {} but got {}".format(self._type, x))
        return x


Enum_Type = TypeVar('Enum_Type', bound=enum.Enum)


class EnumToStr(Generic[Enum_Type]):
    def __init__(self, type: Type[Enum_Type], enforce=True):
        self._type=type
        self._enforce=enforce

    def __call__(self, src: Enum_Type) -> str:
        if not self._enforce and isinstance(src, str) and src in map(lambda x: x.value, self._type):
            warnings.warn("Using deprecated string parameter {}".format(src))
            return src
        if not isinstance(src, self._type):
            raise InvalidArgumentException("Argument must be of type {} but got {}".format(self._type, src))
        return src.value


class StrToEnum(Generic[Enum_Type]):
    def __init__(self, type: Enum_Type):
        self._type=type
    def __call__(self, dest: str
               ) -> Enum_Type:
        return self._type(dest)


class StringEnum(Bijection[Enum_Type, str, EnumToStr[Enum_Type], StrToEnum[Enum_Type]]):
    def __init__(self, type: Type[Enum_Type]):
        super(StringEnum, self).__init__(EnumToStr(type),StrToEnum(type))


class StringEnumLoose(Bijection[Enum_Type, str, EnumToStr[Enum_Type], StrToEnum[Enum_Type]]):
    def __init__(self, type: Type[Enum_Type]):
        """
        Like StringEnum bijection, but allows use of string constants as src (falling back to identity transform)

        :param type: type of enum
        """
        super(StringEnumLoose, self).__init__(EnumToStr(type, False),StrToEnum(type))


NumberType = TypeVar('NumberType', bound=Union[float, int])


class TimedeltaToSeconds(object):
    def __init__(self, dest_type: Type[NumberType]):
        self._numtype=dest_type

    def __call__(self, td: datetime.timedelta) -> float:
        if isinstance(td, (float, int)):
            return self._numtype(td)
        return self._numtype(td.total_seconds())


def _seconds_to_timedelta(seconds: NumberType) -> datetime.timedelta:
    try:
        return datetime.timedelta(seconds=seconds)
    except (OverflowError, ValueError) as e:
        raise InvalidArgumentException("Invalid duration arg: {} ".format(seconds)) from e


class Timedelta(Bijection[datetime.timedelta, NumberType, TimedeltaToSeconds, _seconds_to_timedelta]):
    def __init__(self, dest_type: Type[NumberType]):
        super(Timedelta, self).__init__(TimedeltaToSeconds(dest_type), _seconds_to_timedelta)


class Division(Bijection[float, float, float.__mul__, float.__mul__]):
    def __init__(self, divisor):
        super(Division, self).__init__((1/divisor).__mul__, divisor.__mul__)


Orig_Mapping = TypeVar('OrigMapping', bound=Mapping[str, Mapping[str, Bijection]])


class BijectiveMapping(object):
    def __init__(self,
                 fwd_mapping: Orig_Mapping
                 ):
        """
        Bijective mapping for JSON serialisation/deserialisation.
        Will calculate the reverse mapping of the given forward mapping.

        :param fwd_mapping: the forward mapping from Src to Dest
        """
        self.mapping=dict()
        self.reverse_mapping=dict()
        for src_key, transform_dict in fwd_mapping.items():
            self.mapping[src_key]={}
            for dest_key, transform in transform_dict.items():
                self.mapping[src_key][dest_key] = transform
                self.reverse_mapping[dest_key] = {src_key: -transform}

    @staticmethod
    def convert(mapping: Orig_Mapping,
                raw_info: Mapping[str, Any]) -> Mapping[str, Any]:
        converted = {}
        for k, v in raw_info.items():
            entry = mapping.get(k, {k:Identity(object)})
            for dest, transform in entry.items():
                try:
                    converted[dest] = transform(v)
                except InvalidArgumentException as e:
                    raise InvalidArgumentException("Problem processing argument {}: {}".format(k, e.message))
        return converted

    def sanitize_src(self, src_data):
        return src_data

    def to_dest(self, src_data):
        """
        Convert src data to destination format

        :param src_data: source data
        :return: the converted data
        """
        return self.convert(self.mapping, src_data)

    def to_src(self, dest_data):
        """
        Convert dest_data to source format

        :param dest_data: destination data
        :return: the converted data
        """
        return self.convert(self.reverse_mapping, dest_data)
