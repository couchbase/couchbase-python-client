#  Copyright 2016-2022. Couchbase, Inc.
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

from __future__ import annotations

import copy
import ctypes
from datetime import timedelta
from enum import Enum, IntEnum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Type,
                    Union,
                    overload)

from couchbase._utils import (timedelta_as_milliseconds,
                              timedelta_as_timestamp,
                              validate_bool,
                              validate_int)
from couchbase.common._durability import DurabilityParser
from couchbase.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from couchbase.durability import DurabilityType
    from couchbase.logic.observability import SpanProtocol


OptionsBase = dict


def _get_temp_opts(
    arg_vars,  # type: Optional[Dict[str,Any]]
    *options  # type: OptionsBase
) -> Dict[str, Any]:
    arg_vars = copy.copy(arg_vars) if arg_vars else {}
    temp_options = (
        copy.copy(
            options[0]) if (
            options and options[0]) else dict())
    kwargs = arg_vars.pop("kwargs", {})
    temp_options.update(kwargs)
    temp_options.update(arg_vars)

    return temp_options


def get_valid_args(
    opt_type,  # type: OptionsBase
    arg_vars,  # type: Optional[Dict[str,Any]]
    *options  # type: OptionsBase
) -> Dict[str, Any]:

    arg_vars = copy.copy(arg_vars) if arg_vars else {}
    temp_options = (
        copy.copy(
            options[0]) if (
            options and options[0]) else dict())
    kwargs = arg_vars.pop("kwargs", {})
    temp_options.update(kwargs)
    temp_options.update(arg_vars)

    valid_opts = opt_type.get_valid_options()
    final_options = {}
    for opt_key, opt_value in temp_options.items():
        if opt_key not in valid_opts:
            continue
        for final_key, transform in valid_opts[opt_key].items():
            converted = transform(opt_value)
            if converted is not None:
                final_options[final_key] = converted

    return final_options


VALID_MULTI_OPTS = {
    'timeout': timedelta_as_milliseconds,
    'expiry': timedelta_as_timestamp,
    'preserve_expiry': validate_bool,
    'with_expiry': validate_bool,
    'cas': validate_int,
    'durability': DurabilityParser.parse_durability,
    'transcoder': lambda x: x,
    'span': lambda x: x,  # kept b/c we previously had this in the allowed opts
    'parent_span': lambda x: x,
    'project': lambda x: x,
    'delta': lambda x: x,
    'initial': lambda x: x,
    'read_preference': lambda x: x.value,
    'per_key_options': lambda x: x,
    'return_exceptions': validate_bool
}


def _get_valid_global_multi_opts(
    temp_options,  # type: Dict[str, Any]
    valid_opt_keys  # type: List[str]
):
    final_opts = {}
    if not temp_options:
        return final_opts

    for opt_key, opt_value in temp_options.items():
        if opt_key not in valid_opt_keys:
            continue
        transform = VALID_MULTI_OPTS.get(opt_key, None)
        if transform:
            final_opts[opt_key] = transform(opt_value)

    return final_opts


def _get_per_key_opts(
    per_key_opts,  # type: Dict[str, Any]
    opt_type,  # type: OptionsBase
    valid_opt_keys,  # type: List[str]
    durability_type     # type: Optional[Union[Type[Dict], Type[int]]]
) -> Dict[str, Any]:
    final_key_opts = {}
    for key, opts in per_key_opts.items():
        if not isinstance(opts, (opt_type, dict)):
            raise InvalidArgumentException(message=f'Expected options to be of type Union[{opt_type.__name__}, dict]')
        key_opts = {}
        for opt_key, opt_value in opts.items():
            if opt_key not in valid_opt_keys:
                continue
            transform = VALID_MULTI_OPTS.get(opt_key, None)
            if transform:
                transformed_value = transform(opt_value)
                if opt_key == 'durability':
                    if durability_type is None:
                        durability_type = dict if isinstance(transformed_value, dict) else int
                    if not isinstance(transformed_value, durability_type):
                        raise InvalidArgumentException('Durability must be either client or server for all operations.')
                    if isinstance(transformed_value, dict):
                        key_opts['persist_to'] = transformed_value['persist_to']
                        key_opts['replicate_to'] = transformed_value['replicate_to']
                    else:
                        key_opts['durability_level'] = transformed_value
                else:
                    key_opts[opt_key] = transformed_value

        final_key_opts[key] = key_opts

    return final_key_opts


def get_valid_multi_args(
    opt_type,  # type: OptionsBase
    arg_vars,  # type: Optional[Dict[str,Any]]
    *options  # type: OptionsBase
) -> Dict[str, Any]:

    temp_options = _get_temp_opts(arg_vars, *options)
    valid_opt_keys = opt_type.get_valid_keys()

    final_opts = _get_valid_global_multi_opts(temp_options, valid_opt_keys)

    if not temp_options:
        return final_opts

    per_key_opts = temp_options.pop('per_key_options', None)
    if not per_key_opts:
        return final_opts

    durability = final_opts.get('durability', None)
    durability_type = None
    if durability is not None:
        durability_type = dict if isinstance(durability, dict) else int

    final_key_opts = _get_per_key_opts(per_key_opts, opt_type, valid_opt_keys, durability_type)

    final_opts['per_key_options'] = final_key_opts
    return final_opts


"""

Couchbase Python SDK Options related Enumerations

"""


class LockMode(IntEnum):
    """
    **DEPRECATED** This Enum will be removed in a future version of the SDK.

    Enum kept for API compatibility with the 2.x/3.x series of the SDK. LockMode
    is not used or needed with the 4.x series of the SDK.
    """
    WAIT = 0
    EXC = 1
    NONE = 2


class TLSVerifyMode(Enum):
    NONE = 'none'
    PEER = 'peer'
    NO_VERIFY = 'no_verify'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE
            elif value == cls.PEER.value:
                return cls.PEER
            elif value == cls.NO_VERIFY.value:
                return cls.NONE

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Expected str representation of type TLSVerifyMode."))

    @classmethod
    def to_str(cls, value  # type: Union[TLSVerifyMode, str]
               ) -> str:
        if isinstance(value, TLSVerifyMode):
            if value == cls.NO_VERIFY:
                return cls.NONE.value
            return value.value
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE.value
            elif value == cls.PEER.value:
                return cls.PEER.value
            elif value == cls.NO_VERIFY.value:
                return cls.NONE.value

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Expected TLS verify mode to be either of type "
                                                "TLSVerifyMode or str representation "
                                                "of TLSVerifyMode."))


class IpProtocol(Enum):
    Any = 'any'
    ForceIPv4 = 'force_ipv4'
    ForceIPv6 = 'force_ipv6'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
                                                "Expected str representation of type IpProtocol."))

    @classmethod
    def to_str(cls, value  # type: Union[IpProtocol, str]
               ) -> str:
        if isinstance(value, IpProtocol):
            return value.value
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any.value
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4.value
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6.value

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
                                                "Expected IP Protocol mode to be either of type "
                                                "IpProtocol or str representation "
                                                "of IpProtocol."))


class Compression(Enum):
    """
    Can be one of:
        NONE:
            The client will not compress or decompress the data.
        IN:
            The data coming back from the server will be decompressed, if it was compressed.
        OUT:
            The data coming into server will be compressed.
        INOUT:
            The data will be compressed on way in, decompressed on way out of server.
        FORCE:
            By default the library will send a HELLO command to the server to determine whether compression
            is supported or not.  Because commands may be
            pipelined prior to the scheduing of the HELLO command it is possible that the first few commands
            may not be compressed when schedule due to the library not yet having negotiated settings with the
            server. Setting this flag will force the client to assume that all servers support compression
            despite a HELLO not having been intially negotiated.
    """

    @classmethod
    def from_int(cls, val):
        if val == 0:
            return cls.NONE
        elif val == 1:
            return cls.IN
        elif val == 2:
            return cls.OUT
        elif val == 3:
            return cls.INOUT
        elif val == 7:
            # note that the lcb flag is a 4, but when you set "force" in the connection
            # string, it sets it as INOUT|FORCE.
            return cls.FORCE
        else:
            raise InvalidArgumentException(
                "cannot convert {} to a Compression".format(val)
            )

    NONE = "off"
    IN = "inflate_only"
    OUT = "deflate_only"
    INOUT = "on"
    FORCE = "force"


class KnownConfigProfiles(Enum):
    """
    **VOLATILE** This API is subject to change at any time.

    Represents the name of a specific configuration profile that is associated with predetermined cluster options.

    """
    WanDevelopment = 'wan_development'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.WanDevelopment.value:
                return cls.WanDevelopment

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Expected str representation of type KnownConfigProfiles."))

    @classmethod
    def to_str(cls, value  # type: Union[KnownConfigProfiles, str]
               ) -> str:
        if isinstance(value, KnownConfigProfiles):
            return value.value

        # just retun the str to allow for future customer config profiles
        if isinstance(value, str):
            return value

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Expected config profile to be either of type "
                                                "KnownConfigProfiles or str representation "
                                                "of KnownConfigProfiles."))


"""

Couchbase Python SDK Cluster related Options

"""


# @TODO: remove this when we remove all the deprecated 3.x imports
TransactionConfig = Union[Dict[str, Any], Any]


"""

Couchbase Python SDK Key-Value related Options

"""


class OptionsTimeoutBase(OptionsBase):
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 span=None,  # type: Optional[SpanProtocol]
                 **kwargs  # type: Any
                 ) -> None:
        """
        Base options with timeout and span options
        :param timeout: Timeout for this operation
        :param span: Parent tracing span to use for this operation
        """
        if timeout:
            kwargs['timeout'] = timeout

        if span:
            kwargs['span'] = span

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def timeout(self, timeout: timedelta) -> OptionsTimeoutBase:
        self['timeout'] = timeout
        return self

    def span(self, span: SpanProtocol) -> OptionsTimeoutBase:
        """**DEPRECATED** use parent_span instead.  This method will be removed in a future version of the SDK."""
        self['span'] = span
        return self

    def parent_span(self, parent_span: SpanProtocol) -> OptionsTimeoutBase:
        self['parent_span'] = parent_span
        return self

# Diagnostic Operations


# Key-Value Operations


class DurabilityOptionBlockBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 durability=None,  # type: Optional[DurabilityType]
                 expiry=None,  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def expiry(self):
        return self.get("expiry", None)


# Sub-document Operations


# Binary Operations


"""

Couchbase Python SDK N1QL related Options

"""


"""

Couchbase Python SDK Analytics related Options

"""


"""

Couchbase Python SDK Full Text Search (FTS) related Options

"""


"""

Couchbase Python SDK View related Options

"""


"""

Couchbase Python SDK constrained integer classes

"""


AcceptableInts = Union[ctypes.c_int64, ctypes.c_uint64, int]


class ConstrainedIntBase():
    def __init__(self, value):
        """
        A signed integer between cls.min() and cls.max() inclusive

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        self.value = type(self).verify_value(value)

    @classmethod
    def verify_value(cls, item  # type: AcceptableInts
                     ):
        # type: (...) -> int
        value = getattr(item, 'value', item)
        if not isinstance(value, int) or not (cls.min() <= value <= cls.max()):
            raise InvalidArgumentException(
                "Integer in range {} and {} inclusiverequired".format(cls.min(), cls.max()))
        return value

    @classmethod
    def is_valid(cls,
                 item  # type: AcceptableInts
                 ):
        return isinstance(item, cls)

    def __neg__(self):
        return -self.value

    # Python 3.8 deprecated the implicit conversion to integers using __int__
    # use __index__ instead
    # still needed for Python 3.7
    def __int__(self):
        return self.value

    # __int__ falls back to __index__
    def __index__(self):
        return self.value

    def __add__(self, other):
        if not (self.min() <= (self.value + int(other)) <= self.max()):
            raise InvalidArgumentException(
                "{} + {} would be out of range {}-{}".format(self.value, other, self.min(), self.min()))

    @classmethod
    def max(cls):
        raise NotImplementedError()

    @classmethod
    def min(cls):
        raise NotImplementedError()

    def __str__(self):
        return "{cls_name} with value {value}".format(
            cls_name=type(self), value=self.value)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.value == other.value

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value


class SignedInt64Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        A signed integer between -0x8000000000000000 and +0x7FFFFFFFFFFFFFFF inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return -0x8000000000000000


class UnsignedInt32Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        An unsigned integer between 0x00000000 and +0x80000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x00000000

    @classmethod
    def min(cls):
        return 0x80000000


class UnsignedInt64Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        An unsigned integer between 0x0000000000000000 and +0x8000000000000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super().__init__(value)

    @classmethod
    def min(cls):
        return 0x0000000000000000

    @classmethod
    def max(cls):
        return 0x8000000000000000


class DeltaValueBase(ConstrainedIntBase):
    def __init__(self,
                 value  # type: AcceptableInts
                 ):
        """
        A non-negative integer between 0 and +0x7FFFFFFFFFFFFFFF inclusive.
        Used as an argument for :meth:`Collection.increment` and :meth:`Collection.decrement`

        :param value: the value to initialise this with.

        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return 0


# BRIDGE: the old *Base names, bound to the final option classes for the code that still annotates
# with them.  Removed by PYCBC-2016.
from couchbase.common.options import AnalyticsOptions as AnalyticsOptionsBase  # noqa: F401, E402
from couchbase.common.options import AppendOptions as AppendOptionsBase  # noqa: F401, E402
from couchbase.common.options import ClusterMetricsOptions as ClusterMetricsOptionsBase  # noqa: F401, E402
from couchbase.common.options import ClusterOptions as ClusterOptionsBase  # noqa: F401, E402
from couchbase.common.options import (  # noqa: F401, E402
    ClusterOrphanReportingOptions as ClusterOrphanReportingOptionsBase)
from couchbase.common.options import ClusterTimeoutOptions as ClusterTimeoutOptionsBase  # noqa: F401, E402
from couchbase.common.options import ClusterTracingOptions as ClusterTracingOptionsBase  # noqa: F401, E402
from couchbase.common.options import DecrementOptions as DecrementOptionsBase  # noqa: F401, E402
from couchbase.common.options import DiagnosticsOptions as DiagnosticsOptionsBase  # noqa: F401, E402
from couchbase.common.options import ExistsOptions as ExistsOptionsBase  # noqa: F401, E402
from couchbase.common.options import GetAllReplicasOptions as GetAllReplicasOptionsBase  # noqa: F401, E402
from couchbase.common.options import GetAndLockOptions as GetAndLockOptionsBase  # noqa: F401, E402
from couchbase.common.options import GetAndTouchOptions as GetAndTouchOptionsBase  # noqa: F401, E402
from couchbase.common.options import GetAnyReplicaOptions as GetAnyReplicaOptionsBase  # noqa: F401, E402
from couchbase.common.options import GetOptions as GetOptionsBase  # noqa: F401, E402
from couchbase.common.options import IncrementOptions as IncrementOptionsBase  # noqa: F401, E402
from couchbase.common.options import InsertOptions as InsertOptionsBase  # noqa: F401, E402
from couchbase.common.options import LookupInAllReplicasOptions as LookupInAllReplicasOptionsBase  # noqa: F401, E402
from couchbase.common.options import LookupInAnyReplicaOptions as LookupInAnyReplicaOptionsBase  # noqa: F401, E402
from couchbase.common.options import LookupInOptions as LookupInOptionsBase  # noqa: F401, E402
from couchbase.common.options import MutateInOptions as MutateInOptionsBase  # noqa: F401, E402
from couchbase.common.options import PingOptions as PingOptionsBase  # noqa: F401, E402
from couchbase.common.options import PrependOptions as PrependOptionsBase  # noqa: F401, E402
from couchbase.common.options import QueryOptions as QueryOptionsBase  # noqa: F401, E402
from couchbase.common.options import RemoveOptions as RemoveOptionsBase  # noqa: F401, E402
from couchbase.common.options import ReplaceOptions as ReplaceOptionsBase  # noqa: F401, E402
from couchbase.common.options import ScanOptions as ScanOptionsBase  # noqa: F401, E402
from couchbase.common.options import SearchOptions as SearchOptionsBase  # noqa: F401, E402
from couchbase.common.options import TouchOptions as TouchOptionsBase  # noqa: F401, E402
from couchbase.common.options import UnlockOptions as UnlockOptionsBase  # noqa: F401, E402
from couchbase.common.options import UpsertOptions as UpsertOptionsBase  # noqa: F401, E402
from couchbase.common.options import VectorSearchOptions as VectorSearchOptionsBase  # noqa: F401, E402
from couchbase.common.options import ViewOptions as ViewOptionsBase  # noqa: F401, E402
from couchbase.common.options import WaitUntilReadyOptions as WaitUntilReadyOptionsBase  # noqa: F401, E402
