#  Copyright 2016-2026. Couchbase, Inc.
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

__all__ = [
    'Compression',
    'IpProtocol',
    'LockMode',
    'OptionValidationMode',
    'TLSVerifyMode',
]

from enum import Enum, IntEnum

from couchbase.common.exceptions import InvalidArgumentException


class LockMode(IntEnum):
    """A locking mode from the 2.x and 3.x series of the SDK, which has no meaning in 4.x.

    .. deprecated:: 4.1.7
        This Enum will be removed in a future version of the SDK.
    """
    WAIT = 0
    EXC = 1
    NONE = 2


class TLSVerifyMode(Enum):
    """Whether the SDK verifies the certificate a server presents over TLS.

    ``PEER`` verifies it; ``NONE`` does not, and ``NO_VERIFY`` is another name for ``NONE``.
    """

    NONE = 'none'
    PEER = 'peer'
    NO_VERIFY = 'no_verify'

    @classmethod
    def from_str(cls, value: str) -> TLSVerifyMode:
        """**INTERNAL** The member a string names."""
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
    def to_str(cls, value: TLSVerifyMode | str) -> str:
        """**INTERNAL** The string a member, or a string naming one, is sent as."""
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
    """Which IP version the SDK connects over.

    ``Any`` uses whichever a server's address resolves to; ``ForceIPv4`` and ``ForceIPv6`` use only the one they
    name.
    """

    Any = 'any'
    ForceIPv4 = 'force_ipv4'
    ForceIPv6 = 'force_ipv6'

    @classmethod
    def from_str(cls, value: str) -> IpProtocol:
        """**INTERNAL** The member a string names."""
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
    def to_str(cls, value: IpProtocol | str) -> str:
        """**INTERNAL** The string a member, or a string naming one, is sent as."""
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
    """How the SDK compresses the data it exchanges with the server.

    ``NONE`` neither compresses nor decompresses.  ``IN`` decompresses what the server sends compressed,
    ``OUT`` compresses what the SDK sends, and ``INOUT`` does both.

    ``FORCE`` is ``INOUT`` without waiting to learn whether the server supports compression.  The SDK asks
    each server when it connects, and a command sent before the answer arrives goes uncompressed unless
    ``FORCE`` is set.
    """

    @classmethod
    def from_int(cls, val: int) -> Compression:
        """**INTERNAL** The member for a compression setting written as an integer."""
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


class _StrEnum(str, Enum):
    """An enum whose members are the strings they hold, on every version this package supports.

    The `str` mixin makes a member compare and hash as its value; it does not make
    `str(member)` or `'{}'.format(member)` the value, and the second changed behaviour in 3.11.
    `enum.StrEnum` settles it upstream and arrives in 3.11; this is the same two methods.
    """

    def __str__(self) -> str:
        return str.__str__(self)

    def __format__(self, format_spec: str) -> str:
        return str.__format__(self, format_spec)


class OptionValidationMode(_StrEnum):
    """What the SDK does with an option key a class does not carry.

    A `str` enum because the same dial is set from the environment, where there is nothing but
    a string to set it with.
    """

    Ignore = 'ignore'
    Warn = 'warn'
    Strict = 'strict'
