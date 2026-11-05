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

__all__ = [
    'JSONTranscoder',
    'LegacyTranscoder',
    'RawBinaryTranscoder',
    'RawJSONTranscoder',
    'RawStringTranscoder',
    'Transcoder',
]

import json
import pickle  # nosec
import sys
from abc import ABC, abstractmethod
from typing import (TYPE_CHECKING,
                    Any,
                    Generic)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

from couchbase.common.constants import (FMT_BYTES,
                                        FMT_COMMON_MASK,
                                        FMT_JSON,
                                        FMT_LEGACY_MASK,
                                        FMT_PICKLE,
                                        FMT_UTF8)
from couchbase.common.exceptions import ValueFormatException
from couchbase.common.serializer import DefaultJsonSerializer

if TYPE_CHECKING:
    from couchbase.common.serializer import Serializer


_UNIFIED_FORMATS = (FMT_JSON, FMT_BYTES, FMT_UTF8, FMT_PICKLE)
_LEGACY_FORMATS = tuple([x & FMT_LEGACY_MASK for x in _UNIFIED_FORMATS])
_COMMON_FORMATS = tuple([x & FMT_COMMON_MASK for x in _UNIFIED_FORMATS])

_COMMON2UNIFIED: dict[int, int] = {}
_LEGACY2UNIFIED: dict[int, int] = {}

for fl in _UNIFIED_FORMATS:
    _COMMON2UNIFIED[fl & FMT_COMMON_MASK] = fl
    _LEGACY2UNIFIED[fl & FMT_LEGACY_MASK] = fl


def _get_decode_format(flags: int | None) -> int | None:
    """Decode the flags value for transcoding.

    Args:
        flags (int, optional): The flags to decode.

    Returns:
        int | None: The common flags or legacy flags format.  If None
            is returned the format is UNKNOWN.
    """
    # return None for unknown format
    if flags is None:
        return flags

    c_flags = flags & FMT_COMMON_MASK
    l_flags = flags & FMT_LEGACY_MASK

    if c_flags:
        # if unknown format, default to None
        return _COMMON2UNIFIED.get(c_flags, None)
    else:
        # if unknown format, default to None
        return _LEGACY2UNIFIED.get(l_flags, None)


# Contravariant because a transcoder that accepts a wider value type is usable wherever one
# accepting a narrower type is expected.  Defaulted to Any so that a bare Transcoder annotation
# keeps meaning what it means today.
_V_contra = TypeVar('_V_contra', contravariant=True, default=Any)


class Transcoder(ABC, Generic[_V_contra]):
    """The interface a custom transcoder implements.

    A transcoder turns a value into the bytes a document stores and the flags that record their format, and turns stored
    bytes and flags back into a value.
    """

    @abstractmethod
    def encode_value(self, value: _V_contra) -> tuple[bytes, int]:
        """Encode a value for storage.

        Args:
            value (Any): The value to store.

        Returns:
            tuple[bytes, int]: The bytes to store and the flags recording their format.

        Raises:
            :class:`~couchbase.exceptions.ValueFormatException`: If the transcoder cannot encode the value.
        """
        raise NotImplementedError()

    @abstractmethod
    def decode_value(self, value: bytes, flags: int | None) -> Any:
        """Decode stored bytes into a value.

        Args:
            value (bytes): The bytes the document stores.
            flags (int | None): The flags stored with them.

        Returns:
            Any: The decoded value.

        Raises:
            :class:`~couchbase.exceptions.ValueFormatException`: If the transcoder cannot decode the format the
                flags record.
        """
        raise NotImplementedError()

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return (hasattr(subclass, 'encode_value') and
                callable(subclass.encode_value) and
                hasattr(subclass, 'decode_value') and
                callable(subclass.decode_value))


class JSONTranscoder(Transcoder[Any]):
    """The default transcoder, which stores every value as JSON.

    It encodes a str, list, tuple, dict, bool, int, float or None, and refuses bytes. It decodes a document flagged as
    JSON, or with no flags, and returns the raw bytes if that is not valid JSON. It refuses a document flagged as binary
    or as a string.

    Args:
        serializer (:class:`~couchbase.serializer.Serializer`, optional): The serializer that does the JSON
            encoding and decoding. Defaults to :class:`~couchbase.serializer.DefaultJsonSerializer`.
    """

    def __init__(self, serializer: Serializer | None = None) -> None:

        if not serializer:
            self._serializer: Serializer = DefaultJsonSerializer()
        else:
            self._serializer = serializer

    @override
    def encode_value(self, value: Any) -> tuple[bytes, int]:

        if isinstance(value, str):
            format = FMT_JSON
        elif isinstance(value, (bytes, bytearray)):
            raise ValueFormatException(
                "The JSONTranscoder (default transcoder) does not support binary data.")
        elif isinstance(value, (list, tuple, dict, bool, int, float)) or value is None:
            format = FMT_JSON
        else:
            raise ValueFormatException(
                "Unrecognized value type {}".format(type(value)))

        if format != FMT_JSON:
            raise ValueFormatException(f"Unrecognized format {format}")

        return self._serializer.serialize(value), FMT_JSON

    @override
    def decode_value(self, value: bytes, flags: int | None) -> Any:

        format = _get_decode_format(flags)

        # flags=[0 | None] special case, attempt JSON deserialize
        if format in [FMT_JSON, 0, None]:
            try:
                return self._serializer.deserialize(value)
            except Exception:
                # if error encountered, assume return bytes
                return value
        elif format == FMT_BYTES:
            raise ValueFormatException("The JSONTranscoder (default transcoder) does not support binary format")
        elif format == FMT_UTF8:
            raise ValueFormatException("The JSONTranscoder (default transcoder) does not support string format")
        else:
            raise ValueFormatException(f"Unrecognized format provided: {format}")


class RawJSONTranscoder(Transcoder[str | bytes | bytearray]):
    """A transcoder for JSON that is already encoded.

    It stores a str as UTF-8 and bytes as given, both flagged as JSON, and returns a JSON document's bytes undecoded. It
    refuses any other value, and a document flagged as binary or as a string.
    """

    @override
    def encode_value(self, value: str | bytes | bytearray) -> tuple[bytes, int]:

        if isinstance(value, str):
            return value.encode('utf-8'), FMT_JSON
        elif isinstance(value, (bytes, bytearray)):
            if isinstance(value, bytearray):
                value = bytes(value)
            return value, FMT_JSON
        else:
            raise ValueFormatException("Only binary and string data supported by RawJSONTranscoder")

    @override
    def decode_value(self, value: bytes, flags: int | None) -> str | bytes:

        format = _get_decode_format(flags)

        if format == FMT_BYTES:
            raise ValueFormatException("Binary format type not supported by RawJSONTranscoder")
        elif format == FMT_UTF8:
            raise ValueFormatException("String format type not supported by RawJSONTranscoder")
        elif format == FMT_JSON:
            if isinstance(value, str):
                value = value.decode('utf-8')
            elif isinstance(value, bytearray):
                value = bytes(value)
            return value
        else:
            raise ValueFormatException(f"Unrecognized format provided: {format}")


class RawStringTranscoder(Transcoder[str]):
    """A transcoder for plain strings.

    It stores a str as UTF-8 flagged as a string, and decodes a document so flagged back to a str. It refuses any other
    value, and a document flagged as binary or as JSON.
    """

    @override
    def encode_value(self, value: str) -> tuple[bytes, int]:

        if isinstance(value, str):
            return value.encode('utf-8'), FMT_UTF8
        else:
            raise ValueFormatException("Only string data supported by RawStringTranscoder")

    @override
    def decode_value(self, value: bytes, flags: int | None) -> str | bytes:

        format = _get_decode_format(flags)

        if format == FMT_BYTES:
            raise ValueFormatException("Binary format type not supported by RawStringTranscoder")
        elif format == FMT_UTF8:
            return value.decode('utf-8')
        elif format == FMT_JSON:
            raise ValueFormatException("JSON format type not supported by RawStringTranscoder")
        else:
            raise ValueFormatException(f"Unrecognized format provided: {format}")


class RawBinaryTranscoder(Transcoder[bytes | bytearray]):
    """A transcoder for binary data.

    It stores bytes or a bytearray flagged as binary, and returns a document so flagged as bytes. It refuses any other
    value, and a document flagged as a string or as JSON.
    """

    @override
    def encode_value(self, value: bytes | bytearray) -> tuple[bytes, int]:

        if isinstance(value, (bytes, bytearray)):
            if isinstance(value, bytearray):
                value = bytes(value)
            return value, FMT_BYTES
        else:
            raise ValueFormatException("Only binary data supported by RawBinaryTranscoder")

    @override
    def decode_value(self, value: bytes, flags: int | None) -> bytes:

        format = _get_decode_format(flags)

        if format == FMT_BYTES:
            if isinstance(value, bytearray):
                value = bytes(value)
            return value
        elif format == FMT_UTF8:
            raise ValueFormatException("String format type not supported by RawBinaryTranscoder")
        elif format == FMT_JSON:
            raise ValueFormatException("JSON format type not supported by RawBinaryTranscoder")
        else:
            raise ValueFormatException(f"Unrecognized format provided: {format}")


class LegacyTranscoder(Transcoder[Any]):
    """A transcoder that picks the format from the value's type.

    It stores a str as a string, bytes as binary, a list, tuple, dict, bool, int, float or None as JSON, and pickles
    anything else. Decoding follows the flags, and returns the raw bytes for a JSON document that does not parse and for
    a format it does not know.

    Decoding unpickles a document flagged as pickled, which can run arbitrary code, so read with it only documents
    written by code you trust.
    """

    @override
    def encode_value(self, value: Any) -> tuple[bytes, int]:

        if isinstance(value, str):
            format = FMT_UTF8
        elif isinstance(value, (bytes, bytearray)):
            format = FMT_BYTES
        elif isinstance(value, (list, tuple, dict, bool, int, float)) or value is None:
            format = FMT_JSON
        else:
            format = FMT_PICKLE

        if format == FMT_BYTES:
            if isinstance(value, bytes):
                pass
            elif isinstance(value, bytearray):
                value = bytes(value)
            else:
                raise ValueFormatException('Expected bytes')
            return value, format
        elif format == FMT_UTF8:
            return value.encode('utf-8'), format
        elif format == FMT_PICKLE:
            return pickle.dumps(value), FMT_PICKLE
        else:  # default to JSON
            return json.dumps(value, ensure_ascii=False).encode('utf-8'), FMT_JSON

    @override
    def decode_value(self, value: bytes, flags: int | None) -> Any:

        format = _get_decode_format(flags)

        # flags=[0 | None] special case, attempt JSON deserialize
        if format in [FMT_JSON, 0, None]:
            try:
                return json.loads(value.decode('utf-8'))
            except Exception:
                # if error encountered, assume bytes
                return value
        elif format == FMT_BYTES:
            return value
        elif format == FMT_UTF8:
            return value.decode('utf-8')
        elif format == FMT_PICKLE:
            return pickle.loads(value)  # nosec
        else:
            # default to returning bytes
            return value
