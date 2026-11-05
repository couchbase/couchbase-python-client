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

__all__ = [
    'DefaultJsonSerializer',
    'Serializer',
]

import json
from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """The interface a custom JSON serializer implements.

    :class:`~couchbase.transcoder.JSONTranscoder` uses one to turn values into JSON bytes and back.
    """

    @abstractmethod
    def serialize(self, value: Any) -> bytes:
        """Encode a value as JSON.

        Args:
            value (Any): The value to encode.

        Returns:
            bytes: The value as UTF-8 JSON.
        """
        raise NotImplementedError()

    @abstractmethod
    def deserialize(self, value: bytes) -> Any:
        """Decode JSON into a value.

        Args:
            value (bytes): UTF-8 JSON.

        Returns:
            Any: The decoded value.
        """
        raise NotImplementedError()

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return (hasattr(subclass, 'serialize') and
                callable(subclass.serialize) and
                hasattr(subclass, 'deserialize') and
                callable(subclass.deserialize))


class DefaultJsonSerializer(Serializer):
    """The serializer the SDK uses when none is given, built on the standard ``json`` module."""

    def serialize(self, value: Any) -> bytes:
        """Encode a value as UTF-8 JSON, leaving non-ASCII characters unescaped."""
        return json.dumps(value, ensure_ascii=False).encode('utf-8')

    def deserialize(self, value: bytes) -> Any:
        """Decode UTF-8 JSON into a value."""
        return json.loads(value.decode('utf-8'))
