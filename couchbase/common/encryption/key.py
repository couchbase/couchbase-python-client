#  Copyright 2016-2025. Couchbase, Inc.
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
    'Key',
]

from couchbase.common._utils import fields_repr, redacted


class Key:
    """An encryption key.

    Args:
        id (str): The key's ID, which an encrypted value records.
        bytes_ (bytes | bytearray): The key itself. The repr hides it.
    """

    def __init__(self, id: str, bytes_: bytes | bytearray) -> None:
        self._id = id
        self._bytes = bytes_ if isinstance(bytes_, bytes) else bytes(bytes_)

    @property
    def id(self) -> str:
        """The key's ID, which an encrypted value records."""
        return self._id

    @property
    def bytes(self) -> bytes:
        """The key itself."""
        return self._bytes

    def __repr__(self) -> str:
        return fields_repr(self, id=self._id, bytes_=redacted(self._bytes))
