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

__all__ = [
    'Keyring',
]

from abc import ABC, abstractmethod

from couchbase.common.encryption import Key


class Keyring(ABC):
    """The interface a store of encryption keys implements."""

    @abstractmethod
    def get_key(self, key_id: str) -> Key:
        """Get a key by its ID.

        Args:
            key_id (str): The key's ID.

        Returns:
            :class:`~couchbase.encryption.Key`: The key.

        Raises:
            :class:`~couchbase.exceptions.CryptoKeyNotFoundException`: If the keyring has no key with that ID.
        """
