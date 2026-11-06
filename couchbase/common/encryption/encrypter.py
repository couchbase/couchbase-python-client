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
    'Encrypter',
]

from abc import ABC, abstractmethod

from couchbase.common.encryption import EncryptionResult, Keyring


class Encrypter(ABC):
    """The interface an encrypter for one algorithm and key implements.

    Args:
        keyring (:class:`~couchbase.encryption.Keyring`): Where the encrypter finds its key.
        key (str): The ID of the key it encrypts with.
    """

    def __init__(self, keyring: Keyring, key: str) -> None:
        self._keyring = keyring
        self._key = key

    @property
    def keyring(self) -> Keyring:
        """Where the encrypter finds its key."""
        return self._keyring

    @property
    def key(self) -> str:
        """The ID of the key the encrypter encrypts with."""
        return self._key

    @abstractmethod
    def encrypt(self, plaintext: str | bytes | bytearray) -> EncryptionResult:
        """Encrypt a value.

        Args:
            plaintext (str | bytes | bytearray): The value to encrypt.

        Returns:
            :class:`~couchbase.encryption.EncryptionResult`: The ciphertext and everything needed to decrypt it.

        Raises:
            :class:`~couchbase.exceptions.InvalidCryptoKeyException`: If the encrypter's key is not valid for
                its algorithm.
        """
