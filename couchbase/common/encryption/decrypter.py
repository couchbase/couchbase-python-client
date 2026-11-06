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
    'Decrypter',
]

from abc import ABC, abstractmethod

from couchbase.common.encryption import EncryptionResult, Keyring


class Decrypter(ABC):
    """The interface a decrypter for one algorithm implements.

    Args:
        keyring (:class:`~couchbase.encryption.Keyring`): Where the decrypter finds its keys.
        alg (str, optional): The name of the algorithm the decrypter handles.
    """

    def __init__(self, keyring: Keyring, alg: str | None = None) -> None:
        self._keyring = keyring
        self._alg = alg

    @property
    def keyring(self) -> Keyring:
        """Where the decrypter finds its keys."""
        return self._keyring

    def algorithm(self) -> str | None:
        """Get the name of the algorithm the decrypter handles.

        Returns:
            str | None: The name of the algorithm, or None if none was given.
        """
        return self._alg

    @abstractmethod
    def decrypt(self, encrypted: EncryptionResult) -> bytes:
        """Decrypt an encrypted value.

        The decrypter's algorithm should match the value's ``alg``.

        Args:
            encrypted (:class:`~couchbase.encryption.EncryptionResult`): The value to decrypt.

        Returns:
            bytes: The decrypted value.

        Raises:
            :class:`~couchbase.exceptions.InvalidCryptoKeyException`: If the key is not valid for the algorithm.
            :class:`~couchbase.exceptions.InvalidCipherTextException`: If the ciphertext cannot be decrypted.
        """
