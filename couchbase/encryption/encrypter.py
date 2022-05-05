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

from abc import ABC, abstractmethod
from typing import Union

from couchbase.encryption import EncryptionResult, Keyring


class Encrypter(ABC):
    def __init__(self,
                 keyring,  # type: Keyring
                 key,  # type: str
                 ):
        self._keyring = keyring
        self._key = key

    @property
    def keyring(self) -> Keyring:
        return self._keyring

    @property
    def key(self) -> str:
        return self._key

    @abstractmethod
    def encrypt(self,
                plaintext,  # type: Union[str, bytes, bytearray]
                ) -> EncryptionResult:
        """Encrypts the given plaintext

        Args:
            plaintext (Union[str, bytes, bytearray]): The plaintext to be encrypted

        Returns:
            :class:``~couchbase.encryption.EncryptionResult`: a :class:`~couchbase.encryption.EncryptionResult`
            containing all the necessary information required for decryption.

        Raises:
            :class:`~couchbase.exceptions.InvalidCryptoKeyException`: If the :class:`.Encrypter` has an invalid
            key for encryption.
        """
        pass
