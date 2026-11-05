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
    'CryptoManager',
]

from abc import ABC, abstractmethod
from typing import Any


class CryptoManager(ABC):
    """The interface field-level encryption is built on.

    A crypto manager encrypts and decrypts field values, and mangles the names of encrypted fields so a reader can tell
    them apart.
    """

    _DEFAULT_ENCRYPTER_ALIAS = '__DEFAULT__'

    @abstractmethod
    def encrypt(self, plaintext: str | bytes | bytearray, encrypter_alias: str | None = None) -> dict[str, Any]:
        """Encrypt a value with the encrypter registered under an alias.

        Args:
            plaintext (str | bytes | bytearray): The value to encrypt.
            encrypter_alias (str, optional): The alias of the encrypter to use. Defaults to None, which uses the
                default encrypter.

        Returns:
            dict[str, Any]: An :class:`~couchbase.encryption.EncryptionResult` as a dict.

        Raises:
            :class:`~couchbase.exceptions.EncryptionFailureException`: If the value could not be encrypted.
        """

    @abstractmethod
    def decrypt(self, encrypted: dict[str, Any]) -> bytes:
        """Decrypt a value with the decrypter its ``alg`` key names.

        Args:
            encrypted (dict[str, Any]): An encrypted value, which must have an ``alg`` key.

        Returns:
            bytes: The decrypted value.

        Raises:
            :class:`~couchbase.exceptions.DecryptionFailureException`: If the value could not be decrypted.
        """

    @abstractmethod
    def mangle(self, field_name: str) -> str:
        """Mangles provided JSON field name.

        Args:
            field_name (str): JSON field name to be mangled.

        Returns:
            str: The mangled field name.
        """

    @abstractmethod
    def demangle(self, field_name: str) -> str:
        """Demangles provided JSON field name.

        Args:
            field_name (str): JSON field name to be demangled.

        Returns:
            str: The demangled field name.
        """

    @abstractmethod
    def is_mangled(self, field_name: str) -> bool:
        """Checks if provided JSON field name has been mangled.

        Args:
            field_name (str): JSON field name to check if mangled.

        Returns:
            bool: True if the field is mangled, False otherwise.

        """
