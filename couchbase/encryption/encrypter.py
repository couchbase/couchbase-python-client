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
