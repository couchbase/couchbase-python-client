from abc import ABC, abstractmethod
from typing import Union

from couchbase.encryption import EncryptionResult, Keyring


class Encrypter(ABC):
    def __init__(
        self,  # type: "Encrypter"
        keyring,  # type: Keyring
        key,  # type: str
    ):
        self._keyring = keyring
        self._key = key

    @property
    def keyring(
        self,  # type: "Encrypter"
    ) -> Keyring:
        return self._keyring

    @property
    def key(
        self,  # type: "Encrypter"
    ) -> str:
        return self._key

    @abstractmethod
    def encrypt(
        self,  # type: "Encrypter"
        plaintext,  # type: Union[str, bytes, bytearray]
    ) -> EncryptionResult:
        """Encryptes the given plaintext

        :param plaintext: The plaintext to be encrypted

        :return: A :class:`couchbase.encryption.EncryptionResult` containing all the necessary information required for decryption

        :raises :class:`couchbase.exceptions.InvalidCryptoKeyException`
        """
        pass
