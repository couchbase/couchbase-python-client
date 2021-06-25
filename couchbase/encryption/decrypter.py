from abc import ABC, abstractmethod

from couchbase.encryption import EncryptionResult, Keyring


class Decrypter(ABC):
    """Interface a Decrypter must implement

    """

    def __init__(
        self,  # type: Decrypter
        keyring,  # type: Keyring
        alg=None,  # type: str
    ):
        self._keyring = keyring
        self._alg = alg

    @property
    def keyring(self):
        return self._keyring

    def algorithm(
        self,  # type: Decrypter
    ) -> str:
        """Provides name of algorithm associated with Decrypter

        :return: The name of the description algorithm
        """
        return self._alg

    @abstractmethod
    def decrypt(
        self,  # type: Decrypter
        encrypted,  # type: EncryptionResult
    ) -> bytes:
        """Decrypts the given :class:`couchbase.encryption.EncryptionResult` ciphertext

        The Decrypter's algorithm should match the `alg` property of the given :class:`couchbase.encryption.EncryptionResult`

        :param encrypted: :class:`couchbase.encryption.EncryptionResult` to decrypt

        :return: The decrypted ciphertext of the given :class:`couchbase.encryption.EncryptionResult`

        :raises :class:`couchbase.exceptions.InvalidCryptoKeyException`
        :raises :class:`couchbase.exceptions.InvalidCipherTextException`
        """
        pass
