from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from couchbase.encryption import EncryptionResult, Keyring


class Decrypter(ABC):
    """Interface a Decrypter must implement

    """

    def __init__(self,
        keyring,  # type: Keyring
        alg=None,  # type: Optional[str]
    ):
        self._keyring = keyring
        self._alg = alg

    @property
    def keyring(self):
        return self._keyring

    def algorithm(self) -> str:
        """Provides name of algorithm associated with Decrypter

        Returns:
            str: The name of the description algorithm
        """
        return self._alg

    @abstractmethod
    def decrypt(self,
                encrypted,  # type: EncryptionResult
                ) -> bytes:
        """Decrypts the given :class:`~couchbase.encryption.EncryptionResult` ciphertext.

        The Decrypter's algorithm should match the `alg` property of the given 
        :class:`~couchbase.encryption.EncryptionResult`

        Args:
            encrypted (:class:`~couchbase.encryption.EncryptionResult`): The encrypted value to decrypt.

        Returns:
            bytes: The decrypted ciphertext.

        Raises:
            :class:`~couchbase.exceptions.InvalidCryptoKeyException`
            :class:`~couchbase.exceptions.InvalidCipherTextException`
        """
