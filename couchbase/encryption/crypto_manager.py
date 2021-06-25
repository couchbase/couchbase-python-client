from abc import ABC, abstractmethod
from typing import Union


class CryptoManager(ABC):
    """Interface a CryptoManager must implement

    """

    _DEFAULT_ENCRYPTER_ALIAS = "__DEFAULT__"

    @abstractmethod
    def encrypt(
        self,  # type: CryptoManager
        plaintext,  # type: Union[str, bytes, bytearray]
        encrypter_alias=None,  # type: str
    ) -> dict:
        """Encrypts the given plaintext using the given encrypter alias

        :param plaintext: Input to be encrypted
        :param encrypter_alias:  Alias of encrypter to use, if None, default alias is used

        :return: A :class:`couchbase.encryption.EncryptionResult` as a dict

        :raises :class:`couchbase.exceptions.EncryptionFailureException`
        """
        pass

    @abstractmethod
    def decrypt(
        self,  # type: CryptoManager
        encrypted,  # type: dict
    ) -> bytes:
        """Decrypts the given encrypted result based on the 'alg' key
        in the encrypted result

        :param encrypted: dict containing encryption information, must have an 'alg' key

        :return A decrypted result based on the given encrypted input

        :raises :class:`couchbase.exceptions.DecryptionFailureException`
        """
        pass

    @abstractmethod
    def mangle(
        self,  # type: CryptoManager
        field_name,  # type: str
    ) -> str:
        """Mangles provided JSON field name

        :param field_name: JSON field name to be mangled

        :return mangled field name
        """
        pass

    @abstractmethod
    def demangle(
        self,  # type: CryptoManager
        field_name,  # type: str
    ) -> str:
        """Demangles provided JSON field name

        :param field_name: JSON field name to be demangled

        :return demangled field name
        """
        pass

    @abstractmethod
    def is_mangled(
        self,  # type: CryptoManager
        field_name,  # type: str
    ) -> bool:
        """Checks if provided JSON field name has been mangled

        :param field_name: JSON field name to check

        :return `True` if mangled, `False` otherwise
        """
        pass
