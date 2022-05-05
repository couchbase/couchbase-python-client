from abc import ABC, abstractmethod
from typing import Optional, Union


class CryptoManager(ABC):
    """Interface a CryptoManager must implement

    """

    _DEFAULT_ENCRYPTER_ALIAS = "__DEFAULT__"

    @abstractmethod
    def encrypt(self,
                plaintext,  # type: Union[str, bytes, bytearray]
                encrypter_alias=None,  # type: Optional[str]
    ) -> dict:
        """Encrypts the given plaintext using the given encrypter alias.

        Args:
            plaintext (Union[str, bytes, bytearray]): Input to be encrypted
            encrypter_alias (str, optional):  Alias of encrypter to use, if None, default alias is used.

        Returns:
            Dict: A :class:`~couchbase.encryption.EncryptionResult` as a dict

        Raises:
            :class:`~couchbase.exceptions.EncryptionFailureException`
        """
        pass

    @abstractmethod
    def decrypt(self,
                encrypted,  # type: dict
            ) -> bytes:
        """Decrypts the given encrypted result based on the 'alg' key in the encrypted result.

        Args:
            encrypted (Dict): A dict containing encryption information, must have an 'alg' key.

        Returns:
            bytes: A decrypted result based on the given encrypted input.

        Raises:
            :class:`~couchbase.exceptions.DecryptionFailureException`

        """
        pass

    @abstractmethod
    def mangle(self,
                field_name,  # type: str
            ) -> str:
        """Mangles provided JSON field name.

        Args:
            field_name (str): JSON field name to be mangled.

        Returns:
            str: The mangled field name.
        """
        pass

    @abstractmethod
    def demangle(self,
                field_name,  # type: str
            ) -> str:
        """Demangles provided JSON field name.

        Args:
            field_name (str): JSON field name to be demangled.

        Returns:
            str: The demangled field name.
        """
        pass

    @abstractmethod
    def is_mangled(self,
                field_name,  # type: str
            ) -> bool:
        """Checks if provided JSON field name has been mangled.

        Args:
            field_name (str): JSON field name to check if mangled.

        Returns:
            bool: True if the field is mangled, False otherwise.

        """
        pass
