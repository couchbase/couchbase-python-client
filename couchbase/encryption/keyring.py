from abc import ABC, abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from couchbase.encryption import Key


class Keyring(ABC):
    @abstractmethod
    def get_key(self,
                key_id,  # type: str
            ) -> Key:
        """Returns requested key

        Args:
            keyid (str): Key ID to retrieve

        Returns:
            :class:`~couchbase.encryption.Key`: The corresponding :class:`~couchbase.encryption.Key`
            of the provided key_id.
        
        Raises:
            :raises :class:`~couchbase.exceptions.CryptoKeyNotFoundException`
        """
