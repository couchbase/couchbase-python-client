from abc import ABC, abstractmethod

from couchbase.encryption import Key


class Keyring(ABC):
    @abstractmethod
    def get_key(
        self,  # type: "Keyring"
        key_id,  # type: str
    ) -> "Key":
        """Returns requested key

        :param keyid: Key ID to retrieve

        :return: The corresponding :class:`couchbase.encryption.Key` of the provided key_id

        :raises :class:`couchbase.exceptions.CryptoKeyNotFoundException`
        """
        pass
