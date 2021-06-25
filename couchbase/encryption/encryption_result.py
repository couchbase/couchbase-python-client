from typing import Optional, Any
import base64

from couchbase.exceptions import CryptoKeyNotFoundException, InvalidArgumentException


class EncryptionResult:
    def __init__(
        self,  # type: "EncryptionResult"
        alg,  # type: str
        kid=None,  # type: str
        ciphertext=None,  # type: str
        **kwargs,  # type: Optional[Any]
    ):
        self._map = {"alg": alg}

        if kid:
            self._map["kid"] = kid

        if ciphertext and self._valid_base64(ciphertext):
            self._map["ciphertext"] = ciphertext

        if kwargs:
            self._map.update(**kwargs)

    @classmethod
    def new_encryption_result_from_dict(
        cls,  # type: "EncryptionResult"
        values,  # type: dict
    ) -> "EncryptionResult":

        alg = values.pop("alg", None)
        if not alg:
            raise InvalidArgumentException(
                "EncryptionResult must include alg property."
            )

        return EncryptionResult(alg, **values)

    def put(
        self,  # type: "EncryptionResult"
        key,  # type: str
        val,  # type: Any
    ):
        self._map[key] = val

    def put_and_base64_encode(
        self,  # type: "EncryptionResult"
        key,  # type: str
        val,  # type: bytes
    ):
        if not isinstance(val, bytes):
            raise ValueError("Provided value must be of type bytes.")
        self._map[key] = base64.b64encode(val)

    def get(
        self,  # type: "EncryptionResult"
        key,  # type: str
    ) -> Any:
        val = self._map.get(key, None)
        if not val:
            raise CryptoKeyNotFoundException(
                message="No mapping to EncryptionResult value found for key: '{}'.".format(
                    key)
            )

        return val

    def algorithm(
        self,  # type: "EncryptionResult"
    ) -> str:
        return self._map["alg"]

    def get_with_base64_decode(
        self,  # type: "EncryptionResult"
        key,  # type: str
    ) -> bytes:
        val = self._map.get(key, None)
        if not val:
            raise CryptoKeyNotFoundException(
                message="No mapping to EncryptionResult value found for key: '{}'.".format(
                    key)
            )

        return base64.b64decode(val)

    def asdict(
        self,  # type: "EncryptionResult"
    ) -> dict:
        return self._map

    def _valid_base64(
        self,  # type: "EncryptionResult"
        val,  # type: str | bytes | bytearray
    ) -> bool:
        try:
            if isinstance(val, str):
                bytes_val = bytes(val, "ascii")
            elif isinstance(val, bytes):
                bytes_val = val
            elif isinstance(val, bytearray):
                bytes_val = val
            else:
                raise ValueError(
                    "Provided value must be of type str, bytes or bytearray"
                )

            return base64.b64encode(base64.b64decode(bytes_val)) == bytes_val

        except Exception as ex:
            return False
