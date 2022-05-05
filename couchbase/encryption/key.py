from typing import Union

class Key:
    def __init__(self,
                id,  # type: str
                bytes_,  # type: Union[bytes, bytearray]
            ):
        self._id = id
        self._bytes = bytes_ if isinstance(bytes_, bytes) else bytes(bytes_)

    @property
    def id(self) -> str:
        return self._id

    @property
    def bytes(self) -> bytes:
        return self._bytes
