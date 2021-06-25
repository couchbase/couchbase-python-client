class Key(object):
    def __init__(
        self,
        id,  # type: str
        bytes_,  # type: bytes | bytearray
    ):
        self._id = id
        self._bytes = bytes_ if isinstance(bytes_, bytes) else bytes(bytes_)

    @property
    def id(
        self,  # type: "Key"
    ) -> str:
        return self._id

    @property
    def bytes(
        self,  # type: "Key"
    ) -> bytes:
        return self._bytes
