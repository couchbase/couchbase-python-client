import json
from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """Interface a Custom Serializer must implement
    """

    @abstractmethod
    def serialize(self,
                  value  # type: Any
                  ) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def deserialize(self,
                    value  # type: bytes
                    ) -> Any:
        raise NotImplementedError()


class DefaultJsonSerializer(Serializer):
    def serialize(self,
                  value,  # type: Any
                  ) -> bytes:

        return json.dumps(value, ensure_ascii=False).encode("utf-8")

    def deserialize(self,
                    value  # type: bytes
                    ) -> Any:

        return json.loads(value.decode('utf-8'))
