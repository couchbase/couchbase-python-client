from typing import TYPE_CHECKING

from couchbase.result import ContentProxy
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.pycbc_core import transaction_get_result


class TransactionGetResult:
    def __init__(self,
                 res,    # type: transaction_get_result
                 serializer  # type: Serializer
                 ):
        self._res = res
        self._serializer = serializer
        self._decoded_value = None

    @property
    def id(self):
        return self._res.get("id")

    @property
    def cas(self):
        return self._res.get("cas")

    @property
    def content_as(self):
        if not self._decoded_value:
            print(f'res.get("value") returns {self._res.get("value")}')
            self._decoded_value = self._serializer.deserialize(self._res.get("value"))
        return ContentProxy(self._decoded_value)

    def __str__(self):
        return f'TransactionGetResult{{id={self.id}, cas={self.cas}, value={self.content_as[str]} }}'
