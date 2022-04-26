from typing import TYPE_CHECKING

from couchbase.result import ContentProxy
from couchbase.serializer import Serializer
import logging

if TYPE_CHECKING:
    from couchbase.pycbc_core import transaction_get_result

log = logging.getLogger(__name__)


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
            self._decoded_value = self._serializer.deserialize(self._res.get("value"))
            log.debug('result has decoded value %s', self._decoded_value)
        return ContentProxy(self._decoded_value)

    def __str__(self):
        return f'TransactionGetResult{{id={self.id}, cas={self.cas}, value={self.content_as[str]} }}'
