from typing import TYPE_CHECKING

from couchbase.constants import FMT_JSON
from couchbase.result import ContentProxy
from couchbase.transcoder import JSONTranscoder

if TYPE_CHECKING:
    from couchbase.pycbc_core import transaction_get_result


class TransactionGetResult:
    def __init__(self,
                 res    # type: transaction_get_result
                 ):
        self._res = res
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
            self._decoded_value = JSONTranscoder().decode_value(self._res.get("value"), FMT_JSON)
        return ContentProxy(self._decoded_value)

    def __str__(self):
        return f'TransactionGetResult{{id={self.id}, cas={self.cas}, value={self.content_as[str]} }}'
