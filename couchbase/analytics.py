from .n1ql import *
from couchbase_core.n1ql import N1QLRequest


class AnalyticsResult(QueryResult):
    def client_context_id(self):
        return super(AnalyticsResult, self).client_context_id()

    def signature(self):
        return super(AnalyticsResult, self).signature()

    def warnings(self):
        return super(AnalyticsResult, self).warnings()

    def request_id(self):
        return super(AnalyticsResult, self).request_id()

    def __init__(self,
                 parent  # type: N1QLRequest
                 ):
        super(AnalyticsResult, self).__init__(parent)
        self._params=parent._params
