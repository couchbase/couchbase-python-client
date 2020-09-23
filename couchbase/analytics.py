from couchbase.options import QueryBaseOptions, enum_value
from couchbase_core.mapper import identity
from .n1ql import *
from couchbase_core.n1ql import N1QLRequest
from enum import Enum
from couchbase_core.analytics import AnalyticsQuery, AnalyticsRequest
from couchbase_core import iterable_wrapper
from typing import *
from datetime import timedelta


class AnalyticsIndex(dict):
    def __init__(self, **kwargs):
        print("creating index from {}".format(kwargs))
        super(AnalyticsIndex, self).__init__(**kwargs['Index'])

    @property
    def name(self):
        return self.get("IndexName", None)

    @property
    def dataset_name(self):
        return self.get("DatasetName", None)

    @property
    def dataverse_name(self):
        return self.get("DataverseName", None)

    @property
    def is_primary(self):
        return self.get("IsPrimary", None)


class AnalyticsDataType(Enum):
    STRING='string'
    INT64='int64'
    DOUBLE='double'


class AnalyticsDataset(dict):
    def __init__(self, **kwargs):
        super(AnalyticsDataset, self).__init__(**kwargs)

    @property
    def dataset_name(self):
        return self.get("DatasetName", None)

    @property
    def dataverse_name(self):
        return self.get('DataverseName', None)

    @property
    def link_name(self):
        return self.get('LinkName', None)

    @property
    def bucket_name(self):
        return self.get('BucketName', None)


class AnalyticsResult(iterable_wrapper(AnalyticsRequest)):
    def client_context_id(self):
        return super(AnalyticsResult, self).client_context_id()

    def signature(self):
        return super(AnalyticsResult, self).signature()

    def warnings(self):
        return super(AnalyticsResult, self).warnings()

    def request_id(self):
        return super(AnalyticsResult, self).request_id()

    def __init__(self,
                 *args, **kwargs  # type: N1QLRequest
                 ):
        super(AnalyticsResult, self).__init__(*args, **kwargs)


class AnalyticsScanConsistency(enum.Enum):
    NOT_BOUNDED = "not_bounded"
    REQUEST_PLUS = "request_plus"


class AnalyticsOptions(QueryBaseOptions):
    VALID_OPTS = {'timeout': {'timeout': timedelta.seconds},
                  'read_only': {'readonly': identity},
                  'scan_consistency': {'consistency': enum_value},
                  'client_context_id': {},
                  'positional_parameters': {},
                  'named_parameters': {},
                  'raw': {}}

    TARGET_CLASS = AnalyticsQuery

    @overload
    def __init__(self,
                 timeout=None,  # type: timedelta
                 read_only=None,  # type: bool
                 scan_consistency=None,  # type: AnalyticsScanConsistency
                 client_context_id=None,  # type: str
                 priority=None,  # type: bool
                 positional_parameters=None,  # type: Iterable[str]
                 named_parameters=None,  # type: Dict[str, str]
                 raw=None,  # type: Dict[str,Any]
                 ):
        """

        :param timeout:
        :param read_only:
        :param scan_consistency:
        :param client_context_id:
        :param priority:
        :param positional_parameters:
        :param named_parameters:
        :param raw:
        """
        pass

    def __init__(self,
                 **kwargs
                 ):
        super(AnalyticsOptions, self).__init__(**kwargs)



