from typing import *
from .options import OptionBlock, timedelta
from couchbase_core import abstractmethod, IterableWrapper, JSON

from couchbase_core.fulltext import SearchRequest
from datetime import timedelta


SearchQueryRow = JSON


class SearchOptions(OptionBlock):
    pass


class MetaData(object):
    def __init__(self,
                 raw_data  # type: JSON
                 ):
        self._raw_data = raw_data

    @property
    def _status(self):
        # type: (...) -> Dict[str,int]
        return self._raw_data.get('status',{})

    def success_count(self):
        # type: (...) -> int
        return self._status.get('successful')

    def error_count(self):
        # type: (...) -> int
        return self._status.get('failed')

    def took(self):
        # type: (...) -> timedelta
        return timedelta(microseconds=self._raw_data.get('took'))

    def total_hits(self):
        # type: (...) -> int
        return self._raw_data.get('total_hits')

    def max_score(self):
        # type: (...) -> float
        return self._raw_data.get('max_score')


class SearchResult(IterableWrapper):
    Facet = object

    def __init__(self,
                 raw_result  # type: SearchRequest
                 ):
        IterableWrapper.__init__(self, raw_result)

    def hits(self):
        # type: (...) -> Iterable[JSON]
        return list(x for x in self)

    def facets(self):
        # type: (...) -> Dict[str, SearchResult.Facet]
        return self.parent.facets

    def metadata(self):  # type: (...) -> MetaData
        return MetaData(IterableWrapper.metadata(self))
