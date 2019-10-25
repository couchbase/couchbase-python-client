from typing import *
from .options import OptionBlock, Seconds
from couchbase_core import abstractmethod, IterableWrapper, JSON

from couchbase_core.fulltext import SearchRequest

SearchQueryRow = JSON


class SearchOptions(OptionBlock):
    pass


class ISearchResult(object):
    Facet = object
    @abstractmethod
    def hits(self):
        # type: (...) -> List[SearchQueryRow]
        pass

    @abstractmethod
    def facets(self):
        # type: (...) -> Mapping[str, Facet]
        pass

    @abstractmethod
    def metadata(self):
        # type: (...) -> IMetaData
        pass


class IMetaData(object):
    @abstractmethod
    def success_count(self):
        # type: (...) -> int
        pass

    @abstractmethod
    def error_count(self):
        # type: (...) -> int
        pass

    @abstractmethod
    def took(self):
        # type: (...) -> Seconds
        pass

    @abstractmethod
    def total_hits(self):
        # type: (...) -> int
        pass

    @abstractmethod
    def max_score(self):
        # type: (...) -> float
        pass


class MetaData(IMetaData):
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
        # type: (...) -> Seconds
        return Seconds(self._raw_data.get('took')/10e6)

    def total_hits(self):
        # type: (...) -> int
        return self._raw_data.get('total_hits')

    def max_score(self):
        # type: (...) -> float
        return self._raw_data.get('max_score')


class SearchResult(ISearchResult, IterableWrapper):
    def __init__(self,
                 raw_result  # type: SearchRequest
                 ):
        IterableWrapper.__init__(self, raw_result)

    def hits(self):
        # type: (...) -> Iterable[JSON]
        return list(x for x in self)

    def facets(self):
        # type: (...) -> Dict[str,ISearchResult.Facet]
        return self.parent.facets

    def metadata(self):  # type: (...) -> IMetaData
        return MetaData(IterableWrapper.metadata(self))
