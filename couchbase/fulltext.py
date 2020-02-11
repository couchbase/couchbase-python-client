from typing import *
from .options import OptionBlockTimeOut, timedelta
from couchbase_core import abstractmethod, IterableWrapper, JSON
from enum import Enum
from couchbase_core.fulltext import SearchRequest
from datetime import timedelta


#SearchQueryRow = JSON

# there is a v2 Params class that does what we want here -
# so for now the SearchOptions can just use it under the
# hood.  Later when we eliminate the v2 stuff, we can move
# that logic over into SearchOptions itself

class HighlightStyle(Enum):
    Ansi = 'ansi'
    Html = 'html'

class SearchOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,           # type: timedelta
                 limit=None,             # type: int
                 skip=None,              # type: int
                 explain=None,           # type: bool
                 fields=None,            # type: List[str]
                 highlight_style=None,   # type: HighlightStyle
                 highlight_fields=None,  # type: List[str]
                 scan_consistency=None,  # type: cluster.QueryScanConsistency
                 consistent_with=None,   # type: couchbase_core.MutationState
                 facets=None             # type: Dict[str, couchbase_core.fulltext.Facet]
                 ):
        pass

    def __init__(self,
                 **kwargs   # type: Any
                 ):
        # convert highlight_style to str if it is present...
        style = kwargs.get('highlight_style', None)
        if(style) :
            kwargs['highlight_style'] = style.value

        super(SearchOptions, self).__init__(**kwargs)


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
