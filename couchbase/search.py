from couchbase.exceptions import NoChildrenException  # noqa: F401
from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  CouchbaseException,
                                  ExceptionMap)
from couchbase.logic.search import DateFacet  # noqa: F401
from couchbase.logic.search import Facet  # noqa: F401
from couchbase.logic.search import HighlightStyle  # noqa: F401
from couchbase.logic.search import NumericFacet  # noqa: F401
from couchbase.logic.search import SearchDateRangeFacet  # noqa: F401
from couchbase.logic.search import SearchFacetResult  # noqa: F401
from couchbase.logic.search import SearchMetaData  # noqa: F401
from couchbase.logic.search import SearchMetrics  # noqa: F401
from couchbase.logic.search import SearchNumericRangeFacet  # noqa: F401
from couchbase.logic.search import SearchQueryBuilder  # noqa: F401
from couchbase.logic.search import SearchRowLocation  # noqa: F401
from couchbase.logic.search import SearchScanConsistency  # noqa: F401
from couchbase.logic.search import SearchTermFacet  # noqa: F401
from couchbase.logic.search import Sort  # noqa: F401
from couchbase.logic.search import SortField  # noqa: F401
from couchbase.logic.search import SortGeoDistance  # noqa: F401
from couchbase.logic.search import SortID  # noqa: F401
from couchbase.logic.search import SortScore  # noqa: F401
from couchbase.logic.search import TermFacet  # noqa: F401
from couchbase.logic.search import (SearchRequestLogic,
                                    SearchRow,
                                    SearchRowLocations)
from couchbase.logic.search_queries import BooleanFieldQuery  # noqa: F401
from couchbase.logic.search_queries import BooleanQuery  # noqa: F401
from couchbase.logic.search_queries import ConjunctionQuery  # noqa: F401
from couchbase.logic.search_queries import DateRangeQuery  # noqa: F401
from couchbase.logic.search_queries import DisjunctionQuery  # noqa: F401
from couchbase.logic.search_queries import DocIdQuery  # noqa: F401
from couchbase.logic.search_queries import MatchAllQuery  # noqa: F401
from couchbase.logic.search_queries import MatchNoneQuery  # noqa: F401
from couchbase.logic.search_queries import MatchOperator  # noqa: F401
from couchbase.logic.search_queries import MatchPhraseQuery  # noqa: F401
from couchbase.logic.search_queries import MatchQuery  # noqa: F401
from couchbase.logic.search_queries import NumericRangeQuery  # noqa: F401
from couchbase.logic.search_queries import PhraseQuery  # noqa: F401
from couchbase.logic.search_queries import PrefixQuery  # noqa: F401
from couchbase.logic.search_queries import QueryStringQuery  # noqa: F401
from couchbase.logic.search_queries import RawQuery  # noqa: F401
from couchbase.logic.search_queries import RegexQuery  # noqa: F401
from couchbase.logic.search_queries import SearchQuery  # noqa: F401
from couchbase.logic.search_queries import TermQuery  # noqa: F401
from couchbase.logic.search_queries import TermRangeQuery  # noqa: F401
from couchbase.logic.search_queries import WildcardQuery  # noqa: F401
from couchbase.options import SearchOptions  # noqa: F401


class SearchRequest(SearchRequestLogic):
    def __init__(self,
                 connection,
                 encoded_query,
                 **kwargs
                 ):
        super().__init__(connection, encoded_query, **kwargs)

    @classmethod
    def generate_search_request(cls, connection, encoded_query, **kwargs):
        return cls(connection, encoded_query, **kwargs)

    def execute(self):
        return [r for r in list(self)]

    def _get_metadata(self):
        try:
            search_response = next(self._streaming_result)
            self._set_metadata(search_response)
        except StopIteration:
            pass

    def __iter__(self):
        if self.done_streaming:
            # @TODO(jc): better exception
            raise Exception("Previously iterated over results.")

        if not self.started_streaming:
            self._submit_query()

        return self

    # def _get_next_row(self):
    #     if self._done_streaming is True:
    #         return

    #     try:
    #         row = next(self._streaming_result)
    #         if issubclass(self.row_factory, SearchRow):
    #             locations = row.get('locations', None)
    #             if locations:
    #                 locations = SearchRowLocations(locations)
    #             row['locations'] = locations
    #             search_row = self.row_factory(**row)
    #         else:
    #             search_row = row
    #         self._rows.put_nowait(search_row)
    #     except StopIteration:
    #         self._done_streaming = True

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        row = next(self._streaming_result)
        if row is None:
            raise StopIteration

        deserialized_row = self.serializer.deserialize(row)
        if issubclass(self.row_factory, SearchRow):
            locations = deserialized_row.get('locations', None)
            if locations:
                locations = SearchRowLocations(locations)
            deserialized_row['locations'] = locations
            return self.row_factory(**deserialized_row)
        else:
            return deserialized_row

    def __next__(self):
        try:
            return self._get_next_row()
        except StopIteration:
            self._done_streaming = True
            self._get_metadata()
            raise
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            print(f'base exception: {ex}')
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            print(exc_cls.__name__)
            excptn = exc_cls(str(ex))
            raise excptn
