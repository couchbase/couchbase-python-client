#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from couchbase.exceptions import NoChildrenException  # noqa: F401
from couchbase.exceptions import (PYCBC_ERROR_MAP,
                                  AlreadyQueriedException,
                                  CouchbaseException,
                                  ErrorMapper,
                                  ExceptionMap)
from couchbase.exceptions import exception as CouchbaseBaseException
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
from couchbase.logic.search import SearchRow  # noqa: F401
from couchbase.logic.search import SearchRowFields  # noqa: F401
from couchbase.logic.search import SearchRowLocation  # noqa: F401
from couchbase.logic.search import SearchRowLocations  # noqa: F401
from couchbase.logic.search import SearchScanConsistency  # noqa: F401
from couchbase.logic.search import SearchTermFacet  # noqa: F401
from couchbase.logic.search import Sort  # noqa: F401
from couchbase.logic.search import SortField  # noqa: F401
from couchbase.logic.search import SortGeoDistance  # noqa: F401
from couchbase.logic.search import SortID  # noqa: F401
from couchbase.logic.search import SortScore  # noqa: F401
from couchbase.logic.search import TermFacet  # noqa: F401
from couchbase.logic.search import FullTextSearchRequestLogic
from couchbase.logic.search_queries import BooleanFieldQuery  # noqa: F401
from couchbase.logic.search_queries import BooleanQuery  # noqa: F401
from couchbase.logic.search_queries import ConjunctionQuery  # noqa: F401
from couchbase.logic.search_queries import DateRangeQuery  # noqa: F401
from couchbase.logic.search_queries import DisjunctionQuery  # noqa: F401
from couchbase.logic.search_queries import DocIdQuery  # noqa: F401
from couchbase.logic.search_queries import GeoBoundingBoxQuery  # noqa: F401
from couchbase.logic.search_queries import GeoDistanceQuery  # noqa: F401
from couchbase.logic.search_queries import GeoPolygonQuery  # noqa: F401
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
from couchbase.logic.search_request import SearchRequest  # noqa: F401
from couchbase.logic.supportability import Supportability


class FullTextSearchRequest(FullTextSearchRequestLogic):
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
            # @TODO:  PYCBC-1524
            search_response = next(self._streaming_result)
            self._set_metadata(search_response)
        except CouchbaseException as ex:
            raise ex
        except Exception as ex:
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn

    def __iter__(self):
        if self.done_streaming:
            raise AlreadyQueriedException()

        if not self.started_streaming:
            self._submit_query()

        return self

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        try:
            row = next(self._streaming_result)
        except StopIteration:
            # @TODO:  PYCBC-1524
            row = next(self._streaming_result)

        if isinstance(row, CouchbaseBaseException):
            raise ErrorMapper.build_exception(row)
        # should only be None one query request is complete and _no_ errors found
        if row is None:
            raise StopIteration

        return self._deserialize_row(row)

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
            exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
            excptn = exc_cls(str(ex))
            raise excptn


"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead use:
    * All options should be imported from `couchbase.options`.

"""

from couchbase.logic.options import SearchOptionsBase  # nopep8 # isort:skip # noqa: E402


@Supportability.import_deprecated('couchbase.search', 'couchbase.options')
class SearchOptions(SearchOptionsBase):  # noqa: F811
    pass
