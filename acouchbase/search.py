#  Copyright 2016-2026. Couchbase, Inc.
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

"""The `acouchbase.search` import path.

The same objects as `couchbase.search`, re-exported so asyncio code imports them from its own
package.
"""

__all__ = [
    'BooleanFieldQuery',
    'BooleanQuery',
    'ConjunctionQuery',
    'DateFacet',
    'DateRangeQuery',
    'DisjunctionQuery',
    'DocIdQuery',
    'Facet',
    'GeoBoundingBoxQuery',
    'GeoDistanceQuery',
    'GeoPolygonQuery',
    'HighlightStyle',
    'MatchAllQuery',
    'MatchNoneQuery',
    'MatchOperator',
    'MatchPhraseQuery',
    'MatchQuery',
    'NumericFacet',
    'NumericRangeQuery',
    'PhraseQuery',
    'PrefixQuery',
    'QueryStringQuery',
    'RawQuery',
    'RegexQuery',
    'SearchDateRangeFacet',
    'SearchFacetResult',
    'SearchMetaData',
    'SearchMetrics',
    'SearchNumericRangeFacet',
    'SearchQuery',
    'SearchRequest',
    'SearchRow',
    'SearchRowFields',
    'SearchRowLocation',
    'SearchRowLocations',
    'SearchScanConsistency',
    'SearchTermFacet',
    'Sort',
    'SortField',
    'SortGeoDistance',
    'SortID',
    'SortScore',
    'TermFacet',
    'TermQuery',
    'TermRangeQuery',
    'WildcardQuery',
]

from typing import TYPE_CHECKING

from couchbase.common._supportability import Supportability
from couchbase.common.search import DateFacet  # noqa: F401
from couchbase.common.search import Facet  # noqa: F401
from couchbase.common.search import HighlightStyle  # noqa: F401
from couchbase.common.search import MatchOperator  # noqa: F401
from couchbase.common.search import NumericFacet  # noqa: F401
from couchbase.common.search import SearchDateRangeFacet  # noqa: F401
from couchbase.common.search import SearchFacetResult  # noqa: F401
from couchbase.common.search import SearchMetaData  # noqa: F401
from couchbase.common.search import SearchMetrics  # noqa: F401
from couchbase.common.search import SearchNumericRangeFacet  # noqa: F401
from couchbase.common.search import SearchRow  # noqa: F401
from couchbase.common.search import SearchRowFields  # noqa: F401
from couchbase.common.search import SearchRowLocation  # noqa: F401
from couchbase.common.search import SearchRowLocations  # noqa: F401
from couchbase.common.search import SearchScanConsistency  # noqa: F401
from couchbase.common.search import SearchTermFacet  # noqa: F401
from couchbase.common.search import Sort  # noqa: F401
from couchbase.common.search import SortField  # noqa: F401
from couchbase.common.search import SortGeoDistance  # noqa: F401
from couchbase.common.search import SortID  # noqa: F401
from couchbase.common.search import SortScore  # noqa: F401
from couchbase.common.search import TermFacet  # noqa: F401
from couchbase.common.search_queries import BooleanFieldQuery  # noqa: F401
from couchbase.common.search_queries import BooleanQuery  # noqa: F401
from couchbase.common.search_queries import ConjunctionQuery  # noqa: F401
from couchbase.common.search_queries import DateRangeQuery  # noqa: F401
from couchbase.common.search_queries import DisjunctionQuery  # noqa: F401
from couchbase.common.search_queries import DocIdQuery  # noqa: F401
from couchbase.common.search_queries import GeoBoundingBoxQuery  # noqa: F401
from couchbase.common.search_queries import GeoDistanceQuery  # noqa: F401
from couchbase.common.search_queries import GeoPolygonQuery  # noqa: F401
from couchbase.common.search_queries import MatchAllQuery  # noqa: F401
from couchbase.common.search_queries import MatchNoneQuery  # noqa: F401
from couchbase.common.search_queries import MatchPhraseQuery  # noqa: F401
from couchbase.common.search_queries import MatchQuery  # noqa: F401
from couchbase.common.search_queries import NumericRangeQuery  # noqa: F401
from couchbase.common.search_queries import PhraseQuery  # noqa: F401
from couchbase.common.search_queries import PrefixQuery  # noqa: F401
from couchbase.common.search_queries import QueryStringQuery  # noqa: F401
from couchbase.common.search_queries import RawQuery  # noqa: F401
from couchbase.common.search_queries import RegexQuery  # noqa: F401
from couchbase.common.search_queries import SearchQuery  # noqa: F401
from couchbase.common.search_queries import TermQuery  # noqa: F401
from couchbase.common.search_queries import TermRangeQuery  # noqa: F401
from couchbase.common.search_queries import WildcardQuery  # noqa: F401
from couchbase.common.search_request import SearchRequest  # noqa: F401

if not TYPE_CHECKING:
    __getattr__ = Supportability.deprecated_paths(__name__)
