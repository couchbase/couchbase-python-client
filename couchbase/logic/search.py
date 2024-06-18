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

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Set,
                    Tuple,
                    Union)

from couchbase._utils import is_null_or_empty, to_microseconds
from couchbase.exceptions import ErrorMapper, InvalidArgumentException
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.logic.options import SearchOptionsBase
from couchbase.logic.supportability import Supportability
from couchbase.logic.vector_search import VectorQueryCombination
from couchbase.options import (SearchOptions,
                               UnsignedInt32,
                               UnsignedInt64)
from couchbase.pycbc_core import search_query
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.tracing import CouchbaseSpan

if TYPE_CHECKING:
    from couchbase.logic.search_queries import SearchQuery
    from couchbase.logic.search_request import SearchRequest
    from couchbase.logic.vector_search import VectorSearch
    from couchbase.mutation_state import MutationState  # noqa: F401

"""

_QueryBuilder class and _COMMON_FIELDS dict are strictly INTERNAL
and used to help implement the search functionality

"""


class _QueryBuilder:

    @staticmethod  # noqa: C901
    def _genprop(converter, *apipaths, **kwargs):  # noqa: C901
        """
        This internal helper method returns a property (similar to the
        @property decorator). In additional to a simple Python property,
        this also adds a type validator (`converter`) and most importantly,
        specifies the path within a dictionary where the value should be
        stored.

        Any object using this property *must* have a dictionary called ``_json``
        as a property

        :param converter: The converter to be used, e.g. `int` or `lambda x: x`
            for passthrough
        :param apipaths:
            Components of a path to store, e.g. `foo, bar, baz` for
                `foo['bar']['baz']`
        :return: A property object
        """
        if not apipaths:
            raise TypeError('Must have at least one API path')

        def fget(self):
            d = self._json_
            try:
                for x in apipaths:
                    d = d[x]
                return d
            except KeyError:
                return None

        def fset(self, value):
            value = converter(value)
            d = self._json_
            for x in apipaths[:-1]:
                d = d.setdefault(x, {})
            d[apipaths[-1]] = value

        def fdel(self):
            d = self._json_
            try:
                for x in apipaths[:-1]:
                    d = d[x]
                del d[apipaths[-1]]
            except KeyError:
                pass

        doc = kwargs.pop(
            'doc', 'Corresponds to the ``{0}`` field'.format('.'.join(apipaths)))
        return property(fget, fset, fdel, doc)

    @staticmethod
    def _genprop_str(*apipaths, **kwargs):
        """
        Convenience function to return a string property in which the value
        is converted to a string
        """
        return _QueryBuilder._genprop(str, *apipaths, **kwargs)

    @staticmethod
    def _gen_location(value):
        if len(value) != 2 or not all(map(lambda pt: isinstance(pt, (int, float)), value)):
            raise InvalidArgumentException(message='Requires a tuple: (lon, lat)')
        return [float(value[0]), float(value[1])]

    @staticmethod
    def _gen_locations(value):
        return [_QueryBuilder._gen_location(loc) for loc in value]

    @staticmethod
    def _mk_range_bucket(name,  # type: str
                         n1,  # type: str
                         n2,  # type: str
                         r1,  # type: Optional[Union[str, int, float]]
                         r2  # type: Optional[Union[str, int, float]]
                         ) -> Dict[str, Any]:
        """
        Create a named range specification for encoding.

        :param name: The name of the range as it should appear in the result
        :param n1: The name of the lower bound of the range specifier
        :param n2: The name of the upper bound of the range specified
        :param r1: The value of the lower bound (user value)
        :param r2: The value of the upper bound (user value)
        :return: A dictionary containing the range bounds. The upper and lower
            bounds are keyed under ``n1`` and ``n2``.

        More than just a simple wrapper, this will not include any range bound
        which has a user value of `None`. Likewise it will raise an exception if
        both range values are ``None``.
        """
        d = {}
        if r1 is not None:
            d[n1] = r1
        if r2 is not None:
            d[n2] = r2
        if not d:
            raise InvalidArgumentException(message='Must specify at least one range boundary!')
        d['name'] = name
        return d

    @staticmethod
    def _assign_kwargs(cls, kwargs):
        """
        Assigns all keyword arguments to a given instance, raising an exception
        if one of the keywords is not already the name of a property.
        """
        for k in kwargs:
            if not hasattr(cls, k):
                raise AttributeError(k, 'Not valid for', cls.__class__.__name__)
            setattr(cls, k, kwargs[k])

    @staticmethod
    def _validate_range_query(self, r1, r2, **kwargs):
        _QueryBuilder._assign_kwargs(self, kwargs)
        if r1 is None and r2 is None:
            raise TypeError('At least one of {0} or {1} should be specified',
                            *self._MINMAX)
        if r1 is not None:
            setattr(self, self._MINMAX[0], r1)
        if r2 is not None:
            setattr(self, self._MINMAX[1], r2)

    @staticmethod
    def _single_term_query(fields=None  # type: Optional[List[str]]
                           ) -> Callable:
        """
            **INTERNAL**
            Class decorator to include common query fields

            :param fields: List of fields to include. These should be keys of the
                `_COMMON_FIELDS` dict
        """

        def class_wrapper(cls):
            if fields:
                for f in fields:
                    field_prop = _COMMON_FIELDS[f]
                    setattr(cls, f, field_prop)

            def new_init(self, term, *args, **kwargs):
                super(type(self), self).__init__()
                if self._TERMPROP not in kwargs:
                    kwargs[self._TERMPROP] = term
                _QueryBuilder._assign_kwargs(self, kwargs)

            cls.__init__ = new_init

            return cls

        return class_wrapper

    @staticmethod
    def _with_fields(fields=None  # type: Optional[List[str]]
                     ) -> Callable:
        """
            **INTERNAL**
            Class decorator to include common query fields

            :param fields: List of fields to include. These should be keys of the
                `_COMMON_FIELDS` dict
        """

        def class_wrapper(cls):
            if fields:
                for f in fields:
                    field_prop = _COMMON_FIELDS[f]
                    setattr(cls, f, field_prop)

            return cls

        return class_wrapper


_COMMON_FIELDS = {
    'prefix_length': _QueryBuilder._genprop(
        int, 'prefix_length',
        doc="""
        When using :attr:`fuzziness`, this controls how much of the term
        or phrase is *excluded* from fuzziness. This may help improve
        performance at the expense of omitting potential fuzzy matches
        at the beginning of the string.
        """),
    'fuzziness': _QueryBuilder._genprop(
        int, 'fuzziness',
        doc="""
        Allow a given degree of fuzz in the match. Matches which are closer to
        the original term will be scored higher.

        You can apply the fuzziness to only a portion of the string by
        specifying :attr:`prefix_length` - indicating that only the part
        of the field after the prefix length should be checked with fuzziness.

        This value is specified as a float
        """),
    'field': _QueryBuilder._genprop(
        str, 'field',
        doc="""
        Restrict searching to a given document field
        """),
    'analyzer': _QueryBuilder._genprop(
        str, 'analyzer',
        doc="""
        Use a defined server-side analyzer to process the input term prior to
        executing the search
        """
    )
}

"""

Enum per the search RFC

"""


class HighlightStyle(Enum):
    """
    HighlightStyle

    Can be either:

    Ansi
        Need Example
    Html
        Need Example
    """
    Ansi = 'ansi'
    Html = 'html'


class SearchScanConsistency(Enum):
    """
    SearchScanConsistency

    This can be:

    NOT_BOUNDED
        Which means we just return what is currently in the indexes.
    """
    NOT_BOUNDED = 'not_bounded'
    REQUEST_PLUS = 'request_plus'
    AT_PLUS = 'at_plus'


class MatchOperator(Enum):
    """
    **UNCOMMITTED** This API may change in the future.
    Specifies how the individual match terms should be logically concatenated.

    Members:
    OR (default): Specifies that individual match terms are concatenated with a logical OR.
    AND: Specifies that individual match terms are concatenated with a logical AND.
    """
    OR = "or"
    AND = "and"
    """

Search Metrics and Metadata per the RFC

"""


class SearchMetrics:
    def __init__(self,
                 raw  # type: Dict[str, Any]
                 ):
        self._raw = raw

    def success_partition_count(self) -> int:
        return self._raw.get("success_partition_count", 0)

    def error_partition_count(self) -> int:
        return self._raw.get("error_partition_count", 0)

    def took(self) -> timedelta:
        us = self._raw.get("took") / 1000
        return timedelta(microseconds=us)

    def total_partition_count(self) -> int:
        return self.success_partition_count() + self.error_partition_count()

    def max_score(self) -> float:
        return self._raw.get("max_score", 0.0)

    def total_rows(self) -> int:
        return self._raw.get("total_rows", 0)

    def __repr__(self):
        return 'SearchMetrics:{}'.format(self._raw)


class SearchMetaData:
    """Represents the meta-data returned along with a search query result."""

    def __init__(self,
                 raw  # type: Dict[str, Any]
                 ):
        self._raw = raw

    def errors(self) -> Dict[str, str]:
        return self._raw.get('errors', {})

    def metrics(self) -> Optional[SearchMetrics]:
        if 'metrics' in self._raw:
            return SearchMetrics(self._raw.get('metrics', {}))
        return None

    def client_context_id(self) -> Optional[str]:
        return self._raw.get('client_context_id', None)

    def __repr__(self):
        return 'SearchMetaData:{}'.format(self._raw)


"""

Sorting Classes

"""


class Sort:
    def __init__(self, by,  # type: str
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        self._json_ = {
            'by': by
        }
        if 'descending' in kwargs:
            kwargs['desc'] = kwargs.pop('descending')
        _QueryBuilder._assign_kwargs(self, kwargs)

    def set_prop(self, key,  # type: str
                 value  # type: Any
                 ) -> None:
        self._json_[key] = value

    # desc = _QueryBuilder._genprop(bool, 'desc', doc='Sort using descending order')

    @property
    def desc(self) -> Optional[bool]:
        return self._json_.get('desc', None)

    @desc.setter
    def desc(self, value  # type: bool
             ) -> None:
        self.set_prop('desc', value)

    @property
    def descending(self) -> Optional[bool]:
        return self._json_.get('desc', None)

    @descending.setter
    def descending(self, value  # type: bool
                   ) -> None:
        self.set_prop('desc', value)

    def as_encodable(self):
        return self._json_


class SortString(Sort):
    """
    Sorts by a list of fields. This is similar to specifying a list of
    fields in :attr:`Params.sort`
    """

    def __init__(self, *fields  # type: str
                 ) -> None:
        self._json_ = list(fields)


class SortScore(Sort):
    """
    Sorts by the score of each match.
    """

    def __init__(self, **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super(SortScore, self).__init__('score', **kwargs)


class SortID(Sort):
    """
    Sorts lexically by the document ID of each match
    """

    def __init__(self, **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super(SortID, self).__init__('id', **kwargs)


class SortField(Sort):
    """
    Sorts according to the properties of a given field
    """

    def __init__(self, field,  # type: str
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        kwargs['field'] = field
        super(SortField, self).__init__('field', **kwargs)

    @property
    def field(self) -> str:
        return self._json_.get('field')

    @field.setter
    def field(self, value  # type: str
              ) -> None:
        self.set_prop('field', value)

    @property
    def type(self) -> Optional[str]:
        return self._json_.get('type', None)

    @type.setter
    def type(self, value  # type: str
             ) -> None:
        self.set_prop('type', value)

    @property
    def mode(self) -> Optional[str]:
        return self._json_.get('mode', None)

    @mode.setter
    def mode(self, value  # type: str
             ) -> None:
        self.set_prop('mode', value)

    @property
    def missing(self) -> Optional[str]:
        return self._json_.get('missing', None)

    @missing.setter
    def missing(self, value  # type: str
                ) -> None:
        self.set_prop('missing', value)

    # field = _QueryBuilder._genprop_str('field', doc='Field to sort by')
    # type = _QueryBuilder._genprop_str('type', doc='Coerce field to this type')
    # mode = _QueryBuilder._genprop_str('mode')
    # missing = _QueryBuilder._genprop_str('missing')


class SortGeoDistance(Sort):
    """
    Sorts matches based on their distance from a specific location
    """

    def __init__(self, location,  # type: Tuple[float, float]
                 field,  # type: str
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        kwargs.update(location=location, field=field)
        super().__init__('geo_distance', **kwargs)

    @property
    def location(self) -> Tuple[float, float]:
        return self._json_.get('location')

    @location.setter
    def location(self, value  # type: Tuple[float, float]
                 ) -> None:
        location = _QueryBuilder._gen_location(value)
        self.set_prop('location', location)

    @property
    def field(self) -> str:
        return self._json_.get('field')

    @field.setter
    def field(self, value  # type: str
              ) -> None:
        self.set_prop('field', value)

    @property
    def unit(self) -> Optional[str]:
        return self._json_.get('unit')

    @unit.setter
    def unit(self, value  # type: str
             ) -> None:
        self.set_prop('unit', value)

    # location = _genprop(_location_conv, 'location',
    #                     doc='`(lon, lat)` of point of origin')
    # field = _QueryBuilder._genprop_str('field', doc='Field that contains the distance')
    # unit = _QueryBuilder._genprop_str('unit', doc='Distance unit used for measuring')


class SortRaw(Sort):
    def __init__(self, raw  # type: Dict[str, Any]
                 ) -> None:
        self._json_ = raw


"""

Facet Classes

"""


class Facet(object):
    """
    Base facet class. Each facet must have a field which it aggregates
    """

    def __init__(self, field,  # type: str
                 limit=None  # type: Optional[int]
                 ) -> None:
        self._json_ = {'field': field}
        if limit:
            self._json_['size'] = limit

    def set_prop(self, key,  # type: str
                 value  # type: Any
                 ) -> None:
        self._json_[key] = value

    @property
    def encodable(self):
        """
        Returns a reprentation of the object suitable for serialization
        """
        return self._json_

    @property
    def field(self) -> str:
        return self._json_.get('field', None)

    @field.setter
    def field(self, value  # type: str
              ) -> None:
        self.set_prop('field', value)

    @property
    def limit(self) -> Optional[int]:
        return self._json_.get('size', None)

    @limit.setter
    def limit(self, value  # type: int
              ) -> None:
        self.set_prop('size', value)

    """
    Field upon which the facet will aggregate
    """
    # field = _QueryBuilder._genprop_str('field')

    # limit = _QueryBuilder._genprop(int, 'size',
    #                  doc="Maximum number of facet results to return")


class TermFacet(Facet):
    """
    Facet aggregating the most frequent terms used.
    """


class DateFacet(Facet):
    """
    Facet to aggregate results based on a date range.
    This facet must have at least one invocation of :meth:`add_range` before
    it is added to :attr:`~Params.facets`.
    """

    def __init__(self, field,  # type: str
                 limit=None  # type: Optional[int]
                 ) -> None:
        super().__init__(field, limit)
        self.date_ranges = []

    def add_range(self, name,  # type: str
                  start=None,  # type: Optional[str]
                  end=None  # type: Optional[str]
                  ) -> DateFacet:
        """
        Adds a date range to the given facet.

        :param str name:
            The name by which the results within the range can be accessed
        :param str start: Lower date range. Should be in RFC 3339 format
        :param str end: Upper date range.
        :return: The `DateFacet` object, so calls to this method may be
            chained
        """
        self.date_ranges.append(_QueryBuilder._mk_range_bucket(name, 'start', 'end', start, end))
        return self

    @property
    def date_ranges(self) -> Optional[List[DateFacet]]:
        return self._json_.get('date_ranges', None)

    @date_ranges.setter
    def date_ranges(self, value  # type: List[DateFacet]
                    ) -> None:
        self.set_prop('date_ranges', value)


class NumericFacet(Facet):
    """
    Facet to aggregate results based on a numeric range.
    This facet must have at least one invocation of :meth:`add_range`
    before it is added to :attr:`Params.facets`
    """

    def __init__(self, field,  # type: str
                 limit=None  # type: Optional[int]
                 ) -> None:
        super().__init__(field, limit)
        self.numeric_ranges = []

    def add_range(self, name,  # type: str
                  min=None,  # type: Optional[Union[int, float]]
                  max=None  # type: Optional[Union[int, float]]
                  ) -> NumericFacet:
        """
        Add a numeric range.

        :param str name:
            the name by which the range is accessed in the results
        :param int | float min: Lower range bound
        :param int | float max: Upper range bound
        :return: This object; suitable for method chaining
        """
        self.numeric_ranges.append(_QueryBuilder._mk_range_bucket(name, 'min', 'max', min, max))
        return self

    @property
    def numeric_ranges(self) -> Optional[List[NumericFacet]]:
        return self._json_.get('numeric_ranges', None)

    @numeric_ranges.setter
    def numeric_ranges(self, value  # type: List[NumericFacet]
                       ) -> None:
        self.set_prop('numeric_ranges', value)

    # _ranges = _genprop(list, 'numeric_ranges')


class _FacetDict(dict):
    """
    Internal dict subclass which ensures that facets added to this dictionary
    have properly defined ranges.
    """

    # noinspection PyMissingConstructor
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        if not isinstance(value, Facet):
            raise ValueError('Can only add facet')
        # if hasattr(value, '_ranges') and not getattr(value, '_ranges'):
        #     raise ValueError('{} object must have at least one range. Use '
        #                      'add_range'.format(value.__class__.__name__))
        if hasattr(value, 'date_ranges') and not getattr(value, 'date_ranges'):
            raise InvalidArgumentException(message=(f'{value.__class__.__name__} object must have at '
                                                    'least one range. Use add_range()'))
        if hasattr(value, 'numeric_ranges') and not getattr(value, 'numeric_ranges'):
            raise InvalidArgumentException(message=(f'{value.__class__.__name__} object must have at '
                                                    'least one range. Use add_range()'))

        super().__setitem__(key, value)

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError('only one merge at a time!')
            other = dict(args[0])
            for key in other:
                self[key] = other[key]
        for key in kwargs:
            self[key] = kwargs[key]

    def setdefault(self, key, value=None):
        if key not in self:
            self[key] = value
        return self[key]


"""

Results Classes

"""


@dataclass
class SearchTermFacet:
    term: str = None
    count: UnsignedInt64 = None


@dataclass
class SearchNumericRangeFacet:
    name: str = None
    count: UnsignedInt64 = None
    min: Union[float, UnsignedInt64] = None
    max: Union[float, UnsignedInt64] = None


@dataclass
class SearchDateRangeFacet:
    name: str = None
    count: UnsignedInt64 = None
    start: Union[str, datetime] = None
    end: Union[str, datetime] = None


@dataclass
class SearchFacetResult:
    """ An individual facet result has both metadata and details,
    as each facet can define ranges into which results are categorized."""
    name: str = None
    field: str = None
    total: UnsignedInt64 = None
    missing: UnsignedInt64 = None
    other: UnsignedInt64 = None
    terms: List[SearchTermFacet] = None
    numeric_ranges: List[SearchNumericRangeFacet] = None
    date_ranges: List[SearchDateRangeFacet] = None


@dataclass
class SearchRowLocation:
    field: str = None
    term: str = None
    position: UnsignedInt32 = None
    start: UnsignedInt32 = None
    end: UnsignedInt32 = None
    array_positions: List[UnsignedInt32] = None


class SearchRowLocations:
    def __init__(self, locations):
        self._raw_locations = locations

    def get_all(self) -> List[SearchRowLocation]:
        """list all locations (any field, any term)"""
        locations = []
        for location in self._raw_locations:
            locations.append(SearchRowLocation(**location))

        # TODO:  maybe needed when using couchbase++ streaming
        # for loc_field, terms in self._raw_locations.items():
        #     for term in terms.keys():
        #         locations.extend(self.get(loc_field, term))

        return locations

    def get(self,
            field,  # type: str
            term  # type: str
            ) -> List[SearchRowLocation]:
        """List all locations for a given field and term"""
        if field not in self._raw_locations:
            raise InvalidArgumentException(f'Cannot find "{field}" field in locations.')
        if term not in self._raw_locations[field]:
            raise InvalidArgumentException(f'Cannot find "{term}" within {field}\'s locations.')

        locations = []
        for loc in self._raw_locations[field][term]:
            new_location = {'field': field, 'term': term, 'position': None}
            new_location.update({k: v for k, v in loc.items() if k != 'pos'})
            new_location['position'] = loc.get('pos', None)
            locations.append(new_location)

        return [SearchRowLocation(**loc) for loc in locations]

    def fields(self) -> List[str]:
        """
        :return: the fields in this location
        """
        return self._raw_locations.keys()

    def terms(self) -> Set[str]:
        """
        List all terms in this locations,
        considering all fields (so a set):
        """
        result = set()
        for field_terms in self._raw_locations.values():
            result.update(field_terms.keys())
        return result

    def terms_for(self,
                  field  # type:str
                  ) -> List[str]:
        """ list the terms for a given field """
        if field not in self._raw_locations:
            raise InvalidArgumentException(f'Cannot find "{field}" field in locations.')
        return list(self._raw_locations[field].keys())

    def __repr__(self):
        if self._raw_locations:
            return f'SearchRowLocations({self._raw_locations})'
        return str(None)

# dunno why 3.x has this...seems sort of pointless


class SearchRowFields(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


@dataclass
class SearchRow:
    """A single entry of search results. The server calls them "hits",
        and represents as a JSON object. The following interface describes
        the contents of the result row."""
    index: str = None
    id: str = None
    score: float = None
    fields: SearchRowFields = field(default_factory=SearchRowFields)
    sort: list = field(default_factory=list)
    locations: SearchRowLocations = field(default_factory=SearchRowLocations)
    fragments: dict = field(default_factory=dict)
    explanation: dict = field(default_factory=dict)


"""

The SearchQueryBuild is the mechanism that holds and stores the
params related to a search query

"""


class SearchQueryBuilder:

    # empty transform will skip updating the attribute when creating an
    # SearchQueryBuilder object
    _VALID_OPTS = {
        "timeout": {"timeout": lambda x: x},
        "limit": {"limit": lambda x: x},
        "skip": {"skip": lambda x: x},
        "explain": {"explain": lambda x: x},
        "fields": {"fields": lambda x: x},
        "highlight_style": {"highlight_style": lambda x: x},
        "highlight_fields": {"highlight_fields": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "consistent_with": {"consistent_with": lambda x: x},
        "raw": {"raw": lambda x: x},
        "disable_scoring": {"disable_scoring": lambda x: x},
        "scope_name": {"scope_name": lambda x: x},
        "collections": {"collections": lambda x: x},
        "include_locations": {"include_locations": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "facets": {},
        "sort": {},
        "show_request": {"show_request": lambda x: x},
        "span": {"span": lambda x: x},
        "vector_query_combination": {"vector_query_combination": lambda x: x},
        "log_request": {"log_request": lambda x: x},
        "log_response": {"log_response": lambda x: x}
    }

    def __init__(self,
                 index_name,  # type: str
                 query=None,  # type: Optional[SearchQuery]
                 vector_search=None,  # type: Optional[VectorSearch]
                 **kwargs  # type: Dict[str, Any]
                 ):

        self._index_name = index_name
        self._params = {}
        self._query = query
        self._vector_search = vector_search
        # self._facets = {}
        # facets = kwargs.pop('facets', {})
        # if facets:
        self.facets = _FacetDict(**kwargs.pop('facets', {}))
        self._sort = None
        sort = kwargs.pop('sort', None)
        if sort is not None:
            self.sort = sort

    def set_option(self, name, value):
        """
        Set a raw option in the query. This option is encoded
        as part of the query parameters without any client-side
        verification. Use this for settings not directly exposed
        by the Python client.

        :param name: The name of the option
        :param value: The value of the option
        """
        self._params[name] = value

    def encode_vector_search(self) -> Optional[List[Dict[str, Any]]]:
        if self._vector_search is None:
            return None

        encoded_queries = []
        for query in self._vector_search.queries:
            encoded_query = {
                'field': query.field_name,
                'k': query.num_candidates if query.num_candidates is not None else 3
            }
            if query.vector is not None:
                encoded_query['vector'] = query.vector
            else:
                encoded_query['vector_base64'] = query.vector_base64
            if query.boost is not None:
                encoded_query['boost'] = query.boost
            encoded_queries.append(encoded_query)

        return encoded_queries

    def as_encodable(self) -> Dict[str, Any]:
        params = {
            'index_name': self._index_name
        }
        query = json.dumps(self._query.encodable)
        params['query'] = query
        vector_search = self.encode_vector_search()
        if vector_search:
            params['vector_search'] = json.dumps(vector_search)

        # deprecate the scope_name option, no need to pass it to the C++ client
        # as the search API will not use
        params.update({k: v for k, v in self._params.items() if k not in ['scope_name']})

        if self.facets:
            encoded_facets = {}
            for name, facet in self.facets.items():
                encoded_facets[name] = json.dumps(facet.encodable)
            params['facets'] = encoded_facets

        if self.sort:
            sort_specs = []
            if all(map(lambda s: isinstance(s, str), self.sort)):
                for s in self.sort:
                    encoded = json.dumps(s)
                    sort_specs.append(encoded)
            else:
                for s in self.sort:
                    encoded = json.dumps(s.as_encodable())
                    sort_specs.append(encoded)

            params['sort_specs'] = sort_specs

        return params

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    @property
    def timeout(self) -> Optional[float]:
        value = self._params.get('timeout', None)
        if not value:
            return None
        value = value[:-1]
        return float(value)

    @timeout.setter
    def timeout(self, value  # type: Union[timedelta,float,int]
                ) -> None:
        if not value:
            self._params.pop('timeout', 0)
        else:
            total_us = to_microseconds(value)
            self.set_option('timeout', total_us)

    @property
    def metrics(self) -> bool:
        return self._params.get('metrics', True)

    @metrics.setter
    def metrics(self, value  # type: bool
                ) -> None:
        self.set_option('metrics', value)

    @property
    def limit(self) -> Optional[int]:
        return self._params.get('limit', None)

    @limit.setter
    def limit(self, value  # type: int
              ) -> None:
        self.set_option('limit', value)

    @property
    def skip(self) -> Optional[int]:
        return self._params.get('skip', None)

    @skip.setter
    def skip(self, value  # type: int
             ) -> None:
        self.set_option('skip', value)

    @property
    def explain(self) -> bool:
        return self._params.get('explain', False)

    @explain.setter
    def explain(self, value  # type: bool
                ) -> None:
        self.set_option('explain', value)

    @property
    def disable_scoring(self) -> bool:
        return self._params.get('disable_scoring', False)

    @disable_scoring.setter
    def disable_scoring(self, value  # type: bool
                        ) -> None:
        self.set_option('disable_scoring', value)

    @property
    def include_locations(self) -> bool:
        return self._params.get('include_locations', False)

    @include_locations.setter
    def include_locations(self, value  # type: bool
                          ) -> None:
        self.set_option('include_locations', value)

    @property
    def fields(self) -> Optional[List[str]]:
        return self._params.get('fields', None)

    @fields.setter
    def fields(self, value  # type: List[str]
               ) -> None:
        if not (isinstance(value, list) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
        self.set_option('fields', value)

    @property
    def highlight_style(self) -> Optional[HighlightStyle]:
        value = self._params.get('highlight_style', None)
        if isinstance(value, HighlightStyle):
            return value
        if isinstance(value, str):
            return HighlightStyle.Html if value == 'html' else HighlightStyle.Ansi

    @highlight_style.setter
    def highlight_style(self, value  # type: Union[HighlightStyle, str]
                        ) -> None:
        if isinstance(value, HighlightStyle):
            self.set_option('highlight_style', value.value)
        elif isinstance(value, str):
            if value in [HighlightStyle.Html.value, HighlightStyle.Ansi.value]:
                self.set_option('highlight_style', value)
        else:
            raise InvalidArgumentException(message=("Excepted highlight_style to be either of type "
                                                    "HighlightStyle or str representation "
                                                    "of HighlightStyle"))

    @property
    def highlight_fields(self) -> Optional[List[str]]:
        return self._params.get('highlight_fields', None)

    @highlight_fields.setter
    def highlight_fields(self, value  # type: List[str]
                         ) -> None:
        if not (isinstance(value, list) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
        self.set_option('highlight_fields', value)

    @property
    def consistency(self) -> SearchScanConsistency:
        value = self._params.get(
            'scan_consistency', None
        )
        if value is None and 'mutation_state' in self._params:
            return SearchScanConsistency.AT_PLUS
        if value is None:
            return SearchScanConsistency.NOT_BOUNDED
        if isinstance(value, str):
            return SearchScanConsistency.REQUEST_PLUS if value == 'request_plus' else SearchScanConsistency.NOT_BOUNDED

    @consistency.setter
    def consistency(self, value  # type: Union[SearchScanConsistency, str]
                    ) -> None:
        invalid_argument = False
        if 'mutation_state' not in self._params:
            if isinstance(value, SearchScanConsistency):
                if value == SearchScanConsistency.AT_PLUS:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value.value)
            elif isinstance(value, str) and value in [sc.value for sc in SearchScanConsistency]:
                if value == SearchScanConsistency.AT_PLUS.value:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value)
            else:
                raise InvalidArgumentException(message=("Excepted consistency to be either of type "
                                                        "SearchScanConsistency or str representation "
                                                        "of SearchScanConsistency"))

        if invalid_argument:
            raise InvalidArgumentException(message=("Cannot set consistency to AT_PLUS.  Use "
                                                    "consistent_with instead or set consistency "
                                                    "to NOT_BOUNDED"))

    @property
    def consistent_with(self) -> Dict[str, Any]:
        return {
            'consistency': self.consistency,
            'scan_vectors': self._params.get('mutation_state', None)
        }

    @consistent_with.setter
    def consistent_with(self, value  # type: MutationState
                        ):
        """
        Indicate that the search should be consistent with one or more
        mutations.

        :param value: The state of the mutations it should be consistent
            with.
        :type state: :class:`~.couchbase.mutation_state.MutationState`
        """
        if self.consistency != SearchScanConsistency.NOT_BOUNDED:
            raise TypeError(
                'consistent_with not valid with other consistency options')

        # avoid circular import
        from couchbase.mutation_state import MutationState  # noqa: F811
        if not (isinstance(value, MutationState) and len(value._sv) > 0):
            raise TypeError('Passed empty or invalid state')
        # 3.x SDK had to set the consistency, couchbase++ will take care of that for us
        self._params.pop('scan_consistency', None)
        self.set_option('mutation_state', list(mt.as_dict() for mt in value._sv))

    @property
    def scope_name(self) -> Optional[str]:
        Supportability.option_deprecated('scope_name', message='The scope_name option is not used by the search API.')
        return self._params.get('scope_name', None)

    @scope_name.setter
    def scope_name(self, value  # type: str
                   ) -> None:
        Supportability.option_deprecated('scope_name', message='The scope_name option is not used by the search API.')
        self.set_option('scope_name', value)

    @property
    def collections(self) -> Optional[List[str]]:
        return self._params.get('collections', None)

    @collections.setter
    def collections(self, value  # type: List[str]
                    ) -> None:
        if not (isinstance(value, list) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
        self.set_option('collections', value)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value  # type: str
                          ) -> None:
        self.set_option('client_context_id', value)

    @property
    def sort(self) -> Optional[Union[List[str], List[Sort]]]:
        return self._sort

    @sort.setter
    def sort(self, value  # type: Union[List[str], List[Sort], List[Union[str, Sort]]]
             ) -> None:
        if all(map(lambda s: isinstance(s, str), value)):
            self._sort = value
        elif all(map(lambda s: isinstance(s, (Sort, str)), value)):
            self._sort = value
        else:
            InvalidArgumentException(message='sort option must be either List[str] | List[Sort] | List[Sort | str]')

    @property
    def raw(self) -> Optional[Dict[str, Any]]:
        return self._params.get('raw', None)

    @raw.setter
    def raw(self, value  # type: Dict[str, Any]
            ) -> None:
        if not isinstance(value, dict):
            raise TypeError("Raw option must be of type Dict[str, Any].")
        for k in value.keys():
            if not isinstance(k, str):
                raise TypeError("key for raw value must be str")
        raw_params = {f'{k}': json.dumps(v) for k, v in value.items()}
        self.set_option('raw', raw_params)

    @property
    def serializer(self) -> Optional[Serializer]:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value  # type: Serializer
                   ):
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException(message='Serializer should implement Serializer interface.')
        self.set_option('serializer', value)

    @property
    def show_request(self) -> bool:
        return self._params.get('show_request', False)

    @show_request.setter
    def show_request(self,
                     value  # type: bool
                     ):
        self.set_option('show_request', value)

    @property
    def span(self) -> Optional[CouchbaseSpan]:
        return self._params.get('span', None)

    @span.setter
    def span(self, value  # type: CouchbaseSpan
             ):
        if not issubclass(value.__class__, CouchbaseSpan):
            raise InvalidArgumentException(message='Span should implement CouchbaseSpan interface')
        self.set_option('span', value)

    @property
    def vector_query_combination(self) -> Optional[VectorQueryCombination]:
        value = self._params.get('highlight_style', None)
        if isinstance(value, VectorQueryCombination):
            return value
        if isinstance(value, str):
            return VectorQueryCombination.AND if value == 'and' else VectorQueryCombination.OR

    @vector_query_combination.setter
    def vector_query_combination(self,
                                 value  # type: Union[VectorQueryCombination, str]
                                 ) -> None:
        if isinstance(value, VectorQueryCombination):
            self.set_option('vector_query_combination', value.value)
        elif isinstance(value, str):
            if value.lower() in [VectorQueryCombination.AND.value, VectorQueryCombination.OR.value]:
                self.set_option('vector_query_combination', value.lower())
        else:
            raise InvalidArgumentException(message=("Excepted vector_query_combination to be either of type "
                                                    "VectorQueryCombination or str representation "
                                                    "of VectorQueryCombination"))

    @property
    def log_request(self) -> bool:
        return self._params.get('log_request', False)

    @log_request.setter
    def log_request(self, value  # type: bool
                    ) -> None:
        self.set_option('log_request', value)

    @property
    def log_response(self) -> bool:
        return self._params.get('log_response', False)

    @log_response.setter
    def log_response(self, value  # type: bool
                     ) -> None:
        self.set_option('log_response', value)

    @classmethod
    def create_search_query_object(cls,
                                   index_name,  # type: str
                                   search_query,  # type: SearchQuery
                                   *options,  # type: Optional[SearchOptions]
                                   **kwargs,  # type: Dict[str, Any]
                                   ) -> SearchQueryBuilder:
        args = SearchQueryBuilder.get_search_query_args(*options, **kwargs)

        facets = args.pop('facets', {})
        sort = args.pop('sort', None)

        query = cls(index_name, query=search_query, facets=facets, sort=sort)

        # metrics defaults to True
        query.metrics = args.get("metrics", True)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query

    @classmethod
    def create_search_query_from_request(cls,
                                         index_name,  # type: str
                                         request,  # type: SearchRequest
                                         *options,  # type: Optional[SearchOptions]
                                         **kwargs,  # type: Dict[str, Any]
                                         ) -> SearchQueryBuilder:
        args = SearchQueryBuilder.get_search_query_args(*options, **kwargs)

        facets = args.pop('facets', {})
        sort = args.pop('sort', None)

        # the search_query should default to MatchNoneQuery if SearchRequest only has vector_search
        search_query = request.search_query
        if search_query is None:
            # avoid circular import
            from couchbase.logic.search_queries import MatchNoneQuery
            search_query = MatchNoneQuery()

        # only set query vector_query_combination if applicable
        if request.vector_search and request.vector_search.options:
            combo = request.vector_search.options.get('vector_query_combination', None)
            if combo:
                args['vector_query_combination'] = combo

        query = cls(index_name,
                    query=search_query,
                    vector_search=request.vector_search,
                    facets=facets,
                    sort=sort)

        # metrics defaults to True
        query.metrics = args.get("metrics", True)
        # show_request defaults to False
        query.show_request = args.get("show_request", False)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query

    @staticmethod
    def get_search_query_args(*options, **kwargs):
        # lets make a copy of the options, and update with kwargs...
        opt = SearchOptions()
        # TODO: is it possible that we could have [SearchOptions, SearchOptions, ...]??
        #       If so, why???
        opts = list(options)
        for o in opts:
            if isinstance(o, (SearchOptions, SearchOptionsBase)):
                opt = o
                opts.remove(o)
        args = opt.copy()
        args.update(kwargs)
        return args


class FullTextSearchRequestLogic:
    def __init__(self,
                 connection,
                 encoded_query,
                 row_factory=SearchRow,
                 **kwargs
                 ):

        self._connection = connection
        self._encoded_query = encoded_query
        self.row_factory = row_factory
        self._streaming_result = None
        self._default_serializer = kwargs.pop('default_serializer', DefaultJsonSerializer())
        self._serializer = None
        self._started_streaming = False
        self._streaming_timeout = kwargs.pop('streaming_timeout', None)
        self._done_streaming = False
        self._metadata = None
        self._result_rows = None
        self._result_facets = None
        self._bucket_name = kwargs.pop('bucket_name', None)
        self._scope_name = kwargs.pop('scope_name', None)

    @property
    def encoded_query(self) -> Dict[str, Any]:
        return self._encoded_query

    @property
    def serializer(self) -> Serializer:
        if self._serializer:
            return self._serializer

        serializer = self.encoded_query.get('serializer', None)
        if not serializer:
            serializer = self._default_serializer

        self._serializer = serializer
        return self._serializer

    @property
    def started_streaming(self) -> bool:
        return self._started_streaming

    @property
    def done_streaming(self) -> bool:
        return self._done_streaming

    def metadata(self):
        # @TODO:  raise if query isn't complete?
        return self._metadata

    def result_rows(self):
        return self._result_rows

    def result_facets(self):
        return self._result_facets

    def _set_facets(self, facets  # type: List[Dict[str, Any]]
                    ) -> None:
        if not facets:
            return

        facet_keys = ['name', 'field', 'total', 'missing', 'other']
        if not self._result_facets:
            self._result_facets = {}
        for facet in facets:
            facet_dict = {k: v for k, v in facet.items() if k in facet_keys}
            new_facet = SearchFacetResult(**facet_dict)
            terms = facet.pop('terms', None)
            numeric_ranges = facet.pop('numeric_ranges', None)
            date_ranges = facet.pop('date_ranges', None)
            if terms:
                new_facet.terms = [SearchTermFacet(**t) for t in terms]
                # facet_results[k].terms = list(
                #     map(lambda t: SearchTermRange(**t), terms))
            if numeric_ranges:
                new_facet.numeric_ranges = [SearchNumericRangeFacet(**nr) for nr in numeric_ranges]
            #     facet_results[k].numeric_ranges = list(
            #         map(lambda nr: SearchNumericRange(**nr), numeric_ranges))
            if date_ranges:
                new_facet.date_ranges = [SearchDateRangeFacet(**dr) for dr in date_ranges]
            #     facet_results[k].date_ranges = list(
            #         map(lambda dr: SearchDateRange(**dr), date_ranges))

            self._result_facets[new_facet.name] = new_facet

    def _set_metadata(self, search_response):
        if isinstance(search_response, CouchbaseBaseException):
            raise ErrorMapper.build_exception(search_response)

        result = search_response.raw_result.get('value', None)
        if result:
            self._metadata = SearchMetaData(result.get('metadata', None))
            self._set_facets(result.get('facets', None))

    def _deserialize_row(self, row):
        # TODO:  until streaming, a dict is returned, no deserializing...
        # deserialized_row = self.serializer.deserialize(row)
        if not issubclass(self.row_factory, SearchRow):
            return row

        deserialized_row = row
        locations = deserialized_row.get('locations', None)
        if locations:
            locations = SearchRowLocations(locations)
        deserialized_row['locations'] = locations

        fields = deserialized_row.get('fields', None)
        if is_null_or_empty(fields):
            fields = None
        else:
            fields = SearchRowFields(**json.loads(fields))
        deserialized_row['fields'] = fields

        explanation = deserialized_row.get('explanation', None)
        if is_null_or_empty(explanation):
            explanation = {}
        else:
            explanation = json.loads(explanation)
        deserialized_row['explanation'] = explanation

        return self.row_factory(**deserialized_row)

    def _submit_query(self, **kwargs):
        if self.done_streaming:
            return

        self._started_streaming = True
        span = self.encoded_query.pop('span', None)
        op_args = self.encoded_query
        if self._bucket_name is not None and self._scope_name is not None:
            op_args['bucket_name'] = self._bucket_name
            op_args['scope_name'] = self._scope_name

        search_kwargs = {
            'conn': self._connection,
            'op_args': op_args
        }
        if span:
            search_kwargs['span'] = span

        streaming_timeout = self.encoded_query.get('timeout', self._streaming_timeout)
        if streaming_timeout:
            search_kwargs['streaming_timeout'] = streaming_timeout

        # this is for txcouchbase...
        callback = kwargs.pop('callback', None)
        if callback:
            search_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            search_kwargs['errback'] = errback

        self._streaming_result = search_query(**search_kwargs)

    def __iter__(self):
        raise NotImplementedError(
            'Cannot use synchronous iterator, are you using `async for`?'
        )

    def __aiter__(self):
        raise NotImplementedError(
            'Cannot use asynchronous iterator.'
        )
