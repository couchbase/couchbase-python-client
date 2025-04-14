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

from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase.exceptions import InvalidArgumentException, NoChildrenException
from couchbase.logic.search import MatchOperator, _QueryBuilder
from couchbase.logic.supportability import Supportability

# Query Types


class SearchQuery:
    """
    Base query object. You probably want to use one of the subclasses.

    .. seealso:: :class:`MatchQuery`, :class:`BooleanQuery`,
        :class:`RegexQuery`, :class:`PrefixQuery`, :class:`NumericRangeQuery`,
        :class:`DateRangeQuery`, :class:`ConjunctionQuery`,
        :class:`DisjunctionQuery`, and others in this module.
    """

    def __init__(self):
        self._json_ = {}

    def set_prop(self, key,  # type: str
                 value  # type: Any
                 ) -> None:
        self._json_[key] = value

    # boost = _genprop(
    #     float, 'boost', doc="""
    #     When specifying multiple queries, you can give this query a
    #     higher or lower weight in order to prioritize it among sibling
    #     queries. See :class:`ConjunctionQuery`
    #     """)

    @property
    def boost(self) -> Optional[float]:
        return self._json_.get('boost', None)

    @boost.setter
    def boost(self, value  # type: float
              ) -> None:
        self.set_prop('boost', value)

    @property
    def encodable(self):
        """
        Returns an object suitable for serializing to JSON

        .. code-block:: python

            json.dumps(query.encodable)
        """
        self.validate()
        return self._json_

    def validate(self):
        """
        Optional validation function. Invoked before encoding
        """
        pass


# Single Term Queries


@_QueryBuilder._single_term_query(fields=['fuzziness', 'prefix_length', 'field'])
class TermQuery(SearchQuery):
    """
    Searches for a given term in documents. Unlike :class:`MatchQuery`,
    the term is not analyzed.

    Example::

        TermQuery('lcb_cntl_string')
    """
    _TERMPROP = 'term'

    @property
    def term(self) -> str:
        return self._json_.get('term', None)

    @term.setter
    def term(self, value  # type: str
             ) -> None:
        self.set_prop('term', value)
    # term = _QueryBuilder._genprop_str('term', doc='Exact term to search for')


@_QueryBuilder._single_term_query()
class QueryStringQuery(SearchQuery):
    """
    Query which allows users to describe a query in a query language.
    The server will then execute the appropriate query based on the contents
    of the query string:

    .. seealso::

        `Query Language <http://www.blevesearch.com/docs/Query-String-Query/>`_

    Example::

        QueryStringQuery('description:water and stuff')
    """

    _TERMPROP = 'query'

    @property
    def query(self) -> str:
        return self._json_.get('query', None)

    @query.setter
    def query(self, value  # type: str
              ) -> None:
        self.set_prop('query', value)


@_QueryBuilder._single_term_query(fields=['field'])
class WildcardQuery(SearchQuery):
    """
    Query in which the characters `*` and `?` have special meaning, where
    `?` matches 1 occurrence and `*` will match 0 or more occurrences of the
    previous character
    """
    _TERMPROP = 'wildcard'

    @property
    def wildcard(self) -> str:
        return self._json_.get('wildcard', None)

    @wildcard.setter
    def wildcard(self, value  # type: str
                 ) -> None:
        self.set_prop('wildcard', value)
    # wildcard = _genprop_str(_TERMPROP, doc='Wildcard pattern to use')


@_QueryBuilder._single_term_query()
class DocIdQuery(SearchQuery):
    """
    Matches document IDs. This is must useful in a compound query
    (for example, :class:`BooleanQuery`). When used as a criteria, only
    documents with the specified IDs will be searched.
    """
    _TERMPROP = 'ids'

    @property
    def ids(self) -> str:
        return self._json_.get('ids', None)

    @ids.setter
    def ids(self, value  # type: str
            ) -> None:
        self.set_prop('ids', value)

    # ids = _genprop(list, 'ids', doc="""
    # List of document IDs to use
    # """)

    def validate(self):
        super(DocIdQuery, self).validate()
        if not self.ids:
            raise NoChildrenException('`ids` must contain at least one ID')


@_QueryBuilder._single_term_query(fields=['prefix_length', 'fuzziness', 'field', 'analyzer'])
class MatchQuery(SearchQuery):
    """
    Query which checks one or more fields for a match
    """
    _TERMPROP = 'match'

    @property
    def match(self) -> str:
        return self._json_.get('match', None)

    @match.setter
    def match(self, value  # type: str
              ) -> None:
        self.set_prop('match', value)

    @property
    def match_operator(self) -> Optional[MatchOperator]:
        value = self._json_.get('operator', None)
        if not value:
            return value

        return MatchOperator.OR if value == 'or' else MatchOperator.AND

    @match_operator.setter
    def match_operator(self, value  # type: Union[MatchOperator, str]
                       ) -> None:
        match_op = value
        if isinstance(value, MatchOperator):
            match_op = value.value
        if match_op and match_op.lower() not in ('or', 'and'):
            raise ValueError(("Excepted match_operator to be either of type "
                              "MatchOperator or str representation "
                              "of MatchOperator"))
        self.set_prop('operator', match_op)

    # match = _genprop_str(
    #     'match', doc="""
    #     String to search for
    #     """)
    # match_operator = _genprop(
    #     _match_operator, 'operator', doc='**UNCOMMITTED** This API may change in the future.
    #     Specifies how the individual match terms should be logically concatenated.')


@_QueryBuilder._single_term_query(fields=['field', 'analyzer'])
class MatchPhraseQuery(SearchQuery):
    """
    Search documents which match a given phrase. The phrase is composed
    of one or more terms.

    Example::

        MatchPhraseQuery("Hello world!")
    """
    _TERMPROP = 'match_phrase'

    @property
    def match_phrase(self) -> str:
        return self._json_.get('match_phrase', None)

    @match_phrase.setter
    def match_phrase(self, value  # type: str
                     ) -> None:
        self.set_prop('match_phrase', value)

    # match_phrase = _genprop_str(_TERMPROP, doc="Phrase to search for")


@_QueryBuilder._with_fields(fields=['field'])
class PhraseQuery(SearchQuery):
    _TERMPROP = 'terms'

    def __init__(self, *phrases,  # type: str
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super().__init__()
        if self._TERMPROP not in kwargs:
            kwargs[self._TERMPROP] = phrases
        _QueryBuilder._assign_kwargs(self, kwargs)

    @property
    def terms(self) -> List[str]:
        return self._json_.get('terms', None)

    @terms.setter
    def terms(self, value  # type: Union[List[str], Tuple[str]]
              ) -> None:
        if not (isinstance(value, (list, tuple)) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
        if isinstance(value, tuple):
            self.set_prop('terms', list(value))
        else:
            self.set_prop('terms', value)

    def validate(self):
        super(PhraseQuery, self).validate()
        if not self.terms:
            raise NoChildrenException('Missing terms')

    # terms = _genprop(list, 'terms', doc='List of terms to search for')


@_QueryBuilder._single_term_query(fields=['field'])
class PrefixQuery(SearchQuery):
    """
    Search documents for fields beginning with a certain prefix. This is
    most useful for type-ahead or lookup queries.
    """
    _TERMPROP = 'prefix'

    @property
    def prefix(self) -> str:
        return self._json_.get('prefix', None)

    @prefix.setter
    def prefix(self, value  # type: str
               ) -> None:
        self.set_prop('prefix', value)

    # prefix = _genprop_str('prefix', doc='The prefix to match')


@_QueryBuilder._single_term_query(fields=['field'])
class RegexQuery(SearchQuery):
    """
    Search documents for fields matching a given regular expression
    """
    _TERMPROP = 'regexp'

    @property
    def regex(self) -> str:
        return self._json_.get('regexp', None)

    @regex.setter
    def regex(self, value  # type: str
              ) -> None:
        self.set_prop('regexp', value)

    @property
    def regexp(self) -> str:
        return self._json_.get('regexp', None)

    @regexp.setter
    def regexp(self, value  # type: str
               ) -> None:
        self.set_prop('regexp', value)

    # regex = _genprop_str('regexp', doc="Regular expression to use")


RegexpQuery = RegexQuery


@_QueryBuilder._single_term_query(fields=['field'])
class BooleanFieldQuery(SearchQuery):
    _TERMPROP = 'bool'

    @property
    def bool(self) -> bool:
        return self._json_.get('bool', None)

    @bool.setter
    def bool(self, value  # type: bool
             ) -> None:
        self.set_prop('bool', value)
    # bool = _genprop(bool, 'bool', doc='Boolean value to search for')


# Geo Queries


@_QueryBuilder._with_fields(fields=['field'])
class GeoDistanceQuery(SearchQuery):
    def __init__(self, distance,  # type: str
                 location,  # type: Tuple[float, float]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        """
        Search for items within a given radius
        :param distance: The distance string specifying the radius
        :param location: A tuple of `(lon, lat)` indicating point of origin
        """
        super(GeoDistanceQuery, self).__init__()
        kwargs['distance'] = distance
        kwargs['location'] = location
        _QueryBuilder._assign_kwargs(self, kwargs)

    @property
    def location(self) -> Tuple[float, float]:
        return self._json_.get('location')

    @location.setter
    def location(self, value  # type: Tuple[float, float]
                 ) -> None:
        location = _QueryBuilder._gen_location(value)
        self.set_prop('location', location)

    @property
    def distance(self) -> str:
        return self._json_.get('distance', None)

    @distance.setter
    def distance(self, value  # type: str
                 ) -> None:
        self.set_prop('distance', value)

    # location = _genprop(_location_conv, 'location', doc='Location')
    # distance = _genprop_str('distance')


@_QueryBuilder._with_fields(fields=['field'])
class GeoBoundingBoxQuery(SearchQuery):
    def __init__(self, top_left,  # type: Tuple[float, float]
                 bottom_right,  # type: Tuple[float, float]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super(GeoBoundingBoxQuery, self).__init__()
        kwargs['top_left'] = top_left
        kwargs['bottom_right'] = bottom_right
        _QueryBuilder._assign_kwargs(self, kwargs)

    @property
    def top_left(self) -> Tuple[float, float]:
        return self._json_.get('top_left')

    @top_left.setter
    def top_left(self, value  # type: Tuple[float, float]
                 ) -> None:
        top_left = _QueryBuilder._gen_location(value)
        self.set_prop('top_left', top_left)

    @property
    def bottom_right(self) -> Tuple[float, float]:
        return self._json_.get('bottom_right')

    @bottom_right.setter
    def bottom_right(self, value  # type: Tuple[float, float]
                     ) -> None:
        bottom_right = _QueryBuilder._gen_location(value)
        self.set_prop('bottom_right', bottom_right)

    # top_left = _genprop(
    #     _location_conv, 'top_left',
    #     doc='Tuple of `(lon, lat)` for the top left corner of bounding box')
    # bottom_right = _genprop(
    #     _location_conv, 'bottom_right',
    #     doc='Tuple of `(lon, lat`) for the bottom right corner of bounding box')


@_QueryBuilder._with_fields(fields=['field'])
class GeoPolygonQuery(SearchQuery):
    def __init__(self, polygon_points,  # type: List[Tuple[float, float]]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super(GeoPolygonQuery, self).__init__()
        kwargs['polygon_points'] = polygon_points
        _QueryBuilder._assign_kwargs(self, kwargs)

    @property
    def polygon_points(self) -> Tuple[float, float]:
        return self._json_.get('polygon_points')

    @polygon_points.setter
    def polygon_points(self, value  # type: Tuple[float, float]
                       ) -> None:
        polygon_points = _QueryBuilder._gen_locations(value)
        self.set_prop('polygon_points', polygon_points)

    # polygon_points = _genprop(
    #     _locations_conv, 'polygon_points',
    #     doc='List of tuples `(lon, lat)` for the points of a polygon')


# Range Queries


@_QueryBuilder._with_fields(fields=['field'])
class NumericRangeQuery(SearchQuery):
    """
    Search documents for fields containing a value within a given numerical
    range.

    At least one of `min` or `max` must be specified.
    """

    def __init__(self,
                 min=None,  # type: Optional[float]
                 max=None,  # type: Optional[float]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        """
        :param float min: See :attr:`min`
        :param float max: See :attr:`max`
        """
        # super(NumericRangeQuery, self).__init__(min, max, **kwargs)
        super().__init__()
        _QueryBuilder._validate_range_query(self, min, max, **kwargs)

    _MINMAX = 'min', 'max'

    @property
    def min(self) -> Optional[float]:
        return self._json_.get('min', None)

    @min.setter
    def min(self,
            value  # type: float
            ) -> None:
        self.set_prop('min', value)

    @property
    def min_inclusive(self) -> Optional[bool]:
        return self._json_.get('min_inclusive', None)

    @min_inclusive.setter
    def min_inclusive(self,
                      value  # type: bool
                      ) -> None:
        Supportability.class_property_deprecated('min_inclusive', 'inclusive_min')
        self.set_prop('inclusive_min', value)

    @property
    def inclusive_min(self) -> Optional[bool]:
        return self._json_.get('inclusive_min', None)

    @inclusive_min.setter
    def inclusive_min(self,
                      value  # type: bool
                      ) -> None:
        self.set_prop('inclusive_min', value)

    @property
    def max(self) -> Optional[float]:
        return self._json_.get('max', None)

    @max.setter
    def max(self,
            value  # type: float
            ) -> None:
        self.set_prop('max', value)

    @property
    def max_inclusive(self) -> Optional[bool]:
        return self._json_.get('max_inclusive', None)

    @max_inclusive.setter
    def max_inclusive(self,
                      value  # type: bool
                      ) -> None:
        Supportability.class_property_deprecated('max_inclusive', 'inclusive_max')
        self.set_prop('inclusive_max', value)

    @property
    def inclusive_max(self) -> Optional[bool]:
        return self._json_.get('inclusive_max', None)

    @inclusive_max.setter
    def inclusive_max(self,
                      value  # type: bool
                      ) -> None:
        self.set_prop('inclusive_max', value)

    # min = _genprop(
    #     float, 'min', doc='Lower bound of range. See :attr:`min_inclusive`')

    # min_inclusive = _genprop(
    #     bool, 'inclusive_min',
    #     doc='Whether matches are inclusive of lower bound')

    # max = _genprop(
    #     float, 'max',
    #     doc='Upper bound of range. See :attr:`max_inclusive`')

    # max_inclusive = _genprop(
    #     bool, 'inclusive_max',
    #     doc='Whether matches are inclusive of upper bound')


@_QueryBuilder._with_fields(fields=['field'])
class DateRangeQuery(SearchQuery):
    """
    Search documents for fields containing a value within a given date
    range.

    The date ranges are parsed according to a given :attr:`datetime_parser`.
    If no parser is specified, the RFC 3339 parser is used. See
    `Generating an RFC 3339 Timestamp <http://goo.gl/LIkV7G>_`.

    The :attr:`start` and :attr:`end` parameters should be specified in the
    constructor. Note that either `start` or `end` (but not both!) may be
    omitted.

    .. code-block:: python

        DateRangeQuery(start='2014-12-25', end='2016-01-01')
    """

    def __init__(self, start=None, end=None, **kwargs):
        """
        :param str start: Start of date range
        :param str end: End of date range
        :param kwargs: Additional options: :attr:`field`, :attr:`boost`
        """
        super().__init__()
        _QueryBuilder._validate_range_query(self, start, end, **kwargs)

    #     super(DateRangeQuery, self).__init__(start, end, **kwargs)

    _MINMAX = 'start', 'end'

    @property
    def start(self) -> Optional[str]:
        return self._json_.get('start', None)

    @start.setter
    def start(self,
              value  # type: str
              ) -> None:
        self.set_prop('start', value)

    @property
    def start_inclusive(self) -> Optional[bool]:
        return self._json_.get('start_inclusive', None)

    @start_inclusive.setter
    def start_inclusive(self,
                        value  # type: bool
                        ) -> None:
        Supportability.class_property_deprecated('start_inclusive', 'inclusive_start')
        self.set_prop('inclusive_start', value)

    @property
    def inclusive_start(self) -> Optional[bool]:
        return self._json_.get('inclusive_start', None)

    @inclusive_start.setter
    def inclusive_start(self,
                        value  # type: bool
                        ) -> None:
        self.set_prop('inclusive_start', value)

    @property
    def end(self) -> Optional[str]:
        return self._json_.get('end', None)

    @end.setter
    def end(self,
            value  # type: str
            ) -> None:
        self.set_prop('end', value)

    @property
    def end_inclusive(self) -> Optional[bool]:
        return self._json_.get('end_inclusive', None)

    @end_inclusive.setter
    def end_inclusive(self,
                      value  # type: bool
                      ) -> None:
        Supportability.class_property_deprecated('end_inclusive', 'inclusive_end')
        self.set_prop('inclusive_end', value)

    @property
    def inclusive_end(self) -> Optional[bool]:
        return self._json_.get('inclusive_end', None)

    @inclusive_end.setter
    def inclusive_end(self,
                      value  # type: bool
                      ) -> None:
        self.set_prop('inclusive_end', value)

    @property
    def datetime_parser(self) -> Optional[str]:
        return self._json_.get('datetime_parser', None)

    @datetime_parser.setter
    def datetime_parser(self,
                        value  # type: str
                        ) -> None:
        self.set_prop('datetime_parser', value)

    # start = _genprop_str('start', doc='Lower bound datetime')
    # end = _genprop_str('end', doc='Upper bound datetime')

    # start_inclusive = _genprop(
    #     bool, 'inclusive_start', doc='If :attr:`start` is inclusive')

    # end_inclusive = _genprop(
    #     bool, 'inclusive_end', doc='If :attr:`end` is inclusive')

    # datetime_parser = _genprop_str(
    #     'datetime_parser',
    #     doc="""
    #     Parser to use when analyzing the :attr:`start` and :attr:`end` fields
    #     on the server.

    #     If not specified, the RFC 3339 parser is used.
    #     Ensure to specify :attr:`start` and :attr:`end` in a format suitable
    #     for the given parser.
    #     """)


@_QueryBuilder._with_fields(fields=['field'])
class TermRangeQuery(SearchQuery):
    """
    Search documents for fields containing a value within a given
    lexical range.
    """

    _MINMAX = 'min', 'max'

    def __init__(self,
                 start=None,  # type: Optional[str]
                 end=None,  # type: Optional[str]
                 min=None,  # type: Optional[str]
                 max=None,  # type: Optional[str]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        """
        Args:
            start (str): **DEPRECATED** Use min.
            end (str): **DEPRECATED** Use max.
            min (str): The lower end of the range.
            max (str): The higher end of the range.
        """
        super().__init__()
        if start is not None and min is None:
            Supportability.class_property_deprecated('start', 'min')
            min = start
        if end is not None and max is None:
            Supportability.class_property_deprecated('end', 'max')
            max = end

        _QueryBuilder._validate_range_query(self, min, max, **kwargs)

    @property
    def start(self) -> Optional[str]:
        return self._json_.get('start', None)

    @start.setter
    def start(self, value  # type: str
              ) -> None:
        Supportability.class_property_deprecated('start', 'min')
        self.set_prop('min', value)

    @property
    def min(self) -> Optional[str]:
        return self._json_.get('min', None)

    @min.setter
    def min(self, value  # type: str
            ) -> None:
        self.set_prop('min', value)

    @property
    def start_inclusive(self) -> Optional[bool]:
        return self._json_.get('start_inclusive', None)

    @start_inclusive.setter
    def start_inclusive(self, value  # type: bool
                        ) -> None:
        Supportability.class_property_deprecated('start_inclusive', 'inclusive_min')
        self.set_prop('inclusive_min', value)

    @property
    def inclusive_min(self) -> Optional[bool]:
        return self._json_.get('start_inclusive', None)

    @inclusive_min.setter
    def inclusive_min(self, value  # type: bool
                      ) -> None:
        self.set_prop('inclusive_min', value)

    @property
    def end(self) -> Optional[str]:
        return self._json_.get('end', None)

    @end.setter
    def end(self, value  # type: str
            ) -> None:
        Supportability.class_property_deprecated('end', 'max')
        self.set_prop('max', value)

    @property
    def max(self) -> Optional[str]:
        return self._json_.get('max', None)

    @max.setter
    def max(self, value  # type: str
            ) -> None:
        self.set_prop('max', value)

    @property
    def end_inclusive(self) -> Optional[bool]:
        return self._json_.get('end_inclusive', None)

    @end_inclusive.setter
    def end_inclusive(self, value  # type: bool
                      ) -> None:
        Supportability.class_property_deprecated('end_inclusive', 'inclusive_max')
        self.set_prop('inclusive_max', value)

    @property
    def inclusive_max(self) -> Optional[bool]:
        return self._json_.get('inclusive_max', None)

    @inclusive_max.setter
    def inclusive_max(self, value  # type: bool
                      ) -> None:
        self.set_prop('inclusive_max', value)

    # def __init__(self, start=None, end=None, **kwargs):
    #     super(TermRangeQuery, self).__init__(start=start, end=end, **kwargs)

    # start = _genprop_str('start', doc='Lower range of term')

    # end = _genprop_str('end', doc='Upper range of term')

    # start_inclusive = _genprop(
    #     bool, 'inclusive_start', doc='If :attr:`start` is inclusive')

    # end_inclusive = _genprop(
    #     bool, 'inclusive_end', doc='If :attr:`end` is inclusive')


# Compound Queries

class ConjunctionQuery(SearchQuery):
    """
    Compound query in which all sub-queries passed must be satisfied
    """
    _COMPOUND_FIELDS = ('conjuncts', 'conjuncts'),

    def __init__(self, *queries, **kwargs):
        super().__init__()
        _QueryBuilder._assign_kwargs(self, kwargs)
        self.conjuncts = list(queries)

    @property
    def encodable(self):
        self.validate()

        # Requires special handling since the compound queries in themselves
        # cannot be JSON unless they are properly encoded.
        # Note that the 'encodable' property also triggers validation
        js = self._json_.copy()
        for src, tgt in self._COMPOUND_FIELDS:
            objs = getattr(self, src)
            if not objs:
                continue
            js[tgt] = [q.encodable for q in objs]
        return js

    def validate(self):
        super(ConjunctionQuery, self).validate()
        if not self.conjuncts:
            raise NoChildrenException('No sub-queries')


class DisjunctionQuery(SearchQuery):
    """
    Compound query in which at least :attr:`min` or more queries must be
    satisfied
    """
    _COMPOUND_FIELDS = ('disjuncts', 'disjuncts'),

    def __init__(self, *queries, **kwargs):
        super().__init__()
        _QueryBuilder._assign_kwargs(self, kwargs)
        self.disjuncts = list(queries)
        if 'min' not in self._json_:
            self.min = 1

    @property
    def min(self) -> int:
        return self._json_.get('min', None)

    @min.setter
    def min(self, value  # type: bool
            ) -> None:
        value = int(value)
        if not value:
            raise InvalidArgumentException(message='Must be > 0')
        self.set_prop('min', value)

    @property
    def encodable(self):
        self.validate()

        # Requires special handling since the compound queries in themselves
        # cannot be JSON unless they are properly encoded.
        # Note that the 'encodable' property also triggers validation
        js = self._json_.copy()
        for src, tgt in self._COMPOUND_FIELDS:
            objs = getattr(self, src)
            if not objs:
                continue
            js[tgt] = [q.encodable for q in objs]
        return js

    # min = _genprop(
    #     _convert_gt0, 'min', doc='Number of queries which must be satisfied')

    def validate(self):
        super(DisjunctionQuery, self).validate()
        if not self.disjuncts:
            raise NoChildrenException('No queries specified')
        if len(self.disjuncts) < self.min:
            raise InvalidArgumentException(message='Specified min is larger than number of queries.')


CompoundQueryType = Union[SearchQuery, ConjunctionQuery, DisjunctionQuery, List[SearchQuery]]


class BooleanQuery(SearchQuery):
    def __init__(self, must=None, should=None, must_not=None):
        super().__init__()
        self._subqueries = {}
        self.must = must
        self.should = should
        self.must_not = must_not

    @property
    def must(self) -> ConjunctionQuery:
        return self._subqueries.get('must')

    @must.setter
    def must(self, value  # type: CompoundQueryType
             ) -> None:
        self._set_query('must', value, ConjunctionQuery)

    @property
    def must_not(self) -> DisjunctionQuery:
        return self._subqueries.get('must_not')

    @must_not.setter
    def must_not(self, value  # type: CompoundQueryType
                 ) -> None:
        self._set_query('must_not', value, DisjunctionQuery)

    @property
    def should(self) -> DisjunctionQuery:
        return self._subqueries.get('should')

    @should.setter
    def should(self, value  # type: CompoundQueryType
               ) -> None:
        self._set_query('should', value, DisjunctionQuery)

    @property
    def encodable(self) -> Dict[str, Any]:
        # Overrides the default `encodable` implementation in order to
        # serialize any sub-queries
        for src, tgt in ((self.must, 'must'),
                         (self.must_not, 'must_not'),
                         (self.should, 'should')):
            if src:
                self._json_[tgt] = src.encodable
        return super(BooleanQuery, self).encodable

    def validate(self) -> None:
        super(BooleanQuery, self).validate()
        if not self.must and not self.must_not and not self.should:
            raise ValueError('No sub-queries specified', self)

    def _set_query(self, name,  # type: str
                   value,  # type: Optional[CompoundQueryType]
                   reqtype  # type: Union[ConjunctionQuery, DisjunctionQuery]
                   ) -> None:
        if value is None:
            if name in self._subqueries:
                del self._subqueries[name]
        elif isinstance(value, reqtype):
            self._subqueries[name] = value
        elif isinstance(value, SearchQuery):
            self._subqueries[name] = reqtype(value)
        else:
            try:
                it = iter(value)
            except ValueError:
                raise TypeError('Value must be iterable')

            sub_query = []
            for query in it:
                if not isinstance(query, SearchQuery):
                    raise TypeError('Item is not a query!', query)
                sub_query.append(query)
            self._subqueries[name] = reqtype(*sub_query)


# Special Queries

class RawQuery(SearchQuery):
    """
    This class is used to wrap a raw query payload. It should be used
    for custom query parameters, or in cases where any of the other
    query classes are insufficient.
    """

    def __init__(self, obj):
        super(RawQuery, self).__init__()
        self._json_ = obj


class MatchAllQuery(SearchQuery):
    """
    Special query which matches all documents
    """

    def __init__(self, **kwargs):
        super(MatchAllQuery, self).__init__()
        self._json_['match_all'] = None
        _QueryBuilder._assign_kwargs(self, kwargs)


class MatchNoneQuery(SearchQuery):
    """
    Special query which matches no documents
    """

    def __init__(self, **kwargs):
        super(MatchNoneQuery, self).__init__()
        self._json_['match_none'] = None
        _QueryBuilder._assign_kwargs(self, kwargs)
