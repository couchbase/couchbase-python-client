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

    def __repr__(self) -> str:
        return f'query={str(self._json_)}'

    def __str__(self) -> str:
        return self.__repr__()


# Single Term Queries


@_QueryBuilder._single_term_query(fields=['fuzziness', 'prefix_length', 'field'])
class TermQuery(SearchQuery):

    """
    Query that specifies the Search Service should search for an exact match to the specified term.

    Examples:
        Basic search using TermQuery::

            from couchbase.search import TermQuery, SearchRequest

            # ... other code ...

            query = TermQuery('park', field='description')
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using TermQuery with kwargs::

            from couchbase.search import TermQuery, SearchRequest

            # ... other code ...

            query = TermQuery(term='park', field='description')
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using TermQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import TermQuery, SearchRequest

            # ... other code ...

            query = TermQuery('park', field='description')
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'term'

    @property
    def term(self) -> str:
        return self._json_.get('term', None)

    @term.setter
    def term(self, value  # type: str
             ) -> None:
        self.set_prop('term', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._single_term_query()
class QueryStringQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for a match to the specified query string.

    .. seealso::

        `Query Language <http://www.blevesearch.com/docs/Query-String-Query/>`_

    Examples:
        Basic search using QueryStringQuery::

            from couchbase.search import QueryStringQuery, SearchRequest

            # ... other code ...

            query = QueryStringQuery('+description:sea -color_hex:fff5ee')
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using QueryStringQuery with kwargs::

            from couchbase.search import QueryStringQuery, SearchRequest

            # ... other code ...

            query = QueryStringQuery(query='+description:sea -color_hex:fff5ee')
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using QueryStringQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import QueryStringQuery, SearchRequest

            # ... other code ...

            query = QueryStringQuery('+description:sea -color_hex:fff5ee')
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'query'

    @property
    def query(self) -> str:
        return self._json_.get('query', None)

    @query.setter
    def query(self, value  # type: str
              ) -> None:
        self.set_prop('query', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._single_term_query(fields=['field'])
class WildcardQuery(SearchQuery):

    """
    Query that specifies the Search Service should search for a match to the specified wildcard string.

    Use `?` to allow a match to any single character.
    Use `*` to allow a match to zero or many characters.


    Examples:
        Basic search using WildcardQuery::

            from couchbase.search import WildcardQuery, SearchRequest

            # ... other code ...

            query = WildcardQuery('f0f???', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using WildcardQuery with kwargs::

            from couchbase.search import WildcardQuery, SearchRequest

            # ... other code ...

            query = WildcardQuery(wildcard='f0f???', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using WildcardQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import WildcardQuery, SearchRequest

            # ... other code ...

            query = WildcardQuery('f0f???', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'wildcard'

    @property
    def wildcard(self) -> str:
        return self._json_.get('wildcard', None)

    @wildcard.setter
    def wildcard(self, value  # type: str
                 ) -> None:
        self.set_prop('wildcard', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._single_term_query()
class DocIdQuery(SearchQuery):

    """
    Query that specifies the Search Service should search matches to the specified
    list of document ids.

    Examples:
        Basic search using DocIdQuery::

            from couchbase.search import DocIdQuery, SearchRequest

            # ... other code ...

            query = DocIdQuery(['34','43','61'])
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using DocIdQuery with kwargs::

            from couchbase.search import DocIdQuery, SearchRequest

            # ... other code ...

            query = DocIdQuery(ids=['34','43','61'])
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using DocIdQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import DocIdQuery, SearchRequest

            # ... other code ...

            query = DocIdQuery(['34','43','61','72])
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'ids'

    @property
    def ids(self) -> str:
        return self._json_.get('ids', None)

    @ids.setter
    def ids(self, value  # type: str
            ) -> None:
        self.set_prop('ids', value)

    def validate(self):
        super(DocIdQuery, self).validate()
        if not self.ids:
            raise NoChildrenException('`ids` must contain at least one ID')

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._single_term_query(fields=['prefix_length', 'fuzziness', 'field', 'analyzer'])
class MatchQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for an exact match to the specified
    term inside the Search index’s default field.

    Examples:
        Basic search using MatchQuery::

            from couchbase.search import MatchQuery, SearchRequest

            # ... other code ...

            query = MatchQuery('secondary', field='color_wheel_pos')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using MatchQuery with kwargs::

            from couchbase.search import MatchQuery, SearchRequest

            # ... other code ...

            query = MatchQuery(match='secondary', field='color_wheel_pos')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using MatchQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import MatchQuery, SearchRequest

            # ... other code ...

            query = MatchQuery('secondary', field='color_wheel_pos')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._single_term_query(fields=['field', 'analyzer'])
class MatchPhraseQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for exact matches to the specified
    phrase. The phrase is composed of one or more terms.

    Examples:
        Basic search using MatchPhraseQuery::

            from couchbase.search import MatchPhraseQuery, SearchRequest

            # ... other code ...

            query = MatchPhraseQuery('white sands', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using MatchPhraseQuery with kwargs::

            from couchbase.search import MatchPhraseQuery, SearchRequest

            # ... other code ...

            query = MatchPhraseQuery(match_phrase='white sands', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using MatchPhraseQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import MatchPhraseQuery, SearchRequest

            # ... other code ...

            query = MatchPhraseQuery('white sands', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'match_phrase'

    @property
    def match_phrase(self) -> str:
        return self._json_.get('match_phrase', None)

    @match_phrase.setter
    def match_phrase(self, value  # type: str
                     ) -> None:
        self.set_prop('match_phrase', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._with_fields(fields=['field'])
class PhraseQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for terms provided in the specified
    order. The `field` property must be provided.

    Examples:
        Basic search using PhraseQuery::

            from couchbase.search import PhraseQuery, SearchRequest

            # ... other code ...

            query = PhraseQuery('white sand', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using PhraseQuery with kwargs::

            from couchbase.search import PhraseQuery, SearchRequest

            # ... other code ...

            query = PhraseQuery(terms='white sand', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using PhraseQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import PhraseQuery, SearchRequest

            # ... other code ...

            query = PhraseQuery('white sand', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'terms'

    def __init__(self,
                 *phrases,  # type: str
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super().__init__()
        if self._TERMPROP in kwargs:
            if isinstance(kwargs[self._TERMPROP], str):
                kwargs[self._TERMPROP] = kwargs[self._TERMPROP].split()
        else:
            if isinstance(phrases, str):
                kwargs[self._TERMPROP] = phrases.split()
            else:
                kwargs[self._TERMPROP] = phrases
        if self._TERMPROP not in kwargs or len(kwargs[self._TERMPROP]) == 0:
            raise ValueError(f'{self.__class__.__name__} missing required property: {self._TERMPROP}')
        _QueryBuilder._assign_kwargs(self, kwargs)

    @property
    def terms(self) -> List[str]:
        return self._json_.get('terms', None)

    @terms.setter
    def terms(self,
              value  # type: Union[List[str], Tuple[str]]
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._single_term_query(fields=['field'])
class PrefixQuery(SearchQuery):

    """
    Query that specifies the Search Service should search for matches to the specified
    prefix.

    Examples:
        Basic search using PrefixQuery::

            from couchbase.search import PrefixQuery, SearchRequest

            # ... other code ...

            query = PrefixQuery('yellow', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using PrefixQuery with kwargs::

            from couchbase.search import PrefixQuery, SearchRequest

            # ... other code ...

            query = PrefixQuery(prefix='yellow', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using PrefixQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import PrefixQuery, SearchRequest

            # ... other code ...

            query = PrefixQuery('yellow', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'prefix'

    @property
    def prefix(self) -> str:
        return self._json_.get('prefix', None)

    @prefix.setter
    def prefix(self, value  # type: str
               ) -> None:
        self.set_prop('prefix', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._single_term_query(fields=['field'])
class RegexQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for matches to the specified
    regular expression.

    Examples:
        Basic search using RegexQuery::

            from couchbase.search import RegexQuery, SearchRequest

            # ... other code ...

            query = RegexQuery('f[58]f.*', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using RegexQuery with kwargs::

            from couchbase.search import RegexQuery, SearchRequest

            # ... other code ...

            query = RegexQuery(regexp='f[58]f.*', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using RegexQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import RegexQuery, SearchRequest

            # ... other code ...

            query = RegexQuery('f[58]f.*', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


RegexpQuery = RegexQuery


@_QueryBuilder._single_term_query(fields=['field'])
class BooleanFieldQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for to the specified
    boolean value in the provided field.

    Examples:
        Basic search using BooleanFieldQuery::

            from couchbase.search import BooleanFieldQuery, SearchRequest

            # ... other code ...

            query = BooleanFieldQuery(True, field='perfect_rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using BooleanFieldQuery with kwargs::

            from couchbase.search import BooleanFieldQuery, SearchRequest

            # ... other code ...

            query = BooleanFieldQuery(bool=True, field='perfect_rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using BooleanFieldQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import BooleanFieldQuery, SearchRequest

            # ... other code ...

            query = BooleanFieldQuery(True, field='perfect_rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _TERMPROP = 'bool'

    @property
    def bool(self) -> bool:
        return self._json_.get('bool', None)

    @bool.setter
    def bool(self, value  # type: bool
             ) -> None:
        self.set_prop('bool', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


# Geo Queries


@_QueryBuilder._with_fields(fields=['field'])
class GeoDistanceQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for geo location values
    in a set radius around a specified latitude and longitude.

    If you use location as an array, your array must contain a longitude value followed by a latitude value.

    The following distance units:
      - mm: Millimeters
      - cm: Centimeters
      - in: Inches
      - yd: Yards
      - ft: Feet
      - m: Meters
      - km: Kilometers
      - mi: Miles
      - nm: Nautical miles


    Examples:
        Basic search using GeoDistanceQuery::

            from couchbase.search import GeoDistanceQuery, SearchRequest

            # ... other code ...

            query = GeoDistanceQuery('130mi', (-115.1391, 36.1716), field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoDistanceQuery with location as dict::

            from couchbase.search import GeoDistanceQuery, SearchRequest

            # ... other code ...

            query = GeoDistanceQuery('130mi', {'lon': -115.1391, 'lat': 36.1716}, field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoDistanceQuery with kwargs::

            from couchbase.search import GeoDistanceQuery, SearchRequest

            # ... other code ...

            query = GeoDistanceQuery(distance='130mi', location=(-115.1391, 36.1716), field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoDistanceQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import GeoDistanceQuery, SearchRequest

            # ... other code ...

            query = GeoDistanceQuery('130mi', (-115.1391, 36.1716), field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    def __init__(self,
                 distance=None,  # type: str
                 location=None,  # type: Union[List[float, float], Tuple[float, float], Dict[str, float]]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super(GeoDistanceQuery, self).__init__()
        kwargs['distance'] = distance
        kwargs['location'] = location
        if kwargs['distance'] is None or kwargs['location'] is None:
            raise ValueError(f'{self.__class__.__name__} requires both a distance and location to be specified.')
        _QueryBuilder._assign_kwargs(self, kwargs)

    @property
    def location(self) -> Tuple[float, float]:
        return self._json_.get('location')

    @location.setter
    def location(self,
                 value  # type: Union[List[float, float], Tuple[float, float], Dict[str, float]]
                 ) -> None:
        location = _QueryBuilder._gen_location(value)
        self.set_prop('location', location)

    @property
    def distance(self) -> str:
        return self._json_.get('distance', None)

    @distance.setter
    def distance(self,
                 value  # type: str
                 ) -> None:
        self.set_prop('distance', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._with_fields(fields=['field'])
class GeoBoundingBoxQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for geo location values
    in a defined rectangle.

    If you use location as an array, your array must contain a longitude value followed by a latitude value.

    Examples:
        Basic search using GeoBoundingBoxQuery::

            from couchbase.search import GeoBoundingBoxQuery, SearchRequest

            # ... other code ...

            query = GeoBoundingBoxQuery((-0.489, 51.686), (0.236, 51.28), field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoBoundingBoxQuery with bounding box points as list::

            from couchbase.search import GeoBoundingBoxQuery, SearchRequest

            # ... other code ...

            query = GeoBoundingBoxQuery([-0.489, 51.686], [0.236, 51.28], field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoBoundingBoxQuery with location as dict::

            from couchbase.search import GeoBoundingBoxQuery, SearchRequest

            # ... other code ...

            query = GeoBoundingBoxQuery({'lon': -0.489, 'lat': 51.686},
                                        {'lon': 0.236, 'lat': 51.28},
                                        field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoBoundingBoxQuery with kwargs::

            from couchbase.search import GeoBoundingBoxQuery, SearchRequest

            # ... other code ...

            query = GeoBoundingBoxQuery(top_left=(-0.489, 51.686), bottom_right=(0.236, 51.28), field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoDistanceQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import GeoBoundingBoxQuery, SearchRequest

            # ... other code ...

            query = GeoBoundingBoxQuery((-0.489, 51.686), (0.236, 51.28), field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    def __init__(self,
                 top_left=None,  # type: Tuple[float, float]
                 bottom_right=None,  # type: Tuple[float, float]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super(GeoBoundingBoxQuery, self).__init__()
        kwargs['top_left'] = top_left
        kwargs['bottom_right'] = bottom_right
        if kwargs['top_left'] is None or kwargs['bottom_right'] is None:
            raise ValueError(f'{self.__class__.__name__} requires both a top_left and bottom_right to be specified.')
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._with_fields(fields=['field'])
class GeoPolygonQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for geo location values
    in a defined polygon.

    The point tuple must contain a longitude value followed by a latitude value.

    Examples:
        Basic search using GeoPolygonQuery::

            from couchbase.search import GeoPolygonQuery, SearchRequest

            # ... other code ...

            query = GeoPolygonQuery([(37.79393211306212, -122.44234633404847),
                                     (37.77995881733997, -122.43977141339417),
                                     (37.788031092020155, -122.42925715405579),
                                     (37.79026946582319, -122.41149020154114),
                                     (37.79571192027403, -122.40735054016113),
                                     (37.79393211306212, -122.44234633404847)],
                                     field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')


        Basic search using GeoPolygonQuery with kwargs::

            from couchbase.search import GeoPolygonQuery, SearchRequest

            # ... other code ...

            pts = [(37.79393211306212, -122.44234633404847),
                   (37.77995881733997, -122.43977141339417),
                   (37.788031092020155, -122.42925715405579),
                   (37.79026946582319, -122.41149020154114),
                   (37.79571192027403, -122.40735054016113),
                   (37.79393211306212, -122.44234633404847)]
            query = GeoPolygonQuery(polygon_points=pts, field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoPolygonQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import GeoPolygonQuery, SearchRequest

            # ... other code ...

            query = GeoPolygonQuery([(37.79393211306212, -122.44234633404847),
                                     (37.77995881733997, -122.43977141339417),
                                     (37.788031092020155, -122.42925715405579),
                                     (37.79026946582319, -122.41149020154114),
                                     (37.79571192027403, -122.40735054016113),
                                     (37.79393211306212, -122.44234633404847)],
                                     field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    def __init__(self,
                 polygon_points=None,  # type: List[Tuple[float, float]]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        super(GeoPolygonQuery, self).__init__()
        pts = polygon_points or kwargs.pop('polygon_points', None)
        if pts is None:
            raise ValueError(f'{self.__class__.__name__} requires a list of tuples for polygon points.')
        kwargs['polygon_points'] = pts
        _QueryBuilder._assign_kwargs(self, kwargs)

    @property
    def polygon_points(self) -> Tuple[float, float]:
        return self._json_.get('polygon_points')

    @polygon_points.setter
    def polygon_points(self, value  # type: Tuple[float, float]
                       ) -> None:
        polygon_points = _QueryBuilder._gen_locations(value)
        self.set_prop('polygon_points', polygon_points)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


# Range Queries


@_QueryBuilder._with_fields(fields=['field'])
class NumericRangeQuery(SearchQuery):
    """
    Query that specifies the Search Service should search for matches to the specified
    numerical range.

    .. note::
        At least one of `min` or `max` must be specified.

    Examples:
        Basic search using NumericRangeQuery::

            from couchbase.search import NumericRangeQuery, SearchRequest

            # ... other code ...

            query = NumericRangeQuery(3.0,
                                      3.2,
                                      inclusive_min=True,
                                      inclusive_max=False,
                                      field='rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using NumericRangeQuery with kwargs::

            from couchbase.search import NumericRangeQuery, SearchRequest

            # ... other code ...

            query = NumericRangeQuery(min=3.0,
                                      max=3.2,
                                      inclusive_min=True,
                                      inclusive_max=False,
                                      field='rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using NumericRangeQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import NumericRangeQuery, SearchRequest

            # ... other code ...

            query = NumericRangeQuery(3.0,
                                      3.2,
                                      inclusive_min=True,
                                      inclusive_max=False,
                                      field='rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    def __init__(self,
                 min=None,  # type: Optional[float]
                 max=None,  # type: Optional[float]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._with_fields(fields=['field'])
class DateRangeQuery(SearchQuery):

    """
    Query that specifies the Search Service should search for matches to the specified
    date range.

    .. note::
        At least one of `start` or `end` must be specified.

    The date ranges are parsed according to a given :attr:`datetime_parser`.
    If no parser is specified, the RFC 3339 parser is used. See
    `Generating an RFC 3339 Timestamp <http://goo.gl/LIkV7G>_`.

    Examples:
        Basic search using DateRangeQuery::

            from couchbase.search import DateRangeQuery, SearchRequest

            # ... other code ...

            query = DateRangeQuery('2024-01-01',
                                   '2024-01-05',
                                   inclusive_start=True,
                                   inclusive_end=True,
                                   field='last_rated')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using DateRangeQuery with kwargs::

            from couchbase.search import DateRangeQuery, SearchRequest

            # ... other code ...

            query = DateRangeQuery(start='2024-01-01',
                                   end='2024-01-05',
                                   inclusive_start=True,
                                   inclusive_end=True,
                                   field='last_rated')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using DateRangeQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import DateRangeQuery, SearchRequest

            # ... other code ...

            query = NumericRangeQuery(3.0,
                                      3.2,
                                      inclusive_min=True,
                                      inclusive_max=False,
                                      field='rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    def __init__(self, start=None, end=None, **kwargs):
        super().__init__()
        _QueryBuilder._validate_range_query(self, start, end, **kwargs)

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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


@_QueryBuilder._with_fields(fields=['field'])
class TermRangeQuery(SearchQuery):

    """
    Query that specifies the Search Service should search for matches to the specified
    range of terms.

    .. note::
        At least one of `min` or `max` must be specified.

    Examples:
        Basic search using TermRangeQuery::

            from couchbase.search import TermRangeQuery, SearchRequest

            # ... other code ...

            query = TermRangeQuery('mist',
                                   'misty',
                                   inclusive_min=True,
                                   inclusive_max=True,
                                   field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using TermRangeQuery with kwargs::

            from couchbase.search import TermRangeQuery, SearchRequest

            # ... other code ...

            query = TermRangeQuery(min='mist',
                                   max='misty',
                                   inclusive_min=True,
                                   inclusive_max=True,
                                   field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using TermRangeQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import TermRangeQuery, SearchRequest

            # ... other code ...

            query = TermRangeQuery(min='mist',
                                   max='misty',
                                   inclusive_min=True,
                                   inclusive_max=True,
                                   field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _MINMAX = 'min', 'max'

    def __init__(self,
                 start=None,  # type: Optional[str]
                 end=None,  # type: Optional[str]
                 min=None,  # type: Optional[str]
                 max=None,  # type: Optional[str]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


# Compound Queries

class ConjunctionQuery(SearchQuery):

    """
    A compound query that specifies multiple child queries.

    For a ConjunctionQuery, every query object in the array must have a match in a document
    to include the document in search results.

    Examples:
        Basic search using ConjunctionQuery::

            from couchbase.search import ConjunctionQuery, DateRangeQuery, TermQuery, SearchRequest

            # ... other code ...

            queries = [DateRangeQuery('2024-01-01',
                                      '2024-02-15',
                                      start_inclusive=False,
                                      end_inclusive=False,
                                      field='last_rated'),
                        TermQuery('sand', field='description')]
            query = ConjunctionQuery(*queries)
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using ConjunctionQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import ConjunctionQuery, DateRangeQuery, TermQuery, SearchRequest

            # ... other code ...

            queries = [DateRangeQuery('2024-01-01',
                                      '2024-02-15',
                                      start_inclusive=False,
                                      end_inclusive=False,
                                      field='last_rated'),
                        TermQuery('sand', field='description')]
            query = ConjunctionQuery(*queries)
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _COMPOUND_FIELDS = ('conjuncts', 'conjuncts'),

    def __init__(self, *queries, **kwargs):
        super().__init__()
        _QueryBuilder._assign_kwargs(self, kwargs)
        if len(queries) == 1 and isinstance(queries[0], list):
            self.conjuncts = queries[0]
        else:
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

    def __repr__(self) -> str:
        query = self.encodable
        return f'{type(self).__name__}(query={query})'

    def __str__(self) -> str:
        return self.__repr__()


class DisjunctionQuery(SearchQuery):

    """
    A compound query that specifies multiple child queries.

    For a DisjunctionQuery, the :attr:`min` property sets the number of query objects
    from the disjuncts array that must have a match in a document.
    If a document does not match the min number of query objects, the Search Service
    does not include the document in search results.

    Examples:
        Basic search using DisjunctionQuery::

            from couchbase.search import DisjunctionQuery, NumericRangeQuery, TermQuery, SearchRequest

            # ... other code ...

            queries = [NumericRangeQuery(3.0,
                                         3.1,
                                         inclusive_min=True,
                                         inclusive_max=True,
                                         field='rating'),
                        TermQuery('smoke', field='description')]
            query = DisjunctionQuery(*queries)
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using DisjunctionQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import DisjunctionQuery, NumericRangeQuery, TermQuery, SearchRequest

            # ... other code ...

            queries = [NumericRangeQuery(3.0,
                                         3.1,
                                         inclusive_min=True,
                                         inclusive_max=True,
                                         field='rating'),
                        TermQuery('smoke', field='description')]
            query = DisjunctionQuery(*queries)
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _COMPOUND_FIELDS = ('disjuncts', 'disjuncts'),

    def __init__(self, *queries, **kwargs):
        super().__init__()
        _QueryBuilder._assign_kwargs(self, kwargs)
        if len(queries) == 1 and isinstance(queries[0], list):
            self.disjuncts = queries[0]
        else:
            self.disjuncts = list(queries)
        if 'min' not in self._json_:
            self.min = 1

    @property
    def min(self) -> int:
        return self._json_.get('min', None)

    @min.setter
    def min(self, value  # type: int
            ) -> None:
        value = int(value)
        if value < 0:
            raise InvalidArgumentException(message='Must be >= 0')
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

    def validate(self):
        super(DisjunctionQuery, self).validate()
        if not self.disjuncts:
            raise NoChildrenException('No queries specified')
        if len(self.disjuncts) < self.min:
            raise InvalidArgumentException(message='Specified min is larger than number of queries.')

    def __repr__(self) -> str:
        query = self.encodable
        return f'{type(self).__name__}(query={query})'

    def __str__(self) -> str:
        return self.__repr__()


CompoundQueryType = Union[SearchQuery, ConjunctionQuery, DisjunctionQuery, List[SearchQuery]]


class BooleanQuery(SearchQuery):
    """
    A compound query that specifies multiple child queries.

    For a BooleanQuery, a document must match the combination of queries to be included in the search results.

    Examples:
        Basic search using BooleanQuery::

            from couchbase.search import (BooleanQuery,
                                          DateRangeQuery,
                                          TermQuery,
                                          PrefixQuery,
                                          WildcardQuery,
                                          SearchRequest)

            # ... other code ...

            must = [DateRangeQuery('2024-10-01',
                                   '2024-12-31',
                                   inclusive_start=True,
                                   inclusive_end=True,
                                   field='last_rated'),
                    WildcardQuery('ff????', field='color_hex')]
            must_not = [TermQuery('smoke', field='description'),
                        PrefixQuery('rain', field='description')]
            query = BooleanQuery(must, None, must_not)
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using BooleanQuery with kwargs::

            from couchbase.search import (BooleanQuery,
                                          DateRangeQuery,
                                          TermQuery,
                                          PrefixQuery,
                                          WildcardQuery,
                                          SearchRequest)

            # ... other code ...

            must = [DateRangeQuery('2024-10-01',
                                   '2024-12-31',
                                   inclusive_start=True,
                                   inclusive_end=True,
                                   field='last_rated'),
                    WildcardQuery('ff????', field='color_hex')]
            must_not = [TermQuery('smoke', field='description'),
                        PrefixQuery('rain', field='description')]
            query = BooleanQuery(must=must, must_not=must_not)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using BooleanQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import (BooleanQuery,
                                          DateRangeQuery,
                                          TermQuery,
                                          PrefixQuery,
                                          WildcardQuery,
                                          SearchRequest)

            # ... other code ...

            must = [DateRangeQuery('2024-10-01',
                                   '2024-12-31',
                                   inclusive_start=True,
                                   inclusive_end=True,
                                   field='last_rated'),
                    WildcardQuery('ff????', field='color_hex')]
            must_not = [TermQuery('smoke', field='description'),
                        PrefixQuery('rain', field='description')]
            query = BooleanQuery(must, None, must_not)
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    def __init__(self, must=None, should=None, must_not=None):
        super().__init__()
        self._subqueries = {}
        self.must = must
        self.should = should
        self.must_not = must_not
        self.validate()

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
            raise ValueError('At least one of the following queries should be specified: must, should, must_not.')

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

    def __repr__(self) -> str:
        query = self.encodable
        return f'{type(self).__name__}(query={query})'

    def __str__(self) -> str:
        return self.__repr__()


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

    Examples:
        Basic search using MatchAllQuery::

            from couchbase.search import MatchAllQuery, SearchRequest

            # ... other code ...

            query = MatchAllQuery()
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

    """

    def __init__(self, **kwargs):
        super(MatchAllQuery, self).__init__()
        self._json_['match_all'] = None
        _QueryBuilder._assign_kwargs(self, kwargs)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'


class MatchNoneQuery(SearchQuery):

    """
    Special query which matches no documents

    Examples:
        Basic search using MatchNoneQuery::

            from couchbase.search import MatchNoneQuery, SearchRequest

            # ... other code ...

            query = MatchNoneQuery()
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

    """

    def __init__(self, **kwargs):
        super(MatchNoneQuery, self).__init__()
        self._json_['match_none'] = None
        _QueryBuilder._assign_kwargs(self, kwargs)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({super().__repr__()})'
