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

"""The search query classes, and their encoding as the Search service's JSON query language.

Each class holds its fields and validates them; none of them knows how a protocol sends it. The
JSON encoding is at the bottom of the module, one function per class, and `SearchQuery.encodable`
is its public face. A protocol that sends queries some other way reads the same fields.
"""

from __future__ import annotations

__all__ = [
    'BooleanFieldQuery',
    'BooleanQuery',
    'CompoundQueryType',
    'ConjunctionQuery',
    'DateRangeQuery',
    'DisjunctionQuery',
    'DocIdQuery',
    'GeoBoundingBoxQuery',
    'GeoDistanceQuery',
    'GeoPolygonQuery',
    'MatchAllQuery',
    'MatchNoneQuery',
    'MatchPhraseQuery',
    'MatchQuery',
    'NumericRangeQuery',
    'PhraseQuery',
    'PrefixQuery',
    'QueryStringQuery',
    'RawQuery',
    'RegexQuery',
    'RegexpQuery',
    'SearchQuery',
    'TermQuery',
    'TermRangeQuery',
    'WildcardQuery',
]

import builtins
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from couchbase.common._supportability import Supportability
from couchbase.common._utils import JSONType
from couchbase.common.exceptions import InvalidArgumentException, NoChildrenException
from couchbase.common.search import (MatchOperator,
                                     _Body,
                                     _OptionalField,
                                     _Point,
                                     _point,
                                     _put)


def _missing(query: SearchQuery, prop: str) -> ValueError:
    return ValueError(f'{type(query).__name__} missing required property: {prop}')


def _field() -> _OptionalField[str]:
    return _OptionalField(str, """
        Restrict searching to a given document field
        """)


def _analyzer() -> _OptionalField[str]:
    return _OptionalField(str, """
        Use a defined server-side analyzer to process the input term prior to
        executing the search
        """)


def _fuzziness() -> _OptionalField[int]:
    return _OptionalField(int, """
        Allow a given degree of fuzz in the match. Matches which are closer to
        the original term will be scored higher.

        You can apply the fuzziness to only a portion of the string by
        specifying :attr:`prefix_length` - indicating that only the part
        of the field after the prefix length should be checked with fuzziness.

        This value is specified as an int
        """)


def _prefix_length() -> _OptionalField[int]:
    return _OptionalField(int, """
        When using :attr:`fuzziness`, this controls how much of the term
        or phrase is *excluded* from fuzziness. This may help improve
        performance at the expense of omitting potential fuzzy matches
        at the beginning of the string.
        """)


# Query Types


class SearchQuery:
    """Base query object. You probably want to use one of the subclasses.

    .. seealso:: :class:`MatchQuery`, :class:`BooleanQuery`,
        :class:`RegexQuery`, :class:`PrefixQuery`, :class:`NumericRangeQuery`,
        :class:`DateRangeQuery`, :class:`ConjunctionQuery`,
        :class:`DisjunctionQuery`, and others in this module.
    """

    _boost: float | None = None

    def __init__(self, *, boost: float | None = None) -> None:
        self._props: _Body = {}
        self.boost = boost

    def set_prop(self, key: str, value: JSONType) -> None:
        """Sets ``key`` in the query's JSON to ``value``, sent as given and without validation.

        The key is written over whatever the query's own fields encode under it, and is not read
        back by any of the query's properties. Use this for a query parameter the class does not
        expose.
        """
        self._props[key] = value

    @property
    def boost(self) -> float | None:
        """A multiplier on the query's part of a hit's score, or None if not set."""
        return self._boost

    @boost.setter
    def boost(self, value: float | None) -> None:
        self._boost = value

    @property
    def encodable(self) -> JSONType:
        """The query as the Search service's JSON query language, validated first.

        The result is ready to serialize to JSON, and each read builds a new one.

        .. code-block:: python

            json.dumps(query.encodable)
        """
        return _encode(self)

    def validate(self) -> None:
        """Check that the query's required fields are set, before it is encoded.

        The base class checks nothing. A subclass raises if a field it needs is missing or empty.
        """
        pass

    def __repr__(self) -> str:
        return f'{type(self).__name__}(query={_encode(self, validate=False)})'

    def __str__(self) -> str:
        return self.__repr__()


# Single Term Queries


class TermQuery(SearchQuery):
    """Query that specifies the Search Service should search for an exact match to the specified term.

    Examples:
        Basic search using TermQuery::

            from couchbase.search import TermQuery, SearchRequest

            # ... other code ...

            query = TermQuery('park', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using TermQuery with kwargs::

            from couchbase.search import TermQuery, SearchRequest

            # ... other code ...

            query = TermQuery(term='park', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using TermQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import TermQuery, SearchRequest

            # ... other code ...

            query = TermQuery('park', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    fuzziness = _fuzziness()
    prefix_length = _prefix_length()
    field = _field()

    _term: str | None = None

    def __init__(self,
                 term: str | None = None,
                 *,
                 field: str | None = None,
                 fuzziness: int | None = None,
                 prefix_length: int | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if term is None:
            raise _missing(self, 'term')
        self.term = term
        self.field = field
        self.fuzziness = fuzziness
        self.prefix_length = prefix_length

    @property
    def term(self) -> str | None:
        """The term to match exactly, without analysis."""
        return self._term

    @term.setter
    def term(self, value: str | None) -> None:
        self._term = value


class QueryStringQuery(SearchQuery):
    """Query that specifies the Search Service should search for a match to the specified query string.

    .. seealso::

        `Query Language <http://www.blevesearch.com/docs/Query-String-Query/>`_

    Examples:
        Basic search using QueryStringQuery::

            from couchbase.search import QueryStringQuery, SearchRequest

            # ... other code ...

            query = QueryStringQuery('+description:sea -color_hex:fff5ee')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using QueryStringQuery with kwargs::

            from couchbase.search import QueryStringQuery, SearchRequest

            # ... other code ...

            query = QueryStringQuery(query='+description:sea -color_hex:fff5ee')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using QueryStringQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import QueryStringQuery, SearchRequest

            # ... other code ...

            query = QueryStringQuery('+description:sea -color_hex:fff5ee')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _query: str | None = None

    def __init__(self, query: str | None = None, *, boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if query is None:
            raise _missing(self, 'query')
        self.query = query

    @property
    def query(self) -> str | None:
        """The query, in the query string syntax."""
        return self._query

    @query.setter
    def query(self, value: str | None) -> None:
        self._query = value


class WildcardQuery(SearchQuery):
    """Query that specifies the Search Service should search for a match to the specified wildcard string.

    Use `?` to allow a match to any single character.
    Use `*` to allow a match to zero or many characters.


    Examples:
        Basic search using WildcardQuery::

            from couchbase.search import WildcardQuery, SearchRequest

            # ... other code ...

            query = WildcardQuery('f0f???', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

    field = _field()

    _wildcard: str | None = None

    def __init__(self,
                 wildcard: str | None = None,
                 *,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if wildcard is None:
            raise _missing(self, 'wildcard')
        self.wildcard = wildcard
        self.field = field

    @property
    def wildcard(self) -> str | None:
        """The term to match, where ``?`` matches any one character and ``*`` any run of them."""
        return self._wildcard

    @wildcard.setter
    def wildcard(self, value: str | None) -> None:
        self._wildcard = value


class DocIdQuery(SearchQuery):
    """A query for the documents with the given IDs.

    Examples:
        Basic search using DocIdQuery::

            from couchbase.search import DocIdQuery, SearchRequest

            # ... other code ...

            query = DocIdQuery(['34','43','61'])
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

            query = DocIdQuery(['34','43','61','72'])
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    _ids: Sequence[str] | None = None

    def __init__(self, ids: Sequence[str] | None = None, *, boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if ids is None:
            raise _missing(self, 'ids')
        self.ids = ids

    @property
    def ids(self) -> Sequence[str] | None:
        """The IDs of the documents to match."""
        return self._ids

    @ids.setter
    def ids(self, value: Sequence[str] | None) -> None:
        self._ids = value

    def validate(self) -> None:
        """Check that the query has at least one ID.

        Raises:
            :class:`~couchbase.exceptions.NoChildrenException`: If ``ids`` is empty.
        """
        super().validate()
        if not self.ids:
            raise NoChildrenException('`ids` must contain at least one ID')


class MatchQuery(SearchQuery):
    """A query for documents matching a text, which the server analyzes into terms first.

    Without a field it searches the index's default field.

    Examples:
        Basic search using MatchQuery::

            from couchbase.search import MatchQuery, SearchRequest

            # ... other code ...

            query = MatchQuery('secondary', field='color_wheel_pos')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

    fuzziness = _fuzziness()
    prefix_length = _prefix_length()
    field = _field()
    analyzer = _analyzer()

    _match: str | None = None
    _match_operator: MatchOperator | None = None

    def __init__(self,
                 match: str | None = None,
                 *,
                 match_operator: MatchOperator | str | None = None,
                 field: str | None = None,
                 analyzer: str | None = None,
                 fuzziness: int | None = None,
                 prefix_length: int | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if match is None:
            raise _missing(self, 'match')
        self.match = match
        self.match_operator = match_operator
        self.field = field
        self.analyzer = analyzer
        self.fuzziness = fuzziness
        self.prefix_length = prefix_length

    @property
    def match(self) -> str | None:
        """The text to match, which the server analyzes into terms."""
        return self._match

    @match.setter
    def match(self, value: str | None) -> None:
        self._match = value

    @property
    def match_operator(self) -> MatchOperator | None:
        """Whether a document must match all the text's terms or any one of them, or None if not set.

        It can be set with the str ``or`` or ``and``.
        """
        return self._match_operator

    @match_operator.setter
    def match_operator(self, value: MatchOperator | str | None) -> None:
        if value is None or isinstance(value, MatchOperator):
            self._match_operator = value
        elif isinstance(value, str) and value.lower() in ('or', 'and'):
            self._match_operator = MatchOperator(value.lower())
        else:
            raise ValueError(("Expected match_operator to be either of type "
                              "MatchOperator or str representation "
                              "of MatchOperator"))


class MatchPhraseQuery(SearchQuery):
    """A query for documents holding a phrase, which the server analyzes into terms first.

    The terms must appear together and in order.

    Examples:
        Basic search using MatchPhraseQuery::

            from couchbase.search import MatchPhraseQuery, SearchRequest

            # ... other code ...

            query = MatchPhraseQuery('white sands', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

    field = _field()
    analyzer = _analyzer()

    _match_phrase: str | None = None

    def __init__(self,
                 match_phrase: str | None = None,
                 *,
                 field: str | None = None,
                 analyzer: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if match_phrase is None:
            raise _missing(self, 'match_phrase')
        self.match_phrase = match_phrase
        self.field = field
        self.analyzer = analyzer

    @property
    def match_phrase(self) -> str | None:
        """The phrase to match, which the server analyzes into terms."""
        return self._match_phrase

    @match_phrase.setter
    def match_phrase(self, value: str | None) -> None:
        self._match_phrase = value


class PhraseQuery(SearchQuery):
    """A query for documents holding terms together and in order, in one field.

    The ``field`` property must be set.

    The terms are matched as given, without analysis, so each must be a term as the index holds
    it. Pass each term as its own argument, or as a list with ``terms=``; a single string is split
    on whitespace. For a phrase the server should analyze first, use :class:`MatchPhraseQuery`.

    Examples:
        Basic search using PhraseQuery::

            from couchbase.search import PhraseQuery, SearchRequest

            # ... other code ...

            query = PhraseQuery('white', 'sand', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using PhraseQuery with kwargs::

            from couchbase.search import PhraseQuery, SearchRequest

            # ... other code ...

            query = PhraseQuery(terms=['white', 'sand'], field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using PhraseQuery with SearchOptions::

            from couchbase.options import SearchOptions
            from couchbase.search import PhraseQuery, SearchRequest

            # ... other code ...

            query = PhraseQuery('white', 'sand', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    field = _field()

    _terms: list[str] | None = None

    def __init__(self,
                 *phrases: str,
                 terms: str | list[str] | tuple[str, ...] | None = None,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        given: str | list[str] | tuple[str, ...]
        if terms is not None:
            given = terms
        elif len(phrases) == 1 and isinstance(phrases[0], str):
            given = phrases[0]
        else:
            given = phrases
        if isinstance(given, str):
            given = given.split()
        if len(given) == 0:
            raise _missing(self, 'terms')
        self.terms = given
        self.field = field

    @property
    def terms(self) -> list[str] | None:
        """The terms to match, in order, without analysis."""
        return self._terms

    @terms.setter
    def terms(self, value: list[str] | tuple[str, ...]) -> None:
        if not (isinstance(value, (list, tuple)) and all(map(lambda f: isinstance(f, str), value))):
            raise InvalidArgumentException(message='Expected a list of strings')
        self._terms = list(value) if isinstance(value, tuple) else value

    def validate(self) -> None:
        """Check that the query has at least one term.

        Raises:
            :class:`~couchbase.exceptions.NoChildrenException`: If ``terms`` is empty.
        """
        super().validate()
        if not self.terms:
            raise NoChildrenException('Missing terms')


class PrefixQuery(SearchQuery):
    """A query for documents holding a term that starts with a prefix.

    Examples:
        Basic search using PrefixQuery::

            from couchbase.search import PrefixQuery, SearchRequest

            # ... other code ...

            query = PrefixQuery('yellow', field='description')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

    field = _field()

    _prefix: str | None = None

    def __init__(self,
                 prefix: str | None = None,
                 *,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if prefix is None:
            raise _missing(self, 'prefix')
        self.prefix = prefix
        self.field = field

    @property
    def prefix(self) -> str | None:
        """The prefix a matching term starts with."""
        return self._prefix

    @prefix.setter
    def prefix(self, value: str | None) -> None:
        self._prefix = value


class RegexQuery(SearchQuery):
    """A query for documents holding a term that matches a regular expression.

    Examples:
        Basic search using RegexQuery::

            from couchbase.search import RegexQuery, SearchRequest

            # ... other code ...

            query = RegexQuery('f[58]f.*', field='color_hex')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

    field = _field()

    _regexp: str | None = None

    def __init__(self,
                 regexp: str | None = None,
                 *,
                 regex: str | None = None,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if regexp is None:
            regexp = regex
        if regexp is None:
            raise _missing(self, 'regexp')
        self.regexp = regexp
        self.field = field

    @property
    def regex(self) -> str | None:
        """The regular expression a matching term matches."""
        return self._regexp

    @regex.setter
    def regex(self, value: str | None) -> None:
        self._regexp = value

    @property
    def regexp(self) -> str | None:
        """Another name for :attr:`regex`."""
        return self._regexp

    @regexp.setter
    def regexp(self, value: str | None) -> None:
        self._regexp = value


RegexpQuery = RegexQuery


class BooleanFieldQuery(SearchQuery):
    """A query for documents whose field holds a boolean value.

    Examples:
        Basic search using BooleanFieldQuery::

            from couchbase.search import BooleanFieldQuery, SearchRequest

            # ... other code ...

            query = BooleanFieldQuery(True, field='perfect_rating')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

    field = _field()

    _bool: builtins.bool | None = None

    def __init__(self,
                 bool: builtins.bool | None = None,
                 *,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if bool is None:
            raise _missing(self, 'bool')
        self.bool = bool
        self.field = field

    @property
    def bool(self) -> builtins.bool | None:
        """The boolean value to match."""
        return self._bool

    @bool.setter
    def bool(self, value: builtins.bool | None) -> None:
        self._bool = value


# Geo Queries


class GeoDistanceQuery(SearchQuery):
    """A query for documents with a location within a distance of a point.

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
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoDistanceQuery with location as dict::

            from couchbase.search import GeoDistanceQuery, SearchRequest

            # ... other code ...

            query = GeoDistanceQuery('130mi', {'lon': -115.1391, 'lat': 36.1716}, field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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

    field = _field()

    _distance: str | None = None
    _location: list[float] | None = None

    def __init__(self,
                 distance: str | None = None,
                 location: _Point | None = None,
                 *,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if distance is None or location is None:
            raise ValueError(f'{type(self).__name__} requires both a distance and location to be specified.')
        self.distance = distance
        self.location = location
        self.field = field

    @property
    def location(self) -> list[float] | None:
        """The point the distance is measured from, as ``[longitude, latitude]``."""
        return self._location

    @location.setter
    def location(self, value: _Point | None) -> None:
        self._location = None if value is None else _point(value)

    @property
    def distance(self) -> str | None:
        """The distance, with its unit, as in ``10mi``."""
        return self._distance

    @distance.setter
    def distance(self, value: str | None) -> None:
        self._distance = value


class GeoBoundingBoxQuery(SearchQuery):
    """A query for documents with a location inside a rectangle.

    If you use location as an array, your array must contain a longitude value followed by a latitude value.

    Examples:
        Basic search using GeoBoundingBoxQuery::

            from couchbase.search import GeoBoundingBoxQuery, SearchRequest

            # ... other code ...

            query = GeoBoundingBoxQuery((-0.489, 51.686), (0.236, 51.28), field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

        Basic search using GeoBoundingBoxQuery with bounding box points as list::

            from couchbase.search import GeoBoundingBoxQuery, SearchRequest

            # ... other code ...

            query = GeoBoundingBoxQuery([-0.489, 51.686], [0.236, 51.28], field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
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
            search_res = scope.search(scope_index_name, req)
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

        Basic search using GeoBoundingBoxQuery with SearchOptions::

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

    field = _field()

    _top_left: list[float] | None = None
    _bottom_right: list[float] | None = None

    def __init__(self,
                 top_left: _Point | None = None,
                 bottom_right: _Point | None = None,
                 *,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if top_left is None or bottom_right is None:
            raise ValueError(f'{type(self).__name__} requires both a top_left and bottom_right to be specified.')
        self.top_left = top_left
        self.bottom_right = bottom_right
        self.field = field

    @property
    def top_left(self) -> list[float] | None:
        """The rectangle's top left corner, as ``[longitude, latitude]``."""
        return self._top_left

    @top_left.setter
    def top_left(self, value: _Point | None) -> None:
        self._top_left = None if value is None else _point(value)

    @property
    def bottom_right(self) -> list[float] | None:
        """The rectangle's bottom right corner, as ``[longitude, latitude]``."""
        return self._bottom_right

    @bottom_right.setter
    def bottom_right(self, value: _Point | None) -> None:
        self._bottom_right = None if value is None else _point(value)


class GeoPolygonQuery(SearchQuery):
    """A query for documents with a location inside a polygon.

    The point tuple must contain a longitude value followed by a latitude value.

    Examples:
        Basic search using GeoPolygonQuery::

            from couchbase.search import GeoPolygonQuery, SearchRequest

            # ... other code ...

            query = GeoPolygonQuery([(-122.44234633404847, 37.79393211306212),
                                     (-122.43977141339417, 37.77995881733997),
                                     (-122.42925715405579, 37.788031092020155),
                                     (-122.41149020154114, 37.79026946582319),
                                     (-122.40735054016113, 37.79571192027403),
                                     (-122.44234633404847, 37.79393211306212)],
                                     field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')


        Basic search using GeoPolygonQuery with kwargs::

            from couchbase.search import GeoPolygonQuery, SearchRequest

            # ... other code ...

            pts = [(-122.44234633404847, 37.79393211306212),
                   (-122.43977141339417, 37.77995881733997),
                   (-122.42925715405579, 37.788031092020155),
                   (-122.41149020154114, 37.79026946582319),
                   (-122.40735054016113, 37.79571192027403),
                   (-122.44234633404847, 37.79393211306212)]
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

            query = GeoPolygonQuery([(-122.44234633404847, 37.79393211306212),
                                     (-122.43977141339417, 37.77995881733997),
                                     (-122.42925715405579, 37.788031092020155),
                                     (-122.41149020154114, 37.79026946582319),
                                     (-122.40735054016113, 37.79571192027403),
                                     (-122.44234633404847, 37.79393211306212)],
                                     field='geo_location')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    field = _field()

    _polygon_points: list[list[float]] | None = None

    def __init__(self,
                 polygon_points: Sequence[_Point] | None = None,
                 *,
                 field: str | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        if not polygon_points:
            raise ValueError(f'{type(self).__name__} requires a list of tuples for polygon points.')
        self.polygon_points = polygon_points
        self.field = field

    @property
    def polygon_points(self) -> list[list[float]] | None:
        """The polygon's corners, each as ``[longitude, latitude]``."""
        return self._polygon_points

    @polygon_points.setter
    def polygon_points(self, value: Sequence[_Point] | None) -> None:
        self._polygon_points = None if value is None else [_point(p) for p in value]


# Range Queries


def _no_bounds(names: tuple[str, str]) -> ValueError:
    return ValueError(f'At least one of the following params should be specified: {", ".join(names)}')


class NumericRangeQuery(SearchQuery):
    """A query for documents whose field holds a number in a range.

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

    field = _field()

    _min: float | None = None
    _max: float | None = None
    _inclusive_min: bool | None = None
    _inclusive_max: bool | None = None

    def __init__(self,
                 min: float | None = None,
                 max: float | None = None,
                 *,
                 inclusive_min: bool | None = None,
                 inclusive_max: bool | None = None,
                 field: str | None = None,
                 boost: float | None = None,
                 min_inclusive: bool | None = None,
                 max_inclusive: bool | None = None) -> None:
        super().__init__(boost=boost)
        if min_inclusive is not None:
            self.min_inclusive = min_inclusive
        if max_inclusive is not None:
            self.max_inclusive = max_inclusive
        if inclusive_min is not None:
            self.inclusive_min = inclusive_min
        if inclusive_max is not None:
            self.inclusive_max = inclusive_max
        self.field = field
        if min is None and max is None:
            raise _no_bounds(('min', 'max'))
        self.min = min
        self.max = max

    @property
    def min(self) -> float | None:
        """The lower bound, or None for no lower bound."""
        return self._min

    @min.setter
    def min(self, value: float | None) -> None:
        self._min = value

    @property
    def min_inclusive(self) -> bool | None:
        """**DEPRECATED** The same as :attr:`inclusive_min`.

        .. deprecated:: 4.4.0
            Use :attr:`inclusive_min` instead.
        """
        Supportability.class_property_deprecated('min_inclusive', 'inclusive_min')
        return self._inclusive_min

    @min_inclusive.setter
    def min_inclusive(self, value: bool | None) -> None:
        Supportability.class_property_deprecated('min_inclusive', 'inclusive_min')
        self._inclusive_min = value

    @property
    def inclusive_min(self) -> bool | None:
        """Whether the lower bound is in the range, or None if not set."""
        return self._inclusive_min

    @inclusive_min.setter
    def inclusive_min(self, value: bool | None) -> None:
        self._inclusive_min = value

    @property
    def max(self) -> float | None:
        """The upper bound, or None for no upper bound."""
        return self._max

    @max.setter
    def max(self, value: float | None) -> None:
        self._max = value

    @property
    def max_inclusive(self) -> bool | None:
        """**DEPRECATED** The same as :attr:`inclusive_max`.

        .. deprecated:: 4.4.0
            Use :attr:`inclusive_max` instead.
        """
        Supportability.class_property_deprecated('max_inclusive', 'inclusive_max')
        return self._inclusive_max

    @max_inclusive.setter
    def max_inclusive(self, value: bool | None) -> None:
        Supportability.class_property_deprecated('max_inclusive', 'inclusive_max')
        self._inclusive_max = value

    @property
    def inclusive_max(self) -> bool | None:
        """Whether the upper bound is in the range, or None if not set."""
        return self._inclusive_max

    @inclusive_max.setter
    def inclusive_max(self, value: bool | None) -> None:
        self._inclusive_max = value


class DateRangeQuery(SearchQuery):
    """A query for documents whose field holds a date in a range.

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

            query = DateRangeQuery('2024-01-01',
                                   '2024-01-05',
                                   inclusive_start=True,
                                   inclusive_end=True,
                                   field='last_rated')
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    field = _field()

    _start: str | None = None
    _end: str | None = None
    _inclusive_start: bool | None = None
    _inclusive_end: bool | None = None
    _datetime_parser: str | None = None

    def __init__(self,
                 start: str | None = None,
                 end: str | None = None,
                 *,
                 inclusive_start: bool | None = None,
                 inclusive_end: bool | None = None,
                 datetime_parser: str | None = None,
                 field: str | None = None,
                 boost: float | None = None,
                 start_inclusive: bool | None = None,
                 end_inclusive: bool | None = None) -> None:
        super().__init__(boost=boost)
        if start_inclusive is not None:
            self.start_inclusive = start_inclusive
        if end_inclusive is not None:
            self.end_inclusive = end_inclusive
        if inclusive_start is not None:
            self.inclusive_start = inclusive_start
        if inclusive_end is not None:
            self.inclusive_end = inclusive_end
        self.datetime_parser = datetime_parser
        self.field = field
        if start is None and end is None:
            raise _no_bounds(('start', 'end'))
        self.start = start
        self.end = end

    @property
    def start(self) -> str | None:
        """The earliest date, or None for no lower bound."""
        return self._start

    @start.setter
    def start(self, value: str | None) -> None:
        self._start = value

    @property
    def start_inclusive(self) -> bool | None:
        """**DEPRECATED** The same as :attr:`inclusive_start`.

        .. deprecated:: 4.4.0
            Use :attr:`inclusive_start` instead.
        """
        Supportability.class_property_deprecated('start_inclusive', 'inclusive_start')
        return self._inclusive_start

    @start_inclusive.setter
    def start_inclusive(self, value: bool | None) -> None:
        Supportability.class_property_deprecated('start_inclusive', 'inclusive_start')
        self._inclusive_start = value

    @property
    def inclusive_start(self) -> bool | None:
        """Whether the start date is in the range, or None if not set."""
        return self._inclusive_start

    @inclusive_start.setter
    def inclusive_start(self, value: bool | None) -> None:
        self._inclusive_start = value

    @property
    def end(self) -> str | None:
        """The latest date, or None for no upper bound."""
        return self._end

    @end.setter
    def end(self, value: str | None) -> None:
        self._end = value

    @property
    def end_inclusive(self) -> bool | None:
        """**DEPRECATED** The same as :attr:`inclusive_end`.

        .. deprecated:: 4.4.0
            Use :attr:`inclusive_end` instead.
        """
        Supportability.class_property_deprecated('end_inclusive', 'inclusive_end')
        return self._inclusive_end

    @end_inclusive.setter
    def end_inclusive(self, value: bool | None) -> None:
        Supportability.class_property_deprecated('end_inclusive', 'inclusive_end')
        self._inclusive_end = value

    @property
    def inclusive_end(self) -> bool | None:
        """Whether the end date is in the range, or None if not set."""
        return self._inclusive_end

    @inclusive_end.setter
    def inclusive_end(self, value: bool | None) -> None:
        self._inclusive_end = value

    @property
    def datetime_parser(self) -> str | None:
        """The name of the index's date parser that reads the dates, or None for the index's default."""
        return self._datetime_parser

    @datetime_parser.setter
    def datetime_parser(self, value: str | None) -> None:
        self._datetime_parser = value


class TermRangeQuery(SearchQuery):
    """A query for documents holding a term in a range, in lexical order.

    .. note::
        At least one of `min` or `max` must be specified.

    The first two positional arguments are the deprecated `start` and `end`, so pass the bounds
    by keyword.

    Examples:
        Basic search using TermRangeQuery::

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

    field = _field()

    _min: str | None = None
    _max: str | None = None
    _inclusive_min: bool | None = None
    _inclusive_max: bool | None = None

    def __init__(self,
                 start: str | None = None,
                 end: str | None = None,
                 min: str | None = None,
                 max: str | None = None,
                 *,
                 inclusive_min: bool | None = None,
                 inclusive_max: bool | None = None,
                 field: str | None = None,
                 boost: float | None = None,
                 start_inclusive: bool | None = None,
                 end_inclusive: bool | None = None) -> None:
        super().__init__(boost=boost)
        if start is not None and min is None:
            Supportability.class_property_deprecated('start', 'min')
            min = start
        if end is not None and max is None:
            Supportability.class_property_deprecated('end', 'max')
            max = end
        if start_inclusive is not None:
            self.start_inclusive = start_inclusive
        if end_inclusive is not None:
            self.end_inclusive = end_inclusive
        if inclusive_min is not None:
            self.inclusive_min = inclusive_min
        if inclusive_max is not None:
            self.inclusive_max = inclusive_max
        self.field = field
        if min is None and max is None:
            raise _no_bounds(('min', 'max'))
        self.min = min
        self.max = max

    @property
    def start(self) -> str | None:
        """**DEPRECATED** The same as :attr:`min`.

        .. deprecated:: 4.4.0
            Use :attr:`min` instead.
        """
        Supportability.class_property_deprecated('start', 'min')
        return self._min

    @start.setter
    def start(self, value: str | None) -> None:
        Supportability.class_property_deprecated('start', 'min')
        self._min = value

    @property
    def min(self) -> str | None:
        """The lower bound, or None for no lower bound."""
        return self._min

    @min.setter
    def min(self, value: str | None) -> None:
        self._min = value

    @property
    def start_inclusive(self) -> bool | None:
        """**DEPRECATED** The same as :attr:`inclusive_min`.

        .. deprecated:: 4.4.0
            Use :attr:`inclusive_min` instead.
        """
        Supportability.class_property_deprecated('start_inclusive', 'inclusive_min')
        return self._inclusive_min

    @start_inclusive.setter
    def start_inclusive(self, value: bool | None) -> None:
        Supportability.class_property_deprecated('start_inclusive', 'inclusive_min')
        self._inclusive_min = value

    @property
    def inclusive_min(self) -> bool | None:
        """Whether the lower bound is in the range, or None if not set."""
        return self._inclusive_min

    @inclusive_min.setter
    def inclusive_min(self, value: bool | None) -> None:
        self._inclusive_min = value

    @property
    def end(self) -> str | None:
        """**DEPRECATED** The same as :attr:`max`.

        .. deprecated:: 4.4.0
            Use :attr:`max` instead.
        """
        Supportability.class_property_deprecated('end', 'max')
        return self._max

    @end.setter
    def end(self, value: str | None) -> None:
        Supportability.class_property_deprecated('end', 'max')
        self._max = value

    @property
    def max(self) -> str | None:
        """The upper bound, or None for no upper bound."""
        return self._max

    @max.setter
    def max(self, value: str | None) -> None:
        self._max = value

    @property
    def end_inclusive(self) -> bool | None:
        """**DEPRECATED** The same as :attr:`inclusive_max`.

        .. deprecated:: 4.4.0
            Use :attr:`inclusive_max` instead.
        """
        Supportability.class_property_deprecated('end_inclusive', 'inclusive_max')
        return self._inclusive_max

    @end_inclusive.setter
    def end_inclusive(self, value: bool | None) -> None:
        Supportability.class_property_deprecated('end_inclusive', 'inclusive_max')
        self._inclusive_max = value

    @property
    def inclusive_max(self) -> bool | None:
        """Whether the upper bound is in the range, or None if not set."""
        return self._inclusive_max

    @inclusive_max.setter
    def inclusive_max(self, value: bool | None) -> None:
        self._inclusive_max = value


# Compound Queries

def _children(queries: tuple[SearchQuery | list[SearchQuery], ...]) -> list[SearchQuery]:
    if len(queries) == 1 and isinstance(queries[0], list):
        return queries[0]
    children: list[SearchQuery] = []
    for item in queries:
        children.extend(item if isinstance(item, list) else (item,))
    return children


class ConjunctionQuery(SearchQuery):
    """A compound query that specifies multiple child queries.

    For a ConjunctionQuery, every query object in the array must have a match in a document
    to include the document in search results.

    Examples:
        Basic search using ConjunctionQuery::

            from couchbase.search import ConjunctionQuery, DateRangeQuery, TermQuery, SearchRequest

            # ... other code ...

            queries = [DateRangeQuery('2024-01-01',
                                      '2024-02-15',
                                      inclusive_start=False,
                                      inclusive_end=False,
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
                                      inclusive_start=False,
                                      inclusive_end=False,
                                      field='last_rated'),
                        TermQuery('sand', field='description')]
            query = ConjunctionQuery(*queries)
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req, SearchOptions(limit=3, fields=['*']))
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')
    """

    def __init__(self,
                 *queries: SearchQuery | list[SearchQuery],
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        self.conjuncts = _children(queries)

    @property
    def conjuncts(self) -> list[SearchQuery]:
        """The queries a document must all match."""
        return self._conjuncts

    @conjuncts.setter
    def conjuncts(self, value: list[SearchQuery]) -> None:
        self._conjuncts = value

    def validate(self) -> None:
        """Check that the query has at least one child query.

        Raises:
            :class:`~couchbase.exceptions.NoChildrenException`: If ``conjuncts`` is empty.
        """
        super().validate()
        if not self.conjuncts:
            raise NoChildrenException('No sub-queries')


class DisjunctionQuery(SearchQuery):
    """A compound query that specifies multiple child queries.

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

    def __init__(self,
                 *queries: SearchQuery | list[SearchQuery],
                 min: int | None = None,
                 boost: float | None = None) -> None:
        super().__init__(boost=boost)
        self.min = 1 if min is None else min
        self.disjuncts = _children(queries)

    @property
    def disjuncts(self) -> list[SearchQuery]:
        """The queries a document must match :attr:`min` of."""
        return self._disjuncts

    @disjuncts.setter
    def disjuncts(self, value: list[SearchQuery]) -> None:
        self._disjuncts = value

    @property
    def min(self) -> int:
        """How many of the queries a document must match. Defaults to 1."""
        return self._min

    @min.setter
    def min(self, value: int) -> None:
        value = int(value)
        if value < 0:
            raise InvalidArgumentException(message='Must be >= 0')
        self._min = value

    def validate(self) -> None:
        """Check that the query has child queries, and at least :attr:`min` of them.

        Raises:
            :class:`~couchbase.exceptions.NoChildrenException`: If ``disjuncts`` is empty.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If :attr:`min` is more than the number of
                queries.
        """
        super().validate()
        if not self.disjuncts:
            raise NoChildrenException('No queries specified')
        if len(self.disjuncts) < self.min:
            raise InvalidArgumentException(message='Specified min is larger than number of queries.')


CompoundQueryType = SearchQuery | ConjunctionQuery | DisjunctionQuery | list[SearchQuery]
_Compound = TypeVar('_Compound', ConjunctionQuery, DisjunctionQuery)


class BooleanQuery(SearchQuery):
    """A compound query that specifies multiple child queries.

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
            req = SearchRequest.create(query)
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

    _must: ConjunctionQuery | None = None
    _should: DisjunctionQuery | None = None
    _must_not: DisjunctionQuery | None = None

    def __init__(self,
                 must: CompoundQueryType | None = None,
                 should: CompoundQueryType | None = None,
                 must_not: CompoundQueryType | None = None) -> None:
        super().__init__()
        self.must = must
        self.should = should
        self.must_not = must_not
        self.validate()

    @property
    def must(self) -> ConjunctionQuery | None:
        """The queries a document must all match, or None if not set."""
        return self._must

    @must.setter
    def must(self, value: CompoundQueryType | None) -> None:
        self._must = _clause(value, ConjunctionQuery)

    @property
    def must_not(self) -> DisjunctionQuery | None:
        """The queries a document must match none of, or None if not set."""
        return self._must_not

    @must_not.setter
    def must_not(self, value: CompoundQueryType | None) -> None:
        self._must_not = _clause(value, DisjunctionQuery)

    @property
    def should(self) -> DisjunctionQuery | None:
        """The queries a document must match the disjunction's ``min`` of, or None if not set."""
        return self._should

    @should.setter
    def should(self, value: CompoundQueryType | None) -> None:
        self._should = _clause(value, DisjunctionQuery)

    def validate(self) -> None:
        """Check that at least one of :attr:`must`, :attr:`should` and :attr:`must_not` is set.

        Raises:
            ValueError: If none of them is.
        """
        super().validate()
        if not self.must and not self.must_not and not self.should:
            raise ValueError('At least one of the following queries should be specified: must, should, must_not.')


def _clause(value: CompoundQueryType | None, reqtype: type[_Compound]) -> _Compound | None:
    if value is None or isinstance(value, reqtype):
        return value
    if isinstance(value, SearchQuery):
        return reqtype(value)
    sub_query = []
    for query in iter(value):
        if not isinstance(query, SearchQuery):
            raise TypeError('Item is not a query!', query)
        sub_query.append(query)
    return reqtype(*sub_query)


# Special Queries

class RawQuery(SearchQuery):
    """A query given as its JSON, for parameters or query types the other classes do not cover.

    The payload is read when the query is encoded, and never written: :attr:`boost` and
    :meth:`set_prop` are added to a copy.
    """

    def __init__(self, obj: JSONType) -> None:
        super().__init__()
        self._raw = obj


class MatchAllQuery(SearchQuery):
    """A query that matches every document.

    Examples:
        Basic search using MatchAllQuery::

            from couchbase.search import MatchAllQuery, SearchRequest

            # ... other code ...

            query = MatchAllQuery()
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

    """


class MatchNoneQuery(SearchQuery):
    """A query that matches no document.

    Examples:
        Basic search using MatchNoneQuery::

            from couchbase.search import MatchNoneQuery, SearchRequest

            # ... other code ...

            query = MatchNoneQuery()
            req = SearchRequest.create(query)
            search_res = scope.search(scope_index_name, req)
            for r in search_res.rows():
                print(f'Found search result: {r}')
            print(f'metadata={search_res.metadata()}')

    """


# The JSON encoding. Each function returns the query's own fields under the Search service's names,
# leaving out a field that is not set; `_encode` adds `boost` and the `set_prop` keys to every one.


def _fuzzy(q: TermQuery | MatchQuery) -> _Body:
    return _put({}, fuzziness=q.fuzziness, prefix_length=q.prefix_length)


def _encode_match(q: MatchQuery, validate: bool) -> _Body:
    operator = None if q.match_operator is None else q.match_operator.value
    return _put(_fuzzy(q), match=q.match, operator=operator, field=q.field, analyzer=q.analyzer)


def _encode_range(q: NumericRangeQuery | TermRangeQuery, validate: bool) -> _Body:
    return _put({}, min=q.min, max=q.max, inclusive_min=q.inclusive_min, inclusive_max=q.inclusive_max,
                field=q.field)


def _encode_date_range(q: DateRangeQuery, validate: bool) -> _Body:
    return _put({}, start=q.start, end=q.end, inclusive_start=q.inclusive_start, inclusive_end=q.inclusive_end,
                datetime_parser=q.datetime_parser, field=q.field)


def _encode_boolean(q: BooleanQuery, validate: bool) -> _Body:
    return _put({},
                must=None if q.must is None else _encode(q.must, validate),
                must_not=None if q.must_not is None else _encode(q.must_not, validate),
                should=None if q.should is None else _encode(q.should, validate))


_JSON: dict[type, Callable[[Any, bool], _Body]] = {
    SearchQuery: lambda q, v: {},
    TermQuery: lambda q, v: _put(_fuzzy(q), term=q.term, field=q.field),
    QueryStringQuery: lambda q, v: _put({}, query=q.query),
    WildcardQuery: lambda q, v: _put({}, wildcard=q.wildcard, field=q.field),
    DocIdQuery: lambda q, v: _put({}, ids=q.ids),
    MatchQuery: _encode_match,
    MatchPhraseQuery: lambda q, v: _put({}, match_phrase=q.match_phrase, field=q.field, analyzer=q.analyzer),
    PhraseQuery: lambda q, v: _put({}, terms=q.terms, field=q.field),
    PrefixQuery: lambda q, v: _put({}, prefix=q.prefix, field=q.field),
    RegexQuery: lambda q, v: _put({}, regexp=q.regexp, field=q.field),
    BooleanFieldQuery: lambda q, v: _put({}, bool=q.bool, field=q.field),
    GeoDistanceQuery: lambda q, v: _put({}, distance=q.distance, location=q.location, field=q.field),
    GeoBoundingBoxQuery: lambda q, v: _put({}, top_left=q.top_left, bottom_right=q.bottom_right, field=q.field),
    GeoPolygonQuery: lambda q, v: _put({}, polygon_points=q.polygon_points, field=q.field),
    NumericRangeQuery: _encode_range,
    DateRangeQuery: _encode_date_range,
    TermRangeQuery: _encode_range,
    ConjunctionQuery: lambda q, v: {'conjuncts': [_encode(c, v) for c in q.conjuncts]},
    DisjunctionQuery: lambda q, v: {'disjuncts': [_encode(c, v) for c in q.disjuncts], 'min': q.min},
    BooleanQuery: _encode_boolean,
    MatchAllQuery: lambda q, v: {'match_all': None},
    MatchNoneQuery: lambda q, v: {'match_none': None},
}


def _encode(query: SearchQuery, validate: bool = True) -> JSONType:
    if validate:
        query.validate()
    if isinstance(query, RawQuery):
        if not isinstance(query._raw, dict):
            if query.boost is not None or query._props:
                raise TypeError(f'RawQuery holds a {type(query._raw).__name__}, which cannot carry boost or set_prop')
            return query._raw
        body = dict(query._raw)
    else:
        encoder = next(_JSON[k] for k in type(query).__mro__ if k in _JSON)
        body = encoder(query, validate)
    _put(body, boost=query.boost)
    body.update(query._props)
    return body
