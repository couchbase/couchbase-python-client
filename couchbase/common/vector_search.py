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

from __future__ import annotations

__all__ = [
    'VectorQuery',
    'VectorQueryCombination',
    'VectorSearch',
]

from enum import Enum
from typing import TYPE_CHECKING

from couchbase.common._utils import fields_repr, is_null_or_empty
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.options import VectorSearchOptions

if TYPE_CHECKING:
    from couchbase.common.search_queries import SearchQuery


class VectorQueryCombination(Enum):
    """How a vector search combines its queries' results.

    AND keeps the documents every query returns, and OR the documents any of them returns.
    """
    AND = 'and'
    OR = 'or'


class VectorQuery:
    """A query for the documents whose vectors are nearest a given vector.

    Args:
        field_name (str): The index field that holds the vectors.
        vector (list[float] | str): The vector to search near, as floats or as a str of its little-endian
            32-bit floats, base64 encoded.
        num_candidates (int, optional): How many nearest neighbours the query returns. Must be 1 or more.
            Defaults to None, which returns 3.
        boost (float, optional): A multiplier on the query's part of a hit's score.
        prefilter (:class:`~couchbase.search.SearchQuery`, optional): A query a document must match to be
            a candidate.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the field name is empty, or the vector
            is empty, not a list of floats or a str.
        :class:`~couchbase.exceptions.InvalidArgumentException`: If ``num_candidates`` is not an int of 1 or
            more, ``boost`` is not a float, or ``prefilter`` is not a search query.
    """

    def __init__(self,
                 field_name: str,
                 vector: list[float] | str,
                 num_candidates: int | None = None,
                 boost: float | None = None,
                 prefilter: SearchQuery | None = None) -> None:
        if is_null_or_empty(field_name):
            raise InvalidArgumentException('Must provide a field name.')
        self._field_name = field_name
        self._vector: list[float] | None = None
        self._vector_base64: str | None = None
        self._validate_and_set_vector(vector)
        self._num_candidates: int | None = None
        self._boost: float | None = None
        self._prefilter: SearchQuery | None = None
        if num_candidates is not None:
            self.num_candidates = num_candidates
        if boost is not None:
            self.boost = boost
        if prefilter is not None:
            self.prefilter = prefilter

    @property
    def boost(self) -> float | None:
        """A multiplier on the query's part of a hit's score, or None if not set."""
        return self._boost

    @boost.setter
    def boost(self, value: float) -> None:
        if not isinstance(value, float):
            raise InvalidArgumentException('boost must be a float.')
        self._boost = value

    @property
    def field_name(self) -> str:
        """The index field that holds the vectors."""
        return self._field_name

    @property
    def num_candidates(self) -> int | None:
        """How many nearest neighbours the query returns, or None for the default of 3."""
        return self._num_candidates

    @num_candidates.setter
    def num_candidates(self, value: int) -> None:
        if not isinstance(value, int):
            raise InvalidArgumentException('num_candidates must be an int.')
        if value < 1:
            raise InvalidArgumentException('num_candidates must be >= 1.')
        self._num_candidates = value

    @property
    def prefilter(self) -> SearchQuery | None:
        """A query a document must match to be a candidate, or None if not set."""
        return self._prefilter

    @prefilter.setter
    def prefilter(self, value: SearchQuery) -> None:
        # avoid circular import
        from couchbase.common.search_queries import SearchQuery
        if not isinstance(value, SearchQuery):
            raise InvalidArgumentException('prefilter must be a SearchQuery.')
        self._prefilter = value

    @property
    def vector(self) -> list[float] | None:
        """The vector as floats, or None if it was given base64 encoded."""
        return self._vector

    @property
    def vector_base64(self) -> str | None:
        """The vector base64 encoded, or None if it was given as floats."""
        return self._vector_base64

    def _validate_and_set_vector(self, vector: list[float] | str) -> None:
        if vector is None:
            raise InvalidArgumentException('Provided vector cannot be empty.')
        if isinstance(vector, list):
            if len(vector) == 0:
                raise InvalidArgumentException('Provided vector cannot be empty.')
            if not all(map(lambda q: isinstance(q, float), vector)):
                raise InvalidArgumentException('All vector values must be a float.')
            self._vector = vector
            return
        elif not isinstance(vector, str):
            raise InvalidArgumentException('Provided vector must be either a list[float] or a base64 encoded str.')

        if len(vector) == 0:
            raise InvalidArgumentException('Provided base64 encoded vector cannot be empty.')

        self._vector_base64 = vector

    @classmethod
    def create(cls,
               field_name: str,
               vector: list[float] | str,
               num_candidates: int | None = None,
               boost: float | None = None,
               prefilter: SearchQuery | None = None) -> VectorQuery:
        """Creates a :class:`~couchbase.vector_search.VectorQuery`.

        Args:
            field_name (str): The index field that holds the vectors.
            vector (list[float] | str): The vector to search near, as floats or as a str of its little-endian
                32-bit floats, base64 encoded.
            num_candidates (int, optional): How many nearest neighbours the query returns. Must be 1 or more.
                Defaults to None, which returns 3.
            boost (float, optional): A multiplier on the query's part of a hit's score.
            prefilter (:class:`~couchbase.search.SearchQuery`, optional): A query a document must match to be
                a candidate.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the field name is empty, or the vector
                is empty, not a list of floats or a str.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If ``num_candidates`` is not an int of 1 or
                more, ``boost`` is not a float, or ``prefilter`` is not a search query.

        Returns:
            :class:`~couchbase.vector_search.VectorQuery`: The query.
        """
        return cls(field_name, vector, num_candidates=num_candidates, boost=boost, prefilter=prefilter)

    def __repr__(self) -> str:
        return fields_repr(self, field_name=self._field_name,
                           vector=self._vector if self._vector is not None else self._vector_base64,
                           num_candidates=self._num_candidates, boost=self._boost, prefilter=self._prefilter)


class VectorSearch:
    """One or more vector queries, sent together as a search request's vector search.

    Args:
        queries (list[:class:`~couchbase.vector_search.VectorQuery`]): The queries.
        options (:class:`~couchbase.options.VectorSearchOptions`, optional): How the queries' results are
            combined.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If ``queries`` is empty or holds anything
            but vector queries.
    """

    def __init__(self, queries: list[VectorQuery], options: VectorSearchOptions | None = None):
        if queries is None or len(queries) == 0:
            raise InvalidArgumentException('Provided queries cannot be empty.')
        if not all(map(lambda q: isinstance(q, VectorQuery), queries)):
            raise InvalidArgumentException('All queries must be a VectorQuery.')

        self._queries = queries
        self._options = options if options is not None else None

    @property
    def queries(self) -> list[VectorQuery]:
        """**INTERNAL** The queries."""
        return self._queries

    @property
    def options(self) -> VectorSearchOptions | None:
        """**INTERNAL** How the queries' results are combined, or None if not set."""
        return self._options

    @classmethod
    def from_vector_query(cls, query: VectorQuery) -> VectorSearch:
        """Creates a :class:`~couchbase.vector_search.VectorSearch` of one query.

        Args:
            query (:class:`~couchbase.vector_search.VectorQuery`): The query.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If ``query`` is not a vector query.

        Returns:
            :class:`~couchbase.vector_search.VectorSearch`: The vector search.
        """
        return cls([query])

    def __repr__(self) -> str:
        return fields_repr(self, queries=self._queries, options=self._options)
