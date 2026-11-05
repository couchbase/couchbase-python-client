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
    """ Specifies how multiple vector searches are combined.

    This can be one of:
        AND: Indicates that multiple vector queries should be combined with logical AND.

        OR: Indicates that multiple vector queries should be combined with logical OR.

    """
    AND = 'and'
    OR = 'or'


class VectorQuery:
    """ Represents a vector query.

    Args:
        field_name (str): The name of the field in the search index that stores the vector.
        vector (list[float] | str): The vector to use in the query.
        num_candidates (int, optional): Specifies the number of results returned. If provided, must be greater or equal
            to 1.
        boost (float, optional): Add boost to query.
        prefilter (`~couchbase.search.SearchQuery`, optional): Specifies a pre-filter to use for the vector query.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the vector is not provided.
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the vector is not a list or str.
        :class:`~couchbase.exceptions.InvalidArgumentException`: If vector is a list and all values of the provided
            vector are not instances of float.

    Returns:
        :class:`~couchbase.vector_search.VectorQuery`: The created vector query.
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
        """
        float | None: Returns vector query's boost value, if it exists.
        """
        return self._boost

    @boost.setter
    def boost(self, value: float) -> None:
        if not isinstance(value, float):
            raise InvalidArgumentException('boost must be a float.')
        self._boost = value

    @property
    def field_name(self) -> str:
        """
        str: Returns vector query's field name
        """
        return self._field_name

    @property
    def num_candidates(self) -> int | None:
        """
        int | None: Returns vector query's num candidates value, if it exists.
        """
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
        """
        SearchQuery | None: Returns vector query's prefilter query, if it exists.
        """
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
        """
        list[float] | None: Returns the vector query's vector.
        """
        return self._vector

    @property
    def vector_base64(self) -> str | None:
        """
        str | None: Returns the vector query's base64 vector str.
        """
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
            raise InvalidArgumentException('Provided vector must be either a List[float] or base64 encoded str.')

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
        """ Creates a :class:`~couchbase.vector_search.VectorQuery`.

        Args:
            field_name (str): The name of the field in the search index that stores the vector.
            vector (list[float] | str): The vector to use in the query.
            num_candidates (int, optional): Specifies the number of results returned. If provided, must be greater or
                equal to 1.
            boost (float, optional): Add boost to query.
            prefilter (`~couchbase.search.SearchQuery`, optional): Specifies a pre-filter to use for the vector query.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the vector is not provided.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the vector is not a list or str.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If vector is a list and all values of the provided
                vector are not instances of float.

        Returns:
            :class:`~couchbase.vector_search.VectorQuery`: The created vector query.
        """
        return cls(field_name, vector, num_candidates=num_candidates, boost=boost, prefilter=prefilter)

    def __repr__(self) -> str:
        return fields_repr(self, field_name=self._field_name,
                           vector=self._vector if self._vector is not None else self._vector_base64,
                           num_candidates=self._num_candidates, boost=self._boost, prefilter=self._prefilter)


class VectorSearch:
    """ Represents a vector search.

    Args:
        queries (list[:class:`~couchbase.vector_search.VectorQuery`]):
            The list of :class:`~couchbase.vector_search.VectorQuery`'s to use for the vector search.
        options (:class:`~couchbase.options.VectorSearchOptions`, optional): Options to set for the vector search.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If a list of
            :class:`~couchbase.vector_search.VectorQuery` is not provided.
        :class:`~couchbase.exceptions.InvalidArgumentException`: If all values of the provided queries are not instances
            of :class:`~couchbase.vector_search.VectorQuery`.

    Returns:
        :class:`~couchbase.vector_search.VectorSearch`: The created vector search.
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
        """
            **INTERNAL**
        """
        return self._queries

    @property
    def options(self) -> VectorSearchOptions | None:
        """
            **INTERNAL**
        """
        return self._options

    @classmethod
    def from_vector_query(cls, query: VectorQuery) -> VectorSearch:
        """ Creates a :class:`~couchbase.vector_search.VectorSearch` from a single
        :class:`~couchbase.vector_search.VectorQuery`.

        Args:
            query (:class:`~couchbase.vector_search.VectorQuery`):
                A :class:`~couchbase.vector_search.VectorQuery`'s to use for the vector search.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided query is not an instance of
                :class:`~couchbase.vector_search.VectorQuery`.

        Returns:
            :class:`~couchbase.vector_search.VectorSearch`: The created vector search.
        """
        return cls([query])

    def __repr__(self) -> str:
        return fields_repr(self, queries=self._queries, options=self._options)
