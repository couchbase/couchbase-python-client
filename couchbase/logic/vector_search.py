from __future__ import annotations

from enum import Enum
from typing import List, Optional

from couchbase.exceptions import InvalidArgumentException
from couchbase.options import VectorSearchOptions

"""

Vector search Enums per the search RFC

"""


class VectorQueryCombination(Enum):
    """
    **VOLATILE** This API is subject to change at any time.

    This can be one of:
        AND
        OR

    """
    AND = 'and'
    OR = 'or'


class VectorQuery:
    """
    **VOLATILE** This API is subject to change at any time.
    """

    def __init__(self,
                 field_name,  # type: str
                 vector,  # type: List[float]
                 num_candidates=None,  # type: Optional[int]
                 boost=None,  # type: Optional[float]
                 ):
        self._field_name = field_name
        if vector is None or len(vector) == 0:
            raise InvalidArgumentException('Provided vector cannot be empty.')
        if not all(map(lambda q: isinstance(q, float), vector)):
            raise InvalidArgumentException('All vector values must be a float.')
        self._vector = vector
        self._num_candidates = self._boost = None
        if num_candidates is not None:
            self.num_candidates = num_candidates
        if boost is not None:
            self.boost = boost

    @property
    def boost(self) -> float:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return self._boost

    @boost.setter
    def boost(self,
              value  # type: float
              ):
        if not isinstance(value, float):
            raise InvalidArgumentException('boost must be a float.')
        self._boost = value

    @property
    def field_name(self) -> str:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return self._field_name

    @property
    def num_candidates(self) -> int:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return self._num_candidates

    @num_candidates.setter
    def num_candidates(self,
                       value  # type: int
                       ):
        if not isinstance(value, int):
            raise InvalidArgumentException('num_candidates must be an int.')
        if value < 1:
            raise InvalidArgumentException('num_candidates must be >= 1.')
        self._num_candidates = value

    @property
    def vector(self) -> List[float]:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return self._vector

    @classmethod
    def create(cls,
               field_name,  # type: str
               vector,  # type: List[float]
               num_candidates=None,  # type: Optional[int]
               boost=None,  # type: Optional[float]
               ) -> VectorQuery:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return cls(field_name, vector, num_candidates=num_candidates, boost=boost)


class VectorSearch:
    """
    **VOLATILE** This API is subject to change at any time.
    """

    def __init__(self,
                 queries,  # type: List[VectorQuery]
                 options=None  # type: Optional[VectorSearchOptions]
                 ):
        if queries is None or len(queries) == 0:
            raise InvalidArgumentException('Provided queries cannot be empty.')
        if not all(map(lambda q: isinstance(q, VectorQuery), queries)):
            raise InvalidArgumentException('All queries must be a VectorQuery.')

        self._queries = queries
        self._options = options if options is not None else None

    @property
    def queries(self) -> List[VectorQuery]:
        return self._queries

    @property
    def options(self) -> Optional[VectorSearchOptions]:
        return self._options

    @classmethod
    def from_vector_query(cls,
                          query  # type: VectorQuery
                          ) -> VectorSearch:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return cls([query])
