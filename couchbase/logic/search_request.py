from __future__ import annotations

from typing import Optional, Union

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.search_queries import SearchQuery
from couchbase.logic.vector_search import VectorSearch


class SearchRequest:
    """
    **VOLATILE** This API is subject to change at any time.
    """

    def __init__(self,
                 query  # type: Union[SearchQuery, VectorSearch]
                 ):
        self._search_query = self._vector_search = None
        if isinstance(query, SearchQuery):
            self._search_query = query
        elif isinstance(query, VectorSearch):
            self._vector_search = query

        if self._search_query is None and self._vector_search is None:
            raise InvalidArgumentException(('Must provide either a SearchQuery or VectorSearch '
                                            'when creating a SearchRequest.'))

    @property
    def search_query(self) -> Optional[SearchQuery]:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return self._search_query

    @property
    def vector_search(self) -> Optional[VectorSearch]:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return self._vector_search

    def with_search_query(self,
                          query  # type: SearchQuery
                          ) -> SearchRequest:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        is_search_query = isinstance(query, SearchQuery)
        if not is_search_query:
            raise InvalidArgumentException('Must provide a SearchQuery.')
        if is_search_query and self._search_query is not None:
            raise InvalidArgumentException('Request already has SearchQuery.')

        self._search_query = query
        return self

    def with_vector_search(self,
                           vector_search  # type: VectorSearch
                           ) -> SearchRequest:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        is_vector_search = isinstance(vector_search, VectorSearch)
        if not is_vector_search:
            raise InvalidArgumentException('Must provide a VectorSearch.')
        if is_vector_search and self._vector_search is not None:
            raise InvalidArgumentException('Request already has VectorSearch.')

        self._vector_search = vector_search
        return self

    @classmethod
    def create(cls,
               query  # type: Union[SearchQuery, VectorSearch]
               ) -> SearchRequest:
        """
        **VOLATILE** This API is subject to change at any time.
        """
        return cls(query)
