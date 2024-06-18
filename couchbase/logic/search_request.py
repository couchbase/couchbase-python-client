from __future__ import annotations

from typing import Optional, Union

from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.search_queries import SearchQuery
from couchbase.logic.vector_search import VectorSearch


class SearchRequest:
    """ Represents a search query and/or vector search to execute via the Couchbase Full Text Search (FTS) service.

    Args:
        query (Union[:class:`~couchbase.search.SearchQuery`, :class:`~couchbase.vector_search.VectorSearch`]): A :class:`~couchbase.search.SearchQuery` or
            :class:`~couchbase.vector_search.VectorSearch` to initialize the search request.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If neither a :class:`~couchbase.search.SearchQuery` or :class:`~couchbase.vector_search.VectorSearch` is provided.

    Returns:
        :class:`~couchbase.search.SearchRequest`: The created search request.
    """  # noqa: E501

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
        Optional[:class:`~couchbase.search.SearchQuery`]: Returns the search request's :class:`~couchbase.search.SearchQuery`, if it exists.
        """  # noqa: E501
        return self._search_query

    @property
    def vector_search(self) -> Optional[VectorSearch]:
        """
        Optional[:class:`~couchbase.vector_search.VectorSearch`]: Returns the search request's :class:`~couchbase.vector_search.VectorSearch`, if it exists.
        """  # noqa: E501
        return self._vector_search

    def with_search_query(self,
                          query  # type: SearchQuery
                          ) -> SearchRequest:
        """ Add a :class:`~couchbase.search.SearchQuery` to the search request.

        Args:
            query (:class:`~couchbase.search.SearchQuery`): The :class:`~couchbase.search.SearchQuery` to add to the search request.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the search request already contains a :class:`~couchbase.search.SearchQuery`.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided query is not an instance of a :class:`~couchbase.search.SearchQuery`.

        Returns:
            :class:`~couchbase.search.SearchRequest`: The search request in order to allow method chaining.
        """  # noqa: E501
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
        """ Add a :class:`~couchbase.vector_search.VectorSearch` to the search request.

        Args:
            vector_search (:class:`~couchbase.vector_search.VectorSearch`): The :class:`~couchbase.vector_search.VectorSearch` to add to the search request.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the search request already contains a :class:`~couchbase.vector_search.VectorSearch`.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided query is not an instance of a :class:`~couchbase.vector_search.VectorSearch`.

        Returns:
            :class:`~couchbase.search.SearchRequest`: The search request in order to allow method chaining.
        """  # noqa: E501
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
        """ Creates a :class:`~couchbase.search.SearchRequest`.

        Args:
            query (Union[:class:`~couchbase.search.SearchQuery`, :class:`~couchbase.vector_search.VectorSearch`]): A :class:`~couchbase.search.SearchQuery` or
                :class:`~couchbase.vector_search.VectorSearch` to initialize the search request.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If neither a :class:`~couchbase.search.SearchQuery` or :class:`~couchbase.vector_search.VectorSearch` is provided.

        Returns:
            :class:`~couchbase.search.SearchRequest`: The created search request.
        """  # noqa: E501
        return cls(query)
