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
    'SearchRequest',
]

from couchbase.common._utils import fields_repr
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.search_queries import SearchQuery
from couchbase.common.vector_search import VectorSearch


class SearchRequest:
    """A search query, a vector search, or both, to run on the Search service.

    A request holding both is a hybrid search, and the request's scoring option sets how their results are combined.

    Args:
        query (:class:`~couchbase.search.SearchQuery` | :class:`~couchbase.vector_search.VectorSearch`): The
            search query or vector search to start with. Add the other with :meth:`with_search_query` or
            :meth:`with_vector_search`.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If query is neither.
    """

    def __init__(self, query: SearchQuery | VectorSearch):
        self._search_query = self._vector_search = None
        if isinstance(query, SearchQuery):
            self._search_query = query
        elif isinstance(query, VectorSearch):
            self._vector_search = query

        if self._search_query is None and self._vector_search is None:
            raise InvalidArgumentException(('Must provide either a SearchQuery or VectorSearch '
                                            'when creating a SearchRequest.'))

    @property
    def search_query(self) -> SearchQuery | None:
        """The request's search query, or None if it has none."""
        return self._search_query

    @property
    def vector_search(self) -> VectorSearch | None:
        """The request's vector search, or None if it has none."""
        return self._vector_search

    def with_search_query(self, query: SearchQuery) -> SearchRequest:
        """Add a :class:`~couchbase.search.SearchQuery` to the search request.

        Args:
            query (:class:`~couchbase.search.SearchQuery`): The :class:`~couchbase.search.SearchQuery` to add to the
                search request.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the search request already contains a
                :class:`~couchbase.search.SearchQuery`.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided query is not an instance of a
                :class:`~couchbase.search.SearchQuery`.

        Returns:
            :class:`~couchbase.search.SearchRequest`: The search request in order to allow method chaining.
        """
        is_search_query = isinstance(query, SearchQuery)
        if not is_search_query:
            raise InvalidArgumentException('Must provide a SearchQuery.')
        if is_search_query and self._search_query is not None:
            raise InvalidArgumentException('Request already has SearchQuery.')

        self._search_query = query
        return self

    def with_vector_search(self, vector_search: VectorSearch) -> SearchRequest:
        """Add a :class:`~couchbase.vector_search.VectorSearch` to the search request.

        Args:
            vector_search (:class:`~couchbase.vector_search.VectorSearch`): The
                :class:`~couchbase.vector_search.VectorSearch` to add to the search request.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the search request already contains a
                :class:`~couchbase.vector_search.VectorSearch`.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided query is not an instance of a
                :class:`~couchbase.vector_search.VectorSearch`.

        Returns:
            :class:`~couchbase.search.SearchRequest`: The search request in order to allow method chaining.
        """
        is_vector_search = isinstance(vector_search, VectorSearch)
        if not is_vector_search:
            raise InvalidArgumentException('Must provide a VectorSearch.')
        if is_vector_search and self._vector_search is not None:
            raise InvalidArgumentException('Request already has VectorSearch.')

        self._vector_search = vector_search
        return self

    @classmethod
    def create(cls, query: SearchQuery | VectorSearch) -> SearchRequest:
        """Creates a :class:`~couchbase.search.SearchRequest`.

        Args:
            query (:class:`~couchbase.search.SearchQuery` | :class:`~couchbase.vector_search.VectorSearch`): A
                :class:`~couchbase.search.SearchQuery` or
                :class:`~couchbase.vector_search.VectorSearch` to initialize the search request.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If neither a :class:`~couchbase.search.SearchQuery`
                or :class:`~couchbase.vector_search.VectorSearch` is provided.

        Returns:
            :class:`~couchbase.search.SearchRequest`: The created search request.
        """
        return cls(query)

    def __repr__(self) -> str:
        if self._search_query is None:
            return fields_repr(self, query=self._vector_search)
        text = fields_repr(self, query=self._search_query)
        return text if self._vector_search is None else f'{text}.with_vector_search({self._vector_search!r})'
