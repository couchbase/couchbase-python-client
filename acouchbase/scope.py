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

__all__ = [
    'AsyncScope',
    'Scope',
]

from typing import TYPE_CHECKING, Any

from acouchbase.collection import Collection
from acouchbase.management.eventing import ScopeEventingFunctionManager
from acouchbase.management.search import ScopeSearchIndexManager
from acouchbase.result import (AsyncAnalyticsResult,
                               AsyncQueryResult,
                               AsyncSearchResult)
from couchbase.common._supportability import Supportability
from couchbase.options import (AnalyticsOptions,
                               QueryOptions,
                               SearchOptions)

if TYPE_CHECKING:
    from acouchbase.bucket import AsyncBucket
    from couchbase.common._interfaces._scope import AsyncScopeInterface
    from couchbase.common._utils import JSONType
    from couchbase.search import SearchQuery, SearchRequest


class AsyncScope:
    """Create a Couchbase Scope instance.

    Exposes the operations which are available to be performed against a scope. Namely the ability to access
    to Collections for performing operations.

    Args:
        bucket (:class:`~acouchbase.bucket.Bucket`): A :class:`~acouchbase.bucket.Bucket` instance.
        scope_name (str): Name of the scope.

    """

    def __init__(self, bucket: AsyncBucket, scope_name: str) -> None:
        self._impl: AsyncScopeInterface = bucket._impl.scope(scope_name)

    @property
    def connection(self) -> object:
        """
        **INTERNAL**

        .. deprecated:: 4.7.0
        """
        Supportability.class_property_deprecated('AsyncScope.connection')
        return self._impl.connection

    @property
    def name(self):
        """
            str: The name of this :class:`~acouchbase.scope.Scope` instance.
        """
        return self._impl.name

    @property
    def bucket_name(self):
        """
            str: The name of the bucket in which this :class:`~acouchbase.scope.Scope` instance belongs.
        """
        return self._impl.bucket_name

    def collection(self, name) -> Collection:
        """Creates a :class:`~acouchbase.collection.Collection` instance of the specified collection.

        Args:
            name (str): Name of the collection to reference.

        Returns:
            :class:`~acouchbase.collection.Collection`: A :class:`~acouchbase.collection.Collection` instance of the
            specified collection.
        """
        return Collection(self, name)

    def query(self, statement: str, *options: QueryOptions | JSONType, **kwargs: Any) -> AsyncQueryResult:
        """Executes a N1QL query against the scope.

        .. note::
            The query is executed lazily in that it is executed once iteration over the
            :class:`~acouchbase.result.AsyncQueryResult` begins.

        .. note::
            Scope-level queries are only supported on Couchbase Server versions that support scopes and collections.

        .. seealso::
            * :class:`~acouchbase.management.queries.QueryIndexManager`: For how to manage query indexes.
            * :meth:`~acouchbase.cluster.AsyncCluster.query`: For how to execute cluster-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~couchbase.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.QueryOptions`

        Returns:
            :class:`~acouchbase.result.AsyncQueryResult`: An instance of a
            :class:`~acouchbase.result.AsyncQueryResult` which provides access to iterate over the
            query results and access metadata and metrics about the query.

        Examples:
            Simple query::

                q_res = scope.query('SELECT * FROM `inventory` WHERE country LIKE 'United%' LIMIT 2;')
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from acouchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `inventory` WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from acouchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from acouchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.query(q_str, QueryOptions(metrics=True))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Query metadata: {q_res.metadata()}')
                print(f'Query metrics: {q_res.metadata().metrics()}')

        """
        return self._impl.query(statement, *options, **kwargs)

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AsyncAnalyticsResult:
        """Executes an analaytics query against the scope.

        .. note::
            The analytics query is executed lazily in that it is executed once iteration over the
            :class:`~acouchbase.result.AsyncAnalyticsResult` begins.

        .. seealso::
            * :class:`~acouchbase.management.analytics.AnalyticsIndexManager`: for how to manage analytics dataverses,
              datasets, indexes and links.
            * :meth:`~acouchbase.cluster.AsyncCluster.analytics_query`: for how to execute cluster-level analytics
              queries

        Args:
            statement (str): The analytics SQL++ statement to execute.
            options (:class:`~couchbase.options.AnalyticsOptions`): Optional parameters for the analytics query
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AnalyticsOptions`

        Returns:
            :class:`~acouchbase.result.AsyncAnalyticsResult`: An instance of a
            :class:`~acouchbase.result.AsyncAnalyticsResult` which provides access to iterate over the analytics query
            results and access metadata and metrics about the analytics query.

        Examples:
            .. note::
                Be sure to setup the necessary dataverse(s), dataset(s) for your analytics queries.
                See :analytics_intro:`Analytics Introduction <>` in Couchbase Server docs.

            Simple analytics query::

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.analytics_query(q_str)
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple analytics query with positional parameters::

                from acouchbase.options import AnalyticsOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.analytics_query(q_str, AnalyticsOptions(positional_parameters=['United%', 5]))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple analytics query with named parameters::

                from acouchbase.options import AnalyticsOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.analytics_query(q_str,
                                                AnalyticsOptions(named_parameters={'country': 'United%', 'lim':2}))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from analytics query::

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.analytics_query(q_str)
                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Analytics query metadata: {q_res.metadata()}')
                print(f'Analytics query metrics: {q_res.metadata().metrics()}')

        """
        return self._impl.analytics_query(statement, *options, **kwargs)

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> AsyncSearchResult:
        """Executes an search query against the scope.

        .. deprecated:: 4.1.11
            Use :meth:`~acouchbase.scope.AsyncScope.search` instead.

        .. note::
            The search query is executed lazily in that it is executed once iteration over the
            :class:`~acouchbase.result.AsyncSearchResult` begins.

        Args:
            index (str): Name of the search query to use.
            query (:class:`~couchbase.search.SearchQuery`): Type of search query to perform.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            :class:`~acouchbase.result.AsyncSearchResult`: An instance of a
            :class:`~acouchbase.result.AsyncSearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If a
                :class:`~couchbase.search.SearchRequest` is provided.  Use
                :meth:`~acouchbase.scope.AsyncScope.search` for a search request.

        Examples:

            .. note::
                Be sure to create a search index prior to executing search queries.  Also, if an application
                desires to utilize search row locations, highlighting, etc. make sure the search index is
                setup appropriately.  See :search_create_idx:`Creating Indexes <>` in Couchbase Server docs.

            Simple search query::

                import couchbase.search as search
                from acouchbase.options import SearchOptions

                # ... other code ...

                query = search.TermQuery('home')
                q_res = scope.search_query('travel-sample-index',
                                            query,
                                            SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')


            Simple search query with facets::

                import couchbase.search as search
                from acouchbase.options import SearchOptions

                # ... other code ...

                facet_name = 'activity'
                facet = search.TermFacet('activity')
                query = search.TermQuery('home')
                q_res = scope.search_query('travel-sample-index',
                                            query,
                                            SearchOptions(limit=10, facets={facet_name: facet}))

                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'facets: {q_res.facets()}')


            Simple search query with fields and locations::

                import couchbase.search as search
                from acouchbase.options import SearchOptions

                # ... other code ...

                search_fields = ['name', 'activity']
                query = search.TermQuery('home')
                q_res = scope.search_query('travel-sample-index',
                                            query,
                                            SearchOptions(limit=10,
                                                        include_locations=True,
                                                        fields=search_fields))

                async for row in q_res.rows():
                    print(f'Found row: {row}')
                    print(f'Fields: {row.fields}')
                    print(f'Locations: {row.locations}')

        """
        return self._impl.search_query(index, query, options, **kwargs)

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> AsyncSearchResult:
        """Executes an search against the scope.

        .. note::
            The search is executed lazily in that it is executed once iteration over the
            :class:`~acouchbase.result.AsyncSearchResult` begins.

        .. seealso::
            * :class:`~couchbase.management.search.ScopeSearchIndexManager`: for how to manage search indexes.
            * :meth:`acouchbase.cluster.AsyncCluster.search`: for how to execute cluster-level search

        Args:
            index (str): Name of the search index to use.
            request (:class:`~couchbase.search.SearchRequest` | :class:`~couchbase.search.SearchQuery`): Type of
                search request to perform.

                .. deprecated:: 4.7.0
                    Passing a :class:`~couchbase.search.SearchQuery` is deprecated; wrap it with
                    :meth:`~couchbase.search.SearchRequest.create` instead.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            :class:`~acouchbase.result.AsyncSearchResult`: An instance of a
            :class:`~acouchbase.result.AsyncSearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.

        Examples:

            .. note::
                Be sure to create a search index prior to executing a search.  Also, if an application
                desires to utilize search row locations, highlighting, etc. make sure the search index is
                setup appropriately.  See :search_create_idx:`Creating Indexes <>` in Couchbase Server docs.

            Simple search::

                import couchbase.search as search
                from acouchbase.options import SearchOptions

                # ... other code ...

                request = search.SearchRequest.create(search.TermQuery('home'))
                q_res = scope.search('travel-sample-index', request, SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple vector search::

                import couchbase.search as search
                from acouchbase.options import SearchOptions
                from acouchbase.vector_search import VectorQuery, VectorSearch

                # ... other code ...

                # NOTE:  the vector is expected to be type list[float], set the vector to the appropriate value,
                # this is an example.
                vector = [-0.014653487130999565, -0.008658270351588726, 0.017129190266132355, -0.015563474968075752]
                vector_search = VectorSearch.from_vector_query(VectorQuery('vector_field', vector))
                request = search.SearchRequest.create(vector_search)
                q_res = scope.search('travel-sample-vector-index', request, SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Combine search and vector search::

                import couchbase.search as search
                from acouchbase.options import SearchOptions
                from acouchbase.vector_search import VectorQuery, VectorSearch

                # ... other code ...

                # NOTE:  the vector is expected to be type list[float], set the vector to the appropriate value,
                # this is an example.
                vector_search = VectorSearch.from_vector_query(VectorQuery('vector_field', [-0.014653487130999565,
                                                                                            -0.008658270351588726,
                                                                                            0.017129190266132355,
                                                                                            -0.015563474968075752]))
                request = search.SearchRequest.create(search.MatchAllQuery()).with_vector_search(vector_search)
                q_res = scope.search('travel-sample-vector-index', request, SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')
        """
        return self._impl.search(index, request, options, **kwargs)

    def search_indexes(self) -> ScopeSearchIndexManager:
        """
        Get a :class:`~acouchbase.management.search.ScopeSearchIndexManager` which can be used to manage the search
        indexes of this scope.

        Returns:
            :class:`~acouchbase.management.search.ScopeSearchIndexManager`: A
            :class:`~acouchbase.management.search.ScopeSearchIndexManager` instance.

        """
        return ScopeSearchIndexManager(self._impl.search_index_mgmt())

    def eventing_functions(self) -> ScopeEventingFunctionManager:
        """
        Get a :class:`~acouchbase.management.eventing.ScopeEventingFunctionManager` which can be used to manage the
        eventing functions of this scope.

        Returns:
            :class:`~acouchbase.management.eventing.ScopeEventingFunctionManager`: A
            :class:`~acouchbase.management.eventing.ScopeEventingFunctionManager` instance.

        """
        return ScopeEventingFunctionManager(self._impl.eventing_function_mgmt())

    @staticmethod
    def default_name() -> str:
        return "_default"


Scope = AsyncScope
