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

from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Dict,
                    Optional)

from acouchbase.analytics import AnalyticsQuery, AsyncAnalyticsRequest
from acouchbase.collection import Collection
from acouchbase.management.eventing import ScopeEventingFunctionManager
from acouchbase.management.search import ScopeSearchIndexManager
from acouchbase.n1ql import AsyncN1QLRequest, N1QLQuery
from acouchbase.search import AsyncFullTextSearchRequest, SearchQueryBuilder
from couchbase.options import (AnalyticsOptions,
                               QueryOptions,
                               SearchOptions)
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.search import SearchQuery, SearchRequest


class AsyncScope:
    """Create a Couchbase Scope instance.

    Exposes the operations which are available to be performed against a scope. Namely the ability to access
    to Collections for performing operations.

    Args:
        bucket (:class:`~acouchbase.bucket.Bucket`): A :class:`~acouchbase.bucket.Bucket` instance.
        scope_name (str): Name of the scope.

    """

    def __init__(self, bucket, scope_name):
        self._bucket = bucket
        self._set_connection()
        self._scope_name = scope_name

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._connection

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._bucket.loop

    @property
    def default_serializer(self) -> Optional[Serializer]:
        return self._bucket.default_serializer

    @property
    def streaming_timeouts(self):
        """
        **INTERNAL**
        """
        return self._bucket.streaming_timeouts

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._bucket.default_transcoder

    @property
    def name(self):
        """
            str: The name of this :class:`~acouchbase.scope.Scope` instance.
        """
        return self._scope_name

    @property
    def bucket_name(self):
        """
            str: The name of the bucket in which this :class:`~acouchbase.scope.Scope` instance belongs.
        """
        return self._bucket.name

    def collection(self, name  # type: str
                   ) -> Collection:
        """Creates a :class:`~acouchbase.collection.Collection` instance of the specified collection.

        Args:
            name (str): Name of the collection to reference.

        Returns:
            :class:`~acouchbase.collection.Collection`: A :class:`~acouchbase.collection.Collection` instance of the specified collection.
        """  # noqa: E501
        return Collection(self, name)

    def _connect_bucket(self) -> Awaitable:
        """
        **INTERNAL**
        """
        return self._bucket.on_connect()

    def _set_connection(self):
        """
        **INTERNAL**
        """
        self._connection = self._bucket.connection

    def query(self,
              statement,  # type: str
              *options,  # type: QueryOptions
              **kwargs   # type: Dict[str, Any]
              ) -> QueryResult:
        """Executes a N1QL query against the scope.

        .. note::
            The query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.QueryResult` begins.

        .. note::
            Scope-level queries are only supported on Couchbase Server versions that support scopes and collections.

        .. seealso::
            * :class:`~acouchbase.management.queries.QueryIndexManager`: For how to manage query indexes.
            * :meth:`~acouchbase.cluster.AsyncCluster.query`: For how to execute cluster-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~couchbase.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.QueryOptions`

        Returns:
            :class:`~couchbase.result.QueryResult`: An instance of a :class:`~couchbase.result.QueryResult` which
            provides access to iterate over the query results and access metadata and metrics about the query.

        Examples:
            Simple query::

                q_res = scope.query('SELECT * FROM `inventory` WHERE country LIKE 'United%' LIMIT 2;')
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `inventory` WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.query(q_str, QueryOptions(metrics=True))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Query metadata: {q_res.metadata()}')
                print(f'Query metrics: {q_res.metadata().metrics()}')

        """
        opt = QueryOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, QueryOptions):
                opt = o
                opts.remove(o)

        request_args = dict()
        num_workers = kwargs.pop('num_workers', None)
        if num_workers:
            request_args['num_workers'] = num_workers

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = '`{}`.`{}`'.format(self.bucket_name, self.name)

        query = N1QLQuery.create_query_object(
            statement, opt, **kwargs)
        return QueryResult(AsyncN1QLRequest.generate_n1ql_request(self.connection,
                                                                  self.loop,
                                                                  query.params,
                                                                  **request_args))

    def analytics_query(self,
                        statement,  # type: str
                        *options,  # type: AnalyticsOptions
                        **kwargs   # type: Dict[str, Any]
                        ) -> AnalyticsResult:
        """Executes an analaytics query against the scope.

        .. note::
            The analytics query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.AnalyticsResult` begins.

        .. seealso::
            * :class:`~acouchbase.management.analytics.AnalyticsIndexManager`: for how to manage analytics dataverses, datasets, indexes and links.
            * :meth:`~acouchbase.cluster.AsyncCluster.analytics_query`: for how to execute cluster-level analytics queries

        Args:
            statement (str): The analytics SQL++ statement to execute.
            options (:class:`~couchbase.options.AnalyticsOptions`): Optional parameters for the analytics query
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AnalyticsOptions`

        Returns:
            :class:`~couchbase.result.AnalyticsResult`: An instance of a :class:`~couchbase.result.AnalyticsResult` which provides access to iterate over the analytics
            query results and access metadata and metrics about the analytics query.

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

                from couchbase.options import AnalyticsOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.analytics_query(q_str, AnalyticsOptions(positional_parameters=['United%', 5]))
                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple analytics query with named parameters::

                from couchbase.options import AnalyticsOptions

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

        """  # noqa: E501
        opt = AnalyticsOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, AnalyticsOptions):
                opt = o
                opts.remove(o)

        request_args = dict()
        num_workers = kwargs.pop('num_workers', None)
        if num_workers:
            request_args['num_workers'] = num_workers

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = 'default:`{}`.`{}`'.format(self.bucket_name, self.name)

        query = AnalyticsQuery.create_query_object(
            statement, *options, **kwargs)
        return AnalyticsResult(AsyncAnalyticsRequest.generate_analytics_request(self.connection,
                                                                                self.loop,
                                                                                query.params,
                                                                                **request_args))

    def search_query(self,
                     index,  # type: str
                     query,  # type: SearchQuery
                     *options,  # type: SearchOptions
                     **kwargs   # type: Dict[str, Any]
                     ) -> SearchResult:
        """Executes an search query against the scope.

        .. warning::
            This method is **DEPRECATED** and will be removed in a future release.
            Use :meth:`~acouchbase.scope.AsyncScope.search`: instead.

        .. note::
            The search query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.SearchResult` begins.

        Args:
            index (str): Name of the search query to use.
            query (:class:`~couchbase.search.SearchQuery`): Type of search query to perform.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            :class:`~couchbase.result.SearchResult`: An instance of a
            :class:`~couchbase.result.SearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.

        Examples:

            .. note::
                Be sure to create a search index prior to executing search queries.  Also, if an application
                desires to utilize search row locations, highlighting, etc. make sure the search index is
                setup appropriately.  See :search_create_idx:`Creating Indexes <>` in Couchbase Server docs.

            Simple search query::

                import couchbase.search as search
                from couchbase.options import SearchOptions

                # ... other code ...

                query = search.TermQuery('home')
                q_res = scope.search_query('travel-sample-index',
                                            query,
                                            SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')


            Simple search query with facets::

                import couchbase.search as search
                from couchbase.options import SearchOptions

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
                from couchbase.options import SearchOptions

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
        opt = SearchOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, SearchOptions):
                opt = o
                opts.remove(o)

        request_args = dict()
        num_workers = kwargs.pop('num_workers', None)
        if num_workers:
            request_args['num_workers'] = num_workers

        # set the scope_name as this scope if not provided
        if not ('scope_name' in opt or 'scope_name' in kwargs):
            kwargs['scope_name'] = f'{self.name}'

        query = SearchQueryBuilder.create_search_query_object(index, query, *options, **kwargs)
        return SearchResult(AsyncFullTextSearchRequest.generate_search_request(self.connection,
                                                                               self.loop,
                                                                               query.as_encodable(),
                                                                               **request_args))

    def search(self,
               index,  # type: str
               request,  # type: SearchRequest
               *options,  # type: SearchOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> SearchResult:
        """Executes an search against the scope.

        .. note::
            The search is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.SearchResult` begins.

        .. seealso::
            * :class:`~couchbase.management.search.ScopeSearchIndexManager`: for how to manage search indexes.
            * :meth:`acouchbase.cluster.AsyncCluster.search`: for how to execute cluster-level search

        Args:
            index (str): Name of the search index to use.
            request (:class:`~couchbase.search.SearchRequest`): Type of search request to perform.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            :class:`~couchbase.result.SearchResult`: An instance of a
            :class:`~couchbase.result.SearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.

        Examples:

            .. note::
                Be sure to create a search index prior to executing a search.  Also, if an application
                desires to utilize search row locations, highlighting, etc. make sure the search index is
                setup appropriately.  See :search_create_idx:`Creating Indexes <>` in Couchbase Server docs.

            Simple search::

                import couchbase.search as search
                from couchbase.options import SearchOptions

                # ... other code ...

                request = search.SearchRequest.create(search.TermQuery('home'))
                q_res = scope.search('travel-sample-index', request, SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple vector search::

                import couchbase.search as search
                from couchbase.options import SearchOptions
                from couchbase.vector_search import VectorQuery, VectorSearch

                # ... other code ...

                # NOTE:  the vector is expected to be type List[float], set the vector to the appropriate value, this is an example.
                vector = [-0.014653487130999565, -0.008658270351588726, 0.017129190266132355, -0.015563474968075752]
                request = search.SearchRequest.create(VectorSearch.from_vector_query(VectorQuery('vector_field', vector)))
                q_res = scope.search('travel-sample-vector-index', request, SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')

            Combine search and vector search::

                import couchbase.search as search
                from couchbase.options import SearchOptions
                from couchbase.vector_search import VectorQuery, VectorSearch

                # ... other code ...

                # NOTE:  the vector is expected to be type List[float], set the vector to the appropriate value, this is an example.
                vector_search = VectorSearch.from_vector_query(VectorQuery('vector_field', [-0.014653487130999565,
                                                                                            -0.008658270351588726,
                                                                                            0.017129190266132355,
                                                                                            -0.015563474968075752]))
                request = search.SearchRequest.create(search.MatchAllQuery()).with_vector_search(vector_search)
                q_res = scope.search('travel-sample-vector-index', request, SearchOptions(limit=10))

                async for row in q_res.rows():
                    print(f'Found row: {row}')
        """  # noqa: E501
        # See couchbase.cluster.search() for note on streaming timeout
        streaming_timeout = self.streaming_timeouts.get('search_timeout', None)
        query = SearchQueryBuilder.create_search_query_from_request(index, request, *options, **kwargs)
        req = AsyncFullTextSearchRequest.generate_search_request(self.connection,
                                                                 self.loop,
                                                                 query.as_encodable(),
                                                                 default_serializer=self.default_serializer,
                                                                 streaming_timeout=streaming_timeout,
                                                                 bucket_name=self.bucket_name,
                                                                 scope_name=self.name)
        return SearchResult(req)

    def search_indexes(self) -> ScopeSearchIndexManager:
        """
        Get a :class:`~acouchbase.management.search.ScopeSearchIndexManager` which can be used to manage the search
        indexes of this scope.

        Returns:
            :class:`~acouchbase.management.search.ScopeSearchIndexManager`: A :class:`~acouchbase.management.search.ScopeSearchIndexManager` instance.

        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return ScopeSearchIndexManager(self.connection, self.loop, self.bucket_name, self.name)

    def eventing_functions(self) -> ScopeEventingFunctionManager:
        """
        Get a :class:`~acouchbase.management.search.ScopeEventingFunctionManager` which can be used to manage the eventing
        functions of this scope.

        Returns:
            :class:`~acouchbase.management.search.ScopeEventingFunctionManager`: A :class:`~acouchbase.management.search.ScopeSearchIndexManager` instance.

        """  # noqa: E501
        return ScopeEventingFunctionManager(self.connection, self.loop, self.bucket_name, self.name)

    @staticmethod
    def default_name():
        return "_default"


Scope = AsyncScope
