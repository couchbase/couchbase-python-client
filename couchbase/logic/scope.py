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
                    Optional)

from couchbase.analytics import AnalyticsQuery, AnalyticsRequest
from couchbase.collection import Collection
from couchbase.n1ql import N1QLQuery, N1QLRequest
from couchbase.options import (AnalyticsOptions,
                               QueryOptions,
                               SearchOptions)
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from couchbase.search import SearchQueryBuilder, SearchRequest
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.search import SearchQuery


class ScopeLogic:
    def __init__(self, bucket, scope_name):
        self._bucket = bucket
        self._scope_name = scope_name

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._bucket.connection

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._bucket.default_transcoder

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~.Scope` instance.
        """
        return self._scope_name

    @property
    def bucket_name(self) -> str:
        """
            str: The name of the bucket in which this :class:`~.Scope` instance belongs.
        """
        return self._bucket.name

    def collection(self, name  # type: str
                   ) -> Collection:
        """Creates a :class:`~.collection.Collection` instance of the specified collection.

        Args:
            name (str): Name of the collection to reference.

        Returns:
            :class:`~.collection.Collection`: A :class:`~.collection.Collection` instance of the specified collection.
        """
        return Collection(self, name)

    def query(
        self,
        statement,  # type: str
        *options,  # type: QueryOptions
        **kwargs  # type: Any
    ) -> QueryResult:
        """Executes a N1QL query against the scope.

        .. note::
            The query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.QueryResult` begins.

        .. note::
            Scope-level queries are only supported on Couchbase Server versions that support scopes and collections.

        .. seealso::
            * :class:`~couchbase.management.queries.QueryIndexManager`: For how to manage query indexes.
            * :meth:`~couchbase.cluster.Cluster.query`: For how to execute cluster-level queries.

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
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `inventory` WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.query(q_str, QueryOptions(metrics=True))
                for row in q_res.rows():
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

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = '`{}`.`{}`'.format(self.bucket_name, self.name)

        query = N1QLQuery.create_query_object(
            statement, opt, **kwargs)
        return QueryResult(N1QLRequest.generate_n1ql_request(self.connection,
                                                             query.params))

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs
    ) -> AnalyticsResult:
        """Executes an analaytics query against the scope.

        .. note::
            The analytics query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.AnalyticsResult` begins.

        .. seealso::
            * :class:`~couchbase.management.analytics.AnalyticsIndexManager`: for how to manage analytics dataverses, datasets, indexes and links.
            * :meth:`~couchbase.cluster.Cluster.analytics_query`: for how to execute cluster-level analytics queries

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
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple analytics query with positional parameters::

                from couchbase.options import AnalyticsOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = scope.analytics_query(q_str, AnalyticsOptions(positional_parameters=['United%', 5]))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple analytics query with named parameters::

                from couchbase.options import AnalyticsOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.analytics_query(q_str,
                                                AnalyticsOptions(named_parameters={'country': 'United%', 'lim':2}))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from analytics query::

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = scope.analytics_query(q_str)
                for row in q_res.rows():
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

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = 'default:`{}`.`{}`'.format(self.bucket_name, self.name)

        query = AnalyticsQuery.create_query_object(
            statement, *options, **kwargs)
        return AnalyticsResult(AnalyticsRequest.generate_analytics_request(self.connection,
                                                                           query.params))

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs
    ) -> SearchResult:
        """Executes an search query against the scope.

        .. note::
            The search query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.SearchResult` begins.

        .. seealso::
            * :class:`~couchbase.management.search.SearchIndexManager`: for how to manage search indexes.
            * :meth:`~couchbase.cluster.Cluster.search_query`: for how to execute cluster-level search queries

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

                for row in q_res.rows():
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

                for row in q_res.rows():
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

                for row in q_res.rows():
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

        # set the scope_name as this scope if not provided
        if not ('scope_name' in opt or 'scope_name' in kwargs):
            kwargs['scope_name'] = f'{self.name}'

        query = SearchQueryBuilder.create_search_query_object(
            index, query, *options, **kwargs
        )
        return SearchResult(SearchRequest.generate_search_request(self.connection,
                                                                  query.as_encodable()))

    @staticmethod
    def default_name():
        return "_default"
