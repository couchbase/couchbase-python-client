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
    'Scope',
    'TxScope',
]

from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred

from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.collection import Collection
from txcouchbase.management.search import ScopeSearchIndexManager

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._scope import TxScopeInterface
    from couchbase.common._utils import JSONType
    from couchbase.common.options import SearchOptionsKwargs
    from couchbase.options import (AnalyticsOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery, SearchRequest
    from txcouchbase.bucket import TxBucket


class Scope:
    """A scope in a bucket: its collections, and the queries and searches run against it.

    Get one from :meth:`~txcouchbase.bucket.Bucket.scope`.

    Args:
        bucket (:class:`~txcouchbase.bucket.Bucket`): The bucket the scope belongs to.
        scope_name (str): The name of the scope.
    """

    def __init__(self, bucket: TxBucket, scope_name: str):
        self._impl: TxScopeInterface = bucket._impl.scope(scope_name)

    @property
    def name(self) -> str:
        """The name of this scope."""
        return self._impl.name

    @property
    def bucket_name(self) -> str:
        """The name of the bucket this scope belongs to."""
        return self._impl.bucket_name

    def collection(self, name: str) -> Collection:
        """Creates a :class:`~txcouchbase.collection.Collection` instance of the specified collection.

        Args:
            name (str): Name of the collection to reference.

        Returns:
            :class:`~txcouchbase.collection.Collection`: A :class:`~txcouchbase.collection.Collection` instance of the
            specified collection.
        """
        return Collection(self, name)

    def query(self, statement: str, *options: QueryOptions | JSONType, **kwargs: Any) -> Deferred[QueryResult]:
        """Executes a N1QL query against the scope.

        .. note::
            The query is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.QueryResult` does not block the reactor.

        .. note::
            Scope-level queries are only supported on Couchbase Server versions that support scopes and collections.

        .. seealso::
            * :class:`~txcouchbase.management.queries.QueryIndexManager`: For how to manage query indexes.
            * :meth:`~txcouchbase.cluster.Cluster.query`: For how to execute cluster-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~couchbase.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.QueryOptions`.  A keyword that is not one of its
                options is sent as a named parameter; prefer ``QueryOptions(named_parameters=...)``, which
                cannot collide with an option name.

        Returns:
            Deferred[:class:`~couchbase.result.QueryResult`]: An instance of a :class:`~couchbase.result.QueryResult`
            which provides access to iterate over the query results and access metadata and metrics about the query.
        """
        return self._impl.query_deferred(statement, *options, **kwargs)

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> Deferred[AnalyticsResult]:
        """Executes an analaytics query against the scope.

        .. note::
            The analytics query is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.AnalyticsResult` does not block the reactor.

        .. seealso::
            * :class:`~txcouchbase.management.analytics.AnalyticsIndexManager`: for how to manage analytics dataverses,
              datasets, indexes and links.
            * :meth:`~txcouchbase.cluster.Cluster.analytics_query`: for how to execute cluster-level analytics queries

        Args:
            statement (str): The analytics SQL++ statement to execute.
            options (:class:`~couchbase.options.AnalyticsOptions`): Optional parameters for the analytics query
                operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AnalyticsOptions`.  A keyword that is not one of its
                options is sent as a named parameter; prefer ``AnalyticsOptions(named_parameters=...)``, which
                cannot collide with an option name.

        Returns:
            Deferred[:class:`~couchbase.result.AnalyticsResult`]: An instance of a
            :class:`~couchbase.result.AnalyticsResult` which provides access to iterate over the analytics query results
            and access metadata and metrics about the analytics query.
        """
        return self._impl.analytics_query_deferred(statement, *options, **kwargs)

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Unpack[SearchOptionsKwargs]) -> Deferred[SearchResult]:
        """Executes an search query against the scope.

        .. deprecated:: 4.1.11
            Use :meth:`~txcouchbase.scope.Scope.search` instead.

        .. note::
            The search query is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.SearchResult` does not block the reactor.

        Args:
            index (str): Name of the search query to use.
            query (:class:`~couchbase.search.SearchQuery`): Type of search query to perform.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query
                operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            Deferred[:class:`~couchbase.result.SearchResult`]: An instance of a
            :class:`~couchbase.result.SearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If a
                :class:`~couchbase.search.SearchRequest` is provided.  Use
                :meth:`~txcouchbase.scope.Scope.search` for a search request.
        """
        return self._impl.search_query_deferred(index, query, options, **kwargs)

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Unpack[SearchOptionsKwargs]) -> Deferred[SearchResult]:
        """Executes an search against the scope.

        .. note::
            The search is sent when this is called, and the Deferred fires once the core has buffered
            the response, so iterating the :class:`~couchbase.result.SearchResult` does not block the reactor.

        .. seealso::
            * :class:`~txcouchbase.management.search.ScopeSearchIndexManager`: for how to manage search indexes.
            * :meth:`~txcouchbase.cluster.Cluster.search`: for how to execute cluster-level search

        Args:
            index (str): Name of the search index to use.
            request (:class:`~couchbase.search.SearchRequest` | :class:`~couchbase.search.SearchQuery`): Type of
                search request to perform.

                .. deprecated:: 4.7.0
                    Passing a :class:`~couchbase.search.SearchQuery` is deprecated; wrap it with
                    :meth:`~couchbase.search.SearchRequest.create` instead.
            options (:class:`~couchbase.options.SearchOptions`): Optional parameters for the search query operation.
            **kwargs: keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.SearchOptions`

        Returns:
            Deferred[:class:`~couchbase.result.SearchResult`]: An instance of a
            :class:`~couchbase.result.SearchResult` which provides access to iterate over the search
            query results and access metadata and metrics about the search query.
        """
        return self._impl.search_deferred(index, request, options, **kwargs)

    def search_indexes(self) -> ScopeSearchIndexManager:
        """Get a manager for the search indexes of this scope.

        Returns:
            :class:`~txcouchbase.management.search.ScopeSearchIndexManager`: A
            :class:`~txcouchbase.management.search.ScopeSearchIndexManager` instance.

        """
        return ScopeSearchIndexManager(self._impl.search_index_mgmt())

    @staticmethod
    def default_name() -> str:
        """The name of a bucket's default scope."""
        return "_default"


TxScope = Scope
