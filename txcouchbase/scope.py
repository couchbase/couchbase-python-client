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
                    Dict,
                    Optional)

from twisted.internet.defer import Deferred

from couchbase.logic.analytics import AnalyticsQuery
from couchbase.logic.n1ql import N1QLQuery
from couchbase.logic.search import SearchQueryBuilder
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from couchbase.transcoder import Transcoder
from txcouchbase.analytics import AnalyticsRequest
from txcouchbase.collection import Collection
from txcouchbase.management.search import ScopeSearchIndexManager
from txcouchbase.n1ql import N1QLRequest
from txcouchbase.search import FullTextSearchRequest

if TYPE_CHECKING:

    from couchbase.options import (AnalyticsOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery, SearchRequest


class Scope:
    def __init__(self, bucket, scope_name):
        self._bucket = bucket
        self._set_connection()
        self._loop = bucket.loop
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
        return self._loop

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._bucket.default_transcoder

    @property
    def name(self):
        return self._scope_name

    @property
    def bucket_name(self):
        return self._bucket.name

    def collection(self, name  # type: str
                   ) -> Collection:

        return Collection(self, name)

    def query(
        self,
        statement,  # type: str
        *options,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[QueryResult]:

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
            statement, *options, **kwargs)
        request = N1QLRequest.generate_n1ql_request(self.connection,
                                                    self.loop,
                                                    query.params,
                                                    default_serializer=self.default_serializer)
        d = Deferred()

        def _on_ok(_):
            d.callback(QueryResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[AnalyticsResult]:

        opt = AnalyticsOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, AnalyticsOptions):
                opt = o
                opts.remove(o)

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = 'default:`{}`.`{}`'.format(self.bucket_name, self.name)

        query = AnalyticsQuery.execute_analytics_query(
            statement, *options, **kwargs)
        request = AnalyticsRequest.generate_analytics_request(self.connection,
                                                              self.loop,
                                                              query.params,
                                                              default_serializer=self.default_serializer)
        d = Deferred()

        def _on_ok(_):
            d.callback(AnalyticsResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_analytics_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[SearchResult]:

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
        request = FullTextSearchRequest.generate_search_request(self.connection,
                                                                self.loop,
                                                                query.as_encodable(),
                                                                default_serializer=self.default_serializer)
        d = Deferred()

        def _on_ok(_):
            d.callback(SearchResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_search_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def search(self,
               index,  # type: str
               request,  # type: SearchRequest
               *options,  # type: SearchOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> Deferred[SearchResult]:
        request_args = dict(default_serialize=self.default_serializer,
                            streaming_timeout=self.streaming_timeouts.get('search_timeout', None),
                            bucket_name=self.bucket_name,
                            scope_name=self.name)
        query = SearchQueryBuilder.create_search_query_from_request(index, request, *options, **kwargs)
        request = FullTextSearchRequest.generate_search_request(self.connection,
                                                                self.loop,
                                                                query.as_encodable(),
                                                                **request_args)

        d = Deferred()

        def _on_ok(_):
            d.callback(SearchResult(request))

        def _on_err(exc):
            d.errback(exc)

        query_d = request.execute_search_query()
        query_d.addCallback(_on_ok)
        query_d.addErrback(_on_err)
        return d

    def search_indexes(self) -> ScopeSearchIndexManager:
        """
        Get a :class:`~txcouchbase.management.search.ScopeSearchIndexManager` which can be used to manage the search
        indexes of this scope.

        Returns:
            :class:`~txcouchbase.management.search.ScopeSearchIndexManager`: A :class:`~txcouchbase.management.search.ScopeSearchIndexManager` instance.

        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return ScopeSearchIndexManager(self.connection, self.loop, self.bucket_name, self.name)

    def _connect_bucket(self):
        """
        **INTERNAL**
        """
        return self._bucket.on_connect()

    def _set_connection(self):
        """
        **INTERNAL**
        """
        self._connection = self._bucket.connection

    @staticmethod
    def default_name():
        return "_default"
