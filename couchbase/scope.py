from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from couchbase.search import SearchQuery


class Scope:
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
    def transcoder(self):
        """
        **INTERNAL**
        """
        return self._bucket.transcoder

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
        **kwargs  # type: Any
    ) -> QueryResult:

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
                                                             self.loop,
                                                             query.params))

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs
    ) -> AnalyticsResult:

        opt = AnalyticsOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, AnalyticsOptions):
                opt = o
                opts.remove(o)

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = '`{}`.`{}`'.format(self.bucket_name, self.name)

        query = AnalyticsQuery.create_query_object(
            statement, *options, **kwargs)
        return AnalyticsResult(AnalyticsRequest.generate_analytics_request(self.connection,
                                                                           self.loop,
                                                                           query.params))

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs
    ) -> SearchResult:

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
                                                                  self.loop,
                                                                  query.as_encodable()))

    @staticmethod
    def default_name():
        return "_default"
