#  Copyright 2016-2023. Couchbase, Inc.
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

from typing import Union

from couchbase.analytics import AnalyticsQuery
from couchbase.logic.scope_types import (AnalyticsQueryRequest,
                                         QueryRequest,
                                         SearchQueryRequest)
from couchbase.n1ql import N1QLQuery
from couchbase.options import (AnalyticsOptions,
                               QueryOptions,
                               SearchOptions)
from couchbase.search import (SearchQuery,
                              SearchQueryBuilder,
                              SearchRequest)


class ScopeRequestBuilder:

    def __init__(self, bucket_name: str, scope_name: str) -> None:
        self._bucket_name = bucket_name
        self._scope_name = scope_name

    def build_analytics_query_request(self,
                                      statement: str,
                                      *options: object,
                                      **kwargs: object) -> AnalyticsQueryRequest:
        opt = AnalyticsOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, AnalyticsOptions):
                opt = o
                opts.remove(o)

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = 'default:`{}`.`{}`'.format(self._bucket_name, self._scope_name)

        num_workers = kwargs.pop('num_workers', None)
        req = AnalyticsQueryRequest(AnalyticsQuery.create_query_object(statement, *options, **kwargs))
        if num_workers:
            req.num_workers = num_workers
        return req

    def build_query_request(self, statement: str, *options: object, **kwargs: object) -> QueryRequest:
        opt = QueryOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, QueryOptions):
                opt = o
                opts.remove(o)

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = '`{}`.`{}`'.format(self._bucket_name, self._scope_name)

        num_workers = kwargs.pop('num_workers', None)
        req = QueryRequest(N1QLQuery.create_query_object(statement, *options, **kwargs))
        if num_workers:
            req.num_workers = num_workers
        return req

    def build_search_request(self,
                             index: str,
                             query: Union[SearchQuery, SearchRequest],
                             *options: object,
                             **kwargs: object) -> SearchQueryRequest:
        num_workers = kwargs.pop('num_workers', None)
        if isinstance(query, SearchQuery):
            opt = SearchOptions()
            opts = list(options)
            for o in opts:
                if isinstance(o, SearchOptions):
                    opt = o
                    opts.remove(o)

            # set the scope_name as this scope if not provided
            if not ('scope_name' in opt or 'scope_name' in kwargs):
                kwargs['scope_name'] = f'{self._scope_name}'
            req = SearchQueryRequest(SearchQueryBuilder.create_search_query_object(index, query, *options, **kwargs))
        else:
            req = SearchQueryRequest(SearchQueryBuilder.create_search_query_from_request(
                index, query, *options, **kwargs))
        if num_workers:
            req.num_workers = num_workers
        return req
