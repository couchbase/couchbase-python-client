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

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._supportability import Supportability
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.options import (AnalyticsOptions,
                                      QueryOptions,
                                      SearchOptions)
from couchbase.common.search_queries import SearchQuery
from couchbase.common.search_request import SearchRequest
from couchbase.protocol.classic._analytics import AnalyticsQuery
from couchbase.protocol.classic._n1ql import N1QLQuery
from couchbase.protocol.classic._scope_types import (AnalyticsQueryRequest,
                                                     QueryRequest,
                                                     ScopeOptionsBlock,
                                                     SearchQueryRequest)
from couchbase.protocol.classic._search import SearchQueryBuilder
from couchbase.protocol.classic._wire import check_streaming_args


class ScopeRequestBuilder:

    def __init__(self,
                 bucket_name: str,
                 scope_name: str,
                 option_validation: OptionValidationMode | None = None) -> None:
        self._bucket_name = bucket_name
        self._scope_name = scope_name
        self._option_validation = option_validation

    def build_analytics_query_request(self,
                                      statement: str,
                                      obs_handler: ObservableRequestHandler,
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

        # Accepted and discarded; the statement's builder would send it as a named parameter.
        kwargs.pop('num_workers', None)
        check_streaming_args(options, kwargs, block=ScopeOptionsBlock.Analytics, options_type=AnalyticsOptions,
                             mode=self._option_validation)
        return AnalyticsQueryRequest(AnalyticsQuery.create_query_object(statement, *options, **kwargs), obs_handler)

    def build_query_request(self,
                            statement: str,
                            obs_handler: ObservableRequestHandler,
                            *options: object,
                            **kwargs: object) -> QueryRequest:
        opt = QueryOptions()
        opts = list(options)
        for o in opts:
            if isinstance(o, QueryOptions):
                opt = o
                opts.remove(o)

        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = '`{}`.`{}`'.format(self._bucket_name, self._scope_name)

        # Accepted and discarded; the statement's builder would send it as a named parameter.
        kwargs.pop('num_workers', None)
        check_streaming_args(options, kwargs, block=ScopeOptionsBlock.Query, options_type=QueryOptions,
                             mode=self._option_validation)
        return QueryRequest(N1QLQuery.create_query_object(statement, *options, **kwargs), obs_handler)

    def build_search_request(self,
                             index: str,
                             request: SearchRequest | SearchQuery,
                             obs_handler: ObservableRequestHandler,
                             options: SearchOptions | None = None,
                             **kwargs: object) -> SearchQueryRequest:
        check_streaming_args((options,), kwargs, block=ScopeOptionsBlock.Search, options_type=SearchOptions,
                             mode=self._option_validation)
        if isinstance(request, SearchQuery):
            Supportability.method_param_type_deprecated('search', 'request', 'SearchQuery', 'SearchRequest')
            query_builder = SearchQueryBuilder.create_search_query_object(index, request, options, **kwargs)
        else:
            query_builder = SearchQueryBuilder.create_search_query_from_request(index, request, options, **kwargs)

        scope_name = query_builder.params.get('scope_name', None) or self._scope_name
        return SearchQueryRequest(query_builder, obs_handler, self._bucket_name, scope_name)

    def build_search_query_request(self,
                                   index: str,
                                   query: SearchQuery,
                                   obs_handler: ObservableRequestHandler,
                                   options: SearchOptions | None = None,
                                   **kwargs: object) -> SearchQueryRequest:
        check_streaming_args((options,), kwargs, block=ScopeOptionsBlock.Search, options_type=SearchOptions,
                             mode=self._option_validation)
        if isinstance(query, SearchRequest):
            raise InvalidArgumentException(
                message=('A SearchRequest is not a valid search_query() query. '
                         'Pass it to search() instead.'))

        query_builder = SearchQueryBuilder.create_search_query_object(index, query, options, **kwargs)

        scope_name = query_builder.params.get('scope_name', None) or self._scope_name
        return SearchQueryRequest(query_builder, obs_handler, self._bucket_name, scope_name)
