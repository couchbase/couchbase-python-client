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

from typing import (TYPE_CHECKING,
                    Any,
                    Union)

from twisted.internet.defer import Deferred

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import StreamingOperationType
from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.collection import Collection
from txcouchbase.management.search import ScopeSearchIndexManager

if TYPE_CHECKING:
    from couchbase.common._interfaces._scope import TxScopeInterface
    from couchbase.common._utils import JSONType
    from couchbase.options import (AnalyticsOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery, SearchRequest
    from txcouchbase.bucket import TxBucket


class Scope:

    def __init__(self, bucket: TxBucket, scope_name: str):
        self._impl: TxScopeInterface = bucket._impl.scope(scope_name)

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~.Scope` instance.
        """
        return self._impl.name

    @property
    def bucket_name(self) -> str:
        """
            str: The name of the bucket in which this :class:`~.Scope` instance belongs.
        """
        return self._impl.bucket_name

    def collection(self, name: str) -> Collection:
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
        *options,  # type: Union[QueryOptions, JSONType]
        **kwargs  # type: Any
    ) -> Deferred[QueryResult]:
        op_type = StreamingOperationType.Query
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_query_request(statement, obs_handler, *options, **kwargs)
        return self._impl.query_deferred(req)

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: Union[AnalyticsOptions, JSONType]
        **kwargs  # type: Any
    ) -> Deferred[AnalyticsResult]:
        op_type = StreamingOperationType.AnalyticsQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_analytics_query_request(statement, obs_handler, *options, **kwargs)
        return self._impl.analytics_query_deferred(req)

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs  # type: Any
    ) -> Deferred[SearchResult]:
        op_type = StreamingOperationType.SearchQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_search_query_request(index, query, obs_handler, *options, **kwargs)
        return self._impl.search_deferred(req)

    def search(self,
               index,  # type: str
               request,  # type: Union[SearchRequest, SearchQuery]
               *options,  # type: SearchOptions
               **kwargs,  # type: Any
               ) -> Deferred[SearchResult]:
        op_type = StreamingOperationType.SearchQuery
        obs_handler = ObservableRequestHandler(op_type, self._impl.observability_instruments)
        req = self._impl.request_builder.build_search_request(index, request, obs_handler, *options, **kwargs)
        return self._impl.search_deferred(req)

    def search_indexes(self) -> ScopeSearchIndexManager:
        """
        Get a :class:`~txcouchbase.management.search.ScopeSearchIndexManager` which can be used to manage the search
        indexes of this scope.

        Returns:
            :class:`~txcouchbase.management.search.ScopeSearchIndexManager`: A :class:`~txcouchbase.management.search.ScopeSearchIndexManager` instance.

        """  # noqa: E501
        return ScopeSearchIndexManager(self._impl._client_adapter,
                                       self.bucket_name,
                                       self.name,
                                       self._impl.observability_instruments)

    @staticmethod
    def default_name() -> str:
        return "_default"


TxScope = Scope
