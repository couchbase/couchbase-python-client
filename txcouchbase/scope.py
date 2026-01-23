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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict)

from twisted.internet.defer import Deferred

from couchbase.result import (AnalyticsResult,
                              QueryResult,
                              SearchResult)
from txcouchbase.collection import Collection
from txcouchbase.logic.scope_impl import TxScopeImpl
from txcouchbase.management.search import ScopeSearchIndexManager

if TYPE_CHECKING:
    from couchbase.options import (AnalyticsOptions,
                                   QueryOptions,
                                   SearchOptions)
    from couchbase.search import SearchQuery, SearchRequest
    from txcouchbase.bucket import TxBucket


class Scope:
    def __init__(self, bucket: TxBucket, scope_name: str):
        self._impl = TxScopeImpl(scope_name, bucket)

    @property
    def name(self) -> str:
        return self._impl.name

    @property
    def bucket_name(self) -> str:
        return self._impl.bucket_name

    def collection(self, name: str) -> Collection:
        return Collection(self, name)

    def query(
        self,
        statement,  # type: str
        *options,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[QueryResult]:
        req = self._impl.request_builder.build_query_request(statement, *options, **kwargs)
        return self._impl.query_deferred(req)

    def analytics_query(
        self,
        statement,  # type: str
        *options,  # type: AnalyticsOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[AnalyticsResult]:
        req = self._impl.request_builder.build_analytics_query_request(statement, *options, **kwargs)
        return self._impl.analytics_query_deferred(req)

    def search_query(
        self,
        index,  # type: str
        query,  # type: SearchQuery
        *options,  # type: SearchOptions
        **kwargs  # type: Dict[str, Any]
    ) -> Deferred[SearchResult]:
        req = self._impl.request_builder.build_search_request(index, query, *options, **kwargs)
        return self._impl.search_deferred(req)

    def search(self,
               index,  # type: str
               request,  # type: SearchRequest
               *options,  # type: SearchOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> Deferred[SearchResult]:
        req = self._impl.request_builder.build_search_request(index, request, *options, **kwargs)
        return self._impl.search_deferred(req)

    def search_indexes(self) -> ScopeSearchIndexManager:
        """
        Get a :class:`~txcouchbase.management.search.ScopeSearchIndexManager` which can be used to manage the search
        indexes of this scope.

        Returns:
            :class:`~txcouchbase.management.search.ScopeSearchIndexManager`: A :class:`~txcouchbase.management.search.ScopeSearchIndexManager` instance.

        """  # noqa: E501
        return ScopeSearchIndexManager(self._impl.connection, self._impl.loop, self.bucket_name, self.name)

    @staticmethod
    def default_name() -> str:
        return "_default"


TxScope = Scope
