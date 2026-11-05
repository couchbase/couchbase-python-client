#  Copyright 2016-2026. Couchbase, Inc.
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
    'AsyncAnalyticsResult',
    'AsyncQueryResult',
    'AsyncScanResultIterable',
    'AsyncSearchResult',
    'AsyncViewResult',
    'ClusterInfoResult',
    'ContentProxy',
    'ContentSubdocProxy',
    'CounterResult',
    'DiagnosticsResult',
    'ExistsResult',
    'GetReplicaResult',
    'GetResult',
    'LookupInReplicaResult',
    'LookupInResult',
    'MultiCounterResult',
    'MultiExistsResult',
    'MultiGetReplicaResult',
    'MultiGetResult',
    'MultiMutationResult',
    'MultiResult',
    'MultiResultType',
    'MutateInResult',
    'MutationResult',
    'MutationToken',
    'OperationResult',
    'PingResult',
    'Result',
    'ScanResult',
    'ValueResult',
]

import asyncio
from typing import TYPE_CHECKING, Any

from couchbase.common.result import (ClusterInfoResult,
                                     ContentProxy,
                                     ContentSubdocProxy,
                                     CounterResult,
                                     DiagnosticsResult,
                                     ExistsResult,
                                     GetReplicaResult,
                                     GetResult,
                                     LookupInReplicaResult,
                                     LookupInResult,
                                     MultiCounterResult,
                                     MultiExistsResult,
                                     MultiGetReplicaResult,
                                     MultiGetResult,
                                     MultiMutationResult,
                                     MultiResult,
                                     MultiResultType,
                                     MutateInResult,
                                     MutationResult,
                                     MutationToken,
                                     OperationResult,
                                     PingResult,
                                     Result,
                                     ScanResult,
                                     ValueResult)
from couchbase.result import (_AnalyticsResult,
                              _QueryResult,
                              _ScanResultIterable,
                              _SearchResult,
                              _ViewResult)

if TYPE_CHECKING:
    from couchbase.common._streaming import AsyncRowIterator
    from couchbase.common.analytics import AnalyticsMetaData
    from couchbase.common.n1ql import QueryMetaData
    from couchbase.common.search import SearchRow, _SearchFinalMetadata
    from couchbase.common.views import ViewMetaData, ViewRow


class AsyncScanResultIterable(_ScanResultIterable):
    """The documents a range scan returns, read with ``async for``."""

    _request: AsyncRowIterator[ScanResult, None]

    def rows(self) -> AsyncRowIterator[ScanResult, None]:
        """Get the documents the scan returns.

        Returns:
            AsyncIterable: An async iterable over the documents.  Use ``async for`` to loop over it.
        """
        return self.__aiter__()

    def __aiter__(self) -> AsyncRowIterator[ScanResult, None]:
        return self._request.__aiter__()


class AsyncQueryResult(_QueryResult[asyncio.Task[list[Any]]]):
    """The result of a query: its rows, read with ``async for``, then its metadata."""

    _request: AsyncRowIterator[Any, QueryMetaData]

    def rows(self) -> AsyncRowIterator[Any, QueryMetaData]:
        """The rows which have been returned by the query.

        Returns:
            AsyncIterable: An async iterable over the query's rows.  Use ``async for`` to loop over it.
        """
        return self.__aiter__()

    def __aiter__(self) -> AsyncRowIterator[Any, QueryMetaData]:
        return self._request.__aiter__()


class AsyncAnalyticsResult(_AnalyticsResult):
    """The result of an analytics query: its rows, read with ``async for``, then its metadata."""

    _request: AsyncRowIterator[Any, AnalyticsMetaData]

    def rows(self) -> AsyncRowIterator[Any, AnalyticsMetaData]:
        """The rows which have been returned by the analytics query.

        Returns:
            AsyncIterable: An async iterable over the analytics query's rows.  Use ``async for`` to loop
            over it.
        """
        return self.__aiter__()

    def __aiter__(self) -> AsyncRowIterator[Any, AnalyticsMetaData]:
        return self._request.__aiter__()


class AsyncSearchResult(_SearchResult):
    """The result of a search: its rows, read with ``async for``, then its metadata and facets."""

    _request: AsyncRowIterator[SearchRow, _SearchFinalMetadata]

    def rows(self) -> AsyncRowIterator[SearchRow, _SearchFinalMetadata]:
        """The rows which have been returned by the search query.

        Returns:
            AsyncIterable: An async iterable over the search query's rows.  Use ``async for`` to loop over it.
        """
        return self.__aiter__()

    def __aiter__(self) -> AsyncRowIterator[SearchRow, _SearchFinalMetadata]:
        return self._request.__aiter__()


class AsyncViewResult(_ViewResult):
    """The result of a view query: its rows, read with ``async for``, then its metadata."""

    _request: AsyncRowIterator[ViewRow, ViewMetaData]

    def rows(self) -> AsyncRowIterator[ViewRow, ViewMetaData]:
        """The rows which have been returned by the view query.

        Returns:
            AsyncIterable: An async iterable over the view query's rows.  Use ``async for`` to loop over it.
        """
        return self.__aiter__()

    def __aiter__(self) -> AsyncRowIterator[ViewRow, ViewMetaData]:
        return self._request.__aiter__()
