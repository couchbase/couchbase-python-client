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

# BRIDGE: the async API returns couchbase.result's classes until each area's own result types land.
# Removed by PYCBC-2001.
from couchbase.result import (ClusterInfoResult,
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
                              ValueResult,
                              _AnalyticsResult,
                              _QueryResult)

if TYPE_CHECKING:
    from couchbase.common._streaming import AsyncRowIterator
    from couchbase.common.analytics import AnalyticsMetaData
    from couchbase.common.n1ql import QueryMetaData


class AsyncQueryResult(_QueryResult[asyncio.Task[list[Any]]]):
    _request: AsyncRowIterator[Any, QueryMetaData]

    def rows(self) -> AsyncRowIterator[Any, QueryMetaData]:
        """The rows which have been returned by the query.

        .. note::
            A ``KeyboardInterrupt`` raised while iterating rows is only checked for at
            internal timeout boundaries while waiting on the server, so it may take a moment
            to be delivered rather than firing instantly.

        Returns:
            AsyncIterable: An async iterable over the query's rows.  Use ``async for`` to loop over it.
        """
        return self.__aiter__()

    def __aiter__(self) -> AsyncRowIterator[Any, QueryMetaData]:
        return self._request.__aiter__()


class AsyncAnalyticsResult(_AnalyticsResult):
    _request: AsyncRowIterator[Any, AnalyticsMetaData]

    def rows(self) -> AsyncRowIterator[Any, AnalyticsMetaData]:
        """The rows which have been returned by the analytics query.

        .. note::
            A ``KeyboardInterrupt`` raised while iterating rows is only checked for at
            internal timeout boundaries while waiting on the server, so it may take a moment
            to be delivered rather than firing instantly.

        Returns:
            AsyncIterable: An async iterable over the analytics query's rows.  Use ``async for`` to loop
            over it.
        """
        return self.__aiter__()

    def __aiter__(self) -> AsyncRowIterator[Any, AnalyticsMetaData]:
        return self._request.__aiter__()
