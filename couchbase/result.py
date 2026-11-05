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
    'AnalyticsResult',
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
    'QueryResult',
    'Result',
    'ScanResult',
    'ScanResultIterable',
    'SearchResult',
    'ValueResult',
    'ViewResult',
]

from typing import (TYPE_CHECKING,
                    Any,
                    Generic,
                    Protocol,
                    TypeVar)

from couchbase.common._supportability import Supportability
from couchbase.common.result import ClusterInfoResult  # noqa: F401
from couchbase.common.result import ContentProxy  # noqa: F401
from couchbase.common.result import ContentSubdocProxy  # noqa: F401
from couchbase.common.result import CounterResult  # noqa: F401
from couchbase.common.result import DiagnosticsResult  # noqa: F401
from couchbase.common.result import ExistsResult  # noqa: F401
from couchbase.common.result import GetReplicaResult  # noqa: F401
from couchbase.common.result import GetResult  # noqa: F401
from couchbase.common.result import LookupInReplicaResult  # noqa: F401
from couchbase.common.result import LookupInResult  # noqa: F401
from couchbase.common.result import MultiCounterResult  # noqa: F401
from couchbase.common.result import MultiExistsResult  # noqa: F401
from couchbase.common.result import MultiGetReplicaResult  # noqa: F401
from couchbase.common.result import MultiGetResult  # noqa: F401
from couchbase.common.result import MultiMutationResult  # noqa: F401
from couchbase.common.result import MultiResult  # noqa: F401
from couchbase.common.result import MultiResultType  # noqa: F401
from couchbase.common.result import MutateInResult  # noqa: F401
from couchbase.common.result import MutationResult  # noqa: F401
from couchbase.common.result import MutationToken  # noqa: F401
from couchbase.common.result import OperationResult  # noqa: F401
from couchbase.common.result import PingResult  # noqa: F401
from couchbase.common.result import Result  # noqa: F401
from couchbase.common.result import ScanResult  # noqa: F401
from couchbase.common.result import ValueResult  # noqa: F401

if TYPE_CHECKING:
    from couchbase.common._streaming import RowIterator
    from couchbase.common.analytics import AnalyticsMetaData
    from couchbase.common.n1ql import QueryMetaData
    from couchbase.common.search import (SearchFacetResult,
                                         SearchMetaData,
                                         SearchRow,
                                         _SearchFinalMetadata)
    from couchbase.common.views import ViewMetaData, ViewRow


_ExecuteT = TypeVar('_ExecuteT', covariant=True)
_FinalT = TypeVar('_FinalT', covariant=True)


class _Rows(Protocol[_ExecuteT, _FinalT]):
    """What a result reads from its rows on either API: the drain, and what follows the rows."""

    def execute(self) -> _ExecuteT: ...

    def final_metadata(self) -> _FinalT | None: ...


class _ScanCanceller(Protocol):
    def cancel_scan(self) -> None: ...


class HttpResult:
    """
    .. deprecated:: 4.7.0
        Nothing in the SDK returns one.
    """

    def __init__(self, orig: object) -> None:
        Supportability.type_deprecated('HttpResult', message='Nothing in the SDK returns one.')
        self._orig = orig


class _ScanResultIterable:
    def __init__(self, rows: object, executor: _ScanCanceller) -> None:
        self._request = rows
        self._executor = executor

    def cancel_scan(self) -> None:
        """Cancel the scan.  Iterating its rows then ends at the next row the scan would return."""
        self._executor.cancel_scan()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class ScanResultIterable(_ScanResultIterable):
    _request: RowIterator[ScanResult, None]

    def rows(self) -> RowIterator[ScanResult, None]:
        """The rows which have been returned by the scan.

        Returns:
            Iterable: An iterable over the scan's rows.
        """
        return self.__iter__()

    def __iter__(self) -> RowIterator[ScanResult, None]:
        return self._request.__iter__()


class _QueryResult(Generic[_ExecuteT]):
    def __init__(self, n1ql_request: _Rows[_ExecuteT, QueryMetaData]) -> None:
        self._request = n1ql_request

    def execute(self) -> _ExecuteT:
        """Convenience method to execute the query.

        Returns:
            list[Any]:  A list of query results.

        Example:
            q_rows = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;').execute()

        """
        return self._request.execute()

    def metadata(self) -> QueryMetaData | None:
        """The meta-data which has been returned by the query.

        Returns:
            :class:`~couchbase.n1ql.QueryMetaData`: An instance of :class:`~couchbase.n1ql.QueryMetaData`.
        """
        return self._request.final_metadata()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class QueryResult(_QueryResult[list[Any]]):
    _request: RowIterator[Any, QueryMetaData]

    def rows(self) -> RowIterator[Any, QueryMetaData]:
        """The rows which have been returned by the query.

        .. note::
            A ``KeyboardInterrupt`` raised while iterating rows is only checked for at
            internal timeout boundaries while waiting on the server, so it may take a moment
            to be delivered rather than firing instantly.

        Returns:
            Iterable: An iterable over the query's rows.
        """
        return self.__iter__()

    def __iter__(self) -> RowIterator[Any, QueryMetaData]:
        return self._request.__iter__()


class _AnalyticsResult:
    def __init__(self, analytics_request: _Rows[object, AnalyticsMetaData]) -> None:
        self._request = analytics_request

    def metadata(self) -> AnalyticsMetaData | None:
        """The meta-data which has been returned by the analytics query.

        Returns:
            :class:`~couchbase.analytics.AnalyticsMetaData`: An instance of
            :class:`~couchbase.analytics.AnalyticsMetaData`.
        """
        return self._request.final_metadata()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class AnalyticsResult(_AnalyticsResult):
    _request: RowIterator[Any, AnalyticsMetaData]

    def rows(self) -> RowIterator[Any, AnalyticsMetaData]:
        """The rows which have been returned by the analytics query.

        .. note::
            A ``KeyboardInterrupt`` raised while iterating rows is only checked for at
            internal timeout boundaries while waiting on the server, so it may take a moment
            to be delivered rather than firing instantly.

        Returns:
            Iterable: An iterable over the analytics query's rows.
        """
        return self.__iter__()

    def __iter__(self) -> RowIterator[Any, AnalyticsMetaData]:
        return self._request.__iter__()


class _SearchResult:
    def __init__(self, search_request: _Rows[object, _SearchFinalMetadata]) -> None:
        self._request = search_request

    def metadata(self) -> SearchMetaData | None:
        """The meta-data which has been returned by the search query.

        Returns:
            :class:`~couchbase.search.SearchMetaData`: An instance of
            :class:`~couchbase.search.SearchMetaData`.
        """
        final_metadata = self._request.final_metadata()
        return None if final_metadata is None else final_metadata.metadata

    def result_rows(self) -> None:
        return None

    def facets(self) -> dict[str, SearchFacetResult] | None:
        final_metadata = self._request.final_metadata()
        return None if final_metadata is None else final_metadata.facets

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class SearchResult(_SearchResult):
    _request: RowIterator[SearchRow, _SearchFinalMetadata]

    def rows(self) -> RowIterator[SearchRow, _SearchFinalMetadata]:
        """The rows which have been returned by the search query.

        .. note::
            A ``KeyboardInterrupt`` raised while iterating rows is only checked for at
            internal timeout boundaries while waiting on the server, so it may take a moment
            to be delivered rather than firing instantly.

        Returns:
            Iterable: An iterable over the search query's rows.
        """
        return self.__iter__()

    def __iter__(self) -> RowIterator[SearchRow, _SearchFinalMetadata]:
        return self._request.__iter__()


class _ViewResult:
    def __init__(self, view_request: _Rows[object, ViewMetaData]) -> None:
        self._request = view_request

    def metadata(self) -> ViewMetaData | None:
        """The meta-data which has been returned by the view query.

        Returns:
            :class:`~couchbase.views.ViewMetaData`: An instance of
            :class:`~couchbase.views.ViewMetaData`.
        """
        return self._request.final_metadata()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class ViewResult(_ViewResult):
    _request: RowIterator[ViewRow, ViewMetaData]

    def rows(self) -> RowIterator[ViewRow, ViewMetaData]:
        """The rows which have been returned by the view query.

        .. note::
            A ``KeyboardInterrupt`` raised while iterating rows is only checked for at
            internal timeout boundaries while waiting on the server, so it may take a moment
            to be delivered rather than firing instantly.

        Returns:
            Iterable: An iterable over the view query's rows.
        """
        return self.__iter__()

    def __iter__(self) -> RowIterator[ViewRow, ViewMetaData]:
        return self._request.__iter__()
