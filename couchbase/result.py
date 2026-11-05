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
    """The documents a range scan returns, read by iterating it."""

    _request: RowIterator[ScanResult, None]

    def rows(self) -> RowIterator[ScanResult, None]:
        """Get the documents the scan returns.

        Returns:
            Iterable: An iterable over the documents.
        """
        return self.__iter__()

    def __iter__(self) -> RowIterator[ScanResult, None]:
        return self._request.__iter__()


class _QueryResult(Generic[_ExecuteT]):
    def __init__(self, n1ql_request: _Rows[_ExecuteT, QueryMetaData]) -> None:
        self._request = n1ql_request

    def execute(self) -> _ExecuteT:
        """Read every row of the query.

        Returns:
            list[Any]: The rows.  On the asyncio API, a task that resolves to them.

        Example::

            rows = cluster.query("SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2").execute()
        """
        return self._request.execute()

    def metadata(self) -> QueryMetaData | None:
        """Get the metadata the service returned after the query's rows.

        Returns:
            :class:`~couchbase.n1ql.QueryMetaData` | None: The metadata, or ``None`` until every row has been read.
        """
        return self._request.final_metadata()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class QueryResult(_QueryResult[list[Any]]):
    """The result of a query: its rows, read by iterating it, then its metadata."""

    _request: RowIterator[Any, QueryMetaData]

    def rows(self) -> RowIterator[Any, QueryMetaData]:
        """The rows which have been returned by the query.

        .. note::
            While iterating waits on the server, a pending ``KeyboardInterrupt`` is checked for once per
            the request's timeout, so it can take up to that long to arrive.

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
        """Get the metadata the service returned after the analytics query's rows.

        Returns:
            :class:`~couchbase.analytics.AnalyticsMetaData` | None: The metadata, or ``None`` until every row
            has been read.
        """
        return self._request.final_metadata()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class AnalyticsResult(_AnalyticsResult):
    """The result of an analytics query: its rows, read by iterating it, then its metadata."""

    _request: RowIterator[Any, AnalyticsMetaData]

    def rows(self) -> RowIterator[Any, AnalyticsMetaData]:
        """The rows which have been returned by the analytics query.

        .. note::
            While iterating waits on the server, a pending ``KeyboardInterrupt`` is checked for once per
            the request's timeout, so it can take up to that long to arrive.

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
        """Get the metadata the service returned after the search's rows.

        Returns:
            :class:`~couchbase.search.SearchMetaData` | None: The metadata, or ``None`` until every row has been
            read.
        """
        final_metadata = self._request.final_metadata()
        return None if final_metadata is None else final_metadata.metadata

    def result_rows(self) -> None:
        return None

    def facets(self) -> dict[str, SearchFacetResult] | None:
        """Get the results of the search's facets, which the service returns after its rows.

        Returns:
            dict[str, :class:`~couchbase.search.SearchFacetResult`] | None: Each facet's result, by the facet's
            name, or ``None`` until every row has been read.
        """
        final_metadata = self._request.final_metadata()
        return None if final_metadata is None else final_metadata.facets

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class SearchResult(_SearchResult):
    """The result of a search: its rows, read by iterating it, then its metadata and facets."""

    _request: RowIterator[SearchRow, _SearchFinalMetadata]

    def rows(self) -> RowIterator[SearchRow, _SearchFinalMetadata]:
        """The rows which have been returned by the search query.

        .. note::
            While iterating waits on the server, a pending ``KeyboardInterrupt`` is checked for once per
            the request's timeout, so it can take up to that long to arrive.

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
        """Get the metadata the service returned after the view query's rows.

        Returns:
            :class:`~couchbase.views.ViewMetaData` | None: The metadata, or ``None`` until every row has been
            read.
        """
        return self._request.final_metadata()

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._request})'


class ViewResult(_ViewResult):
    """The result of a view query: its rows, read by iterating it, then its metadata."""

    _request: RowIterator[ViewRow, ViewMetaData]

    def rows(self) -> RowIterator[ViewRow, ViewMetaData]:
        """The rows which have been returned by the view query.

        .. note::
            While iterating waits on the server, a pending ``KeyboardInterrupt`` is checked for once per
            the request's timeout, so it can take up to that long to arrive.

        Returns:
            Iterable: An iterable over the view query's rows.
        """
        return self.__iter__()

    def __iter__(self) -> RowIterator[ViewRow, ViewMetaData]:
        return self._request.__iter__()


if not TYPE_CHECKING:
    __getattr__ = Supportability.deprecated_paths(__name__)
