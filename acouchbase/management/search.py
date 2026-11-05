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
    'ScopeSearchIndexManager',
    'SearchIndex',
    'SearchIndexManager',
]

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common.management.search import SearchIndex

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from couchbase.common._interfaces._search_index_mgmt import AsyncSearchIndexMgmtInterface
    from couchbase.common.management.options import (AllowQueryingSearchIndexOptionsKwargs,
                                                     AnalyzeDocumentSearchIndexOptionsKwargs,
                                                     DisallowQueryingSearchIndexOptionsKwargs,
                                                     DropSearchIndexOptionsKwargs,
                                                     FreezePlanSearchIndexOptionsKwargs,
                                                     GetAllSearchIndexesOptionsKwargs,
                                                     GetAllSearchIndexStatsOptionsKwargs,
                                                     GetSearchIndexedDocumentsCountOptionsKwargs,
                                                     GetSearchIndexOptionsKwargs,
                                                     GetSearchIndexStatsOptionsKwargs,
                                                     PauseIngestSearchIndexOptionsKwargs,
                                                     ResumeIngestSearchIndexOptionsKwargs,
                                                     UnfreezePlanSearchIndexOptionsKwargs,
                                                     UpsertSearchIndexOptionsKwargs)
    from couchbase.management.options import (AllowQueryingSearchIndexOptions,
                                              AnalyzeDocumentSearchIndexOptions,
                                              DisallowQueryingSearchIndexOptions,
                                              DropSearchIndexOptions,
                                              FreezePlanSearchIndexOptions,
                                              GetAllSearchIndexesOptions,
                                              GetAllSearchIndexStatsOptions,
                                              GetSearchIndexedDocumentsCountOptions,
                                              GetSearchIndexOptions,
                                              GetSearchIndexStatsOptions,
                                              PauseIngestSearchIndexOptions,
                                              ResumeIngestSearchIndexOptions,
                                              UnfreezePlanSearchIndexOptions,
                                              UpsertSearchIndexOptions)


class SearchIndexManager:
    """Performs management operations on search indexes.

    For managing the search indexes of a scope, :class:`.ScopeSearchIndexManager` should be used.
    """

    def __init__(self, impl: AsyncSearchIndexMgmtInterface) -> None:
        self._impl = impl

    async def upsert_index(self,
                           index: SearchIndex,
                           options: UpsertSearchIndexOptions | None = None,
                           **kwargs: Unpack[UpsertSearchIndexOptionsKwargs]) -> None:
        """Creates or updates an index.

        Args:
            index (:class:`.SearchIndex`): The index definition.
            options (:class:`~couchbase.management.options.UpsertSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index definition is invalid.
        """
        await self._impl.upsert_index(index, options, **kwargs)

    async def drop_index(self,
                         index_name: str,
                         options: DropSearchIndexOptions | None = None,
                         **kwargs: Unpack[DropSearchIndexOptionsKwargs]) -> None:
        """Drops an index.

        Args:
            index_name (str): The name of the index.
            options (:class:`~couchbase.management.options.DropSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.drop_index(index_name, options, **kwargs)

    async def get_index(self,
                        index_name: str,
                        options: GetSearchIndexOptions | None = None,
                        **kwargs: Unpack[GetSearchIndexOptionsKwargs]) -> SearchIndex:
        """Fetches an index from the server if it exists.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            :class:`.SearchIndex`: The index definition if it exists.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.get_index(index_name, options, **kwargs)

    async def get_all_indexes(self,
                              options: GetAllSearchIndexesOptions | None = None,
                              **kwargs: Unpack[GetAllSearchIndexesOptionsKwargs]) -> Iterable[SearchIndex]:
        """Fetches all indexes from the server.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexesOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.SearchIndex`]: A list of all indexes.
        """
        return await self._impl.get_all_indexes(options, **kwargs)

    async def get_indexed_documents_count(self,
                                          index_name: str,
                                          options: GetSearchIndexedDocumentsCountOptions | None = None,
                                          **kwargs: Unpack[GetSearchIndexedDocumentsCountOptionsKwargs]) -> int:
        """Retrieves the number of documents that have been indexed for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexedDocumentsCountOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            int: The number of documents indexed for the specified index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.get_indexed_documents_count(index_name, options, **kwargs)

    async def pause_ingest(self,
                           index_name: str,
                           options: PauseIngestSearchIndexOptions | None = None,
                           **kwargs: Unpack[PauseIngestSearchIndexOptionsKwargs]) -> None:
        """Pauses updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.PauseIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.pause_ingest(index_name, options, **kwargs)

    async def resume_ingest(self,
                            index_name: str,
                            options: ResumeIngestSearchIndexOptions | None = None,
                            **kwargs: Unpack[ResumeIngestSearchIndexOptionsKwargs]) -> None:
        """Resumes updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.ResumeIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.resume_ingest(index_name, options, **kwargs)

    async def allow_querying(self,
                             index_name: str,
                             options: AllowQueryingSearchIndexOptions | None = None,
                             **kwargs: Unpack[AllowQueryingSearchIndexOptionsKwargs]) -> None:
        """Allows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.AllowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.allow_querying(index_name, options, **kwargs)

    async def disallow_querying(self,
                                index_name: str,
                                options: DisallowQueryingSearchIndexOptions | None = None,
                                **kwargs: Unpack[DisallowQueryingSearchIndexOptionsKwargs]) -> None:
        """Disallows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.DisallowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.disallow_querying(index_name, options, **kwargs)

    async def freeze_plan(self,
                          index_name: str,
                          options: FreezePlanSearchIndexOptions | None = None,
                          **kwargs: Unpack[FreezePlanSearchIndexOptionsKwargs]) -> None:
        """Freezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.FreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.freeze_plan(index_name, options, **kwargs)

    async def unfreeze_plan(self,
                            index_name: str,
                            options: UnfreezePlanSearchIndexOptions | None = None,
                            **kwargs: Unpack[UnfreezePlanSearchIndexOptionsKwargs]) -> None:
        """Unfreezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.UnfreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.unfreeze_plan(index_name, options, **kwargs)

    async def analyze_document(self,
                               index_name: str,
                               document: Any,
                               options: AnalyzeDocumentSearchIndexOptions | None = None,
                               **kwargs: Unpack[AnalyzeDocumentSearchIndexOptionsKwargs]) -> dict[str, Any]:
        """Allows to see how a document is analyzed against a specific index.

        Args:
            index_name (str): The name of the search index.
            document (Any): The document to analyze.
            options (:class:`~couchbase.management.options.AnalyzeDocumentSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            dict[str, Any]: The analyzed sections for the document.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.analyze_document(index_name, document, options, **kwargs)

    async def get_index_stats(self,
                              index_name: str,
                              options: GetSearchIndexStatsOptions | None = None,
                              **kwargs: Unpack[GetSearchIndexStatsOptionsKwargs]) -> dict[str, Any]:
        """Retrieves metrics, timings and counters for a given index.

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            dict[str, Any]: The stats for the index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.get_index_stats(index_name, options, **kwargs)

    async def get_all_index_stats(self,
                                  options: GetAllSearchIndexStatsOptions | None = None,
                                  **kwargs: Unpack[GetAllSearchIndexStatsOptionsKwargs]) -> dict[str, Any]:
        """Retrieves statistics on the search service.

        The statistics cover documents, partition indexes, mutations, compactions, queries, and more.

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            dict[str, Any]: The stats report.
        """
        return await self._impl.get_all_index_stats(options, **kwargs)


class ScopeSearchIndexManager:
    """Performs management operations on the search indexes of a scope."""

    def __init__(self, impl: AsyncSearchIndexMgmtInterface) -> None:
        self._impl = impl

    async def upsert_index(self,
                           index: SearchIndex,
                           options: UpsertSearchIndexOptions | None = None,
                           **kwargs: Unpack[UpsertSearchIndexOptionsKwargs]) -> None:
        """Creates or updates an index.

        Args:
            index (:class:`.SearchIndex`): The index definition.
            options (:class:`~couchbase.management.options.UpsertSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index definition is invalid.
        """
        await self._impl.upsert_index(index, options, **kwargs)

    async def drop_index(self,
                         index_name: str,
                         options: DropSearchIndexOptions | None = None,
                         **kwargs: Unpack[DropSearchIndexOptionsKwargs]) -> None:
        """Drops an index.

        Args:
            index_name (str): The name of the index.
            options (:class:`~couchbase.management.options.DropSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.drop_index(index_name, options, **kwargs)

    async def get_index(self,
                        index_name: str,
                        options: GetSearchIndexOptions | None = None,
                        **kwargs: Unpack[GetSearchIndexOptionsKwargs]) -> SearchIndex:
        """Fetches an index from the server if it exists.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            :class:`.SearchIndex`: The index definition if it exists.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.get_index(index_name, options, **kwargs)

    async def get_all_indexes(self,
                              options: GetAllSearchIndexesOptions | None = None,
                              **kwargs: Unpack[GetAllSearchIndexesOptionsKwargs]) -> Iterable[SearchIndex]:
        """Fetches all indexes from the server.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexesOptions`): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.SearchIndex`]: A list of all indexes.
        """
        return await self._impl.get_all_indexes(options, **kwargs)

    async def get_indexed_documents_count(self,
                                          index_name: str,
                                          options: GetSearchIndexedDocumentsCountOptions | None = None,
                                          **kwargs: Unpack[GetSearchIndexedDocumentsCountOptionsKwargs]) -> int:
        """Retrieves the number of documents that have been indexed for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexedDocumentsCountOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            int: The number of documents indexed for the specified index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.get_indexed_documents_count(index_name, options, **kwargs)

    async def pause_ingest(self,
                           index_name: str,
                           options: PauseIngestSearchIndexOptions | None = None,
                           **kwargs: Unpack[PauseIngestSearchIndexOptionsKwargs]) -> None:
        """Pauses updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.PauseIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.pause_ingest(index_name, options, **kwargs)

    async def resume_ingest(self,
                            index_name: str,
                            options: ResumeIngestSearchIndexOptions | None = None,
                            **kwargs: Unpack[ResumeIngestSearchIndexOptionsKwargs]) -> None:
        """Resumes updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.ResumeIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.resume_ingest(index_name, options, **kwargs)

    async def allow_querying(self,
                             index_name: str,
                             options: AllowQueryingSearchIndexOptions | None = None,
                             **kwargs: Unpack[AllowQueryingSearchIndexOptionsKwargs]) -> None:
        """Allows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.AllowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.allow_querying(index_name, options, **kwargs)

    async def disallow_querying(self,
                                index_name: str,
                                options: DisallowQueryingSearchIndexOptions | None = None,
                                **kwargs: Unpack[DisallowQueryingSearchIndexOptionsKwargs]) -> None:
        """Disallows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.DisallowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.disallow_querying(index_name, options, **kwargs)

    async def freeze_plan(self,
                          index_name: str,
                          options: FreezePlanSearchIndexOptions | None = None,
                          **kwargs: Unpack[FreezePlanSearchIndexOptionsKwargs]) -> None:
        """Freezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.FreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.freeze_plan(index_name, options, **kwargs)

    async def unfreeze_plan(self,
                            index_name: str,
                            options: UnfreezePlanSearchIndexOptions | None = None,
                            **kwargs: Unpack[UnfreezePlanSearchIndexOptionsKwargs]) -> None:
        """Unfreezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.UnfreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        await self._impl.unfreeze_plan(index_name, options, **kwargs)

    async def analyze_document(self,
                               index_name: str,
                               document: Any,
                               options: AnalyzeDocumentSearchIndexOptions | None = None,
                               **kwargs: Unpack[AnalyzeDocumentSearchIndexOptionsKwargs]) -> dict[str, Any]:
        """Allows to see how a document is analyzed against a specific index.

        Args:
            index_name (str): The name of the search index.
            document (Any): The document to analyze.
            options (:class:`~couchbase.management.options.AnalyzeDocumentSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            dict[str, Any]: The analyzed sections for the document.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.analyze_document(index_name, document, options, **kwargs)

    async def get_index_stats(self,
                              index_name: str,
                              options: GetSearchIndexStatsOptions | None = None,
                              **kwargs: Unpack[GetSearchIndexStatsOptionsKwargs]) -> dict[str, Any]:
        """Retrieves metrics, timings and counters for a given index.

        .. note::
            **UNCOMMITTED:**
            This is an uncommitted API call that is unlikely to change, but may still change as
            final consensus on its behavior has not yet been reached.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            dict[str, Any]: The stats for the index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        return await self._impl.get_index_stats(index_name, options, **kwargs)

    async def get_all_index_stats(self,
                                  options: GetAllSearchIndexStatsOptions | None = None,
                                  **kwargs: Unpack[GetAllSearchIndexStatsOptionsKwargs]) -> dict[str, Any]:
        """Retrieves statistics on the search service.

        The statistics cover documents, partition indexes, mutations, compactions, queries, and more.

        .. note::
            **UNCOMMITTED:**
            This is an uncommitted API call that is unlikely to change, but may still change as
            final consensus on its behavior has not yet been reached.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            dict[str, Any]: The stats report.
        """
        return await self._impl.get_all_index_stats(options, **kwargs)
