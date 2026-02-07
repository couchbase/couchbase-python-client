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
                    Dict,
                    Iterable)

from couchbase.management.logic.search_index_mgmt_impl import SearchIndexMgmtImpl
from couchbase.management.logic.search_index_mgmt_types import SearchIndex

# @TODO:  lets deprecate import of options from couchbase.management.search
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

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter


class SearchIndexManager:
    """
    Allows to manage search indexes in a Couchbase cluster.
    """

    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._impl = SearchIndexMgmtImpl(client_adapter)
        self._scope_context = None

    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Any
                     ) -> None:
        """Creates or updates an index.

        Args:
            index (:class:`.SearchIndex`): The index definition.
            options (:class:`~couchbase.management.options.UpsertSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index definition is invalid.
        """
        req = self._impl.request_builder.build_upsert_index_request(index, self._scope_context, *options, **kwargs)
        self._impl.upsert_index(req)

    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Any
                   ) -> None:
        """Drops an index.

        Args:
            index_name (str): The name of the index.
            options (:class:`~couchbase.management.options.DropSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_drop_index_request(index_name, self._scope_context, *options, **kwargs)
        self._impl.drop_index(req)

    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Any
                  ) -> SearchIndex:
        """Fetches an index from the server if it exists.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            :class:`.SearchIndex`: The index definition if it exists.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_get_index_request(index_name, self._scope_context, *options, **kwargs)
        return self._impl.get_index(req)

    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Any
                        ) -> Iterable[SearchIndex]:
        """Fetches all indexes from the server.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexesOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.SearchIndex`]: A list of all indexes.
        """
        req = self._impl.request_builder.build_get_all_indexes_request(self._scope_context, *options, **kwargs)
        return self._impl.get_all_indexes(req)

    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Any
                                    ) -> int:
        """Retrieves the number of documents that have been indexed for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexedDocumentsCountOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            int: The number of documents indexed for the specified index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                   self._scope_context,
                                                                                   *options,
                                                                                   **kwargs)
        return self._impl.get_indexed_documents_count(req)

    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Any
                     ) -> None:
        """Pauses updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.PauseIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_pause_ingest_request(index_name, self._scope_context, *options, **kwargs)
        self._impl.pause_ingest(req)

    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Any
                      ) -> None:
        """Resumes updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.ResumeIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_resume_ingest_request(index_name,
                                                                     self._scope_context,
                                                                     *options,
                                                                     **kwargs)
        self._impl.resume_ingest(req)

    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Any
                       ) -> None:
        """Allows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.AllowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_allow_querying_request(index_name,
                                                                      self._scope_context,
                                                                      *options,
                                                                      **kwargs)
        self._impl.allow_querying(req)

    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Any
                          ) -> None:
        """Disallows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.DisallowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_disallow_querying_request(index_name,
                                                                         self._scope_context,
                                                                         *options,
                                                                         **kwargs)
        self._impl.disallow_querying(req)

    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Any
                    ) -> None:
        """Freezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.FreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_freeze_plan_request(index_name, self._scope_context, *options, **kwargs)
        self._impl.freeze_plan(req)

    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Any
                      ) -> None:
        """Unfreezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.UnfreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_unfreeze_plan_request(index_name,
                                                                     self._scope_context,
                                                                     *options,
                                                                     **kwargs)
        self._impl.unfreeze_plan(req)

    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Any
                         ) -> Dict[str, Any]:
        """Allows to see how a document is analyzed against a specific index.

        Args:
            index_name (str): The name of the search index.
            document (Any): The document to analyze.
            options (:class:`~couchbase.management.options.AnalyzeDocumentSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Dict[str, Any]: The analyzed sections for the document.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_analyze_document_request(index_name,
                                                                        document,
                                                                        self._scope_context,
                                                                        *options,
                                                                        **kwargs)
        return self._impl.analyze_document(req)

    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Any
                        ) -> Dict[str, Any]:
        """Retrieves metrics, timings and counters for a given index.

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Dict[str, Any]: The stats for the index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_get_index_stats_request(index_name,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        return self._impl.get_index_stats(req)

    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Any
                            ) -> Dict[str, Any]:
        """Retrieves statistics on search service. Information is provided on documents, partition indexes, mutations,
        compactions, queries, and more.

        **UNCOMMITTED**
        This is an uncommitted API call that is unlikely to change, but may still change as
        final consensus on its behavior has not yet been reached.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Dict[str, Any]: The stats report.
        """
        req = self._impl.request_builder.build_get_all_index_stats_request(*options, **kwargs)
        return self._impl.get_all_index_stats(req)


class ScopeSearchIndexManager:
    """
    Allows to manage scope-level search indexes in a Couchbase cluster.
    """

    def __init__(self, client_adapter: ClientAdapter, bucket_name: str, scope_name: str) -> None:
        self._impl = SearchIndexMgmtImpl(client_adapter)
        self._scope_context = bucket_name, scope_name

    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Any
                     ) -> None:
        """Creates or updates an index.

        Args:
            index (:class:`.SearchIndex`): The index definition.
            options (:class:`~couchbase.management.options.UpsertSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the index definition is invalid.
        """
        req = self._impl.request_builder.build_upsert_index_request(index,
                                                                    self._scope_context,
                                                                    *options,
                                                                    **kwargs)
        self._impl.upsert_index(req)

    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Any
                   ) -> None:
        """Drops an index.

        Args:
            index_name (str): The name of the index.
            options (:class:`~couchbase.management.options.DropSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_drop_index_request(index_name,
                                                                  self._scope_context,
                                                                  *options,
                                                                  **kwargs)
        self._impl.drop_index(req)

    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Any
                  ) -> SearchIndex:
        """Fetches an index from the server if it exists.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            :class:`.SearchIndex`: The index definition if it exists.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_get_index_request(index_name,
                                                                 self._scope_context,
                                                                 *options,
                                                                 **kwargs)
        return self._impl.get_index(req)

    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Any
                        ) -> Iterable[SearchIndex]:
        """Fetches all indexes from the server.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexesOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.SearchIndex`]: A list of all indexes.
        """
        req = self._impl.request_builder.build_get_all_indexes_request(self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        return self._impl.get_all_indexes(req)

    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Any
                                    ) -> int:
        """Retrieves the number of documents that have been indexed for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexedDocumentsCountOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            int: The number of documents indexed for the specified index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_get_indexed_documents_count_request(index_name,
                                                                                   self._scope_context,
                                                                                   *options,
                                                                                   **kwargs)
        return self._impl.get_indexed_documents_count(req)

    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Any
                     ) -> None:
        """Pauses updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.PauseIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_pause_ingest_request(index_name,
                                                                    self._scope_context,
                                                                    *options,
                                                                    **kwargs)
        self._impl.pause_ingest(req)

    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Any
                      ) -> None:
        """Resumes updates and maintenance for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.ResumeIngestSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_resume_ingest_request(index_name,
                                                                     self._scope_context,
                                                                     *options,
                                                                     **kwargs)
        self._impl.resume_ingest(req)

    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Any
                       ) -> None:
        """Allows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.AllowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_allow_querying_request(index_name,
                                                                      self._scope_context,
                                                                      *options,
                                                                      **kwargs)
        self._impl.allow_querying(req)

    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Any
                          ) -> None:
        """Disallows querying against an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.DisallowQueryingSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_disallow_querying_request(index_name,
                                                                         self._scope_context,
                                                                         *options,
                                                                         **kwargs)
        self._impl.disallow_querying(req)

    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Any
                    ) -> None:
        """Freezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.FreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_freeze_plan_request(index_name,
                                                                   self._scope_context,
                                                                   *options,
                                                                   **kwargs)
        self._impl.freeze_plan(req)

    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Any
                      ) -> None:
        """Unfreezes the assignment of index partitions to nodes for an index.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.UnfreezePlanSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_unfreeze_plan_request(index_name,
                                                                     self._scope_context,
                                                                     *options,
                                                                     **kwargs)
        self._impl.unfreeze_plan(req)

    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Any
                         ) -> Dict[str, Any]:
        """Allows to see how a document is analyzed against a specific index.

        Args:
            index_name (str): The name of the search index.
            document (Any): The document to analyze.
            options (:class:`~couchbase.management.options.AnalyzeDocumentSearchIndexOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Dict[str, Any]: The analyzed sections for the document.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_analyze_document_request(index_name,
                                                                        document,
                                                                        self._scope_context,
                                                                        *options,
                                                                        **kwargs)
        return self._impl.analyze_document(req)

    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Any
                        ) -> Dict[str, Any]:
        """Retrieves metrics, timings and counters for a given index.

        .. note::
            **UNCOMMITTED:**
            This is an uncommitted API call that is unlikely to change, but may still change as
            final consensus on its behavior has not yet been reached.

        Args:
            index_name (str): The name of the search index.
            options (:class:`~couchbase.management.options.GetSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Dict[str, Any]: The stats for the index.

        Raises:
            :class:`~couchbase.exceptions.SearchIndexNotFoundException`: If the index does not exist.
        """
        req = self._impl.request_builder.build_get_index_stats_request(index_name,
                                                                       self._scope_context,
                                                                       *options,
                                                                       **kwargs)
        return self._impl.get_index_stats(req)

    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Any
                            ) -> Dict[str, Any]:
        """Retrieves statistics on search service. Information is provided on documents, partition indexes, mutations,
        compactions, queries, and more.

        .. note::
            **UNCOMMITTED:**
            This is an uncommitted API call that is unlikely to change, but may still change as
            final consensus on its behavior has not yet been reached.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexStatsOptions`): Optional parameters
                for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Dict[str, Any]: The stats report.
        """
        req = self._impl.request_builder.build_get_all_index_stats_request(*options, **kwargs)
        return self._impl.get_all_index_stats(req)
