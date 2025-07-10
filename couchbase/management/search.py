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

from typing import (Any,
                    Dict,
                    Iterable)

from couchbase.management.logic.search_index_logic import SearchIndex, SearchIndexManagerLogic
from couchbase.management.logic.wrappers import BlockingMgmtWrapper, ManagementType

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


class SearchIndexManager(SearchIndexManagerLogic):
    """
    Allows to manage search indexes in a Couchbase cluster.
    """

    def __init__(self,
                 connection
                 ):
        super().__init__(connection)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Dict[str, Any]
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

        return super().upsert_index(index, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Dict[str, Any]
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

        return super().drop_index(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(SearchIndex, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Dict[str, Any]
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

        return super().get_index(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(SearchIndex, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Iterable[SearchIndex]:
        """Fetches all indexes from the server.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexesOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.SearchIndex`]: A list of all indexes.
        """

        return super().get_all_indexes(*options, **kwargs)

    @BlockingMgmtWrapper.block(int, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Dict[str, Any]
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

        return super().get_indexed_documents_count(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Dict[str, Any]
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

        return super().pause_ingest(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
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

        return super().resume_ingest(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Dict[str, Any]
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

        return super().allow_querying(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Dict[str, Any]
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

        return super().disallow_querying(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Dict[str, Any]
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

        return super().freeze_plan(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
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

        return super().unfreeze_plan(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Dict[str, Any]
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

        return super().analyze_document(index_name, document, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Dict[str, Any]
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

        return super().get_index_stats(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Dict[str, Any]
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

        return super().get_all_index_stats(*options, **kwargs)


class ScopeSearchIndexManager(SearchIndexManagerLogic):
    """
    Allows to manage scope-level search indexes in a Couchbase cluster.
    """

    def __init__(self,
                 connection,
                 bucket_name,  # type: str
                 scope_name  # type: str
                 ):
        super().__init__(connection, bucket_name=bucket_name, scope_name=scope_name)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def upsert_index(self,
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Dict[str, Any]
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

        return super().upsert_index(index, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   index_name,  # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Dict[str, Any]
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

        return super().drop_index(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(SearchIndex, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_index(self,
                  index_name,  # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Dict[str, Any]
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

        return super().get_index(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(SearchIndex, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        *options,  # type: GetAllSearchIndexesOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> Iterable[SearchIndex]:
        """Fetches all indexes from the server.

        Args:
            options (:class:`~couchbase.management.options.GetAllSearchIndexesOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters for this operation.

        Returns:
            Iterable[:class:`.SearchIndex`]: A list of all indexes.
        """

        return super().get_all_indexes(*options, **kwargs)

    @BlockingMgmtWrapper.block(int, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_indexed_documents_count(self,
                                    index_name,  # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Dict[str, Any]
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

        return super().get_indexed_documents_count(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def pause_ingest(self,
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Dict[str, Any]
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

        return super().pause_ingest(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def resume_ingest(self,
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
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

        return super().resume_ingest(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def allow_querying(self,
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Dict[str, Any]
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

        return super().allow_querying(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def disallow_querying(self,
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Dict[str, Any]
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

        return super().disallow_querying(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def freeze_plan(self,
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Dict[str, Any]
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

        return super().freeze_plan(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(None, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def unfreeze_plan(self,
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Dict[str, Any]
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

        return super().unfreeze_plan(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def analyze_document(self,
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Dict[str, Any]
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

        return super().analyze_document(index_name, document, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_index_stats(self,
                        index_name,  # type: str
                        *options,  # type: GetSearchIndexStatsOptions
                        **kwargs  # type: Dict[str, Any]
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

        return super().get_index_stats(index_name, *options, **kwargs)

    @BlockingMgmtWrapper.block(dict, ManagementType.SearchIndexMgmt, SearchIndexManagerLogic._ERROR_MAPPING)
    def get_all_index_stats(self,
                            *options,  # type: GetAllSearchIndexStatsOptions
                            **kwargs  # type: Dict[str, Any]
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

        return super().get_all_index_stats(*options, **kwargs)
