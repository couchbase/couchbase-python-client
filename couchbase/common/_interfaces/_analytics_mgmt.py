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

"""What a protocol's analytics management impl must provide to the `AnalyticsIndexManager` facades.

The impl is built by its cluster impl.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.analytics import (AnalyticsDataset,
                                                       AnalyticsDataType,
                                                       AnalyticsIndex,
                                                       AnalyticsLink)
    from couchbase.common.management.options import (ConnectLinkOptions,
                                                     CreateAnalyticsIndexOptions,
                                                     CreateDatasetOptions,
                                                     CreateDataverseOptions,
                                                     CreateLinkAnalyticsOptions,
                                                     DisconnectLinkOptions,
                                                     DropAnalyticsIndexOptions,
                                                     DropDatasetOptions,
                                                     DropDataverseOptions,
                                                     DropLinkAnalyticsOptions,
                                                     GetAllAnalyticsIndexesOptions,
                                                     GetAllDatasetOptions,
                                                     GetLinksAnalyticsOptions,
                                                     GetPendingMutationsOptions,
                                                     ReplaceLinkAnalyticsOptions)


class AnalyticsMgmtInterface(Protocol):
    def create_dataverse(self,
                         dataverse_name: str,
                         options: CreateDataverseOptions | None = None,
                         **kwargs: Any) -> None: ...

    def drop_dataverse(self,
                       dataverse_name: str,
                       options: DropDataverseOptions | None = None,
                       **kwargs: Any) -> None: ...

    def create_dataset(self,
                       dataset_name: str,
                       bucket_name: str,
                       options: CreateDatasetOptions | None = None,
                       **kwargs: Any) -> None: ...

    def drop_dataset(self,
                     dataset_name: str,
                     options: DropDatasetOptions | None = None,
                     **kwargs: Any) -> None: ...

    def get_all_datasets(self,
                         options: GetAllDatasetOptions | None = None,
                         **kwargs: Any) -> Iterable[AnalyticsDataset]: ...

    def create_index(self,
                     index_name: str,
                     dataset_name: str,
                     fields: dict[str, AnalyticsDataType],
                     options: CreateAnalyticsIndexOptions | None = None,
                     **kwargs: Any) -> None: ...

    def drop_index(self,
                   index_name: str,
                   dataset_name: str,
                   options: DropAnalyticsIndexOptions | None = None,
                   **kwargs: Any) -> None: ...

    def get_all_indexes(self,
                        options: GetAllAnalyticsIndexesOptions | None = None,
                        **kwargs: Any) -> Iterable[AnalyticsIndex]: ...

    def connect_link(self, options: ConnectLinkOptions | None = None, **kwargs: Any) -> None: ...

    def disconnect_link(self, options: DisconnectLinkOptions | None = None, **kwargs: Any) -> None: ...

    def get_pending_mutations(self, options: GetPendingMutationsOptions |
                              None = None, **kwargs: Any) -> dict[str, int]: ...

    def create_link(self,
                    link: AnalyticsLink,
                    options: CreateLinkAnalyticsOptions | None = None,
                    **kwargs: Any) -> None: ...

    def replace_link(self,
                     link: AnalyticsLink,
                     options: ReplaceLinkAnalyticsOptions | None = None,
                     **kwargs: Any) -> None: ...

    def drop_link(self,
                  link_name: str,
                  dataverse_name: str,
                  options: DropLinkAnalyticsOptions | None = None,
                  **kwargs: Any) -> None: ...

    def get_links(self, options: GetLinksAnalyticsOptions | None = None, **kwargs: Any) -> Iterable[AnalyticsLink]: ...


class AsyncAnalyticsMgmtInterface(Protocol):
    async def create_dataverse(self,
                               dataverse_name: str,
                               options: CreateDataverseOptions | None = None,
                               **kwargs: Any) -> None: ...

    async def drop_dataverse(self,
                             dataverse_name: str,
                             options: DropDataverseOptions | None = None,
                             **kwargs: Any) -> None: ...

    async def create_dataset(self,
                             dataset_name: str,
                             bucket_name: str,
                             options: CreateDatasetOptions | None = None,
                             **kwargs: Any) -> None: ...

    async def drop_dataset(self,
                           dataset_name: str,
                           options: DropDatasetOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def get_all_datasets(self,
                               options: GetAllDatasetOptions | None = None,
                               **kwargs: Any) -> Iterable[AnalyticsDataset]: ...

    async def create_index(self,
                           index_name: str,
                           dataset_name: str,
                           fields: dict[str, AnalyticsDataType],
                           options: CreateAnalyticsIndexOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def drop_index(self,
                         index_name: str,
                         dataset_name: str,
                         options: DropAnalyticsIndexOptions | None = None,
                         **kwargs: Any) -> None: ...

    async def get_all_indexes(self,
                              options: GetAllAnalyticsIndexesOptions | None = None,
                              **kwargs: Any) -> Iterable[AnalyticsIndex]: ...

    async def connect_link(self, options: ConnectLinkOptions | None = None, **kwargs: Any) -> None: ...

    async def disconnect_link(self, options: DisconnectLinkOptions | None = None, **kwargs: Any) -> None: ...

    async def get_pending_mutations(self,
                                    options: GetPendingMutationsOptions | None = None,
                                    **kwargs: Any) -> dict[str, int]: ...

    async def create_link(self,
                          link: AnalyticsLink,
                          options: CreateLinkAnalyticsOptions | None = None,
                          **kwargs: Any) -> None: ...

    async def replace_link(self,
                           link: AnalyticsLink,
                           options: ReplaceLinkAnalyticsOptions | None = None,
                           **kwargs: Any) -> None: ...

    async def drop_link(self,
                        link_name: str,
                        dataverse_name: str,
                        options: DropLinkAnalyticsOptions | None = None,
                        **kwargs: Any) -> None: ...

    async def get_links(self,
                        options: GetLinksAnalyticsOptions | None = None,
                        **kwargs: Any) -> Iterable[AnalyticsLink]: ...


class TxAnalyticsMgmtInterface(Protocol):
    def create_dataverse_deferred(self,
                                  dataverse_name: str,
                                  options: CreateDataverseOptions | None = None,
                                  **kwargs: Any) -> Deferred[None]: ...

    def drop_dataverse_deferred(self,
                                dataverse_name: str,
                                options: DropDataverseOptions | None = None,
                                **kwargs: Any) -> Deferred[None]: ...

    def create_dataset_deferred(self,
                                dataset_name: str,
                                bucket_name: str,
                                options: CreateDatasetOptions | None = None,
                                **kwargs: Any) -> Deferred[None]: ...

    def drop_dataset_deferred(self,
                              dataset_name: str,
                              options: DropDatasetOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def get_all_datasets_deferred(self,
                                  options: GetAllDatasetOptions | None = None,
                                  **kwargs: Any) -> Deferred[Iterable[AnalyticsDataset]]: ...

    def create_index_deferred(self,
                              index_name: str,
                              dataset_name: str,
                              fields: dict[str, AnalyticsDataType],
                              options: CreateAnalyticsIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def drop_index_deferred(self,
                            index_name: str,
                            dataset_name: str,
                            options: DropAnalyticsIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]: ...

    def get_all_indexes_deferred(self,
                                 options: GetAllAnalyticsIndexesOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[AnalyticsIndex]]: ...

    def connect_link_deferred(self, options: ConnectLinkOptions | None = None, **kwargs: Any) -> Deferred[None]: ...

    def disconnect_link_deferred(self, options: DisconnectLinkOptions |
                                 None = None, **kwargs: Any) -> Deferred[None]: ...

    def get_pending_mutations_deferred(self,
                                       options: GetPendingMutationsOptions | None = None,
                                       **kwargs: Any) -> Deferred[dict[str, int]]: ...

    def create_link_deferred(self,
                             link: AnalyticsLink,
                             options: CreateLinkAnalyticsOptions | None = None,
                             **kwargs: Any) -> Deferred[None]: ...

    def replace_link_deferred(self,
                              link: AnalyticsLink,
                              options: ReplaceLinkAnalyticsOptions | None = None,
                              **kwargs: Any) -> Deferred[None]: ...

    def drop_link_deferred(self,
                           link_name: str,
                           dataverse_name: str,
                           options: DropLinkAnalyticsOptions | None = None,
                           **kwargs: Any) -> Deferred[None]: ...

    def get_links_deferred(self,
                           options: GetLinksAnalyticsOptions | None = None,
                           **kwargs: Any) -> Deferred[Iterable[AnalyticsLink]]: ...
