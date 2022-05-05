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
                    Awaitable,
                    Dict,
                    Iterable,
                    Optional)

from acouchbase.management.logic.wrappers import AsyncMgmtWrapper
from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic import ManagementType
from couchbase.management.logic.analytics_logic import (AnalyticsDataset,
                                                        AnalyticsDataType,
                                                        AnalyticsIndex,
                                                        AnalyticsLink,
                                                        AnalyticsManagerLogic,
                                                        AzureBlobExternalAnalyticsLink,
                                                        CouchbaseRemoteAnalyticsLink,
                                                        S3ExternalAnalyticsLink)

if TYPE_CHECKING:
    from couchbase.management.options import (ConnectLinkOptions,
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


class AnalyticsIndexManager(AnalyticsManagerLogic):

    def __init__(self, connection, loop):
        super().__init__(connection)
        self._loop = loop

    @property
    def loop(self):
        """
        **INTERNAL**
        """
        return self._loop

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_dataverse(self,
                         dataverse_name,    # type: str
                         options=None,      # type: Optional[CreateDataverseOptions]
                         **kwargs           # type: Dict[str, Any]
                         ) -> Awaitable[None]:

        if not isinstance(dataverse_name, str):
            raise InvalidArgumentException("dataverse_name must be provided when creating an analytics dataverse.")

        super().create_dataverse(dataverse_name, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_dataverse(self,
                       dataverse_name,    # type: str
                       options=None,      # type: Optional[DropDataverseOptions]
                       **kwargs           # type: Dict[str, Any]
                       ) -> Awaitable[None]:

        if not isinstance(dataverse_name, str):
            raise InvalidArgumentException("dataverse_name must be provided when dropping an analytics dataverse.")

        super().drop_dataverse(dataverse_name, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       options=None,    # type: Optional[CreateDatasetOptions]
                       **kwargs         # type: Dict[str, Any]
                       ) -> Awaitable[None]:

        if not isinstance(dataset_name, str):
            raise InvalidArgumentException("dataset_name must be provided when creating an analytics dataset.")

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException("bucket_name must be provided when creating an analytics dataset.")

        super().create_dataset(dataset_name, bucket_name, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_dataset(self,
                     dataset_name,  # type: str
                     options=None,  # type: Optional[DropDatasetOptions]
                     **kwargs       # type: Dict[str, Any]
                     ) -> Awaitable[None]:

        if not isinstance(dataset_name, str):
            raise InvalidArgumentException("dataset_name must be provided when dropping an analytics dataset.")

        super().drop_dataset(dataset_name, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(AnalyticsDataset, ManagementType.AnalyticsIndexMgmt,
                                       AnalyticsManagerLogic._ERROR_MAPPING)
    def get_all_datasets(self,
                         options=None,   # type: Optional[GetAllDatasetOptions]
                         **kwargs   # type: Dict[str, Any]
                         ) -> Awaitable[Iterable[AnalyticsDataset]]:

        super().get_all_datasets(options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type: Dict[str, AnalyticsDataType]
                     options=None,  # type: Optional[CreateAnalyticsIndexOptions]
                     **kwargs       # type: Dict[str, Any]
                     ) -> Awaitable[None]:

        if not isinstance(index_name, str):
            raise InvalidArgumentException("index_name must be provided when creating an analytics index.")

        if not isinstance(dataset_name, str):
            raise InvalidArgumentException("dataset_name must be provided when creating an analytics index.")

        if fields is not None:
            if not isinstance(fields, dict):
                raise InvalidArgumentException("fields must be provided when creating an analytics index.")

            if not all(map(lambda v: isinstance(v, AnalyticsDataType), fields.values())):
                raise InvalidArgumentException("fields must all be an AnalyticsDataType.")

        super().create_index(index_name, dataset_name, fields, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_index(self,
                   index_name,    # type: str
                   dataset_name,  # type: str
                   options=None,  # type: Optional[DropAnalyticsIndexOptions]
                   **kwargs       # type: Dict[str, Any]
                   ) -> Awaitable[None]:

        if not isinstance(index_name, str):
            raise InvalidArgumentException("index_name must be provided when dropping an analytics index.")

        if not isinstance(dataset_name, str):
            raise InvalidArgumentException("dataset_name must be provided when dropping an analytics index.")

        super().drop_index(index_name, dataset_name, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(AnalyticsIndex, ManagementType.AnalyticsIndexMgmt,
                                       AnalyticsManagerLogic._ERROR_MAPPING)
    def get_all_indexes(self,
                        options=None,   # type: Optional[GetAllAnalyticsIndexesOptions]
                        **kwargs   # type: Dict[str, Any]
                        ) -> Awaitable[Iterable[AnalyticsIndex]]:

        super().get_all_indexes(options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def connect_link(self,
                     options=None,  # type: Optional[ConnectLinkOptions]
                     **kwargs   # type: Dict[str, Any]
                     ) -> Awaitable[None]:
        super().connect_link(options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def disconnect_link(self,
                        options=None,  # type: Optional[DisconnectLinkOptions]
                        **kwargs   # type: Dict[str, Any]
                        ) -> Awaitable[None]:
        super().disconnect_link(options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(dict, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def get_pending_mutations(self,
                              options=None,     # type: Optional[GetPendingMutationsOptions]
                              **kwargs     # type: Dict[str, Any]
                              ) -> Dict[str, int]:

        super().get_pending_mutations(options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def create_link(
        self,
        link,  # type: AnalyticsLink
        options=None,     # type: Optional[CreateLinkAnalyticsOptions]
        **kwargs
    ) -> Awaitable[None]:
        super().create_link(link, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def replace_link(
        self,
        link,  # type: AnalyticsLink
        options=None,     # type: Optional[ReplaceLinkAnalyticsOptions]
        **kwargs
    ) -> Awaitable[None]:
        super().replace_link(link, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks(None, ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def drop_link(
        self,
        link_name,  # type: str
        dataverse_name,  # type: str
        options=None,     # type: Optional[DropLinkAnalyticsOptions]
        **kwargs
    ) -> Awaitable[None]:

        if not isinstance(link_name, str):
            raise InvalidArgumentException("link_name must be provided when dropping an analytics link.")

        if not isinstance(dataverse_name, str):
            raise InvalidArgumentException("dataverse_name must be provided when dropping an analytics link.")

        super().drop_link(link_name, dataverse_name, options, **kwargs)

    @AsyncMgmtWrapper.inject_callbacks((CouchbaseRemoteAnalyticsLink,
                                        S3ExternalAnalyticsLink,
                                        AzureBlobExternalAnalyticsLink),
                                       ManagementType.AnalyticsIndexMgmt, AnalyticsManagerLogic._ERROR_MAPPING)
    def get_links(
        self,
        options=None,  # type: Optional[GetLinksAnalyticsOptions]
        **kwargs
    ) -> Awaitable[Iterable[AnalyticsLink]]:
        super().get_links(options, **kwargs)
