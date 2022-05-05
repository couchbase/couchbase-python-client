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
                    Iterable,
                    Optional)

from twisted.internet.defer import Deferred

from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.analytics_logic import (AnalyticsDataset,
                                                        AnalyticsDataType,
                                                        AnalyticsIndex,
                                                        AnalyticsLink,
                                                        AnalyticsManagerLogic)

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

    def create_dataverse(self,
                         dataverse_name,    # type: str
                         options=None,      # type: Optional[CreateDataverseOptions]
                         **kwargs           # type: Dict[str, Any]
                         ) -> Deferred[None]:

        if not isinstance(dataverse_name, str):
            raise ValueError("dataverse_name must be provided when creating an analytics dataverse.")

        return Deferred.fromFuture(super().create_dataverse(dataverse_name, options, **kwargs))

    def drop_dataverse(self,
                       dataverse_name,    # type: str
                       options=None,      # type: Optional[DropDataverseOptions]
                       **kwargs           # type: Dict[str, Any]
                       ) -> Deferred[None]:

        if not isinstance(dataverse_name, str):
            raise ValueError("dataverse_name must be provided when dropping an analytics dataverse.")

        return Deferred.fromFuture(super().drop_dataverse(dataverse_name, options, **kwargs))

    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       options=None,    # type: Optional[CreateDatasetOptions]
                       **kwargs         # type: Dict[str, Any]
                       ) -> Deferred[None]:

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when creating an analytics dataset.")

        if not isinstance(bucket_name, str):
            raise ValueError("bucket_name must be provided when creating an analytics dataset.")

        return Deferred.fromFuture(super().create_dataset(dataset_name, bucket_name, options, **kwargs))

    def drop_dataset(self,
                     dataset_name,  # type: str
                     options=None,  # type: Optional[DropDatasetOptions]
                     **kwargs       # type: Dict[str, Any]
                     ) -> Deferred[None]:

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when dropping an analytics dataset.")

        return Deferred.fromFuture(super().drop_dataset(dataset_name, options, **kwargs))

    def get_all_datasets(self,
                         options=None,   # type: Optional[GetAllDatasetOptions]
                         **kwargs   # type: Dict[str, Any]
                         ) -> Deferred[Iterable[AnalyticsDataset]]:

        return Deferred.fromFuture(super().get_all_datasets(options, **kwargs))

    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type: Dict[str, AnalyticsDataType]
                     options=None,  # type: Optional[CreateAnalyticsIndexOptions]
                     **kwargs       # type: Dict[str, Any]
                     ) -> Deferred[None]:

        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when creating an analytics index.")

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when creating an analytics index.")

        if fields is not None:
            if not isinstance(fields, dict):
                raise ValueError("fields must be provided when creating an analytics index.")

            if not all(map(lambda v: isinstance(v, AnalyticsDataType), fields.values())):
                raise InvalidArgumentException("fields must all be an AnalyticsDataType.")

        return Deferred.fromFuture(super().create_index(index_name, dataset_name, fields, options, **kwargs))

    def drop_index(self,
                   index_name,    # type: str
                   dataset_name,  # type: str
                   options=None,  # type: Optional[DropAnalyticsIndexOptions]
                   **kwargs       # type: Dict[str, Any]
                   ) -> Deferred[None]:

        if not isinstance(index_name, str):
            raise ValueError("index_name must be provided when dropping an analytics index.")

        if not isinstance(dataset_name, str):
            raise ValueError("dataset_name must be provided when dropping an analytics index.")

        return Deferred.fromFuture(super().drop_index(index_name, dataset_name, options, **kwargs))

    def get_all_indexes(self,
                        options=None,   # type: Optional[GetAllAnalyticsIndexesOptions]
                        **kwargs   # type: Dict[str, Any]
                        ) -> Deferred[Iterable[AnalyticsIndex]]:

        return Deferred.fromFuture(super().get_all_indexes(options, **kwargs))

    def connect_link(self,
                     options=None,  # type: Optional[ConnectLinkOptions]
                     **kwargs   # type: Dict[str, Any]
                     ) -> Deferred[None]:
        return Deferred.fromFuture(super().connect_link(options, **kwargs))

    def disconnect_link(self,
                        options=None,  # type: Optional[DisconnectLinkOptions]
                        **kwargs   # type: Dict[str, Any]
                        ) -> Deferred[None]:
        return Deferred.fromFuture(super().disconnect_link(options, **kwargs))

    def get_pending_mutations(self,
                              options=None,     # type: Optional[GetPendingMutationsOptions]
                              **kwargs     # type: Dict[str, Any]
                              ) -> Dict[str, int]:

        return Deferred.fromFuture(super().get_pending_mutations(options, **kwargs))

    def create_link(
        self,
        link,  # type: AnalyticsLink
        options=None,     # type: Optional[CreateLinkAnalyticsOptions]
        **kwargs
    ) -> Deferred[None]:
        return Deferred.fromFuture(super().create_link(link, options, **kwargs))

    def replace_link(
        self,
        link,  # type: AnalyticsLink
        options=None,     # type: Optional[ReplaceLinkAnalyticsOptions]
        **kwargs
    ) -> Deferred[None]:
        return Deferred.fromFuture(super().replace_link(link, options, **kwargs))

    def drop_link(
        self,
        link_name,  # type: str
        dataverse_name,  # type: str
        options=None,     # type: Optional[DropLinkAnalyticsOptions]
        **kwargs
    ) -> Deferred[None]:

        if not isinstance(link_name, str):
            raise ValueError("link_name must be provided when dropping an analytics link.")

        if not isinstance(dataverse_name, str):
            raise ValueError("dataverse_name must be provided when dropping an analytics link.")

        return Deferred.fromFuture(super().drop_link(link_name, dataverse_name, options, **kwargs))

    def get_links(
        self,
        options=None,  # type: Optional[GetLinksAnalyticsOptions]
        **kwargs
    ) -> Deferred[Iterable[AnalyticsLink]]:
        return Deferred.fromFuture(super().get_links(options, **kwargs))
