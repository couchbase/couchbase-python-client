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

from couchbase.management.logic.analytics_mgmt_impl import AnalyticsMgmtImpl
from couchbase.management.logic.analytics_mgmt_types import AnalyticsEncryptionLevel  # noqa: F401
from couchbase.management.logic.analytics_mgmt_types import AnalyticsLinkType  # noqa: F401
from couchbase.management.logic.analytics_mgmt_types import AzureBlobExternalAnalyticsLink  # noqa: F401
from couchbase.management.logic.analytics_mgmt_types import CouchbaseAnalyticsEncryptionSettings  # noqa: F401
from couchbase.management.logic.analytics_mgmt_types import CouchbaseRemoteAnalyticsLink  # noqa: F401
from couchbase.management.logic.analytics_mgmt_types import S3ExternalAnalyticsLink  # noqa: F401
from couchbase.management.logic.analytics_mgmt_types import (AnalyticsDataset,
                                                             AnalyticsDataType,
                                                             AnalyticsIndex,
                                                             AnalyticsLink)

# @TODO:  lets deprecate import of options from couchbase.management.analytics
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

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter


class AnalyticsIndexManager:

    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._impl = AnalyticsMgmtImpl(client_adapter)

    def create_dataverse(self,
                         dataverse_name,    # type: str
                         options=None,      # type: Optional[CreateDataverseOptions]
                         **kwargs           # type: Any
                         ) -> None:
        req = self._impl.request_builder.build_create_dataverse_request(dataverse_name, options, **kwargs)
        self._impl.create_dataverse(req)

    def drop_dataverse(self,
                       dataverse_name,    # type: str
                       options=None,      # type: Optional[DropDataverseOptions]
                       **kwargs           # type: Any
                       ) -> None:
        req = self._impl.request_builder.build_drop_dataverse_request(dataverse_name, options, **kwargs)
        self._impl.drop_dataverse(req)

    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       options=None,    # type: Optional[CreateDatasetOptions]
                       **kwargs         # type: Any
                       ) -> None:
        req = self._impl.request_builder.build_create_dataset_request(dataset_name, bucket_name, options, **kwargs)
        self._impl.create_dataset(req)

    def drop_dataset(self,
                     dataset_name,  # type: str
                     options=None,  # type: Optional[DropDatasetOptions]
                     **kwargs       # type: Any
                     ) -> None:
        req = self._impl.request_builder.build_drop_dataset_request(dataset_name, options, **kwargs)
        self._impl.drop_dataset(req)

    def get_all_datasets(self,
                         options=None,   # type: Optional[GetAllDatasetOptions]
                         **kwargs   # type: Any
                         ) -> Iterable[AnalyticsDataset]:

        req = self._impl.request_builder.build_get_all_datasets_request(options, **kwargs)
        return self._impl.get_all_datasets(req)

    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type: Dict[str, AnalyticsDataType]
                     options=None,  # type: Optional[CreateAnalyticsIndexOptions]
                     **kwargs       # type: Any
                     ) -> None:
        req = self._impl.request_builder.build_create_index_request(index_name,
                                                                    dataset_name,
                                                                    fields,
                                                                    options,
                                                                    **kwargs)
        self._impl.create_index(req)

    def drop_index(self,
                   index_name,    # type: str
                   dataset_name,  # type: str
                   options=None,  # type: Optional[DropAnalyticsIndexOptions]
                   **kwargs       # type: Any
                   ) -> None:
        req = self._impl.request_builder.build_drop_index_request(index_name, dataset_name, options, **kwargs)
        self._impl.drop_index(req)

    def get_all_indexes(self,
                        options=None,   # type: Optional[GetAllAnalyticsIndexesOptions]
                        **kwargs   # type: Any
                        ) -> Iterable[AnalyticsIndex]:
        req = self._impl.request_builder.build_get_all_indexes_request(options, **kwargs)
        return self._impl.get_all_indexes(req)

    def connect_link(self,
                     options=None,  # type: Optional[ConnectLinkOptions]
                     **kwargs   # type: Any
                     ) -> None:
        req = self._impl.request_builder.build_connect_link_request(options, **kwargs)
        self._impl.connect_link(req)

    def disconnect_link(self,
                        options=None,  # type: Optional[DisconnectLinkOptions]
                        **kwargs   # type: Any
                        ) -> None:
        req = self._impl.request_builder.build_disconnect_link_request(options, **kwargs)
        self._impl.disconnect_link(req)

    def get_pending_mutations(self,
                              options=None,     # type: Optional[GetPendingMutationsOptions]
                              **kwargs     # type: Any
                              ) -> Dict[str, int]:
        req = self._impl.request_builder.build_get_pending_mutations_request(options, **kwargs)
        return self._impl.get_pending_mutations(req)

    def create_link(self,
                    link,  # type: AnalyticsLink
                    options=None,     # type: Optional[CreateLinkAnalyticsOptions]
                    **kwargs           # type: Any
                    ) -> None:
        req = self._impl.request_builder.build_create_link_request(link, options, **kwargs)
        self._impl.create_link(req)

    def replace_link(self,
                     link,  # type: AnalyticsLink
                     options=None,     # type: Optional[ReplaceLinkAnalyticsOptions]
                     **kwargs           # type: Any
                     ) -> None:
        req = self._impl.request_builder.build_replace_link_request(link, options, **kwargs)
        self._impl.replace_link(req)

    def drop_link(self,
                  link_name,  # type: str
                  dataverse_name,  # type: str
                  options=None,     # type: Optional[DropLinkAnalyticsOptions]
                  **kwargs        # type: Any
                  ) -> None:
        req = self._impl.request_builder.build_drop_link_request(link_name, dataverse_name, options, **kwargs)
        self._impl.drop_link(req)

    def get_links(self,
                  options=None,  # type: Optional[GetLinksAnalyticsOptions]
                  **kwargs        # type: Any
                  ) -> Iterable[AnalyticsLink]:
        req = self._impl.request_builder.build_get_links_request(options, **kwargs)
        return self._impl.get_links(req)
