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

from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.operation_types import AnalyticsMgmtOperationType
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
    from couchbase.logic.observability import ObservabilityInstruments


class AnalyticsIndexManager:

    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._impl = AnalyticsMgmtImpl(client_adapter, observability_instruments)

    def create_dataverse(self,
                         dataverse_name,    # type: str
                         options=None,      # type: Optional[CreateDataverseOptions]
                         **kwargs           # type: Any
                         ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsDataverseCreate
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_dataverse_request(
                dataverse_name, obs_handler, options, **kwargs)
            self._impl.create_dataverse(req, obs_handler)

    def drop_dataverse(self,
                       dataverse_name,    # type: str
                       options=None,      # type: Optional[DropDataverseOptions]
                       **kwargs           # type: Any
                       ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsDataverseDrop
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_dataverse_request(
                dataverse_name, obs_handler, options, **kwargs)
            self._impl.drop_dataverse(req, obs_handler)

    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       options=None,    # type: Optional[CreateDatasetOptions]
                       **kwargs         # type: Any
                       ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsDatasetCreate
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_dataset_request(
                dataset_name, bucket_name, obs_handler, options, **kwargs)
            self._impl.create_dataset(req, obs_handler)

    def drop_dataset(self,
                     dataset_name,  # type: str
                     options=None,  # type: Optional[DropDatasetOptions]
                     **kwargs       # type: Any
                     ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsDatasetDrop
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_dataset_request(dataset_name, obs_handler, options, **kwargs)
            self._impl.drop_dataset(req, obs_handler)

    def get_all_datasets(self,
                         options=None,   # type: Optional[GetAllDatasetOptions]
                         **kwargs   # type: Any
                         ) -> Iterable[AnalyticsDataset]:
        op_type = AnalyticsMgmtOperationType.AnalyticsDatasetGetAll
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_datasets_request(obs_handler, options, **kwargs)
            return self._impl.get_all_datasets(req, obs_handler)

    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type: Dict[str, AnalyticsDataType]
                     options=None,  # type: Optional[CreateAnalyticsIndexOptions]
                     **kwargs       # type: Any
                     ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsIndexCreate
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_index_request(index_name,
                                                                        dataset_name,
                                                                        fields,
                                                                        obs_handler,
                                                                        options,
                                                                        **kwargs)
            self._impl.create_index(req, obs_handler)

    def drop_index(self,
                   index_name,    # type: str
                   dataset_name,  # type: str
                   options=None,  # type: Optional[DropAnalyticsIndexOptions]
                   **kwargs       # type: Any
                   ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsIndexDrop
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_index_request(
                index_name, dataset_name, obs_handler, options, **kwargs)
            self._impl.drop_index(req, obs_handler)

    def get_all_indexes(self,
                        options=None,   # type: Optional[GetAllAnalyticsIndexesOptions]
                        **kwargs   # type: Any
                        ) -> Iterable[AnalyticsIndex]:
        op_type = AnalyticsMgmtOperationType.AnalyticsIndexGetAll
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_all_indexes_request(obs_handler, options, **kwargs)
            return self._impl.get_all_indexes(req, obs_handler)

    def connect_link(self,
                     options=None,  # type: Optional[ConnectLinkOptions]
                     **kwargs   # type: Any
                     ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkConnect
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_connect_link_request(obs_handler, options, **kwargs)
            self._impl.connect_link(req, obs_handler)

    def disconnect_link(self,
                        options=None,  # type: Optional[DisconnectLinkOptions]
                        **kwargs   # type: Any
                        ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkDisconnect
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_disconnect_link_request(obs_handler, options, **kwargs)
            self._impl.disconnect_link(req, obs_handler)

    def get_pending_mutations(self,
                              options=None,     # type: Optional[GetPendingMutationsOptions]
                              **kwargs     # type: Any
                              ) -> Dict[str, int]:
        op_type = AnalyticsMgmtOperationType.AnalyticsGetPendingMutations
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_pending_mutations_request(obs_handler, options, **kwargs)
            return self._impl.get_pending_mutations(req, obs_handler)

    def create_link(self,
                    link,  # type: AnalyticsLink
                    options=None,     # type: Optional[CreateLinkAnalyticsOptions]
                    **kwargs           # type: Any
                    ) -> None:
        # We choose AnalyticsLinkCreateCouchbaseRemoteLink arbitrarily b/c the ObservableRequestHandler will
        # translate all LinkCreate options in AnalyticsMgmtOperationType to the appropriate op name and
        # the request builder will build the appropriate request type based on the link passed in.
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkCreateCouchbaseRemoteLink
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_create_link_request(link, obs_handler, options, **kwargs)
            self._impl.create_link(req, obs_handler)

    def replace_link(self,
                     link,  # type: AnalyticsLink
                     options=None,     # type: Optional[ReplaceLinkAnalyticsOptions]
                     **kwargs           # type: Any
                     ) -> None:
        # We choose AnalyticsLinkReplaceCouchbaseRemoteLink arbitrarily b/c the ObservableRequestHandler will
        # translate all LinkReplace options in AnalyticsMgmtOperationType to the appropriate op name and
        # the request builder will build the appropriate request type based on the link passed in.
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkReplaceCouchbaseRemoteLink
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_replace_link_request(link, obs_handler, options, **kwargs)
            self._impl.replace_link(req, obs_handler)

    def drop_link(self,
                  link_name,  # type: str
                  dataverse_name,  # type: str
                  options=None,     # type: Optional[DropLinkAnalyticsOptions]
                  **kwargs        # type: Any
                  ) -> None:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkDrop
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_drop_link_request(
                link_name, dataverse_name, obs_handler, options, **kwargs)
            self._impl.drop_link(req, obs_handler)

    def get_links(self,
                  options=None,  # type: Optional[GetLinksAnalyticsOptions]
                  **kwargs        # type: Any
                  ) -> Iterable[AnalyticsLink]:
        op_type = AnalyticsMgmtOperationType.AnalyticsLinkGetAll
        with ObservableRequestHandler(op_type, self._impl.observability_instruments) as obs_handler:
            req = self._impl.request_builder.build_get_links_request(obs_handler, options, **kwargs)
            return self._impl.get_links(req, obs_handler)
