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

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.management.analytics import (AnalyticsDataset,
                                                   AnalyticsIndex,
                                                   AnalyticsLink,
                                                   AzureBlobExternalAnalyticsLink,
                                                   CouchbaseRemoteAnalyticsLink,
                                                   S3ExternalAnalyticsLink)
from couchbase.protocol.classic._management._analytics_mgmt_req_builder import AnalyticsMgmtRequestBuilder

if TYPE_CHECKING:
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.analytics import AnalyticsDataType
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
    from couchbase.protocol.classic._client_adapter import ClientAdapter


class AnalyticsMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter, observability_instruments: ObservabilityInstruments) -> None:
        self._client_adapter = client_adapter
        self._request_builder = AnalyticsMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments

    def create_dataverse(self,
                         dataverse_name: str,
                         options: CreateDataverseOptions | None = None,
                         **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsDataverseCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_dataverse_request(dataverse_name, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_dataverse(self,
                       dataverse_name: str,
                       options: DropDataverseOptions | None = None,
                       **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsDataverseDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_dataverse_request(dataverse_name, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def create_dataset(self,
                       dataset_name: str,
                       bucket_name: str,
                       options: CreateDatasetOptions | None = None,
                       **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsDatasetCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_dataset_request(dataset_name,
                                                                     bucket_name,
                                                                     obs_handler,
                                                                     options,
                                                                     **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_dataset(self,
                     dataset_name: str,
                     options: DropDatasetOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsDatasetDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_dataset_request(dataset_name, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_datasets(self,
                         options: GetAllDatasetOptions | None = None,
                         **kwargs: Any) -> Iterable[AnalyticsDataset]:
        with ObservableRequestHandler(Op.AnalyticsDatasetGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_datasets_request(obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_datasets = ret.raw_result['datasets']
            return [AnalyticsDataset.from_server(ds) for ds in raw_datasets]

    def create_index(self,
                     index_name: str,
                     dataset_name: str,
                     fields: dict[str, AnalyticsDataType],
                     options: CreateAnalyticsIndexOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsIndexCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_index_request(index_name,
                                                                   dataset_name,
                                                                   fields,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_index(self,
                   index_name: str,
                   dataset_name: str,
                   options: DropAnalyticsIndexOptions | None = None,
                   **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsIndexDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_index_request(index_name,
                                                                 dataset_name,
                                                                 obs_handler,
                                                                 options,
                                                                 **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_indexes(self,
                        options: GetAllAnalyticsIndexesOptions | None = None,
                        **kwargs: Any) -> Iterable[AnalyticsIndex]:
        with ObservableRequestHandler(Op.AnalyticsIndexGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_indexes_request(obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            raw_indexes = ret.raw_result['indexes']
            return [AnalyticsIndex(**ds) for ds in raw_indexes]

    def connect_link(self, options: ConnectLinkOptions | None = None, **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsLinkConnect, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_connect_link_request(obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def disconnect_link(self, options: DisconnectLinkOptions | None = None, **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsLinkDisconnect, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_disconnect_link_request(obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_pending_mutations(self, options: GetPendingMutationsOptions | None = None, **kwargs: Any) -> dict[str, int]:
        with ObservableRequestHandler(Op.AnalyticsGetPendingMutations, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_pending_mutations_request(obs_handler, options, **kwargs)
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            return ret.raw_result.get('stats', {})

    def create_link(self,
                    link: AnalyticsLink,
                    options: CreateLinkAnalyticsOptions | None = None,
                    **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsLinkCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_link_request(link, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def replace_link(self,
                     link: AnalyticsLink,
                     options: ReplaceLinkAnalyticsOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsLinkReplace, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_replace_link_request(link, obs_handler, options, **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_link(self,
                  link_name: str,
                  dataverse_name: str,
                  options: DropLinkAnalyticsOptions | None = None,
                  **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.AnalyticsLinkDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_link_request(link_name,
                                                                dataverse_name,
                                                                obs_handler,
                                                                options,
                                                                **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_links(self, options: GetLinksAnalyticsOptions | None = None, **kwargs: Any) -> Iterable[AnalyticsLink]:
        with ObservableRequestHandler(Op.AnalyticsLinkGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_links_request(obs_handler, options, **kwargs)
            analytics_links: list[AnalyticsLink] = []
            ret = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
            cb_links = ret.raw_result.get('couchbase', None)
            if cb_links and len(cb_links) > 0:
                analytics_links.extend(map(lambda l: CouchbaseRemoteAnalyticsLink.link_from_server_json(l), cb_links))
            s3_links = ret.raw_result.get('s3', None)
            if s3_links and len(s3_links) > 0:
                analytics_links.extend(map(lambda l: S3ExternalAnalyticsLink.link_from_server_json(l), s3_links))
            azure_blob_links = ret.raw_result.get('azure_blob', None)
            if azure_blob_links and len(azure_blob_links) > 0:
                analytics_links.extend(
                    map(lambda l: AzureBlobExternalAnalyticsLink.link_from_server_json(l), azure_blob_links))

            return analytics_links
