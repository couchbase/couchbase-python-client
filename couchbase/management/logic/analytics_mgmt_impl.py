#  Copyright 2016-2023. Couchbase, Inc.
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
                    Dict,
                    Iterable)

from couchbase.management.logic.analytics_mgmt_req_builder import AnalyticsMgmtRequestBuilder
from couchbase.management.logic.analytics_mgmt_types import (AnalyticsDataset,
                                                             AnalyticsIndex,
                                                             AnalyticsLink,
                                                             AzureBlobExternalAnalyticsLink,
                                                             CouchbaseRemoteAnalyticsLink,
                                                             S3ExternalAnalyticsLink)

if TYPE_CHECKING:
    from couchbase.logic.client_adapter import ClientAdapter
    from couchbase.management.logic.analytics_mgmt_types import (ConnectLinkRequest,
                                                                 CreateDatasetRequest,
                                                                 CreateDataverseRequest,
                                                                 CreateIndexRequest,
                                                                 CreateLinkRequest,
                                                                 DisconnectLinkRequest,
                                                                 DropDatasetRequest,
                                                                 DropDataverseRequest,
                                                                 DropIndexRequest,
                                                                 DropLinkRequest,
                                                                 GetAllDatasetsRequest,
                                                                 GetAllIndexesRequest,
                                                                 GetLinksRequest,
                                                                 GetPendingMutationsRequest,
                                                                 ReplaceLinkRequest)


class AnalyticsMgmtImpl:
    def __init__(self, client_adapter: ClientAdapter) -> None:
        self._client_adapter = client_adapter
        self._request_builder = AnalyticsMgmtRequestBuilder()

    @property
    def request_builder(self) -> AnalyticsMgmtRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    def connect_link(self, req: ConnectLinkRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def create_dataset(self, req: CreateDatasetRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def create_dataverse(self, req: CreateDataverseRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def create_index(self, req: CreateIndexRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def create_link(self, req: CreateLinkRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def disconnect_link(self, req: DisconnectLinkRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_dataset(self, req: DropDatasetRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_dataverse(self, req: DropDataverseRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_index(self, req: DropIndexRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def drop_link(self, req: DropLinkRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)

    def get_all_datasets(self, req: GetAllDatasetsRequest) -> Iterable[AnalyticsDataset]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req)
        raw_datasets = ret.raw_result['datasets']
        return [AnalyticsDataset(**ds) for ds in raw_datasets]

    def get_all_indexes(self, req: GetAllIndexesRequest) -> Iterable[AnalyticsIndex]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req)
        raw_indexes = ret.raw_result['indexes']
        return [AnalyticsIndex(**ds) for ds in raw_indexes]

    def get_links(self, req: GetLinksRequest) -> Iterable[AnalyticsLink]:
        """**INTERNAL**"""
        analytics_links = []
        ret = self._client_adapter.execute_mgmt_request(req)
        cb_links = ret.raw_result.get('couchbase_links', None)
        if cb_links and len(cb_links) > 0:
            analytics_links.extend(map(lambda l: CouchbaseRemoteAnalyticsLink.link_from_server_json(l), cb_links))
        s3_links = ret.raw_result.get('s3_links', None)
        if s3_links and len(s3_links) > 0:
            analytics_links.extend(map(lambda l: S3ExternalAnalyticsLink.link_from_server_json(l), s3_links))
        azure_blob_links = ret.raw_result.get('azure_blob_links', None)
        if azure_blob_links and len(azure_blob_links) > 0:
            analytics_links.extend(
                map(lambda l: AzureBlobExternalAnalyticsLink.link_from_server_json(l), azure_blob_links))

        return analytics_links

    def get_pending_mutations(self, req: GetPendingMutationsRequest) -> Dict[str, int]:
        """**INTERNAL**"""
        ret = self._client_adapter.execute_mgmt_request(req)
        return ret.raw_result.get('stats', {})

    def replace_link(self, req: ReplaceLinkRequest) -> None:
        """**INTERNAL**"""
        self._client_adapter.execute_mgmt_request(req)
