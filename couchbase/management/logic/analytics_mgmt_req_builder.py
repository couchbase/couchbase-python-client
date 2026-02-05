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

from typing import TYPE_CHECKING, Dict

from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.analytics_mgmt_types import (ANALYTICS_MGMT_ERROR_MAP,
                                                             AnalyticsDataType,
                                                             AnalyticsLinkType,
                                                             ConnectLinkRequest,
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
from couchbase.options import forward_args
from couchbase.pycbc_core import analytics_mgmt_operations, mgmt_operations

if TYPE_CHECKING:
    from couchbase.management.logic.analytics_mgmt_types import AnalyticsLink


class AnalyticsMgmtRequestBuilder:

    def __init__(self) -> None:
        self._error_map = ANALYTICS_MGMT_ERROR_MAP

    def _validate_bucket_name(self, bucket_name: str, msg_suffix: str) -> None:
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException(f'The bucket_name must be provided when {msg_suffix}.')

    def _validate_dataset_name(self, dataset_name: str, msg_suffix: str) -> None:
        if not isinstance(dataset_name, str):
            raise InvalidArgumentException(f'The dataset_name must be provided when {msg_suffix}.')

    def _validate_dataverse_name(self, dataverse_name: str, msg_suffix: str) -> None:
        if not isinstance(dataverse_name, str):
            raise InvalidArgumentException(f'The dataverse_name must be provided when {msg_suffix}.')

    def _validate_fields(self, fields: Dict[str, AnalyticsDataType], msg_suffix: str) -> None:
        if fields is not None:
            if not isinstance(fields, dict):
                raise InvalidArgumentException(f'fields must be provided when {msg_suffix}.')

            if not all(map(lambda v: isinstance(v, AnalyticsDataType), fields.values())):
                raise InvalidArgumentException('All fields must be an AnalyticsDataType.')

    def _validate_index_name(self, index_name: str, msg_suffix: str) -> None:
        if not isinstance(index_name, str):
            raise InvalidArgumentException(f'The index_name must be provided when {msg_suffix}.')

    def _validate_link_name(self, link_name: str, msg_suffix: str) -> None:
        if not isinstance(link_name, str):
            raise InvalidArgumentException(f'The link_name must be provided when {msg_suffix}.')

    def build_connect_link_request(self, *options: object, **kwargs: object) -> ConnectLinkRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = ConnectLinkRequest(self._error_map,
                                 mgmt_operations.ANALYTICS.value,
                                 analytics_mgmt_operations.LINK_CONNECT.value,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_dataset_request(self,
                                     dataset_name: str,
                                     bucket_name: str,
                                     *options: object,
                                     **kwargs: object) -> CreateDatasetRequest:
        self._validate_dataset_name(dataset_name, 'creating an analytics dataset')
        self._validate_bucket_name(bucket_name, 'creating an analytics dataset')
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = CreateDatasetRequest(self._error_map,
                                   mgmt_operations.ANALYTICS.value,
                                   analytics_mgmt_operations.CREATE_DATASET.value,
                                   dataset_name=dataset_name,
                                   bucket_name=bucket_name,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_dataverse_request(self,
                                       dataverse_name: str,
                                       *options: object,
                                       **kwargs: object) -> CreateDataverseRequest:
        self._validate_dataverse_name(dataverse_name, 'creating an analytics dataverse')
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = CreateDataverseRequest(self._error_map,
                                     mgmt_operations.ANALYTICS.value,
                                     analytics_mgmt_operations.CREATE_DATAVERSE.value,
                                     dataverse_name=dataverse_name,
                                     **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_index_request(self,
                                   index_name: str,
                                   dataset_name: str,
                                   fields: Dict[str, AnalyticsDataType],
                                   *options: object,
                                   **kwargs: object) -> CreateIndexRequest:
        self._validate_index_name(index_name, 'creating an analytics index')
        self._validate_dataset_name(dataset_name, 'creating an analytics index')
        self._validate_fields(fields, 'creating an analytics index')
        final_args = forward_args(kwargs, *options)
        fields = {k: v.value for k, v in fields.items()}
        timeout = final_args.pop('timeout', None)
        req = CreateIndexRequest(self._error_map,
                                 mgmt_operations.ANALYTICS.value,
                                 analytics_mgmt_operations.CREATE_INDEX.value,
                                 index_name=index_name,
                                 dataset_name=dataset_name,
                                 fields=fields,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_link_request(self,
                                  link: AnalyticsLink,
                                  *options: object,
                                  **kwargs: object) -> CreateLinkRequest:
        final_args = forward_args(kwargs, *options)
        link.validate()
        link_dict = link.as_dict()
        link_type = link.link_type().value
        timeout = final_args.pop('timeout', None)
        req = CreateLinkRequest(self._error_map,
                                mgmt_operations.ANALYTICS.value,
                                analytics_mgmt_operations.LINK_CREATE.value,
                                link=link_dict,
                                link_type=link_type,
                                **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_disconnect_link_request(self, *options: object, **kwargs: object) -> DisconnectLinkRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = DisconnectLinkRequest(self._error_map,
                                    mgmt_operations.ANALYTICS.value,
                                    analytics_mgmt_operations.LINK_DISCONNECT.value,
                                    **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_dataset_request(self,
                                   dataset_name: str,
                                   *options: object,
                                   **kwargs: object) -> DropDatasetRequest:
        self._validate_dataset_name(dataset_name, 'dropping an analytics dataset')
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        ignore_if_does_not_exist = final_args.pop('ignore_if_not_exists', None)
        req = DropDatasetRequest(self._error_map,
                                 mgmt_operations.ANALYTICS.value,
                                 analytics_mgmt_operations.DROP_DATASET.value,
                                 dataset_name=dataset_name,
                                 ignore_if_does_not_exist=ignore_if_does_not_exist,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_dataverse_request(self,
                                     dataverse_name: str,
                                     *options: object,
                                     **kwargs: object) -> DropDataverseRequest:
        self._validate_dataverse_name(dataverse_name, 'dropping an analytics dataverse')
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        ignore_if_does_not_exist = final_args.pop('ignore_if_not_exists', None)
        req = DropDataverseRequest(self._error_map,
                                   mgmt_operations.ANALYTICS.value,
                                   analytics_mgmt_operations.DROP_DATAVERSE.value,
                                   dataverse_name=dataverse_name,
                                   ignore_if_does_not_exist=ignore_if_does_not_exist,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_index_request(self,
                                 index_name: str,
                                 dataset_name: str,
                                 *options: object,
                                 **kwargs: object) -> DropIndexRequest:
        self._validate_index_name(index_name, 'dropping an analytics index')
        self._validate_dataset_name(dataset_name, 'dropping an analytics index')
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        ignore_if_does_not_exist = final_args.pop('ignore_if_not_exists', None)
        req = DropIndexRequest(self._error_map,
                               mgmt_operations.ANALYTICS.value,
                               analytics_mgmt_operations.DROP_INDEX.value,
                               index_name=index_name,
                               dataset_name=dataset_name,
                               ignore_if_does_not_exist=ignore_if_does_not_exist,
                               **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_link_request(self,
                                link_name: str,
                                dataverse_name: str,
                                *options: object,
                                **kwargs: object) -> DropLinkRequest:
        self._validate_link_name(link_name, 'dropping an analytics link')
        self._validate_dataverse_name(dataverse_name, 'dropping an analytics link')
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = DropLinkRequest(self._error_map,
                              mgmt_operations.ANALYTICS.value,
                              analytics_mgmt_operations.DROP_LINK.value,
                              link_name=link_name,
                              dataverse_name=dataverse_name,
                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_datasets_request(self, *options: object, **kwargs: object) -> GetAllDatasetsRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetAllDatasetsRequest(self._error_map,
                                    mgmt_operations.ANALYTICS.value,
                                    analytics_mgmt_operations.GET_ALL_DATASETS.value,
                                    **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_indexes_request(self, *options: object, **kwargs: object) -> GetAllIndexesRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetAllIndexesRequest(self._error_map,
                                   mgmt_operations.ANALYTICS.value,
                                   analytics_mgmt_operations.GET_ALL_INDEXES.value,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_links_request(self, *options: object, **kwargs: object) -> GetLinksRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        link_name = final_args.pop('name', None)
        link_type = final_args.pop('link_type', None)
        if link_type:
            if isinstance(link_type, AnalyticsLinkType):
                link_type = link_type.value
        req = GetLinksRequest(self._error_map,
                              mgmt_operations.ANALYTICS.value,
                              analytics_mgmt_operations.GET_ALL_LINKS.value,
                              link_name=link_name,
                              link_type=link_type,
                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_pending_mutations_request(self, *options: object, **kwargs: object) -> GetPendingMutationsRequest:
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        req = GetPendingMutationsRequest(self._error_map,
                                         mgmt_operations.ANALYTICS.value,
                                         analytics_mgmt_operations.GET_PENDING_MUTATIONS.value,
                                         **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_replace_link_request(self,
                                   link: AnalyticsLink,
                                   *options: object,
                                   **kwargs: object) -> ReplaceLinkRequest:
        final_args = forward_args(kwargs, *options)
        link_dict = link.as_dict()
        link_type = link.link_type().value
        timeout = final_args.pop('timeout', None)
        req = ReplaceLinkRequest(self._error_map,
                                 mgmt_operations.ANALYTICS.value,
                                 analytics_mgmt_operations.LINK_REPLACE.value,
                                 link=link_dict,
                                 link_type=link_type,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req
