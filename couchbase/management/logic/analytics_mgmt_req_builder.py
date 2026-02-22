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
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.management.logic.analytics_mgmt_types import (ANALYTICS_MGMT_ERROR_MAP,
                                                             AnalyticsDataType,
                                                             AnalyticsLinkType,
                                                             ConnectLinkRequest,
                                                             CreateAzureBlobExternalLinkRequest,
                                                             CreateCouchbaseRemoteLinkRequest,
                                                             CreateDatasetRequest,
                                                             CreateDataverseRequest,
                                                             CreateIndexRequest,
                                                             CreateLinkRequest,
                                                             CreateS3ExternalLinkRequest,
                                                             DisconnectLinkRequest,
                                                             DropDatasetRequest,
                                                             DropDataverseRequest,
                                                             DropIndexRequest,
                                                             DropLinkRequest,
                                                             GetAllDatasetsRequest,
                                                             GetAllIndexesRequest,
                                                             GetLinksRequest,
                                                             GetPendingMutationsRequest,
                                                             ReplaceAzureBlobExternalLinkRequest,
                                                             ReplaceCouchbaseRemoteLinkRequest,
                                                             ReplaceLinkRequest,
                                                             ReplaceS3ExternalLinkRequest)
from couchbase.options import forward_args

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

    def build_connect_link_request(self,
                                   obs_handler: ObservableRequestHandler = None,
                                   *options: object,
                                   **kwargs: object) -> ConnectLinkRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        timeout = final_args.pop('timeout', None)
        req = ConnectLinkRequest(self._error_map, **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_dataset_request(self,
                                     dataset_name: str,
                                     bucket_name: str,
                                     obs_handler: ObservableRequestHandler = None,
                                     *options: object,
                                     **kwargs: object) -> CreateDatasetRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_dataset_name(dataset_name, 'creating an analytics dataset')
        self._validate_bucket_name(bucket_name, 'creating an analytics dataset')
        timeout = final_args.pop('timeout', None)
        req = CreateDatasetRequest(self._error_map,
                                   dataset_name=dataset_name,
                                   bucket_name=bucket_name,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_dataverse_request(self,
                                       dataverse_name: str,
                                       obs_handler: ObservableRequestHandler = None,
                                       *options: object,
                                       **kwargs: object) -> CreateDataverseRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_dataverse_name(dataverse_name, 'creating an analytics dataverse')
        timeout = final_args.pop('timeout', None)
        req = CreateDataverseRequest(self._error_map,
                                     dataverse_name=dataverse_name,
                                     **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_index_request(self,
                                   index_name: str,
                                   dataset_name: str,
                                   fields: Dict[str, AnalyticsDataType],
                                   obs_handler: ObservableRequestHandler = None,
                                   *options: object,
                                   **kwargs: object) -> CreateIndexRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_index_name(index_name, 'creating an analytics index')
        self._validate_dataset_name(dataset_name, 'creating an analytics index')
        self._validate_fields(fields, 'creating an analytics index')
        fields = {k: v.value for k, v in fields.items()}
        timeout = final_args.pop('timeout', None)
        req = CreateIndexRequest(self._error_map,
                                 index_name=index_name,
                                 dataset_name=dataset_name,
                                 fields=fields,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_link_request(self,
                                  link: AnalyticsLink,
                                  obs_handler: ObservableRequestHandler = None,
                                  *options: object,
                                  **kwargs: object) -> CreateLinkRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        link.validate()
        link_dict = link.as_dict()
        timeout = final_args.pop('timeout', None)
        if link.link_type() == AnalyticsLinkType.AzureBlobExternal:
            req = CreateAzureBlobExternalLinkRequest(self._error_map, link=link_dict, **final_args)
        elif link.link_type() == AnalyticsLinkType.CouchbaseRemote:
            req = CreateCouchbaseRemoteLinkRequest(self._error_map, link=link_dict, **final_args)
        else:
            req = CreateS3ExternalLinkRequest(self._error_map, link=link_dict, **final_args)

        if timeout is not None:
            req.timeout = timeout

        return req

    def build_disconnect_link_request(self,
                                      obs_handler: ObservableRequestHandler = None,
                                      *options: object,
                                      **kwargs: object) -> DisconnectLinkRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        timeout = final_args.pop('timeout', None)
        req = DisconnectLinkRequest(self._error_map, **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_dataset_request(self,
                                   dataset_name: str,
                                   obs_handler: ObservableRequestHandler = None,
                                   *options: object,
                                   **kwargs: object) -> DropDatasetRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_dataset_name(dataset_name, 'dropping an analytics dataset')
        timeout = final_args.pop('timeout', None)
        ignore_if_does_not_exist = final_args.pop('ignore_if_not_exists', None)
        req = DropDatasetRequest(self._error_map,
                                 dataset_name=dataset_name,
                                 ignore_if_does_not_exist=ignore_if_does_not_exist,
                                 **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_dataverse_request(self,
                                     dataverse_name: str,
                                     obs_handler: ObservableRequestHandler = None,
                                     *options: object,
                                     **kwargs: object) -> DropDataverseRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_dataverse_name(dataverse_name, 'dropping an analytics dataverse')
        timeout = final_args.pop('timeout', None)
        ignore_if_does_not_exist = final_args.pop('ignore_if_not_exists', None)
        req = DropDataverseRequest(self._error_map,
                                   dataverse_name=dataverse_name,
                                   ignore_if_does_not_exist=ignore_if_does_not_exist,
                                   **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_index_request(self,
                                 index_name: str,
                                 dataset_name: str,
                                 obs_handler: ObservableRequestHandler = None,
                                 *options: object,
                                 **kwargs: object) -> DropIndexRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_index_name(index_name, 'dropping an analytics index')
        self._validate_dataset_name(dataset_name, 'dropping an analytics index')
        timeout = final_args.pop('timeout', None)
        ignore_if_does_not_exist = final_args.pop('ignore_if_not_exists', None)
        req = DropIndexRequest(self._error_map,
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
                                obs_handler: ObservableRequestHandler = None,
                                *options: object,
                                **kwargs: object) -> DropLinkRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_link_name(link_name, 'dropping an analytics link')
        self._validate_dataverse_name(dataverse_name, 'dropping an analytics link')
        timeout = final_args.pop('timeout', None)
        req = DropLinkRequest(self._error_map,
                              link_name=link_name,
                              dataverse_name=dataverse_name,
                              **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_datasets_request(self,
                                       obs_handler: ObservableRequestHandler = None,
                                       *options: object,
                                       **kwargs: object) -> GetAllDatasetsRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        timeout = final_args.pop('timeout', None)
        req = GetAllDatasetsRequest(self._error_map, **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_indexes_request(self,
                                      obs_handler: ObservableRequestHandler = None,
                                      *options: object,
                                      **kwargs: object) -> GetAllIndexesRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        timeout = final_args.pop('timeout', None)
        req = GetAllIndexesRequest(self._error_map, **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_links_request(self,
                                obs_handler: ObservableRequestHandler = None,
                                *options: object,
                                **kwargs: object) -> GetLinksRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        timeout = final_args.pop('timeout', None)
        link_name = final_args.pop('name', None)
        link_type = final_args.pop('link_type', None)
        if link_type:
            if isinstance(link_type, AnalyticsLinkType):
                link_type = link_type.value
        req = GetLinksRequest(self._error_map, link_name=link_name, link_type=link_type, **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_pending_mutations_request(self,
                                            obs_handler: ObservableRequestHandler = None,
                                            *options: object,
                                            **kwargs: object) -> GetPendingMutationsRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        timeout = final_args.pop('timeout', None)
        req = GetPendingMutationsRequest(self._error_map, **final_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_replace_link_request(self,
                                   link: AnalyticsLink,
                                   obs_handler: ObservableRequestHandler = None,
                                   *options: object,
                                   **kwargs: object) -> ReplaceLinkRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        link_dict = link.as_dict()
        timeout = final_args.pop('timeout', None)
        if link.link_type() == AnalyticsLinkType.AzureBlobExternal:
            req = ReplaceAzureBlobExternalLinkRequest(self._error_map, link=link_dict, **final_args)
        elif link.link_type() == AnalyticsLinkType.CouchbaseRemote:
            req = ReplaceCouchbaseRemoteLinkRequest(self._error_map, link=link_dict, **final_args)
        else:
            req = ReplaceS3ExternalLinkRequest(self._error_map, link=link_dict, **final_args)

        if timeout is not None:
            req.timeout = timeout

        return req
