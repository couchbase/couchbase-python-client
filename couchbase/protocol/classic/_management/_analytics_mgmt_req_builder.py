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

from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management.analytics import (AnalyticsDataType,
                                                   AnalyticsLink,
                                                   AnalyticsLinkType)
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
from couchbase.common.option_enums import OptionValidationMode
from couchbase.protocol.classic._management._analytics_mgmt_types import (ANALYTICS_MGMT_ERROR_MAP,
                                                                          AnalyticsMgmtOptionsBlock,
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
from couchbase.protocol.classic._wire import forward_args


class AnalyticsMgmtRequestBuilder:

    def __init__(self, option_validation: OptionValidationMode | None = None) -> None:
        self._error_map = ANALYTICS_MGMT_ERROR_MAP
        self._option_validation = option_validation

    def _validate_bucket_name(self, bucket_name: object, msg_suffix: str) -> str:
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException(f'The bucket_name must be provided when {msg_suffix}.')
        return bucket_name

    def _validate_dataset_name(self, dataset_name: object, msg_suffix: str) -> str:
        if not isinstance(dataset_name, str):
            raise InvalidArgumentException(f'The dataset_name must be provided when {msg_suffix}.')
        return dataset_name

    def _validate_dataverse_name(self, dataverse_name: object, msg_suffix: str) -> str:
        if not isinstance(dataverse_name, str):
            raise InvalidArgumentException(f'The dataverse_name must be provided when {msg_suffix}.')
        return dataverse_name

    def _validate_fields(self, fields: object, msg_suffix: str) -> dict[str, str]:
        if not isinstance(fields, dict):
            raise InvalidArgumentException(f'fields must be provided when {msg_suffix}.')

        if not all(isinstance(v, AnalyticsDataType) for v in fields.values()):
            raise InvalidArgumentException('All fields must be an AnalyticsDataType.')

        return {k: v.value for k, v in fields.items()}

    def _validate_index_name(self, index_name: object, msg_suffix: str) -> str:
        if not isinstance(index_name, str):
            raise InvalidArgumentException(f'The index_name must be provided when {msg_suffix}.')
        return index_name

    def _validate_link(self, link: object, msg_suffix: str) -> AnalyticsLink:
        if not isinstance(link, AnalyticsLink):
            raise InvalidArgumentException(f'An AnalyticsLink must be provided when {msg_suffix}.')
        return link

    @staticmethod
    def _link_to_binding_dict(link: AnalyticsLink) -> dict[str, Any]:
        # The binding reads the encryption level under 'level' and leaves a missing key at its default, none.
        link_dict = link.as_dict()
        encryption = link_dict.get('encryption')
        if isinstance(encryption, dict) and 'encryption_level' in encryption:
            link_dict['encryption'] = {('level' if k == 'encryption_level' else k): v for k, v in encryption.items()}
        return link_dict

    def _validate_link_name(self, link_name: object, msg_suffix: str) -> str:
        if not isinstance(link_name, str):
            raise InvalidArgumentException(f'The link_name must be provided when {msg_suffix}.')
        return link_name

    def build_connect_link_request(self,
                                   obs_handler: ObservableRequestHandler,
                                   options: ConnectLinkOptions | None = None,
                                   **kwargs: object) -> ConnectLinkRequest:
        final_args = forward_args(kwargs, options, block=AnalyticsMgmtOptionsBlock.ConnectLink,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return ConnectLinkRequest(self._error_map,
                                  dataverse_name=final_args.pop('dataverse_name', None),
                                  link_name=final_args.pop('link_name', None),
                                  force=final_args.pop('force', None),
                                  timeout=final_args.pop('timeout', None))

    def build_create_dataset_request(self,
                                     dataset_name: str,
                                     bucket_name: str,
                                     obs_handler: ObservableRequestHandler,
                                     options: CreateDatasetOptions | None = None,
                                     **kwargs: object) -> CreateDatasetRequest:
        final_args = forward_args(kwargs, options, block=AnalyticsMgmtOptionsBlock.CreateDataset,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        valid_dataset_name = self._validate_dataset_name(dataset_name, 'creating an analytics dataset')
        return CreateDatasetRequest(self._error_map,
                                    dataset_name=valid_dataset_name,
                                    bucket_name=self._validate_bucket_name(bucket_name,
                                                                           'creating an analytics dataset'),
                                    dataverse_name=final_args.pop('dataverse_name', None),
                                    ignore_if_exists=final_args.pop('ignore_if_exists', None),
                                    condition=final_args.pop('condition', None),
                                    timeout=final_args.pop('timeout', None))

    def build_create_dataverse_request(self,
                                       dataverse_name: str,
                                       obs_handler: ObservableRequestHandler,
                                       options: CreateDataverseOptions | None = None,
                                       **kwargs: object) -> CreateDataverseRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.CreateDataverse, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return CreateDataverseRequest(self._error_map,
                                      dataverse_name=self._validate_dataverse_name(dataverse_name,
                                                                                   'creating an analytics dataverse'),
                                      ignore_if_exists=final_args.pop('ignore_if_exists', None),
                                      timeout=final_args.pop('timeout', None))

    def build_create_index_request(self,
                                   index_name: str,
                                   dataset_name: str,
                                   fields: dict[str, AnalyticsDataType],
                                   obs_handler: ObservableRequestHandler,
                                   options: CreateAnalyticsIndexOptions | None = None,
                                   **kwargs: object) -> CreateIndexRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.CreateAnalyticsIndex, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        valid_index_name = self._validate_index_name(index_name, 'creating an analytics index')
        valid_dataset_name = self._validate_dataset_name(dataset_name, 'creating an analytics index')
        return CreateIndexRequest(self._error_map,
                                  index_name=valid_index_name,
                                  dataset_name=valid_dataset_name,
                                  fields=self._validate_fields(fields, 'creating an analytics index'),
                                  dataverse_name=final_args.pop('dataverse_name', None),
                                  ignore_if_exists=final_args.pop('ignore_if_exists', None),
                                  timeout=final_args.pop('timeout', None))

    def build_create_link_request(self,
                                  link: AnalyticsLink,
                                  obs_handler: ObservableRequestHandler,
                                  options: CreateLinkAnalyticsOptions | None = None,
                                  **kwargs: object) -> CreateLinkRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.CreateLinkAnalytics, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        valid_link = self._validate_link(link, 'creating an analytics link')
        valid_link.validate()
        link_dict = self._link_to_binding_dict(valid_link)
        timeout = final_args.pop('timeout', None)
        if valid_link.link_type() == AnalyticsLinkType.AzureBlobExternal:
            return CreateAzureBlobExternalLinkRequest(self._error_map, link=link_dict, timeout=timeout)
        if valid_link.link_type() == AnalyticsLinkType.CouchbaseRemote:
            return CreateCouchbaseRemoteLinkRequest(self._error_map, link=link_dict, timeout=timeout)
        return CreateS3ExternalLinkRequest(self._error_map, link=link_dict, timeout=timeout)

    def build_disconnect_link_request(self,
                                      obs_handler: ObservableRequestHandler,
                                      options: DisconnectLinkOptions | None = None,
                                      **kwargs: object) -> DisconnectLinkRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.DisconnectLink, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return DisconnectLinkRequest(self._error_map,
                                     dataverse_name=final_args.pop('dataverse_name', None),
                                     link_name=final_args.pop('link_name', None),
                                     timeout=final_args.pop('timeout', None))

    def build_drop_dataset_request(self,
                                   dataset_name: str,
                                   obs_handler: ObservableRequestHandler,
                                   options: DropDatasetOptions | None = None,
                                   **kwargs: object) -> DropDatasetRequest:
        final_args = forward_args(kwargs, options, block=AnalyticsMgmtOptionsBlock.DropDataset,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return DropDatasetRequest(self._error_map,
                                  dataset_name=self._validate_dataset_name(dataset_name,
                                                                           'dropping an analytics dataset'),
                                  dataverse_name=final_args.pop('dataverse_name', None),
                                  ignore_if_does_not_exist=final_args.pop('ignore_if_not_exists', None),
                                  timeout=final_args.pop('timeout', None))

    def build_drop_dataverse_request(self,
                                     dataverse_name: str,
                                     obs_handler: ObservableRequestHandler,
                                     options: DropDataverseOptions | None = None,
                                     **kwargs: object) -> DropDataverseRequest:
        final_args = forward_args(kwargs, options, block=AnalyticsMgmtOptionsBlock.DropDataverse,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return DropDataverseRequest(self._error_map,
                                    dataverse_name=self._validate_dataverse_name(dataverse_name,
                                                                                 'dropping an analytics dataverse'),
                                    ignore_if_does_not_exist=final_args.pop('ignore_if_not_exists', None),
                                    timeout=final_args.pop('timeout', None))

    def build_drop_index_request(self,
                                 index_name: str,
                                 dataset_name: str,
                                 obs_handler: ObservableRequestHandler,
                                 options: DropAnalyticsIndexOptions | None = None,
                                 **kwargs: object) -> DropIndexRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.DropAnalyticsIndex, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        valid_index_name = self._validate_index_name(index_name, 'dropping an analytics index')
        valid_dataset_name = self._validate_dataset_name(dataset_name, 'dropping an analytics index')
        return DropIndexRequest(self._error_map,
                                index_name=valid_index_name,
                                dataset_name=valid_dataset_name,
                                dataverse_name=final_args.pop('dataverse_name', None),
                                ignore_if_does_not_exist=final_args.pop('ignore_if_not_exists', None),
                                timeout=final_args.pop('timeout', None))

    def build_drop_link_request(self,
                                link_name: str,
                                dataverse_name: str,
                                obs_handler: ObservableRequestHandler,
                                options: DropLinkAnalyticsOptions | None = None,
                                **kwargs: object) -> DropLinkRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.DropLinkAnalytics, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        valid_link_name = self._validate_link_name(link_name, 'dropping an analytics link')
        valid_dataverse_name = self._validate_dataverse_name(dataverse_name, 'dropping an analytics link')
        return DropLinkRequest(self._error_map,
                               link_name=valid_link_name,
                               dataverse_name=valid_dataverse_name,
                               timeout=final_args.pop('timeout', None))

    def build_get_all_datasets_request(self,
                                       obs_handler: ObservableRequestHandler,
                                       options: GetAllDatasetOptions | None = None,
                                       **kwargs: object) -> GetAllDatasetsRequest:
        final_args = forward_args(kwargs, options, block=AnalyticsMgmtOptionsBlock.GetAllDataset,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetAllDatasetsRequest(self._error_map, timeout=final_args.pop('timeout', None))

    def build_get_all_indexes_request(self,
                                      obs_handler: ObservableRequestHandler,
                                      options: GetAllAnalyticsIndexesOptions | None = None,
                                      **kwargs: object) -> GetAllIndexesRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.GetAllAnalyticsIndexes, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetAllIndexesRequest(self._error_map, timeout=final_args.pop('timeout', None))

    def build_get_links_request(self,
                                obs_handler: ObservableRequestHandler,
                                options: GetLinksAnalyticsOptions | None = None,
                                **kwargs: object) -> GetLinksRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.GetLinksAnalytics, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        link_type = final_args.pop('link_type', None)
        if isinstance(link_type, AnalyticsLinkType):
            link_type = link_type.value
        return GetLinksRequest(self._error_map,
                               dataverse_name=final_args.pop('dataverse_name', None),
                               link_name=final_args.pop('name', None),
                               link_type=link_type,
                               timeout=final_args.pop('timeout', None))

    def build_get_pending_mutations_request(self,
                                            obs_handler: ObservableRequestHandler,
                                            options: GetPendingMutationsOptions | None = None,
                                            **kwargs: object) -> GetPendingMutationsRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.GetPendingMutations, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        return GetPendingMutationsRequest(self._error_map, timeout=final_args.pop('timeout', None))

    def build_replace_link_request(self,
                                   link: AnalyticsLink,
                                   obs_handler: ObservableRequestHandler,
                                   options: ReplaceLinkAnalyticsOptions | None = None,
                                   **kwargs: object) -> ReplaceLinkRequest:
        final_args = forward_args(
            kwargs, options, block=AnalyticsMgmtOptionsBlock.ReplaceLinkAnalytics, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        valid_link = self._validate_link(link, 'replacing an analytics link')
        valid_link.validate()
        link_dict = self._link_to_binding_dict(valid_link)
        timeout = final_args.pop('timeout', None)
        if valid_link.link_type() == AnalyticsLinkType.AzureBlobExternal:
            return ReplaceAzureBlobExternalLinkRequest(self._error_map, link=link_dict, timeout=timeout)
        if valid_link.link_type() == AnalyticsLinkType.CouchbaseRemote:
            return ReplaceCouchbaseRemoteLinkRequest(self._error_map, link=link_dict, timeout=timeout)
        return ReplaceS3ExternalLinkRequest(self._error_map, link=link_dict, timeout=timeout)
