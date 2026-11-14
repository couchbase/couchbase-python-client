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

from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common.management._collection_mgmt_args import collection_overload_args, settings_fields
from couchbase.common.management._mgmt_args import validate_name
from couchbase.common.management.collections import UpdateCollectionSettings
from couchbase.common.management.options import (CreateScopeOptions,
                                                 DropScopeOptions,
                                                 GetAllScopesOptions,
                                                 UpdateCollectionOptions)
from couchbase.common.option_enums import OptionValidationMode
from couchbase.protocol.classic._management._collection_mgmt_types import (COLLECTION_MGMT_ERROR_MAP,
                                                                           CollectionMgmtOptionsBlock,
                                                                           CreateCollectionRequest,
                                                                           CreateScopeRequest,
                                                                           DropCollectionRequest,
                                                                           DropScopeRequest,
                                                                           GetAllScopesRequest,
                                                                           UpdateCollectionRequest)
from couchbase.protocol.classic._wire import forward_args


class CollectionMgmtRequestBuilder:

    def __init__(self, option_validation: OptionValidationMode | None = None) -> None:
        self._error_map = COLLECTION_MGMT_ERROR_MAP
        self._option_validation = option_validation

    def _validate_bucket_name(self, bucket_name: object) -> str:
        return validate_name(bucket_name, 'bucket_name')

    def _validate_collection_name(self, collection_name: object) -> str:
        return validate_name(collection_name, 'collection_name')

    def _validate_scope_name(self, scope_name: object) -> str:
        return validate_name(scope_name, 'scope_name')

    def build_create_collection_request(self,
                                        bucket_name: str,
                                        *args: Any,
                                        obs_handler: ObservableRequestHandler,
                                        **kwargs: object) -> CreateCollectionRequest:
        scope_name, collection_name, settings, options = collection_overload_args(
            'create_collection', args, kwargs)

        final_args = forward_args(
            kwargs, options, block=CollectionMgmtOptionsBlock.CreateCollection, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     collection_name=collection_name)

        self._validate_bucket_name(bucket_name)
        scope_name = self._validate_scope_name(scope_name)
        collection_name = self._validate_collection_name(collection_name)

        timeout = final_args.pop('timeout', None)

        max_expiry, history = settings_fields(settings)
        return CreateCollectionRequest(self._error_map,
                                       bucket_name=bucket_name,
                                       scope_name=scope_name,
                                       collection_name=collection_name,
                                       max_expiry=max_expiry,
                                       history=history,
                                       timeout=timeout)

    def build_create_scope_request(self,
                                   bucket_name: str,
                                   scope_name: str,
                                   obs_handler: ObservableRequestHandler,
                                   options: CreateScopeOptions | None = None,
                                   **kwargs: object) -> CreateScopeRequest:
        final_args = forward_args(kwargs, options, block=CollectionMgmtOptionsBlock.CreateScope,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name)
        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)

        timeout = final_args.pop('timeout', None)

        return CreateScopeRequest(self._error_map,
                                  bucket_name=bucket_name,
                                  scope_name=scope_name,
                                  timeout=timeout)

    def build_drop_collection_request(self,
                                      bucket_name: str,
                                      *args: Any,
                                      obs_handler: ObservableRequestHandler,
                                      **kwargs: object) -> DropCollectionRequest:
        scope_name, collection_name, _, options = collection_overload_args(
            'drop_collection', args, kwargs)

        final_args = forward_args(
            kwargs, options, block=CollectionMgmtOptionsBlock.DropCollection, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     collection_name=collection_name)

        self._validate_bucket_name(bucket_name)
        scope_name = self._validate_scope_name(scope_name)
        collection_name = self._validate_collection_name(collection_name)

        timeout = final_args.pop('timeout', None)

        return DropCollectionRequest(self._error_map,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     collection_name=collection_name,
                                     timeout=timeout)

    def build_drop_scope_request(self,
                                 bucket_name: str,
                                 scope_name: str,
                                 obs_handler: ObservableRequestHandler,
                                 options: DropScopeOptions | None = None,
                                 **kwargs: object) -> DropScopeRequest:
        final_args = forward_args(kwargs, options, block=CollectionMgmtOptionsBlock.DropScope,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name)

        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)

        timeout = final_args.pop('timeout', None)

        return DropScopeRequest(self._error_map,
                                bucket_name=bucket_name,
                                scope_name=scope_name,
                                timeout=timeout)

    def build_get_all_scopes_request(self,
                                     bucket_name: str,
                                     obs_handler: ObservableRequestHandler,
                                     options: GetAllScopesOptions | None = None,
                                     **kwargs: object) -> GetAllScopesRequest:
        final_args = forward_args(kwargs, options, block=CollectionMgmtOptionsBlock.GetAllScopes,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name)
        self._validate_bucket_name(bucket_name)

        timeout = final_args.pop('timeout', None)

        return GetAllScopesRequest(self._error_map,
                                   bucket_name=bucket_name,
                                   timeout=timeout)

    def build_update_collection_request(self,
                                        bucket_name: str,
                                        scope_name: str,
                                        collection_name: str,
                                        settings: UpdateCollectionSettings,
                                        obs_handler: ObservableRequestHandler,
                                        options: UpdateCollectionOptions | None = None,
                                        **kwargs: object) -> UpdateCollectionRequest:
        final_args = forward_args(
            kwargs, options, block=CollectionMgmtOptionsBlock.UpdateCollection, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     collection_name=collection_name)

        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)
        self._validate_collection_name(collection_name)

        timeout = final_args.pop('timeout', None)

        max_expiry, history = settings_fields(settings)
        return UpdateCollectionRequest(self._error_map,
                                       bucket_name=bucket_name,
                                       scope_name=scope_name,
                                       collection_name=collection_name,
                                       max_expiry=max_expiry,
                                       history=history,
                                       timeout=timeout)
