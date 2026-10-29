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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode
from couchbase.protocol.classic._management._query_index_mgmt_types import (QUERY_INDEX_MGMT_ERROR_MAP,
                                                                            BuildDeferredIndexesRequest,
                                                                            CreateIndexRequest,
                                                                            DropIndexRequest,
                                                                            GetAllIndexesRequest,
                                                                            QueryIndexMgmtOptionsBlock,
                                                                            WatchIndexesRequest)
from couchbase.protocol.classic._wire import forward_args

if TYPE_CHECKING:
    from couchbase.common.management.options import (BuildDeferredQueryIndexOptions,
                                                     CreatePrimaryQueryIndexOptions,
                                                     CreateQueryIndexOptions,
                                                     DropPrimaryQueryIndexOptions,
                                                     DropQueryIndexOptions,
                                                     GetAllQueryIndexOptions,
                                                     WatchQueryIndexOptions)


class QueryIndexMgmtRequestBuilder:

    def __init__(self, option_validation: OptionValidationMode | None = None) -> None:
        self._error_map = QUERY_INDEX_MGMT_ERROR_MAP
        self._option_validation = option_validation

    def _create_index_request(self,
                              bucket_name: str,
                              keys: Iterable[str],
                              *,
                              index_name: str | None = None,
                              primary: bool | None = False,
                              condition: str | None = None,
                              collection_name: str | None = None,
                              scope_name: str | None = None,
                              ignore_if_exists: bool | None = None,
                              deferred: bool | None = None,
                              num_replicas: int | None = None,
                              timeout: int | None = None
                              ) -> CreateIndexRequest:

        if primary and keys:
            raise InvalidArgumentException('Cannot create primary index with explicit keys')
        elif not primary and not keys:
            raise InvalidArgumentException('Keys required for non-primary index')

        if condition and primary:
            raise InvalidArgumentException('cannot specify condition for primary index')

        return CreateIndexRequest(self._error_map,
                                  bucket_name=bucket_name,
                                  index_name=index_name or None,
                                  is_primary=primary or None,
                                  condition=condition or None,
                                  keys=list(keys) if keys else None,
                                  ignore_if_exists=ignore_if_exists,
                                  scope_name=scope_name,
                                  collection_name=collection_name,
                                  deferred=deferred,
                                  num_replicas=num_replicas,
                                  timeout=timeout)

    def _drop_index_request(self,
                            bucket_name: str,
                            *,
                            index_name: str | None = None,
                            collection_name: str | None = None,
                            scope_name: str | None = None,
                            ignore_if_not_exists: bool | None = None,
                            primary: bool | None = None,
                            timeout: int | None = None
                            ) -> DropIndexRequest:
        return DropIndexRequest(self._error_map,
                                bucket_name=bucket_name,
                                index_name=index_name or None,
                                is_primary=primary or None,
                                ignore_if_does_not_exist=ignore_if_not_exists or None,
                                scope_name=scope_name,
                                collection_name=collection_name,
                                timeout=timeout)

    def _validate_bucket_name(self, bucket_name: str) -> None:
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when creating a secondary index.')

    def _validate_index_name(self, index_name: str) -> None:
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided when creating a secondary index.')

    def _validate_index_names(self, index_names: Iterable[str]) -> list[str]:
        if isinstance(index_names, str) or not isinstance(index_names, Iterable):
            raise InvalidArgumentException(
                'An iterable of index names must be provided when watching indexes.')
        return list(index_names)

    def _validate_index_keys(self, keys: Iterable[str]) -> list[str]:
        if isinstance(keys, str) or not isinstance(keys, Iterable):
            raise InvalidArgumentException('Index keys must be provided when creating a secondary index.')
        return list(keys)

    def _valid_collection_context(self, final_args: dict[str, Any]) -> None:
        if 'scope_name' in final_args:
            raise InvalidArgumentException('scope_name cannot be set in the options when using the '
                                           'collection-level query index manager')
        if 'collection_name' in final_args:
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

    def build_create_index_request(self,
                                   bucket_name: str,
                                   index_name: str,
                                   keys: Iterable[str],
                                   obs_handler: ObservableRequestHandler,
                                   collection_context: tuple[str, str] | None = None,
                                   options: CreateQueryIndexOptions | None = None,
                                   **kwargs: object) -> CreateIndexRequest:
        final_args = forward_args(
            kwargs, options, block=QueryIndexMgmtOptionsBlock.CreateQueryIndex, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_bucket_name(bucket_name)
        self._validate_index_name(index_name)
        keys = self._validate_index_keys(keys)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        return self._create_index_request(bucket_name,
                                          keys,
                                          index_name=index_name,
                                          collection_name=collection_name,
                                          scope_name=scope_name,
                                          condition=final_args.get('condition'),
                                          ignore_if_exists=final_args.get('ignore_if_exists'),
                                          deferred=final_args.get('deferred'),
                                          num_replicas=final_args.get('num_replicas'),
                                          timeout=timeout)

    def build_create_primary_index_request(self,
                                           bucket_name: str,
                                           obs_handler: ObservableRequestHandler,
                                           collection_context: tuple[str, str] | None = None,
                                           options: CreatePrimaryQueryIndexOptions | None = None,
                                           **kwargs: object) -> CreateIndexRequest:
        final_args = forward_args(kwargs, options, block=QueryIndexMgmtOptionsBlock.CreatePrimaryQueryIndex,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_bucket_name(bucket_name)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        return self._create_index_request(bucket_name,
                                          [],
                                          primary=True,
                                          collection_name=collection_name,
                                          scope_name=scope_name,
                                          index_name=final_args.get('index_name'),
                                          condition=final_args.get('condition'),
                                          ignore_if_exists=final_args.get('ignore_if_exists'),
                                          deferred=final_args.get('deferred'),
                                          num_replicas=final_args.get('num_replicas'),
                                          timeout=timeout)

    def build_drop_index_request(self,
                                 bucket_name: str,
                                 index_name: str,
                                 obs_handler: ObservableRequestHandler,
                                 collection_context: tuple[str, str] | None = None,
                                 options: DropQueryIndexOptions | None = None,
                                 **kwargs: object) -> DropIndexRequest:
        final_args = forward_args(
            kwargs, options, block=QueryIndexMgmtOptionsBlock.DropQueryIndex, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_bucket_name(bucket_name)
        self._validate_index_name(index_name)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        return self._drop_index_request(bucket_name,
                                        index_name=index_name,
                                        collection_name=collection_name,
                                        scope_name=scope_name,
                                        ignore_if_not_exists=final_args.get('ignore_if_not_exists'),
                                        timeout=timeout)

    def build_drop_primary_index_request(self,
                                         bucket_name: str,
                                         obs_handler: ObservableRequestHandler,
                                         collection_context: tuple[str, str] | None = None,
                                         options: DropPrimaryQueryIndexOptions | None = None,
                                         **kwargs: object) -> DropIndexRequest:
        final_args = forward_args(
            kwargs, options, block=QueryIndexMgmtOptionsBlock.DropPrimaryQueryIndex, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_bucket_name(bucket_name)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        return self._drop_index_request(bucket_name,
                                        primary=True,
                                        collection_name=collection_name,
                                        scope_name=scope_name,
                                        index_name=final_args.get('index_name'),
                                        ignore_if_not_exists=final_args.get('ignore_if_not_exists'),
                                        timeout=timeout)

    def build_get_all_indexes_request(self,
                                      bucket_name: str,
                                      obs_handler: ObservableRequestHandler,
                                      collection_context: tuple[str, str] | None = None,
                                      options: GetAllQueryIndexOptions | None = None,
                                      **kwargs: object) -> GetAllIndexesRequest:
        final_args = forward_args(
            kwargs, options, block=QueryIndexMgmtOptionsBlock.GetAllQueryIndex, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_bucket_name(bucket_name)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        return GetAllIndexesRequest(self._error_map,
                                    bucket_name=bucket_name,
                                    scope_name=scope_name,
                                    collection_name=collection_name,
                                    timeout=timeout)

    def build_build_deferred_indexes_request(self,
                                             bucket_name: str,
                                             obs_handler: ObservableRequestHandler,
                                             collection_context: tuple[str, str] | None = None,
                                             options: BuildDeferredQueryIndexOptions | None = None,
                                             **kwargs: object) -> BuildDeferredIndexesRequest:
        final_args = forward_args(kwargs, options, block=QueryIndexMgmtOptionsBlock.BuildDeferredQueryIndex,
                                  mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_bucket_name(bucket_name)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        return BuildDeferredIndexesRequest(self._error_map,
                                           bucket_name=bucket_name,
                                           scope_name=scope_name,
                                           collection_name=collection_name,
                                           timeout=timeout)

    def build_watch_indexes_request(self,
                                    bucket_name: str,
                                    index_names: Iterable[str],
                                    obs_handler: ObservableRequestHandler,
                                    collection_context: tuple[str, str] | None = None,
                                    options: WatchQueryIndexOptions | None = None,
                                    **kwargs: object) -> WatchIndexesRequest:
        final_args = forward_args(
            kwargs, options, block=QueryIndexMgmtOptionsBlock.WatchQueryIndex, mode=self._option_validation)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span)
        self._validate_bucket_name(bucket_name)
        index_names = self._validate_index_names(index_names)
        timeout = final_args.pop('timeout', None)
        if timeout is None:
            raise InvalidArgumentException('Must specify a timeout when watching indexes.')
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        if final_args.get('watch_primary', False):
            index_names = [*index_names, '#primary']

        return WatchIndexesRequest(bucket_name=bucket_name,
                                   index_names=index_names,
                                   timeout=timeout,
                                   scope_name=scope_name,
                                   collection_name=collection_name)
