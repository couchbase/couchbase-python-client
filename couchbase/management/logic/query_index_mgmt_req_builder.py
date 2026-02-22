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

from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase.exceptions import InvalidArgumentException
from couchbase.management.logic.query_index_mgmt_req_types import (QUERY_INDEX_MGMT_ERROR_MAP,
                                                                   BuildDeferredIndexesRequest,
                                                                   CreateIndexRequest,
                                                                   DropIndexRequest,
                                                                   GetAllIndexesRequest,
                                                                   WatchIndexesRequest)
from couchbase.options import forward_args


class QueryIndexMgmtRequestBuilder:

    def __init__(self) -> None:
        self._error_map = QUERY_INDEX_MGMT_ERROR_MAP

    def _get_create_index_op_args(self,  # noqa: C901
                                  bucket_name: str,
                                  keys: Union[List[str], Tuple[str]],
                                  *,
                                  index_name: Optional[str] = None,
                                  primary: Optional[bool] = False,
                                  condition: Optional[str] = None,
                                  collection_name: Optional[str] = None,
                                  scope_name: Optional[str] = None,
                                  ignore_if_exists: Optional[bool] = None,
                                  deferred: Optional[bool] = None,
                                  num_replicas: Optional[int] = None
                                  ) -> Dict[str, Any]:

        if primary and keys:
            # TODO(incorrect error): InvalidArgumentException
            raise TypeError('Cannot create primary index with explicit keys')
        elif not primary and not keys:
            # TODO(incorrect error): InvalidArgumentException
            raise ValueError('Keys required for non-primary index')

        if condition and primary:
            # TODO(incorrect error): InvalidArgumentException
            raise ValueError('cannot specify condition for primary index')

        op_args = {
            'bucket_name': bucket_name,
        }
        if index_name:
            op_args['index_name'] = index_name
        if primary is True:
            op_args['is_primary'] = primary
        if condition:
            op_args['condition'] = condition
        if keys and len(keys) > 0:
            if isinstance(keys, list):
                op_args['keys'] = keys
            else:
                op_args['keys'] = list(keys)
        if ignore_if_exists is not None:
            op_args['ignore_if_exists'] = ignore_if_exists
        if scope_name is not None:
            op_args['scope_name'] = scope_name
        if collection_name is not None:
            op_args['collection_name'] = collection_name
        if deferred is not None:
            op_args['deferred'] = deferred
        if num_replicas is not None:
            op_args['num_replicas'] = num_replicas

        return op_args

    def _get_drop_index_op_args(self,
                                bucket_name: str,
                                *,
                                index_name: Optional[str] = None,
                                collection_name: Optional[str] = None,
                                scope_name: Optional[str] = None,
                                ignore_if_not_exists: Optional[bool] = None,
                                ignore_if_missing: Optional[bool] = None,
                                primary: Optional[bool] = None,
                                ) -> Dict[str, Any]:
        op_args = {
            'bucket_name': bucket_name,
        }
        if index_name:
            op_args['index_name'] = index_name
        if primary is True:
            op_args['is_primary'] = primary
        # TODO: deprecate/remove ignore_missing
        if ignore_if_not_exists is True or ignore_if_missing is True:
            op_args['ignore_if_does_not_exist'] = True
        if scope_name is not None:
            op_args['scope_name'] = scope_name
        if collection_name is not None:
            op_args['collection_name'] = collection_name

        return op_args

    def _validate_bucket_name(self, bucket_name: str) -> None:
        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be provided when creating a secondary index.')

    def _validate_index_name(self, index_name: str) -> None:
        if not isinstance(index_name, str):
            raise InvalidArgumentException('The index_name must be provided when creating a secondary index.')

    def _validate_index_names(self, index_names: Union[List[str], Tuple[str]]) -> None:
        if not isinstance(index_names, (list, tuple)):
            raise InvalidArgumentException(
                'A list/tuple of at least one index_name must be provided when watching indexes.')

    def _validate_index_keys(self, keys: Union[List[str], Tuple[str]]) -> None:
        if not isinstance(keys, (list, tuple)):
            raise InvalidArgumentException('Index keys must be provided when creating a secondary index.')

    def _valid_collection_context(self, final_args: Dict[str, Any]) -> None:
        if 'scope_name' in final_args:
            raise InvalidArgumentException('scope_name cannot be set in the options when using the '
                                           'collection-level query index manager')
        if 'collection_name' in final_args:
            raise InvalidArgumentException('collection_name cannot be set in the options when using the '
                                           'collection-level query index manager')

    def build_create_index_request(self,
                                   bucket_name: str,
                                   index_name: str,
                                   keys: Union[List[str], Tuple[str]],
                                   collection_context: Optional[Tuple[str, str]] = None,
                                   *options: object,
                                   **kwargs: object) -> CreateIndexRequest:

        self._validate_bucket_name(bucket_name)
        self._validate_index_name(index_name)
        self._validate_index_keys(keys)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        op_args = self._get_create_index_op_args(bucket_name,
                                                 keys,
                                                 index_name=index_name,
                                                 collection_name=collection_name,
                                                 scope_name=scope_name,
                                                 **final_args)

        req = CreateIndexRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_primary_index_request(self,
                                           bucket_name: str,
                                           collection_context: Optional[Tuple[str, str]] = None,
                                           *options: object,
                                           **kwargs: object) -> CreateIndexRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        op_args = self._get_create_index_op_args(bucket_name,
                                                 [],
                                                 primary=True,
                                                 collection_name=collection_name,
                                                 scope_name=scope_name,
                                                 **final_args)

        req = CreateIndexRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_index_request(self,
                                 bucket_name: str,
                                 index_name: str,
                                 collection_context: Optional[Tuple[str, str]] = None,
                                 *options: object,
                                 **kwargs: object) -> DropIndexRequest:
        self._validate_bucket_name(bucket_name)
        self._validate_index_name(index_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        op_args = self._get_drop_index_op_args(bucket_name,
                                               index_name=index_name,
                                               collection_name=collection_name,
                                               scope_name=scope_name,
                                               **final_args)

        req = DropIndexRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_primary_index_request(self,
                                         bucket_name: str,
                                         collection_context: Optional[Tuple[str, str]] = None,
                                         *options: object,
                                         **kwargs: object) -> DropIndexRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        op_args = self._get_drop_index_op_args(bucket_name,
                                               primary=True,
                                               collection_name=collection_name,
                                               scope_name=scope_name,
                                               **final_args)

        req = DropIndexRequest(self._error_map, **op_args)

        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_indexes_request(self,
                                      bucket_name: str,
                                      collection_context: Optional[Tuple[str, str]] = None,
                                      *options: object,
                                      **kwargs: object) -> GetAllIndexesRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        op_args = {
            'bucket_name': bucket_name,
        }
        if scope_name is not None:
            op_args['scope_name'] = scope_name
        if collection_name is not None:
            op_args['collection_name'] = collection_name

        req = GetAllIndexesRequest(self._error_map, **op_args)

        if timeout is not None:
            req.timeout = timeout

        return req

    def build_build_deferred_indexes_request(self,
                                             bucket_name: str,
                                             collection_context: Optional[Tuple[str, str]] = None,
                                             *options: object,
                                             **kwargs: object) -> BuildDeferredIndexesRequest:
        self._validate_bucket_name(bucket_name)
        final_args = forward_args(kwargs, *options)
        timeout = final_args.pop('timeout', None)
        if collection_context is not None:
            self._valid_collection_context(final_args)
            collection_name = collection_context[0]
            scope_name = collection_context[1]
        else:
            collection_name = final_args.pop('collection_name', None)
            scope_name = final_args.pop('scope_name', None)

        op_args = {
            'bucket_name': bucket_name,
        }
        if scope_name is not None:
            op_args['scope_name'] = scope_name
        if collection_name is not None:
            op_args['collection_name'] = collection_name

        req = BuildDeferredIndexesRequest(self._error_map, **op_args)

        if timeout is not None:
            req.timeout = timeout

        return req

    def build_watch_indexes_request(self,
                                    bucket_name: str,
                                    index_names: Union[List[str], Tuple[str]],
                                    collection_context: Optional[Tuple[str, str]] = None,
                                    *options: object,
                                    **kwargs: object) -> WatchIndexesRequest:
        self._validate_bucket_name(bucket_name)
        self._validate_index_names(index_names)
        final_args = forward_args(kwargs, *options)
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
            index_names.append('#primary')

        op_args = {
            'bucket_name': bucket_name,
            'index_names': index_names,
            'timeout': timeout,
        }
        if scope_name is not None:
            op_args['scope_name'] = scope_name
        if collection_name is not None:
            op_args['collection_name'] = collection_name

        return WatchIndexesRequest(self._error_map, **op_args)
