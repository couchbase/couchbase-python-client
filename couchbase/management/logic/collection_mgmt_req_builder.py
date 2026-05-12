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

from inspect import Parameter, Signature
from typing import (Any,
                    Dict,
                    Optional,
                    Tuple)

from couchbase._utils import is_null_or_empty
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.observability import ObservableRequestHandler
from couchbase.logic.supportability import Supportability
from couchbase.management.logic.collection_mgmt_req_types import (COLLECTION_MGMT_ERROR_MAP,
                                                                  CollectionSpec,
                                                                  CreateCollectionRequest,
                                                                  CreateCollectionSettings,
                                                                  CreateScopeRequest,
                                                                  DropCollectionRequest,
                                                                  DropScopeRequest,
                                                                  GetAllScopesRequest,
                                                                  UpdateCollectionRequest,
                                                                  UpdateCollectionSettings)
from couchbase.management.options import CreateCollectionOptions, DropCollectionOptions
from couchbase.options import forward_args


class CollectionMgmtRequestBuilder:

    def __init__(self) -> None:
        self._error_map = COLLECTION_MGMT_ERROR_MAP

    def _get_collection_spec_details(self,
                                     collection_spec: CollectionSpec,
                                     method_name: str) -> Tuple[str, str, Optional[CreateCollectionSettings]]:
        settings = None
        if method_name == 'create_collection':
            Supportability.method_signature_deprecated(
                method_name,
                Signature(
                    parameters=[
                        Parameter('collection', Parameter.POSITIONAL_OR_KEYWORD, annotation=CollectionSpec),
                        Parameter('options', Parameter.VAR_POSITIONAL, annotation=CreateCollectionOptions),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                    ],
                    return_annotation=None
                ),
                Signature(
                    parameters=[
                        Parameter('scope_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('collection_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('settings',
                                  Parameter.POSITIONAL_OR_KEYWORD,
                                  annotation=Optional[CreateCollectionSettings]),
                        Parameter('options', Parameter.VAR_POSITIONAL, annotation=CreateCollectionOptions),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                    ],
                    return_annotation=None
                )
            )
        elif method_name == 'drop_collection':
            Supportability.method_signature_deprecated(
                method_name,
                Signature(
                    parameters=[
                        Parameter('collection', Parameter.POSITIONAL_OR_KEYWORD, annotation=CollectionSpec),
                        Parameter('options', Parameter.VAR_POSITIONAL, annotation=DropCollectionOptions),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                    ],
                    return_annotation=None,
                ),
                Signature(
                    parameters=[
                        Parameter('scope_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('collection_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('options', Parameter.VAR_POSITIONAL, annotation=DropCollectionOptions),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=Dict[str, Any]),
                    ],
                    return_annotation=None
                )
            )
        if collection_spec.max_expiry is not None:
            settings = CreateCollectionSettings(max_expiry=collection_spec.max_expiry)
        scope_name = collection_spec.scope_name
        collection_name = collection_spec.name
        return scope_name, collection_name, settings

    def _extract_collection_overload_args(self,  # noqa: C901
                                          method_name: str,
                                          args: Tuple[object, ...],
                                          kwargs: Dict[str, Any]
                                          ) -> Tuple[Optional[str],
                                                     Optional[str],
                                                     Optional[CreateCollectionSettings],
                                                     Tuple[object, ...]]:
        supports_settings = method_name == 'create_collection'

        spec_positional = bool(args) and isinstance(args[0], CollectionSpec)
        spec_keyword = isinstance(kwargs.get('collection'), CollectionSpec)

        if spec_positional and spec_keyword:
            raise InvalidArgumentException(
                f"{method_name}: CollectionSpec provided both positionally and as 'collection' keyword.")

        if spec_positional or spec_keyword:
            conflicting = [k for k in ('scope_name', 'collection_name', 'settings') if k in kwargs]
            if conflicting:
                raise InvalidArgumentException(
                    f"{method_name}: cannot mix CollectionSpec form with keyword argument(s) "
                    f"{', '.join(repr(c) for c in conflicting)}.")
            if spec_positional:
                collection_spec = args[0]
                options = args[1:]
            elif args:
                raise InvalidArgumentException(
                    f"{method_name}: cannot mix 'collection' keyword (CollectionSpec) with positional arguments.")
            else:
                collection_spec = kwargs.pop('collection')
                options = ()
            scope_name, collection_name, settings = self._get_collection_spec_details(collection_spec, method_name)
            if not supports_settings:
                settings = None
            return scope_name, collection_name, settings, options

        pos = list(args)

        if pos:
            if 'scope_name' in kwargs:
                raise InvalidArgumentException(
                    f"{method_name}: 'scope_name' provided both positionally and as a keyword argument.")
            scope_name = pos.pop(0)
        else:
            scope_name = kwargs.pop('scope_name', None)

        if pos:
            if 'collection_name' in kwargs:
                raise InvalidArgumentException(
                    f"{method_name}: 'collection_name' provided both positionally and as a keyword argument.")
            collection_name = pos.pop(0)
        else:
            collection_name = kwargs.pop('collection_name', None)

        settings = None
        if supports_settings:
            if pos and (isinstance(pos[0], CreateCollectionSettings) or pos[0] is None):
                if 'settings' in kwargs:
                    raise InvalidArgumentException(
                        f"{method_name}: 'settings' provided both positionally and as a keyword argument.")
                settings = pos.pop(0)
            else:
                settings = kwargs.pop('settings', None)
                if settings is not None and not isinstance(settings, CreateCollectionSettings):
                    raise InvalidArgumentException(
                        f"{method_name}: 'settings' must be a CreateCollectionSettings instance.")

        return scope_name, collection_name, settings, tuple(pos)

    def _validate_bucket_name(self, bucket_name: str) -> None:
        if is_null_or_empty(bucket_name):
            raise InvalidArgumentException('The bucket_name cannot be empty.')

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be str.')

    def _validate_collection_name(self, collection_name: str) -> None:
        if is_null_or_empty(collection_name):
            raise InvalidArgumentException('The collection_name cannot be empty.')

        if not isinstance(collection_name, str):
            raise InvalidArgumentException('The collection_name must be str.')

    def _validate_scope_name(self, scope_name: str) -> None:
        if is_null_or_empty(scope_name):
            raise InvalidArgumentException('The scope_name cannot be empty.')

        if not isinstance(scope_name, str):
            raise InvalidArgumentException('The scope_name must be str.')

    def build_create_collection_request(self,
                                        bucket_name: str,
                                        *args: object,
                                        **kwargs: object) -> CreateCollectionRequest:
        # the obs_handler is required, let pop fail if not provided
        obs_handler: ObservableRequestHandler = kwargs.pop('obs_handler')
        scope_name, collection_name, settings, options = self._extract_collection_overload_args(
            'create_collection', args, kwargs)

        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     collection_name=collection_name)

        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)
        self._validate_collection_name(collection_name)

        timeout = final_args.pop('timeout', None)

        op_args = {
            "bucket_name": bucket_name,
            "scope_name": scope_name,
            "collection_name": collection_name,
        }

        if settings is not None:
            if settings.max_expiry is not None:
                op_args["max_expiry"] = int(settings.max_expiry.total_seconds())
            if settings.history is not None:
                op_args["history"] = settings.history

        req = CreateCollectionRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_create_scope_request(self,
                                   bucket_name: str,
                                   scope_name: str,
                                   obs_handler: ObservableRequestHandler,
                                   *options: object,
                                   **kwargs: object) -> CreateScopeRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name)
        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)

        timeout = final_args.pop('timeout', None)

        op_args = {
            "bucket_name": bucket_name,
            "scope_name": scope_name,
        }

        req = CreateScopeRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_collection_request(self,
                                      bucket_name: str,
                                      *args: object,
                                      **kwargs: object) -> CreateCollectionRequest:
        # the obs_handler is required, let pop fail if not provided
        obs_handler: ObservableRequestHandler = kwargs.pop('obs_handler')
        scope_name, collection_name, _, options = self._extract_collection_overload_args(
            'drop_collection', args, kwargs)

        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     collection_name=collection_name)

        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)
        self._validate_collection_name(collection_name)

        timeout = final_args.pop('timeout', None)

        op_args = {
            "bucket_name": bucket_name,
            "scope_name": scope_name,
            "collection_name": collection_name,
        }

        req = DropCollectionRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_drop_scope_request(self,
                                 bucket_name: str,
                                 scope_name: str,
                                 obs_handler: ObservableRequestHandler,
                                 *options: object,
                                 **kwargs: object) -> DropScopeRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name)

        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)

        timeout = final_args.pop('timeout', None)

        op_args = {
            "bucket_name": bucket_name,
            "scope_name": scope_name,
        }

        req = DropScopeRequest(self._error_map, **op_args)

        if timeout is not None:
            req.timeout = timeout

        return req

    def build_get_all_scopes_request(self,
                                     bucket_name: str,
                                     obs_handler: ObservableRequestHandler,
                                     *options: object,
                                     **kwargs: object) -> GetAllScopesRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span, bucket_name=bucket_name)
        self._validate_bucket_name(bucket_name)

        timeout = final_args.pop('timeout', None)

        op_args = {
            "bucket_name": bucket_name,
        }

        req = GetAllScopesRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req

    def build_update_collection_request(self,
                                        bucket_name: str,
                                        scope_name: str,
                                        collection_name: str,
                                        settings: UpdateCollectionSettings,
                                        obs_handler: ObservableRequestHandler,
                                        *options: object,
                                        **kwargs: object) -> UpdateCollectionRequest:
        final_args = forward_args(kwargs, *options)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        obs_handler.create_http_span(parent_span=parent_span,
                                     bucket_name=bucket_name,
                                     scope_name=scope_name,
                                     collection_name=collection_name)

        self._validate_bucket_name(bucket_name)
        self._validate_scope_name(scope_name)
        self._validate_collection_name(collection_name)

        timeout = final_args.pop('timeout', None)

        op_args = {
            "bucket_name": bucket_name,
            "scope_name": scope_name,
            "collection_name": collection_name,
        }

        if settings is not None:
            if settings.max_expiry is not None:
                op_args["max_expiry"] = int(settings.max_expiry.total_seconds())
            if settings.history is not None:
                op_args["history"] = settings.history

        req = UpdateCollectionRequest(self._error_map, **op_args)
        if timeout is not None:
            req.timeout = timeout

        return req
