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

from inspect import Parameter, Signature
from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._supportability import Supportability
from couchbase.common._utils import is_null_or_empty
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management.collections import (CollectionSpec,
                                                     CreateCollectionSettings,
                                                     UpdateCollectionSettings)
from couchbase.common.management.options import (CreateCollectionOptions,
                                                 CreateScopeOptions,
                                                 DropCollectionOptions,
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

    def _get_collection_spec_details(self,
                                     collection_spec: CollectionSpec,
                                     method_name: str) -> tuple[str, str, CreateCollectionSettings | None]:
        settings = None
        if method_name == 'create_collection':
            Supportability.method_signature_deprecated(
                method_name,
                Signature(
                    parameters=[
                        Parameter('collection', Parameter.POSITIONAL_OR_KEYWORD, annotation=CollectionSpec),
                        Parameter('options',
                                  Parameter.POSITIONAL_OR_KEYWORD,
                                  annotation=CreateCollectionOptions | None,
                                  default=None),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=dict[str, Any]),
                    ],
                    return_annotation=None
                ),
                Signature(
                    parameters=[
                        Parameter('scope_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('collection_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('settings',
                                  Parameter.POSITIONAL_OR_KEYWORD,
                                  annotation=CreateCollectionSettings | None,
                                  default=None),
                        Parameter('options',
                                  Parameter.POSITIONAL_OR_KEYWORD,
                                  annotation=CreateCollectionOptions | None,
                                  default=None),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=dict[str, Any]),
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
                        Parameter('options',
                                  Parameter.POSITIONAL_OR_KEYWORD,
                                  annotation=DropCollectionOptions | None,
                                  default=None),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=dict[str, Any]),
                    ],
                    return_annotation=None,
                ),
                Signature(
                    parameters=[
                        Parameter('scope_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('collection_name', Parameter.POSITIONAL_OR_KEYWORD, annotation=str),
                        Parameter('options',
                                  Parameter.POSITIONAL_OR_KEYWORD,
                                  annotation=DropCollectionOptions | None,
                                  default=None),
                        Parameter('kwargs', Parameter.VAR_KEYWORD, annotation=dict[str, Any]),
                    ],
                    return_annotation=None
                )
            )
        if collection_spec.max_expiry is not None or collection_spec.history is not None:
            settings = CreateCollectionSettings(max_expiry=collection_spec.max_expiry, history=collection_spec.history)
        scope_name = collection_spec.scope_name
        collection_name = collection_spec.name
        return scope_name, collection_name, settings

    def _extract_collection_overload_args(self,  # noqa: C901
                                          method_name: str,
                                          args: tuple[Any, ...],
                                          kwargs: dict[str, Any]
                                          ) -> tuple[Any, Any, CreateCollectionSettings | None, Any]:
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
                options = self._single_options(method_name, args[1:], kwargs)
            elif args:
                raise InvalidArgumentException(
                    f"{method_name}: cannot mix 'collection' keyword (CollectionSpec) with positional arguments.")
            else:
                collection_spec = kwargs.pop('collection')
                options = self._single_options(method_name, (), kwargs)
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

        return scope_name, collection_name, settings, self._single_options(method_name, tuple(pos), kwargs)

    def _single_options(self, method_name: str, rest: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if len(rest) > 1:
            raise TypeError(f'{method_name}() takes one options object, {len(rest)} given')
        if 'options' in kwargs:
            if rest:
                raise TypeError(f"{method_name}() got multiple values for argument 'options'")
            return kwargs.pop('options')
        return rest[0] if rest else None

    def _settings_fields(self,
                         settings: CreateCollectionSettings | UpdateCollectionSettings | None
                         ) -> tuple[int | None, bool | None]:
        if settings is None:
            return None, None
        max_expiry = int(settings.max_expiry.total_seconds()) if settings.max_expiry is not None else None
        return max_expiry, settings.history

    def _validate_bucket_name(self, bucket_name: object) -> str:
        if bucket_name is None:
            raise InvalidArgumentException('The bucket_name cannot be empty.')

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException('The bucket_name must be str.')

        if is_null_or_empty(bucket_name):
            raise InvalidArgumentException('The bucket_name cannot be empty.')

        return bucket_name

    def _validate_collection_name(self, collection_name: object) -> str:
        if collection_name is None:
            raise InvalidArgumentException('The collection_name cannot be empty.')

        if not isinstance(collection_name, str):
            raise InvalidArgumentException('The collection_name must be str.')

        if is_null_or_empty(collection_name):
            raise InvalidArgumentException('The collection_name cannot be empty.')

        return collection_name

    def _validate_scope_name(self, scope_name: object) -> str:
        if scope_name is None:
            raise InvalidArgumentException('The scope_name cannot be empty.')

        if not isinstance(scope_name, str):
            raise InvalidArgumentException('The scope_name must be str.')

        if is_null_or_empty(scope_name):
            raise InvalidArgumentException('The scope_name cannot be empty.')

        return scope_name

    def build_create_collection_request(self,
                                        bucket_name: str,
                                        *args: Any,
                                        obs_handler: ObservableRequestHandler,
                                        **kwargs: object) -> CreateCollectionRequest:
        scope_name, collection_name, settings, options = self._extract_collection_overload_args(
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

        max_expiry, history = self._settings_fields(settings)
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
        scope_name, collection_name, _, options = self._extract_collection_overload_args(
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

        max_expiry, history = self._settings_fields(settings)
        return UpdateCollectionRequest(self._error_map,
                                       bucket_name=bucket_name,
                                       scope_name=scope_name,
                                       collection_name=collection_name,
                                       max_expiry=max_expiry,
                                       history=history,
                                       timeout=timeout)
