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

from couchbase.common._supportability import Supportability
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management._mgmt_args import single_options
from couchbase.common.management.collections import (CollectionSpec,
                                                     CreateCollectionSettings,
                                                     UpdateCollectionSettings)
from couchbase.common.management.options import CreateCollectionOptions, DropCollectionOptions


def _collection_spec_details(collection_spec: CollectionSpec,
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


def collection_overload_args(method_name: str,  # noqa: C901
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
            options = single_options(method_name, args[1:], kwargs)
        elif args:
            raise InvalidArgumentException(
                f"{method_name}: cannot mix 'collection' keyword (CollectionSpec) with positional arguments.")
        else:
            collection_spec = kwargs.pop('collection')
            options = single_options(method_name, (), kwargs)
        scope_name, collection_name, settings = _collection_spec_details(collection_spec, method_name)
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

    return scope_name, collection_name, settings, single_options(method_name, tuple(pos), kwargs)


def settings_fields(settings: CreateCollectionSettings | UpdateCollectionSettings | None
                    ) -> tuple[int | None, bool | None]:
    if settings is None:
        return None, None
    max_expiry = int(settings.max_expiry.total_seconds()) if settings.max_expiry is not None else None
    return max_expiry, settings.history
