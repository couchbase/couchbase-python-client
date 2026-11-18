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

"""A collection manager's requests to the gateway, shared by the sync, asyncio and Twisted impls."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_args import merge
from couchbase.common.exceptions import FeatureUnavailableException, InvalidArgumentException
from couchbase.common.management._collection_mgmt_args import collection_overload_args, settings_fields
from couchbase.common.management._mgmt_args import validate_name
from couchbase.protocol.couchbase2._core import timeout_seconds
from couchbase.protocol.couchbase2._generated.admin.collection.v1 import collection_pb2
from couchbase.protocol.couchbase2._management._collection_mgmt_types import (CollectionMgmtOptionsBlock,
                                                                              CollectionMgmtRequest)

if TYPE_CHECKING:
    from couchbase.common._option_diagnostics import OptionsBlock
    from couchbase.common.management.collections import UpdateCollectionSettings
    from couchbase.protocol.couchbase2._core import ClusterCore

# The server refuses a maximum expiry past a signed 32-bit int.
_MAX_EXPIRY_SECS = 2**31 - 1


class CollectionMgmtRequestBuilder:

    def __init__(self, core: ClusterCore, bucket_name: str) -> None:
        self._core = core
        self._bucket_name = bucket_name

    def _final_args(self,
                    block: OptionsBlock,
                    options: Any,
                    kwargs: dict[str, Any],
                    obs_handler: ObservableRequestHandler | None,
                    scope_name: str | None = None,
                    collection_name: str | None = None) -> dict[str, Any]:
        final_args = merge(kwargs, options)
        self._core.validate_options(block, final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_http_span(parent_span=parent_span,
                                         bucket_name=self._bucket_name,
                                         scope_name=scope_name,
                                         collection_name=collection_name)
        validate_name(self._bucket_name, 'bucket_name')
        return final_args

    def build_create_scope_request(self,
                                   scope_name: str,
                                   obs_handler: ObservableRequestHandler | None,
                                   options: Any = None,
                                   **kwargs: Any) -> CollectionMgmtRequest[collection_pb2.CreateScopeRequest]:
        final_args = self._final_args(CollectionMgmtOptionsBlock.CreateScope, options, kwargs, obs_handler, scope_name)
        message = collection_pb2.CreateScopeRequest(bucket_name=self._bucket_name,
                                                    scope_name=validate_name(scope_name, 'scope_name'))
        return CollectionMgmtRequest(message,
                                     timeout_seconds(final_args, self._core.management_timeout),
                                     obs_handler=obs_handler)

    def build_drop_scope_request(self,
                                 scope_name: str,
                                 obs_handler: ObservableRequestHandler | None,
                                 options: Any = None,
                                 **kwargs: Any) -> CollectionMgmtRequest[collection_pb2.DeleteScopeRequest]:
        final_args = self._final_args(CollectionMgmtOptionsBlock.DropScope, options, kwargs, obs_handler, scope_name)
        message = collection_pb2.DeleteScopeRequest(bucket_name=self._bucket_name,
                                                    scope_name=validate_name(scope_name, 'scope_name'))
        return CollectionMgmtRequest(message,
                                     timeout_seconds(final_args, self._core.management_timeout),
                                     obs_handler=obs_handler)

    def build_get_all_scopes_request(self,
                                     obs_handler: ObservableRequestHandler | None,
                                     options: Any = None,
                                     **kwargs: Any) -> CollectionMgmtRequest[collection_pb2.ListCollectionsRequest]:
        final_args = self._final_args(CollectionMgmtOptionsBlock.GetAllScopes, options, kwargs, obs_handler)
        message = collection_pb2.ListCollectionsRequest(bucket_name=self._bucket_name)
        return CollectionMgmtRequest(message,
                                     timeout_seconds(final_args, self._core.management_timeout),
                                     obs_handler=obs_handler)

    def build_create_collection_request(self,
                                        *args: Any,
                                        obs_handler: ObservableRequestHandler | None,
                                        **kwargs: Any
                                        ) -> CollectionMgmtRequest[collection_pb2.CreateCollectionRequest]:
        scope_name, collection_name, settings, options = collection_overload_args('create_collection', args, kwargs)
        final_args = self._final_args(CollectionMgmtOptionsBlock.CreateCollection,
                                      options,
                                      kwargs,
                                      obs_handler,
                                      scope_name,
                                      collection_name)
        message = collection_pb2.CreateCollectionRequest(
            bucket_name=self._bucket_name,
            scope_name=validate_name(scope_name, 'scope_name'),
            collection_name=validate_name(collection_name, 'collection_name'))
        max_expiry, history = settings_fields(settings)
        if max_expiry is not None and max_expiry != 0:
            message.max_expiry_secs = _max_expiry_secs(max_expiry)
        if history is not None:
            message.history_retention_enabled = history
        return CollectionMgmtRequest(message,
                                     timeout_seconds(final_args, self._core.management_timeout),
                                     obs_handler=obs_handler)

    def build_update_collection_request(self,
                                        scope_name: str,
                                        collection_name: str,
                                        settings: UpdateCollectionSettings,
                                        obs_handler: ObservableRequestHandler | None,
                                        options: Any = None,
                                        **kwargs: Any
                                        ) -> CollectionMgmtRequest[collection_pb2.UpdateCollectionRequest]:
        """A maximum expiry of zero cannot be sent: the gateway reads an absent one as unchanged."""
        final_args = self._final_args(CollectionMgmtOptionsBlock.UpdateCollection,
                                      options,
                                      kwargs,
                                      obs_handler,
                                      scope_name,
                                      collection_name)
        message = collection_pb2.UpdateCollectionRequest(
            bucket_name=self._bucket_name,
            scope_name=validate_name(scope_name, 'scope_name'),
            collection_name=validate_name(collection_name, 'collection_name'))
        max_expiry, history = settings_fields(settings)
        if max_expiry == 0:
            raise FeatureUnavailableException(
                "Updating a collection's maximum expiry to the bucket's is not available over the couchbase2 "
                'protocol.')
        if max_expiry is not None:
            message.max_expiry_secs = _max_expiry_secs(max_expiry)
        if history is not None:
            message.history_retention_enabled = history
        return CollectionMgmtRequest(message,
                                     timeout_seconds(final_args, self._core.management_timeout),
                                     obs_handler=obs_handler)

    def build_drop_collection_request(self,
                                      *args: Any,
                                      obs_handler: ObservableRequestHandler | None,
                                      **kwargs: Any
                                      ) -> CollectionMgmtRequest[collection_pb2.DeleteCollectionRequest]:
        scope_name, collection_name, _, options = collection_overload_args('drop_collection', args, kwargs)
        final_args = self._final_args(CollectionMgmtOptionsBlock.DropCollection,
                                      options,
                                      kwargs,
                                      obs_handler,
                                      scope_name,
                                      collection_name)
        message = collection_pb2.DeleteCollectionRequest(
            bucket_name=self._bucket_name,
            scope_name=validate_name(scope_name, 'scope_name'),
            collection_name=validate_name(collection_name, 'collection_name'))
        return CollectionMgmtRequest(message,
                                     timeout_seconds(final_args, self._core.management_timeout),
                                     obs_handler=obs_handler)


def _max_expiry_secs(seconds: int) -> int:
    """The SDK's -1, never to expire, is the gateway's zero."""
    if seconds == -1:
        return 0
    if not 0 < seconds <= _MAX_EXPIRY_SECS:
        raise InvalidArgumentException(f'Expected a maximum expiry of -1 (for no expiry) or 0 to {_MAX_EXPIRY_SECS} '
                                       f'seconds, got {seconds}.')
    return seconds
