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

"""A query index manager's requests to the gateway, shared by the sync, asyncio and Twisted impls.

The gateway builds each statement itself, so a request carries no statement text but the index
keys, which it joins into the statement as they arrive.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol,
                    TypeVar)

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_args import merge
from couchbase.common.exceptions import FeatureUnavailableException, InvalidArgumentException
from couchbase.common.management._query_index_mgmt_args import (check_create,
                                                                index_keys,
                                                                scope_and_collection,
                                                                validate_bucket_name,
                                                                validate_index_name,
                                                                watched_names)
from couchbase.protocol.couchbase2._core import timeout_seconds
from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2
from couchbase.protocol.couchbase2._management._query_index_mgmt_types import (QueryIndexMgmtOptionsBlock,
                                                                               QueryIndexMgmtRequest,
                                                                               WatchIndexesRequest)

if TYPE_CHECKING:
    from couchbase.common._option_diagnostics import OptionsBlock
    from couchbase.protocol.couchbase2._core import ClusterCore


class _Keyspaced(Protocol):
    bucket_name: str
    scope_name: str
    collection_name: str


K = TypeVar('K', bound=_Keyspaced)

# The bucket, scope and collection a request addresses.
_Keyspace = tuple[str, 'str | None', 'str | None']


class QueryIndexMgmtRequestBuilder:

    def __init__(self, core: ClusterCore, collection_context: tuple[str, str] | None = None) -> None:
        self._core = core
        self._collection_context = collection_context

    def _final_args(self,
                    block: OptionsBlock,
                    bucket_name: str,
                    options: Any,
                    kwargs: dict[str, Any],
                    obs_handler: ObservableRequestHandler | None) -> tuple[dict[str, Any], _Keyspace]:
        """The caller's options, and the keyspace every request of the service carries."""
        final_args = merge(kwargs, options)
        self._core.validate_options(block, final_args)
        parent_span = ObservableRequestHandler.maybe_get_parent_span(parent_span=final_args.pop('parent_span', None))
        if obs_handler:
            obs_handler.create_http_span(parent_span=parent_span)
        validate_bucket_name(bucket_name)
        return final_args, (bucket_name, *scope_and_collection(final_args, self._collection_context))

    def _timeout(self, final_args: dict[str, Any]) -> float:
        return timeout_seconds(final_args, self._core.management_timeout)

    def build_create_index_request(self,
                                   bucket_name: str,
                                   index_name: str,
                                   keys: Iterable[str],
                                   obs_handler: ObservableRequestHandler | None,
                                   options: Any = None,
                                   **kwargs: Any) -> QueryIndexMgmtRequest[query_pb2.CreateIndexRequest]:
        final_args, ks = self._final_args(QueryIndexMgmtOptionsBlock.CreateQueryIndex,
                                          bucket_name,
                                          options,
                                          kwargs,
                                          obs_handler)
        validate_index_name(index_name)
        listed_keys = index_keys(keys)
        check_create(False, listed_keys, final_args.get('condition'))
        if final_args.get('condition'):
            # Sent without its condition, the index would cover the whole collection under the
            # name asked for.
            raise FeatureUnavailableException(
                'A query index with a condition is not available over the couchbase2 protocol.')
        message = _addressed(query_pb2.CreateIndexRequest(name=index_name,
                                                          fields=[_escaped_key(k) for k in listed_keys]),
                             ks)
        _set_create_fields(message, final_args)
        return QueryIndexMgmtRequest(message, self._timeout(final_args), obs_handler=obs_handler)

    def build_create_primary_index_request(self,
                                           bucket_name: str,
                                           obs_handler: ObservableRequestHandler | None,
                                           options: Any = None,
                                           **kwargs: Any
                                           ) -> QueryIndexMgmtRequest[query_pb2.CreatePrimaryIndexRequest]:
        final_args, ks = self._final_args(QueryIndexMgmtOptionsBlock.CreatePrimaryQueryIndex,
                                          bucket_name,
                                          options,
                                          kwargs,
                                          obs_handler)
        check_create(True, [], final_args.get('condition'))
        message = _addressed(query_pb2.CreatePrimaryIndexRequest(), ks)
        # An absent name is the server's own, `#primary`.
        if final_args.get('index_name'):
            message.name = final_args['index_name']
        _set_create_fields(message, final_args)
        return QueryIndexMgmtRequest(message, self._timeout(final_args), obs_handler=obs_handler)

    def build_drop_index_request(self,
                                 bucket_name: str,
                                 index_name: str,
                                 obs_handler: ObservableRequestHandler | None,
                                 options: Any = None,
                                 **kwargs: Any) -> QueryIndexMgmtRequest[query_pb2.DropIndexRequest]:
        final_args, ks = self._final_args(QueryIndexMgmtOptionsBlock.DropQueryIndex,
                                          bucket_name,
                                          options,
                                          kwargs,
                                          obs_handler)
        validate_index_name(index_name)
        message = _addressed(query_pb2.DropIndexRequest(name=index_name), ks)
        if final_args.get('ignore_if_not_exists') is not None:
            message.ignore_if_missing = final_args['ignore_if_not_exists']
        return QueryIndexMgmtRequest(message, self._timeout(final_args), obs_handler=obs_handler)

    def build_drop_primary_index_request(self,
                                         bucket_name: str,
                                         obs_handler: ObservableRequestHandler | None,
                                         options: Any = None,
                                         **kwargs: Any
                                         ) -> QueryIndexMgmtRequest[query_pb2.DropPrimaryIndexRequest]:
        final_args, ks = self._final_args(QueryIndexMgmtOptionsBlock.DropPrimaryQueryIndex,
                                          bucket_name,
                                          options,
                                          kwargs,
                                          obs_handler)
        message = _addressed(query_pb2.DropPrimaryIndexRequest(), ks)
        if final_args.get('index_name'):
            message.name = final_args['index_name']
        if final_args.get('ignore_if_not_exists') is not None:
            message.ignore_if_missing = final_args['ignore_if_not_exists']
        return QueryIndexMgmtRequest(message, self._timeout(final_args), obs_handler=obs_handler)

    def build_get_all_indexes_request(self,
                                      bucket_name: str,
                                      obs_handler: ObservableRequestHandler | None,
                                      options: Any = None,
                                      **kwargs: Any) -> QueryIndexMgmtRequest[query_pb2.GetAllIndexesRequest]:
        final_args, ks = self._final_args(QueryIndexMgmtOptionsBlock.GetAllQueryIndex,
                                          bucket_name,
                                          options,
                                          kwargs,
                                          obs_handler)
        return QueryIndexMgmtRequest(_addressed(query_pb2.GetAllIndexesRequest(), ks),
                                     self._timeout(final_args),
                                     obs_handler=obs_handler)

    def build_build_deferred_indexes_request(self,
                                             bucket_name: str,
                                             obs_handler: ObservableRequestHandler | None,
                                             options: Any = None,
                                             **kwargs: Any
                                             ) -> QueryIndexMgmtRequest[query_pb2.BuildDeferredIndexesRequest]:
        final_args, ks = self._final_args(QueryIndexMgmtOptionsBlock.BuildDeferredQueryIndex,
                                          bucket_name,
                                          options,
                                          kwargs,
                                          obs_handler)
        return QueryIndexMgmtRequest(_addressed(query_pb2.BuildDeferredIndexesRequest(), ks),
                                     self._timeout(final_args),
                                     obs_handler=obs_handler)

    def build_watch_indexes_request(self,
                                    bucket_name: str,
                                    index_names: Iterable[str],
                                    obs_handler: ObservableRequestHandler | None,
                                    options: Any = None,
                                    **kwargs: Any) -> WatchIndexesRequest:
        final_args, ks = self._final_args(QueryIndexMgmtOptionsBlock.WatchQueryIndex,
                                          bucket_name,
                                          options,
                                          kwargs,
                                          obs_handler)
        names = watched_names(index_names)
        if final_args.get('timeout') is None:
            raise InvalidArgumentException('Must specify a timeout when watching indexes.')
        if final_args.get('watch_primary', False):
            names.append('#primary')
        waits = [_addressed(query_pb2.WaitForIndexOnlineRequest(name=name), ks) for name in names]
        return WatchIndexesRequest(waits, self._timeout(final_args))


def _addressed(message: K, ks: _Keyspace) -> K:
    """`message`, addressed to the keyspace.

    An empty scope or collection is left out: the gateway reads an absent one as the whole bucket,
    and an empty one as a name it refuses.
    """
    bucket_name, scope_name, collection_name = ks
    message.bucket_name = bucket_name
    if scope_name:
        message.scope_name = scope_name
    if collection_name:
        message.collection_name = collection_name
    return message


def _set_create_fields(message: query_pb2.CreateIndexRequest | query_pb2.CreatePrimaryIndexRequest,
                       final_args: dict[str, Any]) -> None:
    if final_args.get('num_replicas') is not None:
        message.num_replicas = final_args['num_replicas']
    if final_args.get('deferred') is not None:
        message.deferred = final_args['deferred']
    if final_args.get('ignore_if_exists') is not None:
        message.ignore_if_exists = final_args['ignore_if_exists']


def _escaped_key(key: str) -> str:
    """One index key as the statement's text for it.

    A key wrapped in backticks is sent as it is, as classic sends it, so a nested path such as
    `` `a`.`b` `` reaches the index as written.  Any other key is one field name, quoted: the gateway
    joins the keys into the statement verbatim, and inside the backticks a backslash escapes the
    character after it, so a backtick in the name cannot end its quoting.
    """
    if len(key) >= 2 and key[0] == '`' and key[-1] == '`':
        return key
    escaped = ''.join(f'\\{c}' if c in '\\`' else c for c in key)
    return f'`{escaped}`'
