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

import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.exceptions import (AmbiguousTimeoutException,
                                         QueryIndexNotFoundException,
                                         WatchQueryIndexTimeoutException)
from couchbase.common.management.queries import QueryIndex
from couchbase.protocol.classic._management._query_index_mgmt_req_builder import QueryIndexMgmtRequestBuilder
from couchbase.protocol.classic._management._query_index_mgmt_types import GetAllIndexesRequest

if TYPE_CHECKING:
    from couchbase.common._observability import ObservabilityInstruments
    from couchbase.common.management.options import (BuildDeferredQueryIndexOptions,
                                                     CreatePrimaryQueryIndexOptions,
                                                     CreateQueryIndexOptions,
                                                     DropPrimaryQueryIndexOptions,
                                                     DropQueryIndexOptions,
                                                     GetAllQueryIndexOptions,
                                                     WatchQueryIndexOptions)
    from couchbase.protocol.classic._client_adapter import ClientAdapter


class QueryIndexMgmtImpl:
    def __init__(self,
                 client_adapter: ClientAdapter,
                 observability_instruments: ObservabilityInstruments,
                 collection_context: tuple[str, str] | None = None) -> None:
        self._client_adapter = client_adapter
        self._request_builder = QueryIndexMgmtRequestBuilder(client_adapter.option_validation)
        self._observability_instruments = observability_instruments
        self._collection_context = collection_context

    def create_index(self,
                     bucket_name: str,
                     index_name: str,
                     keys: Iterable[str],
                     options: CreateQueryIndexOptions | None = None,
                     **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.QueryIndexCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_index_request(bucket_name,
                                                                   index_name,
                                                                   keys,
                                                                   obs_handler,
                                                                   self._collection_context,
                                                                   options,
                                                                   **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def create_primary_index(self,
                             bucket_name: str,
                             options: CreatePrimaryQueryIndexOptions | None = None,
                             **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.QueryIndexCreate, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_create_primary_index_request(bucket_name,
                                                                           obs_handler,
                                                                           self._collection_context,
                                                                           options,
                                                                           **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_index(self,
                   bucket_name: str,
                   index_name: str,
                   options: DropQueryIndexOptions | None = None,
                   **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.QueryIndexDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_index_request(bucket_name,
                                                                 index_name,
                                                                 obs_handler,
                                                                 self._collection_context,
                                                                 options,
                                                                 **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def drop_primary_index(self,
                           bucket_name: str,
                           options: DropPrimaryQueryIndexOptions | None = None,
                           **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.QueryIndexDrop, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_primary_index_request(bucket_name,
                                                                         obs_handler,
                                                                         self._collection_context,
                                                                         options,
                                                                         **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def get_all_indexes(self,
                        bucket_name: str,
                        options: GetAllQueryIndexOptions | None = None,
                        **kwargs: Any) -> Iterable[QueryIndex]:
        with ObservableRequestHandler(Op.QueryIndexGetAll, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_indexes_request(bucket_name,
                                                                      obs_handler,
                                                                      self._collection_context,
                                                                      options,
                                                                      **kwargs)
            return self._get_all_indexes(req, obs_handler)

    def build_deferred_indexes(self,
                               bucket_name: str,
                               options: BuildDeferredQueryIndexOptions | None = None,
                               **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.QueryIndexBuildDeferred, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_build_deferred_indexes_request(bucket_name,
                                                                             obs_handler,
                                                                             self._collection_context,
                                                                             options,
                                                                             **kwargs)
            self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)

    def watch_indexes(self,
                      bucket_name: str,
                      index_names: Iterable[str],
                      options: WatchQueryIndexOptions | None = None,
                      **kwargs: Any) -> None:
        with ObservableRequestHandler(Op.QueryIndexWatchIndexes, self._observability_instruments) as obs_handler:
            req = self._request_builder.build_watch_indexes_request(bucket_name,
                                                                    index_names,
                                                                    obs_handler,
                                                                    self._collection_context,
                                                                    options,
                                                                    **kwargs)
            # timeout is converted to millisecs via options processing
            deadline = time.monotonic() + req.timeout / 1000
            delay = 0.1  # seconds

            get_all_indexes_req = GetAllIndexesRequest(self._request_builder._error_map,
                                                       bucket_name=req.bucket_name,
                                                       scope_name=req.scope_name,
                                                       collection_name=req.collection_name, timeout=req.timeout)

            while True:
                indexes: list[QueryIndex] | None
                try:
                    with ObservableRequestHandler(Op.QueryIndexGetAll,
                                                  self._observability_instruments) as sub_obs_handler:
                        sub_obs_handler.create_http_span(parent_span=obs_handler.wrapped_span)
                        indexes = self._get_all_indexes(get_all_indexes_req, sub_obs_handler)
                except AmbiguousTimeoutException:
                    # A poll that timed out says nothing about the indexes; poll again until the deadline.
                    indexes = None

                if indexes is not None and self._check_indexes(req.index_names, indexes):
                    break

                if deadline < (time.monotonic() + delay):
                    msg = 'Failed to find all indexes online within the alloted time.'
                    raise WatchQueryIndexTimeoutException(msg)
                time.sleep(delay)
                # needs to be int b/c req.timeout expects int (this is what the bindings want)
                get_all_indexes_req.timeout = int((deadline - time.monotonic()) * 1e3)

    def _get_all_indexes(self, req: GetAllIndexesRequest, obs_handler: ObservableRequestHandler) -> list[QueryIndex]:
        res = self._client_adapter.execute_mgmt_request(req, obs_handler=obs_handler)
        raw_indexes = res.raw_result['indexes']
        return [QueryIndex.from_server(idx) for idx in raw_indexes]

    def _check_indexes(self, index_names: Iterable[str], indexes: Iterable[QueryIndex]) -> bool:
        """**INTERNAL**"""
        watched = []
        for idx_name in index_names:
            match = next((i for i in indexes if i.name == idx_name), None)
            if not match:
                raise QueryIndexNotFoundException(f'Cannot find index with name: {idx_name}')
            watched.append(match)

        # Only the named indexes: another index on the keyspace, deferred and never built, is not
        # the caller's to wait for.
        return all(i.state == 'online' for i in watched)
