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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.classic._management._query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.classic._deferred import defer

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.management.options import (BuildDeferredQueryIndexOptions,
                                                     CreatePrimaryQueryIndexOptions,
                                                     CreateQueryIndexOptions,
                                                     DropPrimaryQueryIndexOptions,
                                                     DropQueryIndexOptions,
                                                     GetAllQueryIndexOptions,
                                                     WatchQueryIndexOptions)
    from couchbase.common.management.queries import QueryIndex


class TxQueryIndexMgmtImpl(AsyncQueryIndexMgmtImpl):

    def create_index_deferred(self,
                              bucket_name: str,
                              index_name: str,
                              keys: Iterable[str],
                              options: CreateQueryIndexOptions | None = None,
                              **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.QueryIndexCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_index_request(bucket_name,
                                                                   index_name,
                                                                   keys,
                                                                   obs_handler,
                                                                   self._collection_context,
                                                                   options,
                                                                   **kwargs)
            return self._create_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def create_primary_index_deferred(self,
                                      bucket_name: str,
                                      options: CreatePrimaryQueryIndexOptions | None = None,
                                      **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.QueryIndexCreate, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_create_primary_index_request(bucket_name,
                                                                           obs_handler,
                                                                           self._collection_context,
                                                                           options,
                                                                           **kwargs)
            return self._create_primary_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_index_deferred(self,
                            bucket_name: str,
                            index_name: str,
                            options: DropQueryIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.QueryIndexDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_index_request(bucket_name,
                                                                 index_name,
                                                                 obs_handler,
                                                                 self._collection_context,
                                                                 options,
                                                                 **kwargs)
            return self._drop_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def drop_primary_index_deferred(self,
                                    bucket_name: str,
                                    options: DropPrimaryQueryIndexOptions | None = None,
                                    **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.QueryIndexDrop, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_drop_primary_index_request(bucket_name,
                                                                         obs_handler,
                                                                         self._collection_context,
                                                                         options,
                                                                         **kwargs)
            return self._drop_primary_index(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def get_all_indexes_deferred(self,
                                 bucket_name: str,
                                 options: GetAllQueryIndexOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[QueryIndex]]:
        obs_handler = ObservableRequestHandler(Op.QueryIndexGetAll, self._observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[QueryIndex]]:
            req = self._request_builder.build_get_all_indexes_request(bucket_name,
                                                                      obs_handler,
                                                                      self._collection_context,
                                                                      options,
                                                                      **kwargs)
            return self._get_all_indexes(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def build_deferred_indexes_deferred(self,
                                        bucket_name: str,
                                        options: BuildDeferredQueryIndexOptions | None = None,
                                        **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.QueryIndexBuildDeferred, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_build_deferred_indexes_request(bucket_name,
                                                                             obs_handler,
                                                                             self._collection_context,
                                                                             options,
                                                                             **kwargs)
            return self._build_deferred_indexes(req, obs_handler)

        return defer(self.loop, obs_handler, start)

    def watch_indexes_deferred(self,
                               bucket_name: str,
                               index_names: Iterable[str],
                               options: WatchQueryIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler(Op.QueryIndexWatchIndexes, self._observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            req = self._request_builder.build_watch_indexes_request(bucket_name,
                                                                    index_names,
                                                                    obs_handler,
                                                                    self._collection_context,
                                                                    options,
                                                                    **kwargs)
            return self._watch_indexes(req, obs_handler)

        return defer(self.loop, obs_handler, start)
