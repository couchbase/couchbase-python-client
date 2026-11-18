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

from collections.abc import Coroutine, Iterable
from typing import TYPE_CHECKING, Any

from acouchbase.protocol.couchbase2._management._query_index_mgmt_impl import AsyncQueryIndexMgmtImpl
from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._operations import Op
from txcouchbase.protocol.couchbase2._deferred import defer

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
        obs_handler = ObservableRequestHandler.create_or_none(Op.QueryIndexCreate, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_create_index(
                self.request_builder().build_create_index_request(bucket_name,
                                                                  index_name,
                                                                  keys,
                                                                  obs_handler,
                                                                  options,
                                                                  **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def create_primary_index_deferred(self,
                                      bucket_name: str,
                                      options: CreatePrimaryQueryIndexOptions | None = None,
                                      **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.QueryIndexCreate, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_create_primary_index(
                self.request_builder().build_create_primary_index_request(bucket_name, obs_handler, options, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def drop_index_deferred(self,
                            bucket_name: str,
                            index_name: str,
                            options: DropQueryIndexOptions | None = None,
                            **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.QueryIndexDrop, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_drop_index(
                self.request_builder().build_drop_index_request(bucket_name,
                                                                index_name,
                                                                obs_handler,
                                                                options,
                                                                **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def drop_primary_index_deferred(self,
                                    bucket_name: str,
                                    options: DropPrimaryQueryIndexOptions | None = None,
                                    **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.QueryIndexDrop, self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_drop_primary_index(
                self.request_builder().build_drop_primary_index_request(bucket_name, obs_handler, options, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def get_all_indexes_deferred(self,
                                 bucket_name: str,
                                 options: GetAllQueryIndexOptions | None = None,
                                 **kwargs: Any) -> Deferred[Iterable[QueryIndex]]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.QueryIndexGetAll, self.observability_instruments)

        def start() -> Coroutine[Any, Any, Iterable[QueryIndex]]:
            return self.send_get_all_indexes(
                self.request_builder().build_get_all_indexes_request(bucket_name, obs_handler, options, **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def build_deferred_indexes_deferred(self,
                                        bucket_name: str,
                                        options: BuildDeferredQueryIndexOptions | None = None,
                                        **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.QueryIndexBuildDeferred,
                                                              self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_build_deferred_indexes(
                self.request_builder().build_build_deferred_indexes_request(bucket_name,
                                                                            obs_handler,
                                                                            options,
                                                                            **kwargs))

        return defer(self._cluster_impl.loop, obs_handler, start)

    def watch_indexes_deferred(self,
                               bucket_name: str,
                               index_names: Iterable[str],
                               options: WatchQueryIndexOptions | None = None,
                               **kwargs: Any) -> Deferred[None]:
        obs_handler = ObservableRequestHandler.create_or_none(Op.QueryIndexWatchIndexes,
                                                              self.observability_instruments)

        def start() -> Coroutine[Any, Any, None]:
            return self.send_watch_indexes(
                self.request_builder().build_watch_indexes_request(bucket_name,
                                                                   index_names,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs),
                obs_handler)

        return defer(self._cluster_impl.loop, obs_handler, start)
