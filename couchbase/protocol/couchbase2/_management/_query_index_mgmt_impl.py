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

"""A sync query index manager over the gateway's query admin service.

A watch asks the gateway to wait on each watched index in turn, each wait traced under the watch.
The gateway refuses to wait on a deferred index, and a deferred index can still be built while the
watch runs, so a refused wait is asked again until the watch's deadline, as classic's listing would.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common.exceptions import (AmbiguousTimeoutException,
                                         CouchbaseException,
                                         UnAmbiguousTimeoutException)
from couchbase.common.management._query_index_mgmt_args import WATCH_POLL_SECONDS, watch_timed_out
from couchbase.protocol.couchbase2._driver import call_unary
from couchbase.protocol.couchbase2._management._query_index_mgmt_req_builder import QueryIndexMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._query_index_mgmt_types import query_indexes, still_deferred

if TYPE_CHECKING:
    import grpc

    from couchbase.common.management.options import (BuildDeferredQueryIndexOptions,
                                                     CreatePrimaryQueryIndexOptions,
                                                     CreateQueryIndexOptions,
                                                     DropPrimaryQueryIndexOptions,
                                                     DropQueryIndexOptions,
                                                     GetAllQueryIndexOptions,
                                                     WatchQueryIndexOptions)
    from couchbase.common.management.queries import QueryIndex
    from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
    from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2
    from couchbase.protocol.couchbase2._management._query_index_mgmt_types import (QueryIndexMgmtRequest,
                                                                                   WatchIndexesRequest)

Req = TypeVar('Req')
Resp = TypeVar('Resp')


class QueryIndexMgmtImpl:

    def __init__(self, cluster_impl: ClusterImpl, collection_context: tuple[str, str] | None = None) -> None:
        self._cluster_impl = cluster_impl
        self._request_builder = QueryIndexMgmtRequestBuilder(cluster_impl.core, collection_context)

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        return self._cluster_impl.observability_instruments

    def create_index(self,
                     bucket_name: str,
                     index_name: str,
                     keys: Iterable[str],
                     options: CreateQueryIndexOptions | None = None,
                     **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.QueryIndexCreate, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_create_index_request(bucket_name,
                                                                   index_name,
                                                                   keys,
                                                                   obs_handler,
                                                                   options,
                                                                   **kwargs)
            self._send(Op.QueryIndexCreate, self._cluster_impl.query_admin.CreateIndex, req)

    def create_primary_index(self,
                             bucket_name: str,
                             options: CreatePrimaryQueryIndexOptions | None = None,
                             **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.QueryIndexCreate, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_create_primary_index_request(bucket_name, obs_handler, options, **kwargs)
            self._send(Op.QueryIndexCreate, self._cluster_impl.query_admin.CreatePrimaryIndex, req)

    def drop_index(self,
                   bucket_name: str,
                   index_name: str,
                   options: DropQueryIndexOptions | None = None,
                   **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.QueryIndexDrop, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_index_request(
                bucket_name, index_name, obs_handler, options, **kwargs)
            self._send(Op.QueryIndexDrop, self._cluster_impl.query_admin.DropIndex, req)

    def drop_primary_index(self,
                           bucket_name: str,
                           options: DropPrimaryQueryIndexOptions | None = None,
                           **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.QueryIndexDrop, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_drop_primary_index_request(bucket_name, obs_handler, options, **kwargs)
            self._send(Op.QueryIndexDrop, self._cluster_impl.query_admin.DropPrimaryIndex, req)

    def get_all_indexes(self,
                        bucket_name: str,
                        options: GetAllQueryIndexOptions | None = None,
                        **kwargs: Any) -> list[QueryIndex]:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.QueryIndexGetAll, self.observability_instruments) as obs_handler:
            req = self._request_builder.build_get_all_indexes_request(bucket_name, obs_handler, options, **kwargs)
            return self._get_all(req.message, req.timeout, req.obs_handler)

    def build_deferred_indexes(self,
                               bucket_name: str,
                               options: BuildDeferredQueryIndexOptions | None = None,
                               **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.QueryIndexBuildDeferred,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_build_deferred_indexes_request(
                bucket_name, obs_handler, options, **kwargs)
            self._send(Op.QueryIndexBuildDeferred,
                       self._cluster_impl.query_admin.BuildDeferredIndexes,
                       req)

    def watch_indexes(self,
                      bucket_name: str,
                      index_names: Iterable[str],
                      options: WatchQueryIndexOptions | None = None,
                      **kwargs: Any) -> None:
        self._cluster_impl.core.ensure_open()
        with ObservableRequestHandler.create(Op.QueryIndexWatchIndexes,
                                             self.observability_instruments) as obs_handler:
            req = self._request_builder.build_watch_indexes_request(bucket_name,
                                                                    index_names,
                                                                    obs_handler,
                                                                    options,
                                                                    **kwargs)
            self._watch(req, obs_handler)

    def _watch(self, req: WatchIndexesRequest, obs_handler: ObservableRequestHandler | None) -> None:
        deadline = time.monotonic() + req.timeout
        for wait in req.waits:
            while not self._wait(wait, deadline, obs_handler):
                if deadline < time.monotonic() + WATCH_POLL_SECONDS:
                    raise watch_timed_out()
                time.sleep(WATCH_POLL_SECONDS)

    def _wait(self,
              message: query_pb2.WaitForIndexOnlineRequest,
              deadline: float,
              obs_handler: ObservableRequestHandler | None) -> bool:
        """Whether the index came online; False when it is still deferred."""
        core = self._cluster_impl.core
        try:
            with ObservableRequestHandler.create(Op.QueryIndexWaitForIndexOnline,
                                                 self.observability_instruments) as sub_obs_handler:
                if sub_obs_handler and obs_handler:
                    sub_obs_handler.create_http_span(parent_span=obs_handler.wrapped_span)
                ctx = core.context(Op.QueryIndexWaitForIndexOnline,
                                   deadline - time.monotonic(),
                                   readonly=True,
                                   obs_handler=sub_obs_handler)
                call_unary(self._cluster_impl.query_admin.WaitForIndexOnline, message, ctx, core.metadata)
        except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
            raise watch_timed_out() from None
        except CouchbaseException as exc:
            if still_deferred(exc):
                return False
            raise
        return True

    def _get_all(self,
                 message: query_pb2.GetAllIndexesRequest,
                 timeout: float,
                 obs_handler: ObservableRequestHandler | None) -> list[QueryIndex]:
        core = self._cluster_impl.core
        ctx = core.context(Op.QueryIndexGetAll, timeout, readonly=True, obs_handler=obs_handler)
        return query_indexes(call_unary(self._cluster_impl.query_admin.GetAllIndexes, message, ctx, core.metadata))

    def _send(self,
              op: Op,
              rpc: grpc.UnaryUnaryMultiCallable[Req, Resp],
              req: QueryIndexMgmtRequest[Req]) -> Resp:
        core = self._cluster_impl.core
        ctx = core.context(op, req.timeout, obs_handler=req.obs_handler)
        return call_unary(rpc, req.message, ctx, core.metadata)
