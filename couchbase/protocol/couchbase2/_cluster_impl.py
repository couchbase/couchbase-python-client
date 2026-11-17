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

"""The sync cluster over the gateway: one `grpc.Channel`, opened lazily, and the operations couchbase2 provides.

Any other operation raises `FeatureUnavailableException`.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn)

from couchbase.common._observability import ObservabilityInstruments
from couchbase.common._operations import Op
from couchbase.protocol.couchbase2._bucket_impl import BucketImpl
from couchbase.protocol.couchbase2._channel import open_channel
from couchbase.protocol.couchbase2._cluster_req_builder import ClusterRequestBuilder
from couchbase.protocol.couchbase2._core import ClusterCore, unsupported
from couchbase.protocol.couchbase2._generated.admin.bucket.v1 import bucket_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.collection.v1 import collection_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2_grpc as query_admin_pb2_grpc
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2_grpc as search_admin_pb2_grpc
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2_grpc
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2_grpc
from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2_grpc
from couchbase.protocol.couchbase2._health import health_check, wait_until_serving

if TYPE_CHECKING:
    from datetime import timedelta

    import grpc

    from couchbase.auth import (CertificateAuthenticator,
                                JwtAuthenticator,
                                PasswordAuthenticator)
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (AnalyticsOptions,
                                          ClusterOptions,
                                          DiagnosticsOptions,
                                          PingOptions,
                                          QueryOptions,
                                          SearchOptions,
                                          WaitUntilReadyOptions)
    from couchbase.common.result import (ClusterInfoResult,
                                         DiagnosticsResult,
                                         PingResult)
    from couchbase.common.search_queries import SearchQuery
    from couchbase.common.search_request import SearchRequest
    from couchbase.result import AnalyticsResult
    from couchbase.transactions import Transactions


class ClusterImpl:

    def __init__(self, connstr: str, *options: ClusterOptions, **kwargs: Any) -> None:
        self._core = ClusterCore(connstr, *options, **kwargs)
        self._request_builder = ClusterRequestBuilder(self._core)
        self._channel: grpc.Channel = open_channel(self._core.channel_config)
        self._kv = kv_pb2_grpc.KvServiceStub(self._channel)
        self._query = query_pb2_grpc.QueryServiceStub(self._channel)
        self._search = search_pb2_grpc.SearchServiceStub(self._channel)
        self._collection_admin = collection_pb2_grpc.CollectionAdminServiceStub(self._channel)
        self._bucket_admin = bucket_pb2_grpc.BucketAdminServiceStub(self._channel)
        self._query_admin = query_admin_pb2_grpc.QueryAdminServiceStub(self._channel)
        self._search_admin = search_admin_pb2_grpc.SearchAdminServiceStub(self._channel)
        self._health = health_check(self._channel)
        self._core.tracing.channel_opened((self._kv,
                                           self._query,
                                           self._search,
                                           self._collection_admin,
                                           self._bucket_admin,
                                           self._query_admin,
                                           self._search_admin),
                                          aio=False)

    @property
    def core(self) -> ClusterCore:
        """**INTERNAL**"""
        return self._core

    @property
    def kv(self) -> kv_pb2_grpc.KvServiceStub:
        """**INTERNAL**"""
        return self._kv

    @property
    def query_service(self) -> query_pb2_grpc.QueryServiceStub:
        """**INTERNAL**"""
        return self._query

    @property
    def search_service(self) -> search_pb2_grpc.SearchServiceStub:
        """**INTERNAL**"""
        return self._search

    @property
    def collection_admin(self) -> collection_pb2_grpc.CollectionAdminServiceStub:
        """**INTERNAL**"""
        return self._collection_admin

    @property
    def bucket_admin(self) -> bucket_pb2_grpc.BucketAdminServiceStub:
        """**INTERNAL**"""
        return self._bucket_admin

    @property
    def query_admin(self) -> query_admin_pb2_grpc.QueryAdminServiceStub:
        """**INTERNAL**"""
        return self._query_admin

    @property
    def search_admin(self) -> search_admin_pb2_grpc.SearchAdminServiceStub:
        """**INTERNAL**"""
        return self._search_admin

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._core.observability_instruments

    @property
    def transactions(self) -> Transactions:
        raise unsupported('Transactions')

    def close_connection(self) -> None:
        if self._core.closed:
            return
        self._core.close()
        self._channel.close()

    def bucket(self, name: str) -> BucketImpl:
        """**INTERNAL**"""
        self._core.ensure_open()
        return BucketImpl(name, self)

    def query(self,
              statement: str,
              *options: QueryOptions | JSONType,
              **kwargs: Any) -> NoReturn:
        raise unsupported('query')

    def get_cluster_info(self) -> ClusterInfoResult:
        raise unsupported('get_cluster_info')

    def ping(self, opts: PingOptions | None = None, **kwargs: Any) -> PingResult:
        raise unsupported('ping')

    def diagnostics(self, opts: DiagnosticsOptions | None = None, **kwargs: Any) -> DiagnosticsResult:
        raise unsupported('diagnostics')

    def update_credentials(self,
                           authenticator: CertificateAuthenticator | JwtAuthenticator | PasswordAuthenticator
                           ) -> None:
        self._core.update_credentials(authenticator)

    def wait_until_ready(self, timeout: timedelta, opts: WaitUntilReadyOptions | None = None, **kwargs: Any) -> None:
        self._core.ensure_open()
        req = self._request_builder.build_wait_until_ready_request(timeout, opts, **kwargs)
        wait_until_serving(self._health,
                           self._core.context(Op.WaitUntilReady, req.timeout, obs_handler=None))

    def analytics_query(self,
                        statement: str,
                        *options: AnalyticsOptions | JSONType,
                        **kwargs: Any) -> AnalyticsResult:
        raise unsupported('analytics_query')

    def search_query(self,
                     index: str,
                     query: SearchQuery,
                     options: SearchOptions | None = None,
                     **kwargs: Any) -> NoReturn:
        raise unsupported('search_query')

    def search(self,
               index: str,
               request: SearchRequest | SearchQuery,
               options: SearchOptions | None = None,
               **kwargs: Any) -> NoReturn:
        raise unsupported('search')

    def bucket_mgmt(self) -> NoReturn:
        raise unsupported('Bucket management')

    def user_mgmt(self) -> NoReturn:
        raise unsupported('User management')

    def query_index_mgmt(self) -> NoReturn:
        raise unsupported('Query index management')

    def analytics_mgmt(self) -> NoReturn:
        raise unsupported('Analytics management')

    def search_index_mgmt(self) -> NoReturn:
        raise unsupported('Search index management')

    def eventing_function_mgmt(self) -> NoReturn:
        raise unsupported('Eventing function management')
