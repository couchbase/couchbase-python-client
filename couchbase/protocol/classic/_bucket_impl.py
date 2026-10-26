#  Copyright 2016-2022. Couchbase, Inc.
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

from typing import TYPE_CHECKING, Any

from couchbase.common._cluster_settings import ClusterSettings
from couchbase.common._observability import ObservabilityInstruments, ObservableRequestHandler
from couchbase.common._operations import Op
from couchbase.common._streaming import RowIterator
from couchbase.common.result import PingResult
from couchbase.protocol.classic._bucket_req_builder import BucketRequestBuilder
from couchbase.protocol.classic._cluster_types import StreamingTimeouts
from couchbase.protocol.classic._pycbc_core import pycbc_connection
from couchbase.protocol.classic._result import _ping_from_core
from couchbase.protocol.classic._scope_impl import ScopeImpl
from couchbase.protocol.classic._streaming import ClassicStreamingExecutor, ViewStreamSpec
from couchbase.result import ViewResult
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.common.options import PingOptions, ViewOptions
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.protocol.classic._client_adapter import ClientAdapter
    from couchbase.protocol.classic._cluster_impl import ClusterImpl


class BucketImpl:

    def __init__(self, bucket_name: str, cluster_impl: ClusterImpl) -> None:
        self._bucket_name = bucket_name
        self._client_adapter: ClientAdapter = cluster_impl._client_adapter
        self._cluster_settings = cluster_impl._cluster_settings
        self._streaming_timeouts = cluster_impl.streaming_timeouts
        self._kv_durable_timeout = cluster_impl.kv_durable_timeout
        self._request_builder = BucketRequestBuilder(self._bucket_name,
                                                     self._client_adapter.option_validation)
        self.open_bucket()

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

    @property
    def bucket_name(self) -> str:
        """**INTERNAL**"""
        return self._bucket_name

    @property
    def connected(self) -> bool:
        """**INTERNAL**"""
        return self._client_adapter.connected

    @property
    def connection(self) -> pycbc_connection:
        """**INTERNAL**"""
        return self._client_adapter.connection

    @property
    def cluster_settings(self) -> ClusterSettings:
        """**INTERNAL**"""
        return self._cluster_settings

    @property
    def default_serializer(self) -> Serializer:
        """**INTERNAL**"""
        return self._cluster_settings.default_serializer

    @property
    def default_transcoder(self) -> Transcoder:
        """**INTERNAL**"""
        return self._cluster_settings.default_transcoder

    @property
    def request_builder(self) -> BucketRequestBuilder:
        """**INTERNAL**"""
        return self._request_builder

    @property
    def streaming_timeouts(self) -> StreamingTimeouts:
        """**INTERNAL**"""
        return self._streaming_timeouts

    @property
    def kv_durable_timeout(self) -> int | None:
        """**INTERNAL**"""
        return self._kv_durable_timeout

    @property
    def observability_instruments(self) -> ObservabilityInstruments:
        """**INTERNAL**"""
        return self._cluster_settings.observability_instruments

    def close_bucket(self) -> None:
        """**INTERNAL**"""
        self._client_adapter.close_bucket(self._bucket_name)

    def open_bucket(self) -> None:
        """**INTERNAL**"""
        self._client_adapter.open_bucket(self._bucket_name)

    def scope(self, name: str) -> ScopeImpl:
        """**INTERNAL**"""
        return ScopeImpl(name, self)

    def ping(self, opts: PingOptions | None = None, **kwargs: Any) -> PingResult:
        req = self._request_builder.build_ping_request(opts, **kwargs)
        return PingResult(_ping_from_core(self._client_adapter.execute_bucket_request(req).raw_result))

    def view_query(self,
                   design_doc: str,
                   view_name: str,
                   view_options: ViewOptions | None = None,
                   **kwargs: Any) -> ViewResult:
        obs_handler = ObservableRequestHandler(Op.ViewQuery, self.observability_instruments)
        req = self._request_builder.build_view_query_request(design_doc,
                                                             view_name,
                                                             obs_handler,
                                                             view_options,
                                                             **kwargs)
        self._client_adapter._ensure_not_closed()
        self._client_adapter._ensure_connected()
        # If the view_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the view_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's view_timeout (set here). If the cluster
        # also does not specify a view_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::view_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._streaming_timeouts.get('view_timeout', None)
        return ViewResult(RowIterator(ClassicStreamingExecutor(self._client_adapter.connection,
                                                               ViewStreamSpec(req.view_query.as_encodable(),
                                                                              self.default_serializer),
                                                               obs_handler=req.obs_handler,
                                                               streaming_timeout=streaming_timeout)))
