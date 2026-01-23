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

from typing import TYPE_CHECKING, Optional

from couchbase.logic.bucket_req_builder import BucketRequestBuilder
from couchbase.logic.cluster_settings import ClusterSettings, StreamingTimeouts
from couchbase.logic.top_level_types import PyCapsuleType
from couchbase.result import PingResult, ViewResult
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder
from couchbase.views import ViewRequest

if TYPE_CHECKING:
    from couchbase.cluster import Cluster
    from couchbase.logic.bucket_types import PingRequest, ViewQueryRequest


class BucketImpl:
    def __init__(self, bucket_name: str, cluster: Cluster) -> None:
        self._bucket_name = bucket_name
        self._client_adapter = cluster._impl._client_adapter
        self._cluster_settings = cluster._impl._cluster_settings
        self._request_builder = BucketRequestBuilder(self._bucket_name)
        self.open_bucket()

    @property
    def bucket_name(self) -> str:
        """**INTERNAL**"""
        return self._bucket_name

    @property
    def connected(self) -> bool:
        """**INTERNAL**"""
        return self._client_adapter.connected

    @property
    def connection(self) -> Optional[PyCapsuleType]:
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
        return self._cluster_settings.streaming_timeouts

    def close_bucket(self) -> None:
        """**INTERNAL**"""
        self._client_adapter.close_bucket(self._bucket_name)

    def open_bucket(self) -> None:
        """**INTERNAL**"""
        self._client_adapter.open_bucket(self._bucket_name)

    def ping(self, req: PingRequest) -> PingResult:
        """**INTERNAL**"""
        return PingResult(self._client_adapter.execute_cluster_request(req))

    def view_query(self, req: ViewQueryRequest) -> ViewResult:
        """**INTERNAL**"""
        # If the view_query was provided a timeout we will use that value for the streaming timeout
        # when the streaming object is created in the bindings.  If the view_query does not specify a
        # timeout, the streaming_timeout defaults to cluster's view_timeout (set here). If the cluster
        # also does not specify a view_timeout we set the streaming_timeout to
        # couchbase::core::timeout_defaults::view_timeout when the streaming object is created in the bindings.
        streaming_timeout = self._cluster_settings.streaming_timeouts.get('view_timeout', None)
        return ViewResult(ViewRequest.generate_view_request(self._client_adapter.connection,
                                                            req.view_query.as_encodable(),
                                                            default_serializer=self.default_serializer,
                                                            streaming_timeout=streaming_timeout))
