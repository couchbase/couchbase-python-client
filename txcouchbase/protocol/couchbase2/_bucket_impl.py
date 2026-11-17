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

from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred, succeed

from acouchbase.protocol.couchbase2._bucket_impl import AsyncBucketImpl
from couchbase.protocol.couchbase2._core import unsupported
from txcouchbase.protocol.couchbase2._scope_impl import TxScopeImpl

if TYPE_CHECKING:
    from couchbase.common.options import PingOptions, ViewOptions
    from couchbase.common.result import PingResult
    from couchbase.result import ViewResult
    from txcouchbase.protocol.couchbase2._cluster_impl import TxClusterImpl


class TxBucketImpl(AsyncBucketImpl):

    def __init__(self, bucket_name: str, cluster_impl: TxClusterImpl) -> None:
        super().__init__(bucket_name, cluster_impl)
        self._tx_cluster_impl = cluster_impl

    @property
    def cluster_impl(self) -> TxClusterImpl:
        """**INTERNAL**"""
        return self._tx_cluster_impl

    def scope(self, name: str) -> TxScopeImpl:
        """**INTERNAL**"""
        return TxScopeImpl(name, self)

    def close_bucket_deferred(self) -> Deferred[None]:
        return succeed(None)

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]:
        return succeed(None)

    def ping_deferred(self, opts: PingOptions | None = None, **kwargs: Any) -> Deferred[PingResult]:
        raise unsupported('ping')

    def view_query_deferred(self,
                            design_doc: str,
                            view_name: str,
                            view_options: ViewOptions | None = None,
                            **kwargs: Any) -> Deferred[ViewResult]:
        raise unsupported('view_query')
