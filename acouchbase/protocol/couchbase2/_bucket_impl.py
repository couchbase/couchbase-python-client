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

"""An asyncio bucket over the gateway, which has nothing to open: the gateway resolves the bucket on each request."""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn)

from acouchbase.protocol.couchbase2._scope_impl import AsyncScopeImpl
from couchbase.protocol.couchbase2._core import unsupported

if TYPE_CHECKING:
    from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
    from acouchbase.result import AsyncViewResult
    from couchbase.common.options import PingOptions, ViewOptions
    from couchbase.common.result import PingResult


class AsyncBucketImpl:

    def __init__(self, bucket_name: str, cluster_impl: AsyncClusterImpl) -> None:
        self._bucket_name = bucket_name
        self._cluster_impl = cluster_impl

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @property
    def cluster_impl(self) -> AsyncClusterImpl:
        """**INTERNAL**"""
        return self._cluster_impl

    async def close_bucket(self) -> None:
        """Nothing was opened, so nothing is closed."""

    async def wait_until_bucket_connected(self) -> None:
        """Nothing is opened, so there is nothing to wait for."""

    def scope(self, name: str) -> AsyncScopeImpl:
        """**INTERNAL**"""
        return AsyncScopeImpl(name, self)

    async def ping(self, opts: PingOptions | None = None, **kwargs: Any) -> PingResult:
        raise unsupported('ping')

    def view_query(self,
                   design_doc: str,
                   view_name: str,
                   view_options: ViewOptions | None = None,
                   **kwargs: Any) -> AsyncViewResult:
        raise unsupported('view_query')

    def collection_mgmt(self) -> NoReturn:
        raise unsupported('Collection management')

    def view_index_mgmt(self) -> NoReturn:
        raise unsupported('View index management')
