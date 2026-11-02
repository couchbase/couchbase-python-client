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

"""What a protocol's bucket impl must provide to the bucket facades.

`BucketInterface` and its async and Twisted twins state the bucket's ping and view query in the public shape, with
what the facade reads besides: the bucket name, closing and connecting, and the impls behind `scope()` and the
bucket-level managers.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from acouchbase.result import AsyncViewResult
    from couchbase.common._interfaces._collection_mgmt import (AsyncCollectionMgmtInterface,
                                                               CollectionMgmtInterface,
                                                               TxCollectionMgmtInterface)
    from couchbase.common._interfaces._scope import (AsyncScopeInterface,
                                                     ScopeInterface,
                                                     TxScopeInterface)
    from couchbase.common._interfaces._view_index_mgmt import (AsyncViewIndexMgmtInterface,
                                                               TxViewIndexMgmtInterface,
                                                               ViewIndexMgmtInterface)
    from couchbase.common.options import PingOptions, ViewOptions
    from couchbase.common.result import PingResult
    from couchbase.result import ViewResult


class BucketInterface(Protocol):
    @property
    def bucket_name(self) -> str: ...

    def close_bucket(self) -> None: ...

    def scope(self, name: str) -> ScopeInterface: ...

    def ping(self,
             opts: PingOptions | None = None,
             **kwargs: Any) -> PingResult: ...

    def view_query(self,
                   design_doc: str,
                   view_name: str,
                   view_options: ViewOptions | None = None,
                   **kwargs: Any) -> ViewResult: ...

    def collection_mgmt(self) -> CollectionMgmtInterface: ...

    def view_index_mgmt(self) -> ViewIndexMgmtInterface: ...


class AsyncBucketInterface(Protocol):
    @property
    def bucket_name(self) -> str: ...

    async def close_bucket(self) -> None: ...

    async def wait_until_bucket_connected(self) -> None: ...

    def scope(self, name: str) -> AsyncScopeInterface: ...

    async def ping(self,
                   opts: PingOptions | None = None,
                   **kwargs: Any) -> PingResult: ...

    def view_query(self,
                   design_doc: str,
                   view_name: str,
                   view_options: ViewOptions | None = None,
                   **kwargs: Any) -> AsyncViewResult: ...

    def collection_mgmt(self) -> AsyncCollectionMgmtInterface: ...

    def view_index_mgmt(self) -> AsyncViewIndexMgmtInterface: ...


class TxBucketInterface(Protocol):
    @property
    def bucket_name(self) -> str: ...

    def close_bucket_deferred(self) -> Deferred[None]: ...

    def wait_until_bucket_connected_deferred(self) -> Deferred[None]: ...

    def scope(self, name: str) -> TxScopeInterface: ...

    def ping_deferred(self,
                      opts: PingOptions | None = None,
                      **kwargs: Any) -> Deferred[PingResult]: ...

    def view_query_deferred(self,
                            design_doc: str,
                            view_name: str,
                            view_options: ViewOptions | None = None,
                            **kwargs: Any) -> Deferred[ViewResult]: ...

    def collection_mgmt(self) -> TxCollectionMgmtInterface: ...

    def view_index_mgmt(self) -> TxViewIndexMgmtInterface: ...
