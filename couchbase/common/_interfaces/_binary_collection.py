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

"""What a protocol's impl must provide to the binary collection facades.

The collection facade's `binary()` takes this impl from the collection impl's own `binary()`.  The SDK imports this
module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred

    from couchbase.common.options import (AppendMultiOptions,
                                          AppendOptions,
                                          DecrementMultiOptions,
                                          DecrementOptions,
                                          IncrementMultiOptions,
                                          IncrementOptions,
                                          PrependMultiOptions,
                                          PrependOptions)
    from couchbase.common.result import (CounterResult,
                                         MultiCounterResult,
                                         MultiMutationResult,
                                         MutationResult)


class BinaryCollectionInterface(Protocol):
    def increment(self,
                  key: str,
                  opts: IncrementOptions | None = None,
                  **kwargs: Any) -> CounterResult: ...

    def decrement(self,
                  key: str,
                  opts: DecrementOptions | None = None,
                  **kwargs: Any) -> CounterResult: ...

    def append(self,
               key: str,
               value: str | bytes | bytearray,
               opts: AppendOptions | None = None,
               **kwargs: Any) -> MutationResult: ...

    def prepend(self,
                key: str,
                value: str | bytes | bytearray,
                opts: PrependOptions | None = None,
                **kwargs: Any) -> MutationResult: ...

    def append_multi(self,
                     keys_and_values: Mapping[str, str | bytes | bytearray],
                     opts: AppendMultiOptions | None = None,
                     **kwargs: Any) -> MultiMutationResult: ...

    def prepend_multi(self,
                      keys_and_values: Mapping[str, str | bytes | bytearray],
                      opts: PrependMultiOptions | None = None,
                      **kwargs: Any) -> MultiMutationResult: ...

    def increment_multi(self,
                        keys: list[str],
                        opts: IncrementMultiOptions | None = None,
                        **kwargs: Any) -> MultiCounterResult: ...

    def decrement_multi(self,
                        keys: list[str],
                        opts: DecrementMultiOptions | None = None,
                        **kwargs: Any) -> MultiCounterResult: ...


class AsyncBinaryCollectionInterface(Protocol):
    async def increment(self,
                        key: str,
                        opts: IncrementOptions | None = None,
                        **kwargs: Any) -> CounterResult: ...

    async def decrement(self,
                        key: str,
                        opts: DecrementOptions | None = None,
                        **kwargs: Any) -> CounterResult: ...

    async def append(self,
                     key: str,
                     value: str | bytes | bytearray,
                     opts: AppendOptions | None = None,
                     **kwargs: Any) -> MutationResult: ...

    async def prepend(self,
                      key: str,
                      value: str | bytes | bytearray,
                      opts: PrependOptions | None = None,
                      **kwargs: Any) -> MutationResult: ...


class TxBinaryCollectionInterface(Protocol):
    def increment_deferred(self,
                           key: str,
                           opts: IncrementOptions | None = None,
                           **kwargs: Any) -> Deferred[CounterResult]: ...

    def decrement_deferred(self,
                           key: str,
                           opts: DecrementOptions | None = None,
                           **kwargs: Any) -> Deferred[CounterResult]: ...

    def append_deferred(self,
                        key: str,
                        value: str | bytes | bytearray,
                        opts: AppendOptions | None = None,
                        **kwargs: Any) -> Deferred[MutationResult]: ...

    def prepend_deferred(self,
                         key: str,
                         value: str | bytes | bytearray,
                         opts: PrependOptions | None = None,
                         **kwargs: Any) -> Deferred[MutationResult]: ...
