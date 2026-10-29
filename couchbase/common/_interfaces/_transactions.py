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

"""What a protocol's transactions impl must provide to the transactions facades.

`TransactionsInterface` and `AttemptContextInterface` are required of every protocol.  Each
`Supports...` interface is a capability, provided by a protocol that declares it, and reached from a
facade only through `require()`.  The SDK imports this module only under `TYPE_CHECKING`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Sequence
from typing import (TYPE_CHECKING,
                    Any,
                    Protocol)

if TYPE_CHECKING:
    from acouchbase.collection import AsyncCollection
    from couchbase.collection import Collection
    from couchbase.common._capabilities import Capability
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (TransactionGetMultiOptions,
                                          TransactionGetMultiReplicasFromPreferredServerGroupOptions,
                                          TransactionGetOptions,
                                          TransactionGetReplicaFromPreferredServerGroupOptions,
                                          TransactionInsertOptions,
                                          TransactionOptions,
                                          TransactionQueryOptions,
                                          TransactionReplaceOptions)
    from couchbase.transactions import (TransactionGetMultiReplicasFromPreferredServerGroupResult,
                                        TransactionGetMultiReplicasFromPreferredServerGroupSpec,
                                        TransactionGetMultiResult,
                                        TransactionGetMultiSpec,
                                        TransactionGetResult,
                                        TransactionQueryResults)


class TransactionsInterface(Protocol):
    def begin_transaction(self,
                          transaction_options: TransactionOptions | None = None,
                          **kwargs: Any) -> AttemptContextInterface: ...

    def close(self) -> None: ...


class AttemptContextInterface(Protocol):
    """One transaction's state and the primitives `Transactions.run` drives over it.

    The state belongs to the protocol and nothing outside the impl reads it: classic holds the core's
    transaction context, and a protocol whose server runs the transaction would hold its ids and
    deadline.  `begin_transaction` returns one per transaction, and `run` calls `begin_attempt` before
    each run of the caller's logic.  Then, by how the logic ended:

    - It returned: `run` calls `commit`.
    - An operation raised `TransactionOperationFailed`: the impl records the failure against the
      attempt, whether or not the logic catches it, and `run` calls `commit`.  A commit with a
      recorded failure commits nothing: it rolls back and either asks for another attempt or raises.
    - It raised anything else: `run` calls `rollback` and fails the transaction.

    `commit` returns the result's fields when the transaction is done, or an empty result to ask for
    another attempt, having rolled back what needed it.  Its raise is final, `TransactionExpired`,
    `TransactionCommitAmbiguous` or `TransactionFailed`, and `run` propagates it without another
    attempt, since an ambiguous commit may have landed.  So the impl owns expiry: past the
    transaction's timeout `commit` raises `TransactionExpired` rather than asking again.
    """

    @property
    def capabilities(self) -> frozenset[Capability]: ...

    def begin_attempt(self) -> None: ...

    def rollback(self) -> None: ...

    def commit(self) -> dict[str, Any] | None: ...

    def get(self,
            coll: Collection,
            key: str,
            options: TransactionGetOptions | None = None,
            **kwargs: Any) -> TransactionGetResult: ...

    def get_multi(self,
                  specs: Sequence[TransactionGetMultiSpec],
                  options: TransactionGetMultiOptions | None = None,
                  **kwargs: Any) -> TransactionGetMultiResult: ...

    def insert(self,
               coll: Collection,
               key: str,
               value: JSONType,
               options: TransactionInsertOptions | None = None,
               **kwargs: Any) -> TransactionGetResult: ...

    def replace(self,
                txn_get_result: TransactionGetResult,
                value: JSONType,
                options: TransactionReplaceOptions | None = None,
                **kwargs: Any) -> TransactionGetResult: ...

    def remove(self, txn_get_result: TransactionGetResult, **kwargs: Any) -> None: ...


class SupportsTransactionsQuery(Protocol):
    def query(self,
              query: str,
              options: TransactionQueryOptions | None = None,
              **kwargs: Any) -> TransactionQueryResults: ...


class SupportsTransactionsPreferredServerGroup(Protocol):
    def get_replica_from_preferred_server_group(
        self,
        coll: Collection,
        key: str,
        options: TransactionGetReplicaFromPreferredServerGroupOptions | None = None,
        **kwargs: Any
    ) -> TransactionGetResult: ...

    def get_multi_replicas_from_preferred_server_group(
        self,
        specs: Sequence[TransactionGetMultiReplicasFromPreferredServerGroupSpec],
        options: TransactionGetMultiReplicasFromPreferredServerGroupOptions | None = None,
        **kwargs: Any
    ) -> TransactionGetMultiReplicasFromPreferredServerGroupResult: ...


class AsyncTransactionsInterface(Protocol):
    def begin_transaction(self,
                          transaction_options: TransactionOptions | None = None,
                          **kwargs: Any) -> AsyncAttemptContextInterface: ...

    def close(self) -> None: ...


class AsyncAttemptContextInterface(Protocol):
    """`AttemptContextInterface`'s contract, with every primitive awaitable."""

    @property
    def capabilities(self) -> frozenset[Capability]: ...

    def begin_attempt(self) -> Awaitable[None]: ...

    def rollback(self) -> Awaitable[None]: ...

    def commit(self) -> Awaitable[dict[str, Any] | None]: ...

    def get(self,
            coll: AsyncCollection,
            key: str,
            options: TransactionGetOptions | None = None,
            **kwargs: Any) -> Awaitable[TransactionGetResult]: ...

    def get_multi(self,
                  specs: Sequence[TransactionGetMultiSpec],
                  options: TransactionGetMultiOptions | None = None,
                  **kwargs: Any) -> Awaitable[TransactionGetMultiResult]: ...

    def insert(self,
               coll: AsyncCollection,
               key: str,
               value: JSONType,
               options: TransactionInsertOptions | None = None,
               **kwargs: Any) -> Awaitable[TransactionGetResult]: ...

    def replace(self,
                txn_get_result: TransactionGetResult,
                value: JSONType,
                options: TransactionReplaceOptions | None = None,
                **kwargs: Any) -> Awaitable[TransactionGetResult]: ...

    def remove(self, txn_get_result: TransactionGetResult, **kwargs: Any) -> Awaitable[None]: ...


class AsyncSupportsTransactionsQuery(Protocol):
    def query(self,
              query: str,
              options: TransactionQueryOptions | None = None,
              **kwargs: Any) -> Awaitable[TransactionQueryResults]: ...


class AsyncSupportsTransactionsPreferredServerGroup(Protocol):
    def get_replica_from_preferred_server_group(
        self,
        coll: AsyncCollection,
        key: str,
        options: TransactionGetReplicaFromPreferredServerGroupOptions | None = None,
        **kwargs: Any
    ) -> Awaitable[TransactionGetResult]: ...

    def get_multi_replicas_from_preferred_server_group(
        self,
        specs: Sequence[TransactionGetMultiReplicasFromPreferredServerGroupSpec],
        options: TransactionGetMultiReplicasFromPreferredServerGroupOptions | None = None,
        **kwargs: Any
    ) -> Awaitable[TransactionGetMultiReplicasFromPreferredServerGroupResult]: ...
