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

from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from couchbase.protocol.classic._pycbc_core import transaction_get_result, transaction_operations
from couchbase.protocol.classic._wire import transaction_wire

if TYPE_CHECKING:
    from acouchbase.collection import AsyncCollection
    from couchbase.collection import Collection
    from couchbase.common._utils import JSONType
    from couchbase.common.options import TransactionQueryOptions
    from couchbase.protocol.classic._pycbc_core.pycbc_core_types import (ParsedTransactionsQueryOptions,
                                                                         TransactionCommitRequest,
                                                                         TransactionContextCapsuleType,
                                                                         TransactionGetMultiOpRequest,
                                                                         TransactionOpRequest,
                                                                         TransactionQueryOpRequest)
    from couchbase.transactions.transaction_get_result import TransactionGetResult
    from couchbase.transactions.transactions_get_multi import (TransactionGetMultiMode,
                                                               TransactionGetMultiReplicasFromPreferredServerGroupMode,
                                                               TransactionGetMultiReplicasFromPreferredServerGroupSpec,
                                                               TransactionGetMultiSpec)
    from couchbase.transcoder import Transcoder


def _binding_result(txn_get_result: TransactionGetResult) -> transaction_get_result:
    # Every result a classic transaction hands back was built from this binding's own read.
    return cast(transaction_get_result, txn_get_result._res)


class TransactionsRequestBuilder:
    """Builds the binding's arguments for one transaction's operations.  It calls nothing."""

    def __init__(self, ctx: TransactionContextCapsuleType) -> None:
        self._ctx = ctx

    def _build_keyed_request(self, coll: Collection | AsyncCollection, key: str, op: int) -> TransactionOpRequest:
        return {
            'ctx': self._ctx,
            'bucket': coll._impl.bucket_name,
            'scope': coll._impl.scope_name,
            'collection_name': coll.name,
            'key': key,
            'op': op,
        }

    def build_attempt_request(self) -> TransactionCommitRequest:
        """The request for a new attempt, a rollback or a commit, all of which take only the context."""
        return {'ctx': self._ctx}

    def build_get_request(self, coll: Collection | AsyncCollection, key: str) -> TransactionOpRequest:
        return self._build_keyed_request(coll, key, transaction_operations.GET.value)

    def build_get_replica_from_preferred_server_group_request(self,
                                                              coll: Collection | AsyncCollection,
                                                              key: str) -> TransactionOpRequest:
        op = transaction_operations.GET_REPLICA_FROM_PREFERRED_SERVER_GROUP.value
        return self._build_keyed_request(coll, key, op)

    def build_get_multi_request(self,
                                specs: Sequence[TransactionGetMultiSpec],
                                mode: TransactionGetMultiMode | None) -> TransactionGetMultiOpRequest:
        return {
            'ctx': self._ctx,
            'op': transaction_operations.GET_MULTI.value,
            'specs': [s._astuple() for s in specs],
            'mode': mode.value if mode else '',
        }

    def build_get_multi_replicas_from_preferred_server_group_request(
            self,
            specs: Sequence[TransactionGetMultiReplicasFromPreferredServerGroupSpec],
            mode: TransactionGetMultiReplicasFromPreferredServerGroupMode | None) -> TransactionGetMultiOpRequest:
        return {
            'ctx': self._ctx,
            'op': transaction_operations.GET_MULTI_REPLICAS_FROM_PREFERRED_SERVER_GROUP.value,
            'specs': [s._astuple() for s in specs],
            'mode': mode.value if mode else '',
        }

    def build_insert_request(self,
                             coll: Collection | AsyncCollection,
                             key: str,
                             value: JSONType,
                             transcoder: Transcoder) -> TransactionOpRequest:
        req = self._build_keyed_request(coll, key, transaction_operations.INSERT.value)
        req['value'] = transcoder.encode_value(value)
        return req

    def build_replace_request(self,
                              txn_get_result: TransactionGetResult,
                              value: JSONType,
                              transcoder: Transcoder) -> TransactionOpRequest:
        return {
            'ctx': self._ctx,
            'op': transaction_operations.REPLACE.value,
            'value': transcoder.encode_value(value),
            'txn_get_result': _binding_result(txn_get_result),
        }

    def build_remove_request(self, txn_get_result: TransactionGetResult) -> TransactionOpRequest:
        return {
            'ctx': self._ctx,
            'op': transaction_operations.REMOVE.value,
            'txn_get_result': _binding_result(txn_get_result),
        }

    def build_query_request(self,
                            statement: str,
                            options: TransactionQueryOptions | None) -> TransactionQueryOpRequest:
        return {
            'ctx': self._ctx,
            'statement': statement,
            'options': cast('ParsedTransactionsQueryOptions',
                            transaction_wire('TransactionQueryOptions', options or {})),
        }
