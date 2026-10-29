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

from collections.abc import Callable, Iterable
from typing import (TYPE_CHECKING,
                    NewType,
                    TypedDict)

if TYPE_CHECKING:
    from ._core import transaction_get_result

# Opaque PyCapsules: the binding hands them out and takes them back, and Python never looks inside.
TransactionsCapsuleType = NewType('TransactionsCapsuleType', object)
TransactionContextCapsuleType = NewType('TransactionContextCapsuleType', object)


class ParsedTransactionOptions(TypedDict, total=False):
    """The wire keys a `TransactionOptions` block reaches the binding as.

    The metadata keyspace arrives as three keys because the core takes a bucket, a scope and a
    collection, and the binding builds one only when all three are present.
    """

    durability_level: int | None
    metadata_bucket: str | None
    metadata_collection: str | None
    metadata_scope: str | None
    scan_consistency: str | None
    timeout: int | None


class ParsedTransactionsConfig(ParsedTransactionOptions, total=False):
    """The wire keys a `TransactionConfig` block reaches the binding as."""

    cleanup_client_attempts: bool | None
    cleanup_lost_attempts: bool | None
    cleanup_window: int | None


class ParsedTransactionsQueryOptions(TypedDict, total=False):
    """The wire keys a `TransactionQueryOptions` block reaches the binding as.

    `bucket_name` and `scope_name` are what a `scope` expands to.  Neither is read by the
    binding, so a block carrying them configures nothing; they are declared because the block
    does carry them and a shape that omitted them would say the expansion does not happen.
    """

    adhoc: bool | None
    bucket_name: str | None
    client_context_id: str | None
    max_parallelism: int | None
    metrics: bool | None
    named_parameters: dict[str, bytes] | None
    pipeline_batch: int | None
    pipeline_cap: int | None
    positional_parameters: Iterable[bytes] | None
    profile: str | None
    raw: dict[str, bytes] | None
    readonly: bool | None
    scan_cap: int | None
    scan_consistency: str | None
    scan_wait: int | None
    scope_name: str | None


class _CompletionCallbacks(TypedDict, total=False):
    """Both or neither.  Without them the call blocks and returns its outcome; with them it returns
    `None` at once and hands the outcome to one of them.  An explicit `None` counts as given."""

    callback: Callable[..., None]
    errback: Callable[..., None]


class CreateNewAttemptContextRequest(_CompletionCallbacks):
    ctx: TransactionContextCapsuleType


class CreateTransactionContextRequest(TypedDict):
    txns: TransactionsCapsuleType
    transaction_options: ParsedTransactionOptions


class DestroyTransactionsRequest(TypedDict):
    txns: TransactionsCapsuleType


class TransactionCommitRequest(_CompletionCallbacks):
    ctx: TransactionContextCapsuleType


class TransactionGetMultiOpRequest(_CompletionCallbacks):
    ctx: TransactionContextCapsuleType
    op: int
    specs: list[tuple[str, str, str, str]]
    mode: str


class _TransactionOpRequired(_CompletionCallbacks):
    ctx: TransactionContextCapsuleType
    op: int


class TransactionOpRequest(_TransactionOpRequired, total=False):
    """A get or an insert names its document by `bucket`, `scope`, `collection_name` and `key`; a replace
    or a remove names it by `txn_get_result`.  An insert or a replace carries the encoded `value`."""

    bucket: str
    scope: str
    collection_name: str
    key: str
    value: tuple[bytes, int]
    txn_get_result: transaction_get_result


class TransactionQueryOpRequest(_CompletionCallbacks):
    ctx: TransactionContextCapsuleType
    statement: str
    options: ParsedTransactionsQueryOptions


class TransactionRollbackRequest(_CompletionCallbacks):
    ctx: TransactionContextCapsuleType


class HdrPercentileReport(TypedDict):
    total_count: int
    percentiles: list[int]
