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

from typing import (TYPE_CHECKING,
                    Callable,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Tuple,
                    TypedDict,
                    TypeVar)

if TYPE_CHECKING:
    from ._core import pycbc_connection, transaction_get_result

TransactionsCapsuleType = TypeVar('TransactionsCapsuleType')
TransactionContextCapsuleType = TypeVar('TransactionContextCapsuleType')


class ParsedTransactionOptions(TypedDict, total=False):
    """The wire keys a `TransactionOptions` block reaches the binding as.

    The metadata keyspace arrives as three keys because the core takes a bucket, a scope and a
    collection, and the binding builds one only when all three are present.
    """

    durability_level: Optional[int]
    metadata_bucket: Optional[str]
    metadata_collection: Optional[str]
    metadata_scope: Optional[str]
    scan_consistency: Optional[str]
    timeout: Optional[int]


class ParsedTransactionsConfig(ParsedTransactionOptions, total=False):
    """The wire keys a `TransactionConfig` block reaches the binding as."""

    cleanup_client_attempts: Optional[bool]
    cleanup_lost_attempts: Optional[bool]
    cleanup_window: Optional[int]


class ParsedTransactionsQueryOptions(TypedDict, total=False):
    """The wire keys a `TransactionQueryOptions` block reaches the binding as.

    `bucket_name` and `scope_name` are what a `scope` expands to.  Neither is read by the
    binding, so a block carrying them configures nothing; they are declared because the block
    does carry them and a shape that omitted them would say the expansion does not happen.
    """

    adhoc: Optional[bool]
    bucket_name: Optional[str]
    client_context_id: Optional[str]
    max_parallelism: Optional[int]
    metrics: Optional[bool]
    named_parameters: Optional[Dict[str, bytes]]
    pipeline_batch: Optional[int]
    pipeline_cap: Optional[int]
    positional_parameters: Optional[Iterable[bytes]]
    profile: Optional[str]
    raw: Optional[Dict[str, bytes]]
    readonly: Optional[bool]
    scan_cap: Optional[int]
    scan_consistency: Optional[str]
    scan_wait: Optional[int]
    scope_name: Optional[str]


class CreateNewAttemptContextRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class CreateTransactionsRequest(TypedDict):
    conn: pycbc_connection
    config: ParsedTransactionsConfig


class CreateTransactionContextRequest(TypedDict):
    txns: TransactionsCapsuleType
    transaction_options: ParsedTransactionOptions


class DestroyTransactionsRequest(TypedDict):
    txns: TransactionsCapsuleType


class TransactionCommitRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class TransactionGetMultiOpRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    op: int
    specs: Tuple[str, str, str, str]
    mode: Optional[str]
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class TransactionOpRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    bucket: str
    scope: str
    collection_name: str
    key: str
    op: int
    value: Optional[Tuple[bytes, int]]
    txn_get_result: Optional[transaction_get_result]
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class TransactionQueryOpRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    statement: str
    options: ParsedTransactionsQueryOptions
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class TransactionRollbackRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class HdrPercentileReport(TypedDict):
    total_count: int
    percentiles: List[int]
