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
    from ._core import transaction_options  # noqa: F401
    from ._core import (pycbc_connection,
                        transaction_config,
                        transaction_get_result,
                        transaction_query_options)

TransactionsCapsuleType = TypeVar('TransactionsCapsuleType')
TransactionContextCapsuleType = TypeVar('TransactionContextCapsuleType')


class ParsedTransactionsQueryOptions(TypedDict, total=False):
    adhoc: Optional[bool]
    bucket_name: Optional[str]
    client_context_id: Optional[str]
    metrics: Optional[bool]
    max_parallelism: Optional[int]
    pipeline_batch: Optional[int]
    pipeline_cap: Optional[int]
    positional_parameters: Optional[Iterable[bytes]]
    profile_mode: Optional[str]
    name_parameters: Optional[Dict[str, bytes]]
    raw: Optional[Dict[str, bytes]]
    read_only: Optional[bool]
    scan_cap: Optional[int]
    scan_consistency: Optional[str]
    scan_wait: Optional[int]


class CreateNewAttemptContextRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class CreateTransactionsRequest(TypedDict):
    conn: pycbc_connection
    config: transaction_config


class CreateTransactionContextRequest(TypedDict):
    txns: TransactionsCapsuleType
    transaction_options: transaction_options


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
    options: transaction_query_options
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class TransactionRollbackRequest(TypedDict):
    ctx: TransactionContextCapsuleType
    callback: Optional[Callable[..., None]]
    errback: Optional[Callable[..., None]]


class HdrPercentileReport(TypedDict):
    total_count: int
    percentiles: List[int]
