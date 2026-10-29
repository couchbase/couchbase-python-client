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

"""The `acouchbase.transactions` import path.

What `couchbase.transactions` publishes, for asyncio code.  The names every API shares are the same
objects as there, and the rest are asyncio's own, from `acouchbase.transactions.transactions`.
"""

__all__ = [
    'AttemptContext',
    'TransactionGetMultiMode',
    'TransactionGetMultiReplicasFromPreferredServerGroupMode',
    'TransactionGetMultiReplicasFromPreferredServerGroupResult',
    'TransactionGetMultiReplicasFromPreferredServerGroupSpec',
    'TransactionGetMultiResult',
    'TransactionGetMultiSpec',
    'TransactionGetResult',
    'TransactionKeyspace',
    'TransactionQueryResults',
    'TransactionResult',
    'Transactions',
]

from acouchbase.transactions.transaction_get_result import TransactionGetResult  # noqa: F401
from acouchbase.transactions.transaction_keyspace import TransactionKeyspace  # noqa: F401
from acouchbase.transactions.transaction_query_results import TransactionQueryResults  # noqa: F401
from acouchbase.transactions.transaction_result import TransactionResult  # noqa: F401
from acouchbase.transactions.transactions import AttemptContext  # noqa: F401
from acouchbase.transactions.transactions import Transactions  # noqa: F401
from acouchbase.transactions.transactions_get_multi import TransactionGetMultiMode  # noqa: F401
from acouchbase.transactions.transactions_get_multi import TransactionGetMultiResult  # noqa: F401
from acouchbase.transactions.transactions_get_multi import TransactionGetMultiSpec  # noqa: F401

from acouchbase.transactions.transactions_get_multi import (  # nopep8 # isort:skip # noqa: F401
    TransactionGetMultiReplicasFromPreferredServerGroupMode,
    TransactionGetMultiReplicasFromPreferredServerGroupResult,
    TransactionGetMultiReplicasFromPreferredServerGroupSpec,
)
