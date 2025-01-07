#  Copyright 2016-2022. Couchbase, Inc.
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

import logging
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Coroutine)

from couchbase.exceptions import TransactionExpired, TransactionFailed
from couchbase.pycbc_core import create_transactions, destroy_transactions
from couchbase.transactions.transaction_result import TransactionResult
from couchbase.transcoder import JSONTranscoder

if TYPE_CHECKING:
    from acouchbase.transactions import AttemptContext as AsyncAttemptContext
    from couchbase.logic.cluster import ClusterLogic
    from couchbase.options import TransactionConfig
    from couchbase.transactions import AttemptContext as BlockingAttemptContext

log = logging.getLogger(__name__)


class TransactionsLogic:
    def __init__(self,
                 cluster,  # type: ClusterLogic
                 config   # type: TransactionConfig
                 ):
        self._config = config
        self._loop = None
        # while the cluster has a default transcoder, it might not be a JSONTranscoder
        self._transcoder = JSONTranscoder(cluster.default_serializer)
        if hasattr(cluster, "loop"):
            self._loop = cluster.loop
        self._txns = create_transactions(cluster.connection, self._config._base)
        log.info('created transactions object using config=%s, transcoder=%s', self._config, self._transcoder)

    def run(self,
            logic,          # type: Callable[[BlockingAttemptContext], None]
            attempt_ctx     # type: BlockingAttemptContext
            ) -> TransactionResult:

        while True:
            attempt_ctx._new_attempt()
            try:
                logic(attempt_ctx)
            except Exception as ex:
                attempt_ctx._rollback()
                if isinstance(ex, TransactionExpired):
                    raise ex from None
                raise TransactionFailed(exc_info={'inner_cause': ex}) from None

            try:
                # calls finalize internally
                res = attempt_ctx._commit()
                if not res:
                    continue
                return TransactionResult(**res)
            except Exception:
                # commit failed, retrying...
                pass  # nosec

    async def run_async(self,
                        logic,          # type: Callable[[AsyncAttemptContext], Coroutine[Any, Any, None]]
                        attempt_ctx     # type: AsyncAttemptContext
                        ) -> TransactionResult:

        while True:
            await attempt_ctx._new_attempt()
            try:
                await logic(attempt_ctx)
            except Exception as ex:
                await attempt_ctx._rollback()
                if isinstance(ex, TransactionExpired):
                    raise ex from None
                raise TransactionFailed(exc_info={'inner_cause': ex}) from None

            try:
                # calls finalize internally
                res = await attempt_ctx._commit()
                if not res:
                    continue
                return res
            except Exception:
                # commit failed, retrying...
                pass  # nosec

    def close(self, **kwargs):
        log.info('shutting down transactions...')
        return destroy_transactions(txns=self._txns, **kwargs)
