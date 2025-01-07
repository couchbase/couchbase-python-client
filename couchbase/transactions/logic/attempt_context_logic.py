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
                    Dict,
                    Optional)

from couchbase.exceptions import ErrorMapper
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.options import TransactionOptions
from couchbase.pycbc_core import (create_new_attempt_context,
                                  create_transaction_context,
                                  transaction_commit,
                                  transaction_op,
                                  transaction_operations,
                                  transaction_query_op,
                                  transaction_rollback)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase._utils import PyCapsuleType
    from couchbase.transcoder import Transcoder

log = logging.getLogger(__name__)


class AttemptContextLogic:
    def __init__(self,
                 txns,    # type: PyCapsuleType
                 transcoder,  # type: Transcoder
                 loop,    # type: Optional[AbstractEventLoop]
                 opts    # type: Optional[PyCapsuleType]
                 ) -> None:
        if opts is None:
            opts = TransactionOptions()._base
        self._txnctx = create_transaction_context(txns=txns, transaction_options=opts)
        self._loop = loop
        self._transcoder = transcoder

    def _handle_exception(self, ex: Any) -> None:
        if isinstance(ex, Exception):
            raise ex
        if isinstance(ex, CouchbaseBaseException):
            raise ErrorMapper.build_exception(ex)

    def _new_attempt(self) -> None:
        new_attempt_ctx = create_new_attempt_context(ctx=self._txnctx)
        self._handle_exception(new_attempt_ctx)

    def _new_attempt_async(self,
                           **kwargs,  # type: Dict[str, Any]
                           ) -> None:
        create_new_attempt_context(ctx=self._txnctx, **kwargs)

    def _rollback(self) -> None:
        rollback_res = transaction_rollback(ctx=self._txnctx)
        self._handle_exception(rollback_res)

    def _rollback_async(self,
                        **kwargs,  # type: Dict[str, Any]
                        ) -> None:
        transaction_rollback(ctx=self._txnctx, **kwargs)

    def _commit(self) -> Optional[Dict[str, Any]]:
        commit_res = transaction_commit(ctx=self._txnctx)
        self._handle_exception(commit_res)
        return commit_res

    def _commit_async(self,
                      **kwargs,  # type: Dict[str, Any]
                      ) -> Optional[Dict[str, Any]]:
        return transaction_commit(ctx=self._txnctx, **kwargs)

    def get(self, coll, key, **kwargs):
        # make sure we don't pass the transcoder along
        kwargs.pop('transcoder', None)
        kwargs.update(coll._get_connection_args())
        kwargs.pop("conn")
        kwargs.update({
            'key': key,
            'ctx': self._txnctx,
            'op': transaction_operations.GET.value
        })
        log.debug('get calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def insert(self, coll, key, value, **kwargs):
        transcoder = kwargs.pop('transcoder', self._transcoder)
        kwargs.update(coll._get_connection_args())
        kwargs.pop("conn")
        kwargs.update({
            'key': key,
            'ctx': self._txnctx,
            'op': transaction_operations.INSERT.value,
            'value': transcoder.encode_value(value)
        })
        log.debug('insert calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def replace(self, txn_get_result, value, **kwargs):
        transcoder = kwargs.pop('transcoder', self._transcoder)
        kwargs.update({
            'ctx': self._txnctx,
            'op': transaction_operations.REPLACE.value,
            'value': transcoder.encode_value(value),
            'txn_get_result': txn_get_result._res
        })
        log.debug('replace calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def remove(self, txn_get_result, **kwargs):
        kwargs.update({'ctx': self._txnctx,
                       'op': transaction_operations.REMOVE.value,
                       'txn_get_result': txn_get_result._res})
        log.debug('remove calling transaction op with %s', kwargs)
        return transaction_op(**kwargs)

    def query(self, query, options, **kwargs):
        kwargs.update({'ctx': self._txnctx, 'statement': query, 'options': options._base})
        log.debug('query calling transaction_op with %s', kwargs)
        return transaction_query_op(**kwargs)
