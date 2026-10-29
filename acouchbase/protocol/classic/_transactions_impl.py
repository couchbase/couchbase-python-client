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

import logging
from collections.abc import Callable, Sequence
from typing import (TYPE_CHECKING,
                    Any,
                    TypeVar)

from couchbase.protocol.classic._capabilities import CAPABILITIES
from couchbase.protocol.classic._pycbc_core import create_new_attempt_context
from couchbase.protocol.classic._pycbc_core import pycbc_exception as PycbcCoreException
from couchbase.protocol.classic._pycbc_core import (transaction_commit,
                                                    transaction_get_multi_op,
                                                    transaction_op,
                                                    transaction_query_op,
                                                    transaction_rollback)
from couchbase.protocol.classic._transactions_impl import (as_transactions_exception,
                                                           check_keywords,
                                                           close_transactions,
                                                           loggable_request,
                                                           loggable_result,
                                                           open_transaction_context,
                                                           open_transactions,
                                                           read_option)
from couchbase.protocol.classic._transactions_req_builder import TransactionsRequestBuilder
from couchbase.transactions import (TransactionGetMultiReplicasFromPreferredServerGroupResult,
                                    TransactionGetMultiResult,
                                    TransactionGetResult,
                                    TransactionQueryResults)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop, Future

    from acouchbase.collection import AsyncCollection
    from acouchbase.protocol.classic._client_adapter import AsyncClientAdapter
    from acouchbase.protocol.classic._cluster_impl import AsyncClusterImpl
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (TransactionGetMultiOptions,
                                          TransactionGetMultiReplicasFromPreferredServerGroupOptions,
                                          TransactionGetOptions,
                                          TransactionGetReplicaFromPreferredServerGroupOptions,
                                          TransactionInsertOptions,
                                          TransactionOptions,
                                          TransactionQueryOptions,
                                          TransactionReplaceOptions)
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.protocol.classic._pycbc_core.pycbc_core_types import TransactionsCapsuleType, _CompletionCallbacks
    from couchbase.transactions import TransactionGetMultiReplicasFromPreferredServerGroupSpec, TransactionGetMultiSpec
    from couchbase.transcoder import Transcoder

log = logging.getLogger('couchbase.transactions')

T = TypeVar('T')


def _discard(res: Any) -> None:
    return None


class AsyncTransactionsImpl:
    """**INTERNAL**"""

    def __init__(self, cluster: AsyncClusterImpl) -> None:
        self._client_adapter = cluster.client_adapter
        self._loop = cluster.loop
        settings = cluster.cluster_settings
        self._txns, self._transcoder = open_transactions(cluster.connection,
                                                         settings.transaction_config,
                                                         settings.default_serializer)

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

    def close(self) -> None:
        close_transactions(self._txns)

    def begin_transaction(self,
                          transaction_options: TransactionOptions | None = None,
                          **kwargs: Any) -> AsyncAttemptContextImpl:
        return AsyncAttemptContextImpl(self._client_adapter,
                                       self._txns,
                                       self._loop,
                                       self._transcoder,
                                       transaction_options)


class AsyncAttemptContextImpl:
    """**INTERNAL**"""

    capabilities = CAPABILITIES

    def __init__(self,
                 client_adapter: AsyncClientAdapter,
                 txns: TransactionsCapsuleType,
                 loop: AbstractEventLoop,
                 transcoder: Transcoder,
                 opts: TransactionOptions | None) -> None:
        self._client_adapter = client_adapter
        self._loop = loop
        self._transcoder = transcoder
        self._request_builder = TransactionsRequestBuilder(open_transaction_context(txns, opts))

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

    def _execute(self, op: Callable[..., Any], req: _CompletionCallbacks, wrap: Callable[[Any], T]) -> Future[T]:
        """Starts `op` and returns the future its callbacks complete, with `wrap` applied to the binding's
        result.  The callbacks run on a binding thread, so they reach the future through the loop."""
        ftr: Future[T] = self._loop.create_future()

        def on_ok(res: Any) -> None:
            log.debug('%s completed with %s', op.__name__, loggable_result(res))
            # BUG(PYCBC-1476): We might not need this once txn bug is fixed
            if isinstance(res, PycbcCoreException):
                self._loop.call_soon_threadsafe(_settle, ftr.set_exception, as_transactions_exception(res))
                return
            try:
                result = wrap(res)
            except Exception as e:
                log.error('%s result could not be built: %s', op.__name__, e)
                self._loop.call_soon_threadsafe(_settle, ftr.set_exception, as_transactions_exception(e))
                return
            self._loop.call_soon_threadsafe(_settle, ftr.set_result, result)

        def on_err(exc: Any) -> None:
            log.error('%s failed with %s', op.__name__, exc)
            err = exc or RuntimeError(f'unknown error calling {op.__name__}')
            self._loop.call_soon_threadsafe(_settle, ftr.set_exception, as_transactions_exception(err))

        def _settle(setter: Callable[[Any], None], value: Any) -> None:
            # Checked on the loop thread, where a cancel of the awaiting task also lands: a late
            # completion of a cancelled future would raise InvalidStateError inside the loop.
            if not ftr.done():
                setter(value)

        req['callback'] = on_ok
        req['errback'] = on_err
        log.debug('%s called with %s', op.__name__, loggable_request(req))
        try:
            op(**req)
        except Exception as e:
            ftr.set_exception(as_transactions_exception(e))
        return ftr

    def begin_attempt(self) -> Future[None]:
        return self._execute(create_new_attempt_context, self._request_builder.build_attempt_request(), _discard)

    def rollback(self) -> Future[None]:
        return self._execute(transaction_rollback, self._request_builder.build_attempt_request(), _discard)

    def commit(self) -> Future[dict[str, Any] | None]:
        req = self._request_builder.build_attempt_request()
        return self._execute(transaction_commit, req, lambda res: res)

    def get(self,
            coll: AsyncCollection,
            key: str,
            options: TransactionGetOptions | None = None,
            **kwargs: Any) -> Future[TransactionGetResult]:
        check_keywords('get', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_get_request(coll, key)
        return self._execute(transaction_op, req, lambda res: TransactionGetResult(res, transcoder))

    def get_replica_from_preferred_server_group(
            self,
            coll: AsyncCollection,
            key: str,
            options: TransactionGetReplicaFromPreferredServerGroupOptions | None = None,
            **kwargs: Any
    ) -> Future[TransactionGetResult]:
        check_keywords('get_replica_from_preferred_server_group', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_get_replica_from_preferred_server_group_request(coll, key)
        return self._execute(transaction_op, req, lambda res: TransactionGetResult(res, transcoder))

    def get_multi(self,
                  specs: Sequence[TransactionGetMultiSpec],
                  options: TransactionGetMultiOptions | None = None,
                  **kwargs: Any) -> Future[TransactionGetMultiResult]:
        check_keywords('get_multi', kwargs, 'mode')
        transcoders = [spec.transcoder for spec in specs]
        req = self._request_builder.build_get_multi_request(specs, read_option(options, kwargs, 'mode'))
        return self._execute(transaction_get_multi_op,
                             req,
                             lambda res: TransactionGetMultiResult(res, transcoders, self._transcoder))

    def get_multi_replicas_from_preferred_server_group(
            self,
            specs: Sequence[TransactionGetMultiReplicasFromPreferredServerGroupSpec],
            options: TransactionGetMultiReplicasFromPreferredServerGroupOptions | None = None,
            **kwargs: Any
    ) -> Future[TransactionGetMultiReplicasFromPreferredServerGroupResult]:
        check_keywords('get_multi_replicas_from_preferred_server_group', kwargs, 'mode')
        transcoders = [spec.transcoder for spec in specs]
        mode = read_option(options, kwargs, 'mode')
        req = self._request_builder.build_get_multi_replicas_from_preferred_server_group_request(specs, mode)
        result_cls = TransactionGetMultiReplicasFromPreferredServerGroupResult
        return self._execute(transaction_get_multi_op, req, lambda res: result_cls(res, transcoders, self._transcoder))

    def insert(self,
               coll: AsyncCollection,
               key: str,
               value: JSONType,
               options: TransactionInsertOptions | None = None,
               **kwargs: Any) -> Future[TransactionGetResult]:
        check_keywords('insert', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_insert_request(coll, key, value, transcoder)
        return self._execute(transaction_op, req, lambda res: TransactionGetResult(res, transcoder))

    def replace(self,
                txn_get_result: TransactionGetResult,
                value: JSONType,
                options: TransactionReplaceOptions | None = None,
                **kwargs: Any) -> Future[TransactionGetResult]:
        check_keywords('replace', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_replace_request(txn_get_result, value, transcoder)
        return self._execute(transaction_op, req, lambda res: TransactionGetResult(res, transcoder))

    def remove(self, txn_get_result: TransactionGetResult, **kwargs: Any) -> Future[None]:
        check_keywords('remove', kwargs)
        return self._execute(transaction_op, self._request_builder.build_remove_request(txn_get_result), _discard)

    def query(self,
              query: str,
              options: TransactionQueryOptions | None = None,
              **kwargs: Any) -> Future[TransactionQueryResults]:
        check_keywords('query', kwargs)
        req = self._request_builder.build_query_request(query, options)
        return self._execute(transaction_query_op, req, TransactionQueryResults)
