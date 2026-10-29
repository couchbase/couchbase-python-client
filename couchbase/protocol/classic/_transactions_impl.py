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
from collections.abc import (Callable,
                             Mapping,
                             Sequence)
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

from couchbase.common.exceptions import CouchbaseException, TransactionsErrorContext
from couchbase.protocol.classic._capabilities import CAPABILITIES
from couchbase.protocol.classic._error_mapping import ErrorMapper
from couchbase.protocol.classic._pycbc_core import (create_new_attempt_context,
                                                    create_transaction_context,
                                                    create_transactions,
                                                    destroy_transactions)
from couchbase.protocol.classic._pycbc_core import pycbc_exception as PycbcCoreException
from couchbase.protocol.classic._pycbc_core import (transaction_commit,
                                                    transaction_get_multi_op,
                                                    transaction_op,
                                                    transaction_query_op,
                                                    transaction_rollback)
from couchbase.protocol.classic._transactions_req_builder import TransactionsRequestBuilder
from couchbase.protocol.classic._wire import transaction_wire
from couchbase.transactions.transaction_get_result import TransactionGetResult
from couchbase.transactions.transaction_query_results import TransactionQueryResults
from couchbase.transactions.transactions_get_multi import (TransactionGetMultiReplicasFromPreferredServerGroupResult,
                                                           TransactionGetMultiResult)
from couchbase.transcoder import JSONTranscoder

if TYPE_CHECKING:
    from couchbase.collection import Collection
    from couchbase.common._utils import JSONType
    from couchbase.common.options import (TransactionConfig,
                                          TransactionGetMultiOptions,
                                          TransactionGetMultiReplicasFromPreferredServerGroupOptions,
                                          TransactionGetOptions,
                                          TransactionGetReplicaFromPreferredServerGroupOptions,
                                          TransactionInsertOptions,
                                          TransactionOptions,
                                          TransactionQueryOptions,
                                          TransactionReplaceOptions)
    from couchbase.common.protocol import CouchbaseProtocol
    from couchbase.protocol.classic._client_adapter import ClientAdapter
    from couchbase.protocol.classic._cluster_impl import ClusterImpl
    from couchbase.protocol.classic._pycbc_core import pycbc_connection
    from couchbase.protocol.classic._pycbc_core.pycbc_core_types import (ParsedTransactionOptions,
                                                                         ParsedTransactionsConfig,
                                                                         TransactionContextCapsuleType,
                                                                         TransactionsCapsuleType)
    from couchbase.serializer import Serializer
    from couchbase.transactions.transactions_get_multi import (TransactionGetMultiReplicasFromPreferredServerGroupSpec,
                                                               TransactionGetMultiSpec)
    from couchbase.transcoder import Transcoder

log = logging.getLogger('couchbase.transactions')


def as_transactions_exception(exc: object) -> Exception:
    """The exception a transaction operation raises for an error the binding raised, returned or passed
    to an errback.  A `SystemError` is the binding returning with an exception set, which it carries as
    its cause."""
    if isinstance(exc, PycbcCoreException):
        return ErrorMapper.build_exception(exc)
    if isinstance(exc, SystemError) and isinstance(exc.__cause__, Exception):
        exc = exc.__cause__
    if isinstance(exc, CouchbaseException):
        return exc
    return CouchbaseException(message=str(exc), context=TransactionsErrorContext())


def open_transactions(connection: pycbc_connection,
                      config: TransactionConfig,
                      default_serializer: Serializer) -> tuple[TransactionsCapsuleType, JSONTranscoder]:
    """Creates the binding's transactions object and the transcoder its operations default to, a
    `JSONTranscoder` over the cluster's serializer because the cluster's own transcoder may not be JSON."""
    transcoder = JSONTranscoder(default_serializer)
    wired = transaction_wire('TransactionConfig', config)
    ret = create_transactions(connection, cast('ParsedTransactionsConfig', wired))
    if isinstance(ret, PycbcCoreException):
        raise ErrorMapper.build_exception(ret)
    log.info('created transactions object using config=%s, transcoder=%s', config, transcoder)
    return ret, transcoder


def close_transactions(txns: TransactionsCapsuleType) -> None:
    log.info('shutting down transactions...')
    destroy_transactions(txns=txns)


def open_transaction_context(txns: TransactionsCapsuleType,
                             opts: TransactionOptions | None) -> TransactionContextCapsuleType:
    txn_opts = cast('ParsedTransactionOptions', transaction_wire('TransactionOptions', opts or {}))
    return create_transaction_context(txns=txns, transaction_options=txn_opts)


def read_option(options: Mapping[str, Any] | None, kwargs: Mapping[str, Any], name: str) -> Any:
    """A keyword argument overrides the same key in the options block."""
    if name in kwargs:
        return kwargs[name]
    return options.get(name) if options is not None else None


def check_keywords(method: str, kwargs: Mapping[str, Any], *accepted: str) -> None:
    """Raises Python's own `TypeError` for a keyword `method` does not take, which a signature declaring
    `**kwargs` would otherwise accept silently."""
    for name in kwargs:
        if name not in accepted:
            raise TypeError(f"{method}() got an unexpected keyword argument '{name}'")


# The query options that carry the caller's data rather than how the query runs.
_QUERY_DATA_OPTIONS = ('positional_parameters', 'named_parameters', 'raw')


def loggable_request(req: Mapping[str, Any]) -> dict[str, Any]:
    """`req` as the DEBUG log shows it: a document body by its size, and no query parameters."""
    out = dict(req)
    if 'value' in out:
        out['value'] = f'<{len(out["value"][0])} bytes>'
    if 'options' in out:
        out['options'] = {k: v for k, v in out['options'].items() if k not in _QUERY_DATA_OPTIONS}
    return out


def loggable_result(res: Any) -> Any:
    """A binding result as the DEBUG log shows it: a query's response body by its size."""
    return f'<{len(res)} bytes>' if isinstance(res, (bytes, bytearray)) else res


def _execute(op: Callable[..., Any], req: Mapping[str, Any]) -> Any:
    log.debug('%s called with %s', op.__name__, loggable_request(req))
    try:
        ret = op(**req)
    except Exception as e:
        raise as_transactions_exception(e)
    log.debug('%s returned %s', op.__name__, loggable_result(ret))
    if isinstance(ret, (PycbcCoreException, Exception)):
        raise as_transactions_exception(ret)
    return ret


class TransactionsImpl:
    """**INTERNAL**"""

    def __init__(self, cluster: ClusterImpl) -> None:
        self._client_adapter = cluster.client_adapter
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
                          **kwargs: Any) -> AttemptContextImpl:
        return AttemptContextImpl(self._client_adapter, self._txns, self._transcoder, transaction_options)


class AttemptContextImpl:
    """**INTERNAL**"""

    capabilities = CAPABILITIES

    def __init__(self,
                 client_adapter: ClientAdapter,
                 txns: TransactionsCapsuleType,
                 transcoder: Transcoder,
                 opts: TransactionOptions | None) -> None:
        self._client_adapter = client_adapter
        self._transcoder = transcoder
        self._request_builder = TransactionsRequestBuilder(open_transaction_context(txns, opts))

    @property
    def couchbase_protocol(self) -> CouchbaseProtocol:
        """**INTERNAL**"""
        return self._client_adapter.couchbase_protocol

    def begin_attempt(self) -> None:
        _execute(create_new_attempt_context, self._request_builder.build_attempt_request())

    def rollback(self) -> None:
        _execute(transaction_rollback, self._request_builder.build_attempt_request())

    def commit(self) -> dict[str, Any] | None:
        res: dict[str, Any] | None = _execute(transaction_commit, self._request_builder.build_attempt_request())
        return res

    def get(self,
            coll: Collection,
            key: str,
            options: TransactionGetOptions | None = None,
            **kwargs: Any) -> TransactionGetResult:
        check_keywords('get', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_get_request(coll, key)
        return TransactionGetResult(_execute(transaction_op, req), transcoder)

    def get_replica_from_preferred_server_group(
            self,
            coll: Collection,
            key: str,
            options: TransactionGetReplicaFromPreferredServerGroupOptions | None = None,
            **kwargs: Any
    ) -> TransactionGetResult:
        check_keywords('get_replica_from_preferred_server_group', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_get_replica_from_preferred_server_group_request(coll, key)
        return TransactionGetResult(_execute(transaction_op, req), transcoder)

    def get_multi(self,
                  specs: Sequence[TransactionGetMultiSpec],
                  options: TransactionGetMultiOptions | None = None,
                  **kwargs: Any) -> TransactionGetMultiResult:
        check_keywords('get_multi', kwargs, 'mode')
        req = self._request_builder.build_get_multi_request(specs, read_option(options, kwargs, 'mode'))
        res = _execute(transaction_get_multi_op, req)
        return TransactionGetMultiResult(res, [spec.transcoder for spec in specs], self._transcoder)

    def get_multi_replicas_from_preferred_server_group(
            self,
            specs: Sequence[TransactionGetMultiReplicasFromPreferredServerGroupSpec],
            options: TransactionGetMultiReplicasFromPreferredServerGroupOptions | None = None,
            **kwargs: Any
    ) -> TransactionGetMultiReplicasFromPreferredServerGroupResult:
        check_keywords('get_multi_replicas_from_preferred_server_group', kwargs, 'mode')
        mode = read_option(options, kwargs, 'mode')
        req = self._request_builder.build_get_multi_replicas_from_preferred_server_group_request(specs, mode)
        res = _execute(transaction_get_multi_op, req)
        transcoders = [spec.transcoder for spec in specs]
        return TransactionGetMultiReplicasFromPreferredServerGroupResult(res, transcoders, self._transcoder)

    def insert(self,
               coll: Collection,
               key: str,
               value: JSONType,
               options: TransactionInsertOptions | None = None,
               **kwargs: Any) -> TransactionGetResult:
        check_keywords('insert', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_insert_request(coll, key, value, transcoder)
        return TransactionGetResult(_execute(transaction_op, req), transcoder)

    def replace(self,
                txn_get_result: TransactionGetResult,
                value: JSONType,
                options: TransactionReplaceOptions | None = None,
                **kwargs: Any) -> TransactionGetResult:
        check_keywords('replace', kwargs, 'transcoder')
        transcoder = read_option(options, kwargs, 'transcoder') or self._transcoder
        req = self._request_builder.build_replace_request(txn_get_result, value, transcoder)
        return TransactionGetResult(_execute(transaction_op, req), transcoder)

    def remove(self, txn_get_result: TransactionGetResult, **kwargs: Any) -> None:
        check_keywords('remove', kwargs)
        _execute(transaction_op, self._request_builder.build_remove_request(txn_get_result))

    def query(self,
              query: str,
              options: TransactionQueryOptions | None = None,
              **kwargs: Any) -> TransactionQueryResults:
        check_keywords('query', kwargs)
        req = self._request_builder.build_query_request(query, options)
        return TransactionQueryResults(_execute(transaction_query_op, req))
