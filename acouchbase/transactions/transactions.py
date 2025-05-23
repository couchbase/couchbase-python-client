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
from functools import wraps
from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Callable,
                    Coroutine,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    Union)

from couchbase.exceptions import ErrorMapper
from couchbase.exceptions import exception as BaseCouchbaseException
from couchbase.logic.supportability import Supportability
from couchbase.options import (TransactionGetMultiOptions,
                               TransactionGetMultiReplicasFromPreferredServerGroupOptions,
                               TransactionGetOptions,
                               TransactionGetReplicaFromPreferredServerGroupOptions,
                               TransactionInsertOptions,
                               TransactionQueryOptions,
                               TransactionReplaceOptions)
from couchbase.transactions import (TransactionGetMultiReplicasFromPreferredServerGroupResult,
                                    TransactionGetMultiReplicasFromPreferredServerGroupSpec,
                                    TransactionGetMultiResult,
                                    TransactionGetMultiSpec,
                                    TransactionGetResult,
                                    TransactionQueryResults,
                                    TransactionResult)
from couchbase.transactions.logic import AttemptContextLogic, TransactionsLogic

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from acouchbase.cluster import AsyncCluster
    from acouchbase.collection import AsyncCollection
    from couchbase._utils import JSONType, PyCapsuleType
    from couchbase.options import TransactionConfig, TransactionOptions
    from couchbase.transcoder import Transcoder

log = logging.getLogger(__name__)


class AsyncWrapper:
    @staticmethod
    def call_async_fn(ftr, self, fn, *args, **kwargs):
        try:
            fn(self, *args, **kwargs)
        except SystemError as e:
            ftr.set_exception(e.__cause__)
        except Exception as e:
            ftr.set_exception(e)

    @classmethod  # noqa: C901
    def inject_callbacks(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ftr = self._loop.create_future()
                tc = kwargs.get('transcoder', self._transcoder)

                def on_ok(res):
                    log.debug('%s completed, with %s', fn.__name__, res)
                    # BUG(PYCBC-1476): We might not need this once txn bug is fixed
                    if isinstance(res, BaseCouchbaseException):
                        self._loop.call_soon_threadsafe(ftr.set_exception, ErrorMapper.build_exception(res))
                    else:
                        try:
                            if return_cls is TransactionGetResult:
                                result = return_cls(res, tc)
                            elif (return_cls is TransactionGetMultiResult
                                  or return_cls is TransactionGetMultiReplicasFromPreferredServerGroupResult):
                                result = return_cls(res, [spec.transcoder for spec in args[0]], tc)
                            else:
                                result = return_cls(res) if return_cls is not None else None
                            self._loop.call_soon_threadsafe(ftr.set_result, result)
                        except Exception as e:
                            log.error('on_ok raised %s, %s', e, e.__cause__)
                            self._loop.call_soon_threadsafe(ftr.set_exception, e)

                def on_err(exc):
                    log.error('%s got on_err called with %s', fn.__name__, exc)
                    try:
                        if not exc:
                            exc = RuntimeError(f'unknown error calling {fn.__name__}')
                        if isinstance(exc, BaseCouchbaseException):
                            self._loop.call_soon_threadsafe(ftr.set_exception, ErrorMapper.build_exception(exc))
                        else:
                            self._loop.call_soon_threadsafe(ftr.set_exception, exc)
                    except Exception as e:
                        self._loop.call_soon_threadsafe(ftr.set_exception, e)

                kwargs["callback"] = on_ok
                kwargs["errback"] = on_err
                AsyncWrapper.call_async_fn(ftr, self, fn, *args, **kwargs)
                return ftr

            return wrapped_fn

        return decorator


class Transactions(TransactionsLogic):

    def __init__(self,
                 cluster,  # type: AsyncCluster
                 config    # type: TransactionConfig
                 ):
        super().__init__(cluster, config)

    async def run(self,
                  txn_logic,  # type:  Callable[[AttemptContextLogic], Coroutine[Any, Any, None]]
                  transaction_options=None,  # type: Optional[TransactionOptions]
                  **kwargs) -> TransactionResult:
        opts = None
        if transaction_options:
            opts = transaction_options._base
        if 'per_txn_config' in kwargs:
            Supportability.method_param_deprecated('per_txn_config', 'transaction_options')
            opts = kwargs.pop('per_txn_config', None)

        txn_result = await super().run_async(txn_logic, AttemptContext(self._txns, self._loop, self._transcoder, opts))
        return TransactionResult(**txn_result)

    # TODO: make async?
    def close(self):
        """
        Close the transactions.   No transactions can be performed on this instance after this is called.  There
        is no need to call this, as the transactions close automatically when the transactions object goes out of scope.

        Returns:
            None
        """
        # stop transactions object -- ideally this is done before closing the cluster.
        super().close()
        log.info("transactions closed")


class AttemptContext(AttemptContextLogic):

    def __init__(self,
                 ctx,         # type: PyCapsuleType
                 loop,        # type: AbstractEventLoop
                 transcoder,  # type: Transcoder
                 opts         # type: Optional[PyCapsuleType]
                 ):
        super().__init__(ctx, transcoder, loop, opts)

    @AsyncWrapper.inject_callbacks(None)
    def _new_attempt(self,
                     **kwargs  # type: Dict[str, Any]
                     ) -> Awaitable[None]:
        return super()._new_attempt_async(**kwargs)

    @AsyncWrapper.inject_callbacks(None)
    def _rollback(self,
                  **kwargs  # type: Dict[str, Any]
                  ) -> Awaitable[None]:
        return super()._rollback_async(**kwargs)

    @AsyncWrapper.inject_callbacks(TransactionResult)
    def _commit(self,
                **kwargs  # type: Dict[str, Any]
                ) -> Awaitable[TransactionResult]:
        return super()._commit_async(**kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def _get(self,
             coll,     # type: AsyncCollection
             key,      # type: str
             **kwargs  # type: Dict[str, Any]
             ) -> Awaitable[TransactionGetResult]:
        return super().get(coll, key, **kwargs)

    def get(self,
            coll,          # type: AsyncCollection
            key,           # type: str
            options=None,  # type: Optional[TransactionGetOptions]
            **kwargs       # type: Dict[str, Any]
            ) -> Awaitable[TransactionGetResult]:
        """
        Get a document within this transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            options (:class:`~couchbase.options.TransactionGetOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionGetOptions`

        Returns:
            Awaitable[:class:`couchbase.transactions.TransactionGetResult`]: Document in collection, in a form useful
            for passing to other transaction operations. Or `None` if the document was not found.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
            no need to handle the exception, as the transaction will rollback regardless.

        """
        if 'transcoder' not in kwargs and isinstance(options, TransactionGetOptions):
            kwargs['transcoder'] = options.get('transcoder', None)
        return self._get(coll, key, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def _get_replica_from_preferred_server_group(self,
                                                 coll,     # type: AsyncCollection
                                                 key,      # type: str
                                                 **kwargs  # type: Dict[str, Any]
                                                 ) -> Awaitable[TransactionGetResult]:
        return super().get_replica_from_preferred_server_group(coll, key, **kwargs)

    def get_replica_from_preferred_server_group(self,
                                                coll,          # type: AsyncCollection
                                                key,           # type: str
                                                options=None,  # type: Optional[TransactionGetReplicaFromPreferredServerGroupOptions]  # noqa: E501
                                                **kwargs       # type: Dict[str, Any]
                                                ) -> Awaitable[TransactionGetResult]:
        """
        Get a document within this transaction from any replica in the preferred server group that is specified in
            the :class:`couchbase.options.ClusterOptions`.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            options (:class:`~couchbase.options.TransactionGetReplicaFromPreferredServerGroupOptions`): Optional
                parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionGetReplicaFromPreferredServerGroupOptions`

        Returns:
            :class:`couchbase.transactions.TransactionGetResult`: Document in collection, in a form useful for passing
                to other transaction operations. Or `None` if the document was not found.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
            :class:`couchbase.exceptions.DocumentUnretrievableException`: If the document could not be retrieved from
                any replica in the preferred server group. The transaction will not rollback if this exception is
                caught.
        """
        if 'transcoder' not in kwargs and isinstance(options, TransactionGetReplicaFromPreferredServerGroupOptions):
            kwargs['transcoder'] = options.get('transcoder', None)
        return self._get_replica_from_preferred_server_group(coll, key, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetMultiResult)
    def _get_multi(self,
                   specs,    # type: Union[List[TransactionGetMultiSpec], Tuple[TransactionGetMultiSpec]]
                   **kwargs  # type: Dict[str, Any]
                   ) -> Awaitable[TransactionGetMultiResult]:
        return super().get_multi(specs, **kwargs)

    def get_multi(self,
                  specs,         # type: Union[List[TransactionGetMultiSpec], Tuple[TransactionGetMultiSpec]]
                  options=None,  # type: Optional[TransactionGetMultiOptions]
                  **kwargs       # type: Dict[str, Any]
                  ) -> Awaitable[TransactionGetMultiResult]:
        """
        Get a document within this transaction.

        Args:
            specs (Union[List[:class:`~couchbase.transactions.TransactionGetMultiSpec`], Tuple[:class:`~couchbase.transactions.TransactionGetMultiSpec`]]):
                The required information for the documents involved for this operation.
            options (:class:`~couchbase.options.TransactionGetMultiOptions`, optional): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionGetMultiOptions`

        Returns:
            :class:`couchbase.transactions.TransactionGetMultiResult`: Document in collection, in a form useful for passing
                to other transaction operations. Or `None` if the document was not found.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """  # noqa: E501
        if 'mode' not in kwargs and isinstance(options, TransactionGetMultiOptions):
            kwargs['mode'] = options.get('mode', None)
        return self._get_multi(specs, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetMultiReplicasFromPreferredServerGroupResult)
    def _get_multi_replicas_from_preferred_server_group(self,
                                                        specs,    # type: Union[List[TransactionGetMultiReplicasFromPreferredServerGroupSpec], Tuple[TransactionGetMultiReplicasFromPreferredServerGroupSpec]]  # noqa: E501
                                                        **kwargs  # type: Dict[str, Any]
                                                        ) -> Awaitable[TransactionGetMultiReplicasFromPreferredServerGroupResult]:  # noqa: E501
        return super().get_multi_replicas_from_preferred_server_group(specs, **kwargs)

    def get_multi_replicas_from_preferred_server_group(self,
                                                       specs,         # type: Union[List[TransactionGetMultiReplicasFromPreferredServerGroupSpec], Tuple[TransactionGetMultiReplicasFromPreferredServerGroupSpec]] # noqa: E501
                                                       options=None,  # type: Optional[TransactionGetMultiReplicasFromPreferredServerGroupOptions]  # noqa: E501
                                                       **kwargs       # type: Dict[str, Any]
                                                       ) -> Awaitable[TransactionGetMultiReplicasFromPreferredServerGroupResult]:  # noqa: E501
        """
        Get multiple documents within this transaction from any replica.

        Args:
            specs (Union[List[:class:`~couchbase.transactions.TransactionGetMultiReplicasFromPreferredServerGroupSpec`], Tuple[:class:`~couchbase.transactions.TransactionGetMultiReplicasFromPreferredServerGroupSpec`]]):
                The required information for the documents involved for this operation.
            options (:class:`~couchbase.options.TransactionGetMultiReplicasFromPreferredServerGroupOptions`, optional): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionGetMultiReplicasFromPreferredServerGroupOptions`

        Returns:
            :class:`~couchbase.transactions.TransactionGetMultiReplicasFromPreferredServerGroupResult`: Collection of results from the operation.
        Raises:
            :class:`~couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """  # noqa: E501
        if 'mode' not in kwargs and isinstance(options, TransactionGetMultiReplicasFromPreferredServerGroupOptions):
            kwargs['mode'] = options.get('mode', None)
        return self._get_multi_replicas_from_preferred_server_group(specs, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def _insert(self,
                coll,     # type: AsyncCollection
                key,      # type: str
                value,    # type: JSONType
                **kwargs  # type: Dict[str, Any]
                ) -> Awaitable[TransactionGetResult]:
        return super().insert(coll, key, value, **kwargs)

    def insert(self,
               coll,          # type: AsyncCollection
               key,           # type: str
               value,         # type: JSONType
               options=None,  # type: Optional[TransactionInsertOptions]
               **kwargs       # type: Dict[str, Any]
               ) -> Awaitable[TransactionGetResult]:
        """
        Insert a new document within a transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            value (:class:`couchbase._utils.JSONType):  Contents of the document.
            options (:class:`~couchbase.options.TransactionInsertOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionInsertOptions`

        Returns:
           Awaitable[:class:`couchbase.transactions.TransactionGetResult`]: Document in collection, in a form
            useful for passing to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.

        """
        if 'transcoder' not in kwargs and isinstance(options, TransactionInsertOptions):
            kwargs['transcoder'] = options.get('transcoder', None)
        return self._insert(coll, key, value, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def _replace(self,
                 txn_get_result,  # type: TransactionGetResult
                 value,           # type: JSONType
                 **kwargs         # type: Dict[str, Any]
                 ) -> Awaitable[TransactionGetResult]:
        return super().replace(txn_get_result, value, **kwargs)

    def replace(self,
                txn_get_result,  # type: TransactionGetResult
                value,           # type: JSONType
                options=None,    # type: Optional[TransactionReplaceOptions]
                **kwargs         # type: Dict[str, Any]
                ) -> Awaitable[TransactionGetResult]:
        """
        Replace the contents of a document within a transaction.

        Args:
            txn_get_result (:class:`couchbase.transactions.TransactionGetResult`): Document to replace, gotten from a
              previous call to another transaction operation.
            value (:class:`couchbase._utils.JSONType):  The new contents of the document.
            options (:class:`~couchbase.options.TransactionReplaceOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionReplaceOptions`

        Returns:
           Awaitable[:class:`couchbase.transactions.TransactionGetResult`]: Document in collection, in a form useful
            for passing to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        if 'transcoder' not in kwargs and isinstance(options, TransactionReplaceOptions):
            kwargs['transcoder'] = options.get('transcoder', None)
        return self._replace(txn_get_result, value, **kwargs)

    @AsyncWrapper.inject_callbacks(None)
    def remove(self,
               txn_get_result,      # type: TransactionGetResult
               **kwargs             # type: Dict[str, Any]
               ) -> Awaitable[None]:
        """
        Remove a document in a transaction.

        Args:
            txn_get_result (:class:`couchbase.transactions.TransactionGetResult`): Document to delete.
            **kwargs (Dict[str, Any]): currently unused.
        Returns:
            Awaitable[None]
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return super().remove(txn_get_result, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionQueryResults)
    def query(self,
              query,         # type: str
              options=None,  # type: Optional[TransactionQueryOptions]
              **kwargs       # type: Dict[str, Any]
              ) -> Awaitable[TransactionQueryResults]:
        """
        Perform a query within a transaction.

        Args:
            query (str): Query to perform.
            options (:class:`couchbase.transactions.TransactionQueryOptions`): Query options to use, if any.
            **kwargs (Dict[str, Any]): currently unused.

        Returns:
            Awaitable[:class:`couchbase.transactions.TransactionQueryResult`]
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
            :class:`couchbase.exceptions.CouchbaseException`: If the operation failed, but the transaction will not
                necessarily be rolled back, a CouchbaseException other than TransactionOperationFailed will be raised.
                If handled, the transaction will not rollback.
        """
        if options is None:
            options = TransactionQueryOptions()
        return super().query(query, options, **kwargs)
