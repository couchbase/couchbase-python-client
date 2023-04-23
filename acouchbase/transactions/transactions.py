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

import asyncio
import logging
from functools import wraps
from typing import (TYPE_CHECKING,
                    Any,
                    Awaitable,
                    Callable,
                    Dict,
                    Optional)

from couchbase.exceptions import ErrorMapper
from couchbase.exceptions import exception as BaseCouchbaseException
from couchbase.logic.supportability import Supportability
from couchbase.options import TransactionQueryOptions
from couchbase.transactions import (TransactionGetResult,
                                    TransactionQueryResults,
                                    TransactionResult)
from couchbase.transactions.logic import AttemptContextLogic, TransactionsLogic

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from acouchbase.cluster import AsyncCluster
    from acouchbase.collection import AsyncCollection
    from couchbase._utils import JSONType, PyCapsuleType
    from couchbase.options import TransactionConfig, TransactionOptions
    from couchbase.serializer import Serializer

log = logging.getLogger(__name__)


class AsyncWrapper:
    @classmethod  # noqa: C901
    def inject_callbacks(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ftr = self._loop.create_future()

                def on_ok(res):
                    log.debug('%s completed, with %s', fn.__name__, res)
                    # BUG(PYCBC-1476): We might not need this once txn bug is fixed
                    if isinstance(res, BaseCouchbaseException):
                        self._loop.call_soon_threadsafe(ftr.set_exception, ErrorMapper.build_exception(res))
                    else:
                        try:
                            if return_cls is TransactionGetResult:
                                result = return_cls(res, self._serializer)
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
                try:
                    fn(self, *args, **kwargs)
                except SystemError as e:
                    ftr.set_exception(e.__cause__)
                except Exception as e:
                    ftr.set_exception(e)
                finally:
                    return ftr

            return wrapped_fn

        return decorator


class Transactions(TransactionsLogic):

    def __init__(self,
                 cluster,  # type: AsyncCluster
                 config    # type: TransactionConfig
                 ):
        super().__init__(cluster, config)

    @AsyncWrapper.inject_callbacks(TransactionResult)
    def run(self,
            txn_logic,  # type:  Callable[[AttemptContextLogic], None]
            transaction_options=None,  # type: Optional[TransactionOptions]
            **kwargs) -> Awaitable[TransactionResult]:
        def wrapped_logic(c):
            try:
                ctx = AttemptContext(c, self._loop, self._serializer)
                asyncio.run_coroutine_threadsafe(txn_logic(ctx), self._loop).result()
                log.debug('wrapped logic completed')
            except Exception as e:
                log.debug('wrapped_logic raised %s', e)
                raise e

        opts = None
        if transaction_options:
            opts = transaction_options
        if 'per_txn_config' in kwargs:
            Supportability.method_param_deprecated('per_txn_config', 'transaction_options')
            opts = kwargs.pop('per_txn_config', None)

        return super().run(wrapped_logic, opts, **kwargs)

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
                 ctx,    # type: PyCapsuleType
                 loop,    # type: AbstractEventLoop
                 serializer  # type: Serializer
                 ):
        super().__init__(ctx, loop, serializer)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def get(self,
            coll,  # type: AsyncCollection
            key,   # type: JSONType
            **kwargs  # type: Dict[str, JSONType]
            ) -> Awaitable[TransactionGetResult]:
        """
        Get a document within this transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            **kwargs (Dict[str, JSONType]): currently unused.

        Returns:
            Awaitable[:class:`couchbase.transactions.TransactionGetResult`]: Document in collection, in a form useful
            for passing to other transaction operations. Or `None` if the document was not found.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
            no need to handle the exception, as the transaction will rollback regardless.

        """
        return super().get(coll, key, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def insert(self,
               coll,    # type: AsyncCollection
               key,     # type: str
               value,   # type: JSONType
               **kwargs  # type: Dict[str, Any]
               ) -> Awaitable[TransactionGetResult]:
        """
        Insert a new document within a transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            value (:class:`couchbase._utils.JSONType):  Contents of the document.
            **kwargs (Dict[str, Any]): currently unused.

        Returns:
           Awaitable[:class:`couchbase.transactions.TransactionGetResult`]: Document in collection, in a form
            useful for passing to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.

        """
        return super().insert(coll, key, value, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def replace(self,
                txn_get_result,  # type: TransactionGetResult
                value,  # type: JSONType
                **kwargs  # type: Dict[str, Any]
                ) -> Awaitable[TransactionGetResult]:
        """
        Replace the contents of a document within a transaction.

        Args:
            txn_get_result (:class:`couchbase.transactions.TransactionGetResult`): Document to replace, gotten from a
              previous call to another transaction operation.
            value (:class:`couchbase._utils.JSONType):  The new contents of the document.
            **kwargs (Dict[str, Any]): currently unused.

        Returns:
           Awaitable[:class:`couchbase.transactions.TransactionGetResult`]: Document in collection, in a form useful
            for passing to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return super().replace(txn_get_result, value, **kwargs)

    @AsyncWrapper.inject_callbacks(None)
    def remove(self,
               txn_get_result,
               **kwargs
               ):
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
              query,
              options=TransactionQueryOptions(),
              **kwargs) -> TransactionQueryResults:
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
        return super().query(query, options, **kwargs)
