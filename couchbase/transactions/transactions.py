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
                    Callable,
                    Dict,
                    Optional)

from couchbase.exceptions import (CouchbaseException,
                                  ErrorMapper,
                                  TransactionsErrorContext)
from couchbase.exceptions import exception as BaseCouchbaseException
from couchbase.logic.supportability import Supportability
from couchbase.options import TransactionQueryOptions
from couchbase.transactions.logic import AttemptContextLogic, TransactionsLogic

from .transaction_get_result import TransactionGetResult
from .transaction_query_results import TransactionQueryResults
from .transaction_result import TransactionResult

if TYPE_CHECKING:
    from couchbase._utils import JSONType, PyCapsuleType
    from couchbase.collection import Collection
    from couchbase.options import TransactionOptions
    from couchbase.serializer import Serializer

log = logging.getLogger(__name__)


class BlockingWrapper:
    @classmethod
    def block(cls, return_cls):
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    ret = fn(self, *args, **kwargs)
                    log.debug('%s returned %s', fn.__name__, ret)
                    if isinstance(ret, Exception):
                        raise ret
                    if isinstance(ret, BaseCouchbaseException):
                        raise ErrorMapper.build_exception(ret)
                    if return_cls is None:
                        return None
                    if return_cls is TransactionGetResult:
                        retval = return_cls(ret, self._serializer)
                    else:
                        retval = return_cls(ret)
                    return retval
                except CouchbaseException as cb_exc:
                    raise cb_exc
                except Exception as e:
                    raise CouchbaseException(message=str(e), context=TransactionsErrorContext())

            return wrapped_fn
        return decorator


class Transactions(TransactionsLogic):

    def run(self,
            txn_logic,  # type: Callable[[AttemptContext], None]
            transaction_options=None,  # type: Optional[TransactionOptions]
            **kwargs    # type: Dict[str, Any]
            ) -> TransactionResult:
        """ Run a set of operations within a transaction.

        Args:
            txn_logic (Callable[:class:`~couchbase.transactions.AttemptContext`]): The transaction logic to perform.
            transaction_options (:class:``): Options to override those in the :class:`couchbase.options.TransactionConfig`
                for this transaction only. ** DEPRECATED ** Use transaction_config instead.
            **kwargs (Dict[str, Any]): Override options for this transaction only - currently unimplemented.

        Returns:
            :class:`~couchbase.transactions.TransactionResult`: Results of the transaction.

        Raises:
              :class:`~couchbase.exceptions.TransactionFailed`: If the transaction failed.
              :class:`~couchbase.exceptions.TransactionExpired`: If the transaction expired.
              :class:`~couchbase.exceptions.TransactionCommitAmbiguous`: If the transaction's commit was ambiguous. See
                  :class:`~couchbase.exceptions.TransactionCommitAmbiguous` for a detailed description.

        Examples:
            Transactional update of 2 documents, using :class:`couchbase.options.TransactionConfig` in the cluster::

                doc1_id = "doc1-key"
                doc2_id = "doc2-key"
                coll = cluster.bucket("default").default_collection()

                def txn_logic(ctx):
                    doc1 = ctx.get(coll, doc1_id)
                    doc2 = ctx.get(coll, doc2_id)
                    ctx.update(doc1, {"some_key": f"I'm in {doc1_id}, and updated"})
                    ctx.update(doc2, {"some_key": f"I'm in {doc2_id}, and updated"})

                cluster.transactions.run(txn_logic

        """  # noqa: E501

        def wrapped_txn_logic(c):
            try:
                ctx = AttemptContext(c, self._serializer)
                return txn_logic(ctx)
            except Exception as e:
                log.debug('wrapped_txn_logic got %s:%s, re-raising it', e.__class__.__name__, e)
                raise e

        opts = None
        if transaction_options:
            opts = transaction_options
        if 'per_txn_config' in kwargs:
            Supportability.method_param_deprecated('per_txn_config', 'transaction_options')
            opts = kwargs.pop('per_txn_config', None)

        return TransactionResult(**super().run(wrapped_txn_logic, opts))

    def close(self):
        super().close()
        log.info("transactions closed")


class AttemptContext(AttemptContextLogic):

    def __init__(self,
                 ctx,  # type: PyCapsuleType
                 serializer  # type: Serializer
                 ):
        super().__init__(ctx, None, serializer)

    @BlockingWrapper.block(TransactionGetResult)
    def get(self,
            coll,   # type: Collection
            key,     # type: str
            **kwargs  # type: Dict[str, Any]
            ) -> TransactionGetResult:
        """
        Get a document within this transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            **kwargs (Dict[str, Any]): currently unused.

        Returns:
            :class:`couchbase.transactions.TransactionGetResult`: Document in collection, in a form useful for passing
                to other transaction operations. Or `None` if the document was not found.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return super().get(coll, key)

    @BlockingWrapper.block(TransactionGetResult)
    def insert(self,
               coll,    # type: Collection
               key,     # type: str
               value,   # type: JSONType
               **kwargs  # type: Dict[str, Any]
               ) -> Optional[TransactionGetResult]:
        """
        Insert a new document within a transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            value (:class:`couchbase._utils.JSONType):  Contents of the document.
            **kwargs (Dict[str, Any]): currently unused.

        Returns:
           :class:`couchbase.transactions.TransactionGetResult`: Document in collection, in a form useful for passing
                to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return super().insert(coll, key, value, **kwargs)

    @BlockingWrapper.block(TransactionGetResult)
    def replace(self,
                txn_get_result,  # type: TransactionGetResult
                value,  # type: JSONType
                **kwargs,  # type: Dict[str, Any]
                ) -> TransactionGetResult:
        """
        Replace the contents of a document within a transaction.

        Args:
            txn_get_result (:class:`couchbase.transactions.TransactionGetResult`): Document to replace, gotten from a
              previous call to another transaction operation.
            value (:class:`couchbase._utils.JSONType):  The new contents of the document.
            **kwargs (Dict[str, Any]): currently unused.

        Returns:
           :class:`couchbase.transactions.TransactionGetResult`: Document in collection, in a form useful for passing
                to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.

        """
        return super().replace(txn_get_result, value, **kwargs)

    @BlockingWrapper.block(None)
    def remove(self,
               txn_get_result,  # type: TransactionGetResult
               **kwargs     # type: Dict[str, Any]
               ) -> None:
        """
        Remove a document in a transaction.

        Args:
            txn_get_result (:class:`couchbase.transactions.TransactionGetResult`): Document to delete.
            **kwargs (Dict[str, Any]): currently unused.
        Returns:
            None
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return super().remove(txn_get_result, **kwargs)

    @BlockingWrapper.block(TransactionQueryResults)
    def query(self,
              query,    # type: str
              options=TransactionQueryOptions(),    # type: TransactionQueryOptions
              **kwargs  # type: Dict[str, Any]
              ) -> TransactionQueryResults:
        """
        Perform a query within a transaction.

        Args:
            query (str): Query to perform.
            options (:class:`couchbase.transactions.TransactionQueryOptions`): Query options to use, if any.
            **kwargs (Dict[str, Any]): currently unused.

        Returns:
            :class:`couchbase.transactions.TransactionQueryResult`
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
            :class:`couchbase.exceptions.CouchbaseException`: If the operation failed, but the transaction will not
                necessarily be rolled back, a CouchbaseException other than TransactionOperationFailed will be raised.
                If handled, the transaction will not rollback.
        """
        return super().query(query, options, **kwargs)
