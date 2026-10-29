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

from __future__ import annotations

__all__ = [
    'AttemptContext',
    'Transactions',
]

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from couchbase.common._capabilities import Capability, require
from couchbase.common._supportability import Supportability
from couchbase.common.exceptions import (TransactionExpired,
                                         TransactionFailed,
                                         TransactionOperationFailed)

from .transaction_result import TransactionResult

if TYPE_CHECKING:
    from couchbase.collection import Collection
    from couchbase.common._interfaces._transactions import AttemptContextInterface, TransactionsInterface
    from couchbase.common._utils import JSONType
    from couchbase.options import (TransactionGetMultiOptions,
                                   TransactionGetMultiReplicasFromPreferredServerGroupOptions,
                                   TransactionGetOptions,
                                   TransactionGetReplicaFromPreferredServerGroupOptions,
                                   TransactionInsertOptions,
                                   TransactionOptions,
                                   TransactionQueryOptions,
                                   TransactionReplaceOptions)

    from .transaction_get_result import TransactionGetResult
    from .transaction_query_results import TransactionQueryResults
    from .transactions_get_multi import (TransactionGetMultiReplicasFromPreferredServerGroupResult,
                                         TransactionGetMultiReplicasFromPreferredServerGroupSpec,
                                         TransactionGetMultiResult,
                                         TransactionGetMultiSpec)


log = logging.getLogger('couchbase.transactions')


class Transactions:

    def __init__(self, impl: TransactionsInterface) -> None:
        self._impl = impl

    def run(self,
            txn_logic: Callable[[AttemptContext], None],
            transaction_options: TransactionOptions | None = None,
            **kwargs: Any) -> TransactionResult:
        """ Run a set of operations within a transaction.

        Args:
            txn_logic (Callable[:class:`~couchbase.transactions.AttemptContext`]): The transaction logic to perform.
            transaction_options (:class:`~couchbase.options.TransactionOptions`): Options to override those in the
                :class:`~couchbase.options.TransactionConfig` for this transaction only.
            **kwargs (dict[str, Any]): Override options for this transaction only - currently unimplemented.

                .. deprecated:: 4.1.0
                    The ``per_txn_config`` keyword is deprecated; pass ``transaction_options`` instead.

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

                cluster.transactions.run(txn_logic)

        """

        opts = transaction_options or None
        if 'per_txn_config' in kwargs:
            Supportability.method_param_deprecated('per_txn_config', 'transaction_options')
            opts = kwargs.pop('per_txn_config', None)

        attempt = self._impl.begin_transaction(opts, **kwargs)
        ctx = AttemptContext(attempt)
        while True:
            attempt.begin_attempt()
            try:
                txn_logic(ctx)
            except TransactionOperationFailed as ex:
                # The failed operation is recorded against the attempt, so committing does not
                # commit: it hands the failure to the impl, which rolls back and either asks for
                # another attempt or raises.
                try:
                    res = attempt.commit()
                except TransactionFailed as failed:
                    raise TransactionFailed(failed.message, context=failed.error_context,
                                            exc_info={'inner_cause': ex}) from None
            except BaseException as ex:
                # Cancelled or interrupted too: the staged mutations still hold their documents.
                attempt.rollback()
                if isinstance(ex, TransactionExpired) or not isinstance(ex, Exception):
                    raise ex from None
                raise TransactionFailed(exc_info={'inner_cause': ex}) from None
            else:
                res = attempt.commit()
            if not res:
                continue
            return TransactionResult(res)

    def close(self) -> None:
        self._impl.close()
        log.info("transactions closed")


class AttemptContext:

    def __init__(self, impl: AttemptContextInterface) -> None:
        self._impl = impl

    def get(self,
            coll: Collection,
            key: str,
            options: TransactionGetOptions | None = None,
            **kwargs: Any) -> TransactionGetResult:
        """
        Get a document within this transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            options (:class:`~couchbase.options.TransactionGetOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionGetOptions`

        Returns:
            :class:`couchbase.transactions.TransactionGetResult`: Document in collection, in a form useful for passing
                to other transaction operations. Or `None` if the document was not found.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return self._impl.get(coll, key, options, **kwargs)

    def get_replica_from_preferred_server_group(
            self,
            coll: Collection,
            key: str,
            options: TransactionGetReplicaFromPreferredServerGroupOptions | None = None,
            **kwargs: Any
    ) -> TransactionGetResult:
        """
        Get a document within this transaction from any replica in the preferred server group that has been specified in
            the :class:`couchbase.options.ClusterOptions`.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            options (:class:`~couchbase.options.TransactionGetReplicaFromPreferredServerGroupOptions`): Optional
                parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
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
        impl = require(self._impl, Capability.TRANSACTIONS_PREFERRED_SERVER_GROUP)
        return impl.get_replica_from_preferred_server_group(coll, key, options, **kwargs)

    def get_multi(self,
                  specs: list[TransactionGetMultiSpec] | tuple[TransactionGetMultiSpec, ...],
                  options: TransactionGetMultiOptions | None = None,
                  **kwargs: Any) -> TransactionGetMultiResult:
        """
        Get multiple documents within this transaction.

        Args:
            specs (list[:class:`~couchbase.transactions.TransactionGetMultiSpec`] | tuple[:class:`~couchbase.transactions.TransactionGetMultiSpec`]):
                The required information for the documents involved for this operation.
            options (:class:`~couchbase.options.TransactionGetMultiOptions`, optional): Optional parameters for this
                operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionGetMultiOptions`

        Returns:
            :class:`~couchbase.transactions.TransactionGetMultiResult`: Collection of results from the operation.

        Raises:
            :class:`~couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """  # noqa: E501
        return self._impl.get_multi(specs, options, **kwargs)

    def get_multi_replicas_from_preferred_server_group(
            self,
            specs: (list[TransactionGetMultiReplicasFromPreferredServerGroupSpec]
                    | tuple[TransactionGetMultiReplicasFromPreferredServerGroupSpec, ...]),
            options: TransactionGetMultiReplicasFromPreferredServerGroupOptions | None = None,
            **kwargs: Any
    ) -> TransactionGetMultiReplicasFromPreferredServerGroupResult:
        """
        Get multiple documents within this transaction from any replica.

        Args:
            specs (list[:class:`~couchbase.transactions.TransactionGetMultiReplicasFromPreferredServerGroupSpec`] | tuple[:class:`~couchbase.transactions.TransactionGetMultiReplicasFromPreferredServerGroupSpec`]):
                The required information for the documents involved for this operation.
            options (:class:`~couchbase.options.TransactionGetMultiReplicasFromPreferredServerGroupOptions`, optional):
                Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionGetMultiReplicasFromPreferredServerGroupOptions`

        Returns:
            :class:`~couchbase.transactions.TransactionGetMultiReplicasFromPreferredServerGroupResult`: Collection of
            results from the operation.
        Raises:
            :class:`~couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """  # noqa: E501
        impl = require(self._impl, Capability.TRANSACTIONS_PREFERRED_SERVER_GROUP)
        return impl.get_multi_replicas_from_preferred_server_group(specs, options, **kwargs)

    def insert(self,
               coll: Collection,
               key: str,
               value: JSONType,
               options: TransactionInsertOptions | None = None,
               **kwargs: Any) -> TransactionGetResult:
        """
        Insert a new document within a transaction.

        Args:
            coll (:class:`couchbase.collection.Collection`): Collection to use to find the document.
            key (str): document key.
            value (:class:`couchbase.common._utils.JSONType):  Contents of the document.
            options (:class:`~couchbase.options.TransactionInsertOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionInsertOptions`

        Returns:
           :class:`couchbase.transactions.TransactionGetResult`: Document in collection, in a form useful for passing
                to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return self._impl.insert(coll, key, value, options, **kwargs)

    def replace(self,
                txn_get_result: TransactionGetResult,
                value: JSONType,
                options: TransactionReplaceOptions | None = None,
                **kwargs: Any) -> TransactionGetResult:
        """
        Replace the contents of a document within a transaction.

        Args:
            txn_get_result (:class:`couchbase.transactions.TransactionGetResult`): Document to replace, gotten from a
              previous call to another transaction operation.
            value (:class:`couchbase.common._utils.JSONType):  The new contents of the document.
            options (:class:`~couchbase.options.TransactionReplaceOptions`): Optional parameters for this operation.
            **kwargs (dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TransactionReplaceOptions`

        Returns:
           :class:`couchbase.transactions.TransactionGetResult`: Document in collection, in a form useful for passing
                to other transaction operations.
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.

        """
        return self._impl.replace(txn_get_result, value, options, **kwargs)

    def remove(self, txn_get_result: TransactionGetResult, **kwargs: Any) -> None:
        """
        Remove a document in a transaction.

        Args:
            txn_get_result (:class:`couchbase.transactions.TransactionGetResult`): Document to delete.
            **kwargs (dict[str, Any]): currently unused.
        Returns:
            None
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
        """
        return self._impl.remove(txn_get_result, **kwargs)

    def query(self,
              query: str,
              options: TransactionQueryOptions | None = None,
              **kwargs: Any) -> TransactionQueryResults:
        """
        Perform a query within a transaction.

        Args:
            query (str): Query to perform.
            options (:class:`couchbase.transactions.TransactionQueryOptions`): Query options to use, if any.
            **kwargs (dict[str, Any]): currently unused.

        Returns:
            :class:`couchbase.transactions.TransactionQueryResult`
        Raises:
            :class:`couchbase.exceptions.TransactionOperationFailed`: If the operation failed.  In practice, there is
                no need to handle the exception, as the transaction will rollback regardless.
            :class:`couchbase.exceptions.CouchbaseException`: If the operation failed, but the transaction will not
                necessarily be rolled back, a CouchbaseException other than TransactionOperationFailed will be raised.
                If handled, the transaction will not rollback.
        """
        return require(self._impl, Capability.TRANSACTIONS_QUERY).query(query, options, **kwargs)
