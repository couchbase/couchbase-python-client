from functools import wraps
from typing import TYPE_CHECKING

from couchbase.exceptions import CouchbaseException, TransactionsErrorContext
from couchbase.transactions.logic import AttemptContextLogic, TransactionsLogic

from .transaction_get_result import TransactionGetResult
from .transaction_query_options import TransactionQueryOptions
from .transaction_query_results import TransactionQueryResults
from .transaction_result import TransactionResult

if TYPE_CHECKING:
    from couchbase._utils import PyCapsuleType


class BlockingWrapper:
    @classmethod
    def block(cls, return_cls):
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    ret = fn(self, *args, **kwargs)
                    print(f'{fn.__name__} got {ret}')
                    if isinstance(ret, Exception):
                        raise ret
                    if return_cls is None:
                        return None
                    retval = return_cls(ret)
                    print(f'{fn.__name__} returning {retval}')
                    return retval
                except CouchbaseException as cb_exc:
                    raise cb_exc
                except Exception as e:
                    raise CouchbaseException(message=str(e), context=TransactionsErrorContext())

            return wrapped_fn
        return decorator


class Transactions(TransactionsLogic):

    def run(self, txn_logic, per_txn_config=None):

        def wrapped_txn_logic(c):
            try:
                ctx = AttemptContext(c)
                print(f'wrapped_txn_logic got {ctx}, calling transaction logic')
                return txn_logic(ctx)
            except Exception as e:
                print(f'wrapped_txn_logic got {e.__class__.__name__}, {e}, re-raising')
                raise e

        return TransactionResult(**super().run(wrapped_txn_logic, per_txn_config))


class AttemptContext(AttemptContextLogic):

    def __init__(self,
                 ctx  # type: PyCapsuleType
                 ):
        super().__init__(ctx, loop=None)

    @BlockingWrapper.block(TransactionGetResult)
    def get(self, coll, key):
        return super().get(coll, key)

    @BlockingWrapper.block(TransactionGetResult)
    def insert(self, coll, key, value, **kwargs):
        return super().insert(coll, key, value, **kwargs)

    @BlockingWrapper.block(TransactionGetResult)
    def replace(self, txn_get_result, value, **kwargs):
        return super().replace(txn_get_result, value, **kwargs)

    @BlockingWrapper.block(None)
    def remove(self, txn_get_result, **kwargs):
        return super().remove(txn_get_result, **kwargs)

    @BlockingWrapper.block(TransactionQueryResults)
    def query(self, query, options=TransactionQueryOptions(), **kwargs):
        return super().query(query, options, **kwargs)
