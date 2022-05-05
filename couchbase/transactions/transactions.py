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

from functools import wraps
from typing import (TYPE_CHECKING,
                    Callable,
                    Optional)

from couchbase.exceptions import CouchbaseException, TransactionsErrorContext
from couchbase.transactions.logic import AttemptContextLogic, TransactionsLogic
import logging

from .transaction_get_result import TransactionGetResult
from .transaction_query_options import TransactionQueryOptions
from .transaction_query_results import TransactionQueryResults
from .transaction_result import TransactionResult

if TYPE_CHECKING:
    from couchbase._utils import PyCapsuleType
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
            per_txn_config=None,  # type: Optional[TransactionOptions]
            **kwargs
            ):

        def wrapped_txn_logic(c):
            try:
                ctx = AttemptContext(c, self._serializer)
                return txn_logic(ctx)
            except Exception as e:
                log.debug('wrapped_txn_logic got %s:%s, re-raising it', e.__class__.__name__, e)
                raise e

        return TransactionResult(**super().run(wrapped_txn_logic, per_txn_config))

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
