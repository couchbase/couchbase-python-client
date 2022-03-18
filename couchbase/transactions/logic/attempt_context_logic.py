from typing import TYPE_CHECKING

from couchbase.pycbc_core import (transaction_op,
                                  transaction_operations,
                                  transaction_query_op)
from couchbase.transcoder import JSONTranscoder

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from couchbase._utils import PyCapsuleType


class AttemptContextLogic:
    def __init__(self,
                 ctx,    # type: PyCapsuleType
                 loop    # type: AbstractEventLoop
                 ):
        print(f'creating new attempt context with context {ctx} and loop {loop}')
        self._ctx = ctx
        self._loop = loop

    def get(self, coll, key, **kwargs):
        kwargs.update(coll._get_connection_args())
        kwargs.pop("conn")
        kwargs["key"] = key
        kwargs["ctx"] = self._ctx
        kwargs["op"] = transaction_operations.GET.value
        print(f'get calling transaction op with {kwargs}')
        return transaction_op(**kwargs)

    def insert(self, coll, key, value, **kwargs):
        kwargs.update(coll._get_connection_args())
        kwargs.pop("conn")
        kwargs["key"] = key
        kwargs["ctx"] = self._ctx
        kwargs["op"] = transaction_operations.INSERT.value
        kwargs["value"] = JSONTranscoder().encode_value(value)[0]
        print(f'insert calling transaction op with {kwargs}')
        return transaction_op(**kwargs)

    def replace(self, txn_get_result, value, **kwargs):
        kwargs.update({"ctx": self._ctx, "op": transaction_operations.REPLACE.value,
                       "value": JSONTranscoder().encode_value(value)[0],
                       "txn_get_result": txn_get_result._res})
        print(f'replace calling transaction op with {kwargs}')
        return transaction_op(**kwargs)

    def remove(self, txn_get_result, **kwargs):
        kwargs.update({"ctx": self._ctx,
                       "op": transaction_operations.REMOVE.value,
                       "txn_get_result": txn_get_result._res})
        print(f'remove calling transaction op with {kwargs}')
        return transaction_op(**kwargs)

    def query(self, query, options, **kwargs):
        kwargs.update({"ctx": self._ctx, "statement": query, "options": options._base})
        print(f'query calling transaction_op with {kwargs}')
        return transaction_query_op(**kwargs)
