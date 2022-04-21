import asyncio
from functools import wraps
from typing import (TYPE_CHECKING,
                    Awaitable,
                    Dict)

from couchbase.exceptions import CouchbaseException, TransactionsErrorContext
from couchbase.transactions import (TransactionGetResult,
                                    TransactionQueryOptions,
                                    TransactionQueryResults,
                                    TransactionResult)
from couchbase.transactions.logic import AttemptContextLogic, TransactionsLogic
if TYPE_CHECKING:
    from asyncio import AbstractEventLoop

    from acouchbase.cluster import AsyncCluster
    from acouchbase.collection import AsyncCollection
    from couchbase._utils import JSONType, PyCapsuleType
    from couchbase.transactions import TransactionConfig


class AsyncWrapper:
    @classmethod  # noqa: C901
    def inject_callbacks(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                ftr = self._loop.create_future()

                def on_ok(res):
                    print(f'{fn.__name__} completed, with {res}')
                    try:
                        result = return_cls(res) if return_cls is not None else None
                        self._loop.call_soon_threadsafe(ftr.set_result, result)
                    except Exception as e:
                        print(f'on_ok raised {e}, {e.__cause__}')
                        self._loop.call_soon_threadsafe(ftr.set_exception, e)

                def on_err(exc):
                    print(f'{fn.__name__} got on_err called with {exc}')
                    try:
                        if not exc:
                            raise RuntimeError(f'unknown error calling {fn.__name__}')
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
    def run(self, txn_logic, per_txn_config=None, **kwargs) -> Awaitable[TransactionResult]:
        def wrapped_logic(c):
            try:
                ctx = AttemptContext(c, self._loop)
                asyncio.run_coroutine_threadsafe(txn_logic(ctx), self._loop).result()
                print('wrapped logic completed')
            except Exception as e:
                print(f'wrapped_logic got {e}')
                raise e

        super().run(wrapped_logic, per_txn_config, **kwargs)

    # TODO: make async?
    def close(self):
        # stop transactions object -- ideally this is done before closing the cluster.
        super().close()


class AttemptContext(AttemptContextLogic):
    def __init__(self,
                 ctx,    # type: PyCapsuleType
                 loop    # type: AbstractEventLoop
                 ):
        print(f'creating new attempt context with context {ctx} and loop {loop}')
        self._ctx = ctx
        self._loop = loop

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def get(self,
            coll,  # type: AsyncCollection
            key,   # type: JSONType
            **kwargs  # type: Dict[str, JSONType]
            ):

        print(f'get called with collection={coll}, key={key}')
        super().get(coll, key, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def insert(self, coll, key, value, **kwargs):
        print(f'insert called with collection={coll}, key={key}, value={value}')
        super().insert(coll, key, value, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionGetResult)
    def replace(self, txn_get_result, value, **kwargs):
        print(f'replace called with txn_get_result={txn_get_result}')
        super().replace(txn_get_result, value, **kwargs)

    @AsyncWrapper.inject_callbacks(None)
    def remove(self, txn_get_result, **kwargs):
        print(f'remove called with txn_get_result={txn_get_result}')
        super().remove(txn_get_result, **kwargs)

    @AsyncWrapper.inject_callbacks(TransactionQueryResults)
    def query(self, query, options=TransactionQueryOptions(), **kwargs) -> TransactionQueryResults:
        print(f'query called with query={query}, options={options}')
        super().query(query, options, **kwargs)
