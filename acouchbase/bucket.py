from asyncio import AbstractEventLoop

try:
    import asyncio
except ImportError:
    import trollius as asyncio

from acouchbase.asyncio_iops import IOPS
from acouchbase.iterator import AView, AN1QLRequest
from couchbase_core.experimental import enable; enable()
from couchbase_core.experimental import enabled_or_raise; enabled_or_raise()
from couchbase_core.asynchronous.bucket import AsyncClient as CoreAsyncClient
from couchbase.collection import AsyncCBCollection as BaseAsyncCBCollection
from couchbase_core.client import Client as CoreClient
from couchbase.bucket import AsyncBucket as V3AsyncBucket
from typing import *

T = TypeVar('T', bound=CoreClient)


class AsyncBucketFactory(type):
    @staticmethod
    def gen_async_bucket(asyncbase  # type: Type[T]
                         ):
        # type: (...) -> Type[T]
        n1ql_query = getattr(asyncbase, 'n1ql_query', getattr(asyncbase, 'query', None))
        view_query = getattr(asyncbase, 'view_query', getattr(asyncbase, 'query', None))

        class Bucket(asyncbase):
            def __init__(self, connstr=None, *args, **kwargs):
                loop = asyncio.get_event_loop()
                if connstr and 'connstr' not in kwargs:
                    kwargs['connstr'] = connstr
                super(Bucket, self).__init__(IOPS(loop), *args, **kwargs)
                self._loop = loop

                cft = asyncio.Future(loop=loop)

                def ftresult(err):
                    if err:
                        cft.set_exception(err)
                    else:
                        cft.set_result(True)

                self._cft = cft
                self._conncb = ftresult

            @property
            def view_itercls(self):
                return AView

            def view_query(self, *args, **kwargs):
                if "itercls" not in kwargs:
                    kwargs["itercls"] = self.view_itercls
                return view_query(self, *args, **kwargs)

            @property
            def query_itercls(self):
                return AN1QLRequest

            def query(self, *args, **kwargs):
                if "itercls" not in kwargs:
                    kwargs["itercls"] = self.query_itercls
                return n1ql_query(self, *args, **kwargs)

            def on_connect(self):
                if not self.connected:
                    self._connect()
                    return self._cft

            locals().update(asyncbase._gen_memd_wrappers(AsyncBucketFactory._meth_factory))

        return Bucket

    @staticmethod
    def _meth_factory(meth, name):
        def ret(self, *args, **kwargs):
            rv = meth(self, *args, **kwargs)
            ft = asyncio.Future()

            def on_ok(res):
                ft.set_result(res)
                rv.clear_callbacks()

            def on_err(res, excls, excval, exctb):
                err = excls(excval)
                ft.set_exception(err)
                rv.clear_callbacks()

            rv.set_callbacks(on_ok, on_err)
            return ft

        return ret


class V3CoreClient(AsyncBucketFactory.gen_async_bucket(CoreAsyncClient)):
    def __init__(self, *args, **kwargs):
        super(V3CoreClient, self).__init__(*args, **kwargs)


class AsyncCBCollection(AsyncBucketFactory.gen_async_bucket(BaseAsyncCBCollection)):
    def __init__(self,
                 *args,
                 **kwargs
                 ):
        super(AsyncCBCollection, self).__init__(*args, **kwargs)


Collection = AsyncCBCollection


class Bucket(AsyncBucketFactory.gen_async_bucket(V3AsyncBucket)):
    def __init__(self, *args, **kwargs):
        super(Bucket,self).__init__(collection_factory=AsyncCBCollection, *args, **kwargs)


def get_event_loop(evloop=None  # type: AbstractEventLoop
                   ):
    """
    Get an event loop compatible with acouchbase.
    Some Event loops, such as ProactorEventLoop (the default asyncio event
    loop for Python 3.8 on Windows) are not compatible with acouchbase as
    they don't implement all members in the abstract base class.

    :param evloop: preferred event loop
    :return: The preferred event loop, if compatible, otherwise, a compatible
    alternative event loop.
    """
    return IOPS.get_event_loop(evloop)
