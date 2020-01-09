try:
    import asyncio
except ImportError:
    import trollius as asyncio

from acouchbase.asyncio_iops import IOPS
from acouchbase.iterator import AView, AN1QLRequest
from couchbase_core.experimental import enable; enable()
from couchbase_core.experimental import enabled_or_raise; enabled_or_raise()
from couchbase_core._pyport import with_metaclass
from couchbase_core.asynchronous.bucket import AsyncClient as CoreAsyncClient
from couchbase.bucket import Bucket as V3SyncBucket
from couchbase.collection import AsyncCBCollection as BaseAsyncCBCollection


class AsyncBucketFactory(type):
    @staticmethod
    def gen_async_bucket(asyncbase):
        n1ql_query = getattr(asyncbase, 'n1ql_query', getattr(asyncbase, 'query', None))
        view_query = getattr(asyncbase, 'view_query', getattr(asyncbase, 'query', None))

        class Bucket(asyncbase):
            def __init__(self, *args, **kwargs):
                loop = asyncio.get_event_loop()
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

            def view_query(self, *args, **kwargs):
                if "itercls" not in kwargs:
                    kwargs["itercls"] = AView
                return view_query(self, *args, **kwargs)

            def query(self, *args, **kwargs):
                if "itercls" not in kwargs:
                    kwargs["itercls"] = AN1QLRequest
                return n1ql_query(self, *args, **kwargs)

            def connect(self):
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


class Bucket(V3SyncBucket):
    def __init__(self, *args, **kwargs):
        kwargs['corebucket_class'] = AsyncCBCollection
        super(Bucket, self).__init__(*args, **kwargs)
