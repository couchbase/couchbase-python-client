try:
    import asyncio
except ImportError:
    import trollius as asyncio

from acouchbase.asyncio_iops import IOPS
from couchbase.async.bucket import AsyncBucket
from couchbase.experimental import enabled_or_raise; enabled_or_raise()


class Bucket(AsyncBucket):
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

    locals().update(AsyncBucket._gen_memd_wrappers(_meth_factory))

    def connect(self):
        if not self.connected:
            self._connect()
            return self._cft
