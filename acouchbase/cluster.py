from typing import TypeVar, Type, Union, Any
import asyncio
from asyncio import AbstractEventLoop

from couchbase_core._libcouchbase import (
    PYCBC_CONN_F_ASYNC,
    PYCBC_CONN_F_ASYNC_DTOR)
from couchbase_core.client import Client as CoreClient
from couchbase.cluster import AsyncCluster as V3AsyncCluster
from couchbase.bucket import AsyncBucket as V3AsyncBucket
from couchbase.management.admin import Admin as AsyncAdminBucket
from couchbase.collection import CBCollection
from acouchbase.asyncio_iops import IOPS
from acouchbase.iterator import (
    AQueryResult,
    ASearchResult,
    AAnalyticsResult,
    AViewResult,
)
from acouchbase.management.buckets import ABucketManager

T = TypeVar("T", bound=CoreClient)


class AIOClientMixin(object):
    def __new__(cls, *args, **kwargs):
        # type: (...) -> Type[T]
        if not hasattr(cls, "AIO_wrapped") and cls.__name__ in ["ACluster", "ABucket"]:
            for m in ["ping"]:
                try:
                    method = cls._meth_factory(getattr(cls, m), m)
                    setattr(cls, m, method)
                except AttributeError:
                    raise
            cls.AIO_wrapped = True
        return super(AIOClientMixin, cls).__new__(cls)

    @staticmethod
    def _meth_factory(meth, _):
        def ret(self, *args, **kwargs):
            rv = meth(self, *args, **kwargs)
            ft = asyncio.Future()

            def on_ok(res):
                ft.set_result(res)
                rv.clear_callbacks()

            def on_err(_, excls, excval, __):
                err = excls(excval)
                ft.set_exception(err)
                rv.clear_callbacks()

            rv.set_callbacks(on_ok, on_err)
            return ft

        return ret

    def __init__(self, connstr=None, *args, **kwargs):
        loop = asyncio.get_event_loop()
        super(
            AIOClientMixin,
            self).__init__(
            connstr,
            *
            args,
            iops=IOPS(loop),
            **kwargs)
        self._loop = loop

        if issubclass(type(self), CBCollection):
            # do not set the connection callback for a collection
            return

        self._setup_connect()

    def _setup_connect(self):
        cft = asyncio.Future()

        def ftresult(err):
            if err:
                cft.set_exception(err)
            else:
                if(issubclass(type(self), V3AsyncCluster)):
                    self._set_server_version()
                cft.set_result(True)

        self._cft = cft
        self._conncb = ftresult

    def on_connect(self):
        # only if the connect callback has already been hit
        # do we want to attempt _connect() again
        if not self.connected and not hasattr(self, "_conncb"):
            self._setup_connect()
            self._connect()

        return self._cft

    connected = CoreClient.connected


class AIOCollectionMixin(object):
    def __new__(cls, *args, **kwargs):
        # type: (...) -> Type[T]
        if not hasattr(cls, "AIO_wrapped"):
            for k, method in cls._gen_memd_wrappers(
                AIOCollectionMixin._meth_factory
            ).items():
                setattr(cls, k, method)
            cls.AIO_wrapped = True
        return super(AIOCollectionMixin, cls).__new__(cls)

    @staticmethod
    def _meth_factory(meth, _):
        def ret(self, *args, **kwargs):
            rv = meth(self, *args, **kwargs)
            ft = asyncio.Future()

            def on_ok(res):
                ft.set_result(res)
                rv.clear_callbacks()

            def on_err(_, excls, excval, __):
                err = excls(excval)
                ft.set_exception(err)
                rv.clear_callbacks()

            rv.set_callbacks(on_ok, on_err)
            return ft

        return ret

    def __init__(self, *args, **kwargs):
        super(AIOCollectionMixin, self).__init__(*args, **kwargs)


class AsyncCBCollection(AIOCollectionMixin, CBCollection):
    def __init__(self, *args, **kwargs):
        super(AsyncCBCollection, self).__init__(*args, **kwargs)


Collection = AsyncCBCollection


class ABucket(AIOClientMixin, V3AsyncBucket):
    def __init__(self, *args, **kwargs):
        super(ABucket, self).__init__(
            collection_factory=AsyncCBCollection, *args, **kwargs
        )

    def view_query(self, *args, **kwargs):
        if "itercls" not in kwargs:
            kwargs["itercls"] = AViewResult
        return super(ABucket, self).view_query(*args, **kwargs)


Bucket = ABucket


class AAdmin(AsyncAdminBucket):
    def __init__(self, connection_string=None, **kwargs):
        loop = asyncio.get_event_loop()

        kwargs.setdefault('_flags', 0)
        # Flags should be async
        kwargs['_flags'] |= PYCBC_CONN_F_ASYNC | PYCBC_CONN_F_ASYNC_DTOR
        super(AAdmin, self).__init__(
            connection_string=connection_string, _iops=IOPS(loop), **kwargs
        )
        self._loop = loop

        self._setup_connect()

    def _setup_connect(self):
        cft = asyncio.Future()

        def ftresult(err):
            if err:
                cft.set_exception(err)
            else:
                cft.set_result(True)

        self._cft = cft
        self._conncb = ftresult

    def on_connect(self):
        # only if the connect callback has already been hit
        # do we want to attempt _connect() again
        if not self.connected and not hasattr(self, "_conncb"):
            self._setup_connect()
            self._connect()

        return self._cft

    connected = CoreClient.connected


Admin = AAdmin


class ACluster(AIOClientMixin, V3AsyncCluster):
    def __init__(self, connection_string, *options, **kwargs):
        if "admin_factory" not in kwargs:
            kwargs["admin_factory"] = Admin
        super(ACluster, self).__init__(
            connection_string, *options, bucket_factory=Bucket, **kwargs
        )

    def query(self, *args, **kwargs):
        if "itercls" not in kwargs:
            kwargs["itercls"] = AQueryResult
        return super(ACluster, self).query(*args, **kwargs)

    def search_query(self, *args, **kwargs):
        if "itercls" not in kwargs:
            kwargs["itercls"] = ASearchResult
        return super(ACluster, self).search_query(*args, **kwargs)

    def analytics_query(self, *args, **kwargs):
        return super(ACluster, self).analytics_query(
            *args, itercls=kwargs.pop("itercls", AAnalyticsResult), **kwargs
        )

    def buckets(self):
        # type: (...) -> ABucketManager
        """
        Get the BucketManager.

        :return: A :class:`~.management.ABucketManager` with which you can create or modify buckets on the cluster.
        """
        self._check_for_shutdown()
        return ABucketManager(self._admin)


Cluster = ACluster


def get_event_loop(
    evloop=None,  # type: AbstractEventLoop
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


def close_event_loop():
    """
    Close the event loop used by acouchbase.
    """
    IOPS.close_event_loop()
