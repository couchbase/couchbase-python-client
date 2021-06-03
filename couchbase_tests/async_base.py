import logging
import asyncio
import traceback

from .base import (CouchbaseTestCase, CouchbaseClusterResource,
                   CouchbaseClusterInfo, CouchbaseClusterInfoException,
                   MockRestartException,
                   CouchbaseClusterResourceException)
from acouchbase.cluster import ACluster


class AsyncioTestCase(CouchbaseTestCase):
    _cluster_info = None
    _use_scopes_and_collections = None

    @classmethod
    def setUpClass(cls, loop, cluster_class=ACluster, use_scopes_and_colls=None) -> None:
        super(AsyncioTestCase, cls).setUpClass()
        if cls._cluster_info:
            return

        cls._use_scopes_and_collections = use_scopes_and_colls or False
        cls._cluster_info = CouchbaseClusterInfo(
            CouchbaseClusterResource(cls.resources), loop)
        cls._cluster_info.cluster_resource.try_n_times(
            3, 3, cls._cluster_info.set_cluster, cluster_class)
        cls._cluster_info.cluster_resource.try_n_times(
            3, 3, cls._cluster_info.set_bucket)
        cls._cluster_info.set_cluster_version()
        cls._cluster_info.set_collection(use_scopes_and_colls)

    def setUp(self):
        super(AsyncioTestCase, self).setUp()

        if not type(self)._cluster_info:
            raise CouchbaseClusterInfoException("Cluster not setup.")

        self.loop = type(self)._cluster_info.loop
        self.cluster = type(self)._cluster_info.cluster
        self.bucket = type(self)._cluster_info.bucket
        self.bucket_name = type(self)._cluster_info.bucket_name
        self.collection = type(self)._cluster_info.collection
        self.cluster_version = type(self)._cluster_info.cluster_version
        self.supports_scopes_and_collections = type(
            self)._cluster_info.supports_scopes_and_collections()

    def tearDown(self):
        super(AsyncioTestCase, self).tearDown()

        self.loop = None
        self.cluster = None
        self.bucket = None
        self.bucket_name = None
        self.collection = None
        self.cluster_version = None

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._use_scopes_and_collections and cls._cluster_info.supports_scopes_and_collections():
            cls._cluster_info.drop_test_scopes_and_colls()
        cls._cluster_info = None
        super(AsyncioTestCase, cls).tearDownClass()

    def factory(self):
        pass

    async def try_n_times_async(self,  # type: AsyncioTestCase
                                num_times,  # type: int
                                seconds_between,  # type: SupportsFloat
                                func,  # type: Callable
                                *args,  # type: Any
                                **kwargs  # type: Any
                                ):
        # type: (...) -> Any

        for _ in range(num_times):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # helpful to have this print statement when tests fail
                # print(e)
                logging.info("Got exception, sleeping: {}".format(
                    traceback.format_exc()))
                await asyncio.sleep(float(seconds_between), loop=self.loop)

        if self.is_mock:

            try:
                self.restart_mock()
                return await func(*args, **kwargs)
            except MockRestartException:
                raise
            except Exception:
                pass

        raise CouchbaseClusterResourceException(
            "unsuccessful {} after {} times, waiting {} seconds between calls".format(func, num_times, seconds_between))

    async def try_n_times_till_exception_async(self,  # type: AsyncioTestCase
                                               num_times,  # type: int
                                               seconds_between,  # type: SupportsFloat
                                               func,  # type: Callable
                                               *args,  # type: Any
                                               # type: Tuple[Type[Exception],...]
                                               expected_exceptions=(
                                                   Exception,),
                                               **kwargs  # type: Any
                                               ):
        # type: (...) -> Any
        for _ in range(num_times):
            try:
                await func(*args, **kwargs)
                await asyncio.sleep(float(seconds_between), loop=self.loop)
            except expected_exceptions as e:
                # helpful to have this print statement when tests fail
                logging.info("Got one of expected exceptions {}, returning: {}".format(
                    expected_exceptions, e))
                return
            except Exception as e:
                logging.info("Got unexpected exception, raising: {}".format(e))
                raise

        self.fail(
            "successful {} after {} times waiting {} seconds between calls".format(func, num_times, seconds_between))
