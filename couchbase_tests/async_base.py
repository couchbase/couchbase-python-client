from .base import (CouchbaseTestCase, CouchbaseClusterResource,
                   CouchbaseClusterInfo, CouchbaseClusterInfoException)
from acouchbase.cluster import ACluster


class AsyncTestCase(CouchbaseTestCase):
    _cluster_info = None

    @classmethod
    def setUpClass(cls, loop, cluster_class=ACluster) -> None:
        super(AsyncTestCase, cls).setUpClass()
        if cls._cluster_info:
            return

        cls._cluster_info = CouchbaseClusterInfo(
            CouchbaseClusterResource(cls.resources), loop)
        cls._cluster_info.cluster_resource.try_n_times(
            3, 3, cls._cluster_info.set_cluster, cluster_class)
        cls._cluster_info.cluster_resource.try_n_times(
            3, 3, cls._cluster_info.set_bucket)
        cls._cluster_info.set_collection()
        cls._cluster_info.cluster_resource.set_cluster_version(
            cls._cluster_info.cluster)

    def setUp(self):
        super(AsyncTestCase, self).setUp()

        if not type(self)._cluster_info:
            raise CouchbaseClusterInfoException("Cluster not setup.")

        self.loop = type(self)._cluster_info.loop
        self.cluster = type(self)._cluster_info.cluster
        self.bucket = type(self)._cluster_info.bucket
        self.bucket_name = type(self)._cluster_info.bucket_name
        self.collection = type(self)._cluster_info.collection
        self.cluster_version = type(self)._cluster_info.cluster_version
        print(f'\ncluster: {self.cluster}')
        print(f'bucket: {self.bucket}')
        print(f'bucket_name: {self.bucket_name}')
        print(f'cluster_version: {self.cluster_version}')

    def tearDown(self):
        super(AsyncTestCase, self).tearDown()

        self.loop = None
        self.cluster = None
        self.bucket = None
        self.bucket_name = None
        self.collection = None
        self.cluster_version = None

    @classmethod
    def tearDownClass(cls) -> None:
        super(AsyncTestCase, cls).tearDownClass()
        cls._cluster_info = None

    def factory(self):
        pass
