from datetime import timedelta
from unittest import SkipTest
import time
import random
import json
import asyncio
from flaky import flaky

from acouchbase.cluster import (Cluster, get_event_loop,
                                close_event_loop)
from couchbase_tests.async_base import AsyncioTestCase, async_test

from couchbase.cluster import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.collection import GetOptions
from couchbase_core import mk_formstr
from couchbase.exceptions import (CollectionNotFoundException, RateLimitedException,
                                  ScopeNotFoundException, QuotaLimitedException,
                                  KeyspaceNotFoundException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.users import GetUserOptions


@flaky(5, 1)
class AcouchbaseRateLimitTests(AsyncioTestCase):

    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"
    USERNAME = "rate-limit-user"
    MIN_VERSION = 7.1
    MIN_BUILD = 1621

    @classmethod
    def setUpClass(cls) -> None:
        super(AcouchbaseRateLimitTests, cls).setUpClass(
            get_event_loop(), cluster_class=Cluster, bucket_name="default")

    @classmethod
    def tearDownClass(cls) -> None:
        super(AcouchbaseRateLimitTests, cls).tearDownClass()
        close_event_loop()

    def setUp(self):
        super(AcouchbaseRateLimitTests, self).setUp()

        if(self.is_mock):
            raise SkipTest("Rate limit testing not available on mock.")

        self._check_version()
        self.loop.run_until_complete(self._enforce_rate_limits())

    def tearDown(self):
        self.loop.run_until_complete(self._drop_rate_limit_user())
        self.loop.run_until_complete(self._drop_scopes())
        return super(AcouchbaseRateLimitTests, self).tearDown()

    def _check_version(self):
        version = self.cluster.get_server_version()
        if version.short_version < self.MIN_VERSION:
            raise SkipTest("Rate limit testing only available on server versions >= {}.0:{}".format(
                self.MIN_VERSION, self.MIN_BUILD))

        build = 0
        try:
            build = int(version.full_version.split("-")[1])
        except:
            # if not able to get the build version, skip the test
            pass

        if build < self.MIN_BUILD:
            raise SkipTest("Rate limit testing only available on server versions >= {}.0:{}".format(
                self.MIN_VERSION, self.MIN_BUILD))

    async def _enforce_rate_limits(self):
        path = "/internalSettings"
        form = mk_formstr({"enforceLimits": "true"})
        await self.cluster._admin.http_request_async(path=path,
                                                     method="POST",
                                                     content_type='application/x-www-form-urlencoded',
                                                     content=form)

    async def _create_rate_limit_user(self, username, limits):
        params = {
            "password": "password",
            "roles": "admin"
        }

        user_limits = {}
        kv_limits = limits.get("kv_limits", None)
        if kv_limits:
            user_limits["kv"] = {
                "num_connections": kv_limits["num_connections"],
                "num_ops_per_min": kv_limits["num_ops_per_min"],
                "ingress_mib_per_min": kv_limits["ingress_mib_per_min"],
                "egress_mib_per_min": kv_limits["egress_mib_per_min"]
            }

        query_limits = limits.get("query_limits", None)
        if query_limits:
            user_limits["query"] = {
                "num_queries_per_min": query_limits["num_queries_per_min"],
                "num_concurrent_requests": query_limits["num_concurrent_requests"],
                "ingress_mib_per_min": query_limits["ingress_mib_per_min"],
                "egress_mib_per_min": query_limits["egress_mib_per_min"]
            }

        fts_limits = limits.get("fts_limits", None)
        if fts_limits:
            user_limits["fts"] = {
                "num_queries_per_min": fts_limits["num_queries_per_min"],
                "num_concurrent_requests": fts_limits["num_concurrent_requests"],
                "ingress_mib_per_min": fts_limits["ingress_mib_per_min"],
                "egress_mib_per_min": fts_limits["egress_mib_per_min"]
            }

        cluster_mgr_limits = limits.get("cluster_mgr_limits", None)
        if cluster_mgr_limits:
            user_limits["clusterManager"] = {
                "num_concurrent_requests": cluster_mgr_limits["num_concurrent_requests"],
                "ingress_mib_per_min": cluster_mgr_limits["ingress_mib_per_min"],
                "egress_mib_per_min": cluster_mgr_limits["egress_mib_per_min"]
            }

        if user_limits:
            params["limits"] = json.dumps(user_limits)

        path = '/settings/rbac/users/local/{}'.format(username)
        form = mk_formstr(params)
        await self.cluster._admin.http_request_async(path=path,
                                                     method='PUT',
                                                     content_type='application/x-www-form-urlencoded',
                                                     content=form)

        # lets verify user exists
        um = self.cluster.users()
        user_metadata = await self.try_n_times_async(10, 1, um.get_user, username,
                                                     GetUserOptions(domain_name="local"))

        self.assertIsNotNone(user_metadata)
        self.assertEqual(username, user_metadata.user.username)

    async def _create_rate_limit_scope(self, cm, scope_name, bucket_name, limits):
        params = {
            "name": scope_name
        }

        scope_limits = {}
        kv_limits = limits.get("kv_limits", None)
        if kv_limits:
            scope_limits["kv"] = {
                "data_size": kv_limits["data_size"]
            }

        index_limits = limits.get("index_limits", None)
        if index_limits:
            scope_limits["index"] = {
                "num_indexes": index_limits["num_indexes"]
            }

        fts_limits = limits.get("fts_limits", None)
        if fts_limits:
            scope_limits["fts"] = {
                "num_fts_indexes": fts_limits["num_fts_indexes"]
            }

        cluster_mgr_limits = limits.get("cluster_mgr_limits", None)
        if cluster_mgr_limits:
            scope_limits["clusterManager"] = {
                "num_collections": cluster_mgr_limits["num_collections"]
            }

        if scope_limits:
            params["limits"] = json.dumps(scope_limits)

        path = '/pools/default/buckets/{}/scopes'.format(bucket_name)
        form = mk_formstr(params)
        await self.cluster._admin.http_request_async(path=path,
                                                     method='POST',
                                                     content_type='application/x-www-form-urlencoded',
                                                     content=form)

        # verify the scope exists
        all_scopes = await cm.get_all_scopes()
        self.assertIn(scope_name, map(lambda s: s.name, all_scopes))

    async def _drop_rate_limit_user(self):
        um = self.cluster.users()
        await self.try_n_times_till_exception_async(10, 3, um.drop_user, self.USERNAME)

    async def _drop_scopes(self):
        cm = self.bucket.collections()
        scopes = await cm.get_all_scopes()
        for scope in scopes:
            if "_default" not in scope.name:
                await cm.drop_scope(scope.name)

    async def _try_until_timeout(self, timeout, interval, func, *args, **kwargs):
        """Execute provided func until specified timeout has been reached.

        :param timeout: timeout in seconds
        :type timeout: int
        :param interval: sleep interval in milliseconds
        :type interval: int
        :param func: function to execute periodically, sleeping interval milliseconds between each execution
        :type func: function
        """
        timeout_ms = timeout * 1000
        time_left = timeout_ms
        interval_ms = float(interval / 1000)
        start = time.perf_counter()
        is_query = kwargs.pop("query", False)
        is_fts = kwargs.pop("fts", False)

        while True:
            if is_query is True:
                query_iter = func(*args, **kwargs)
                [r async for r in query_iter]
            elif is_fts is True:
                fts_iter = func(*args, **kwargs)
                [r async for r in fts_iter]
            else:
                await func(*args, **kwargs)
            time_left = timeout_ms - ((time.perf_counter() - start) * 1000)
            if time_left <= 0:
                break

            await asyncio.sleep(interval_ms)

    def _random_doc_by_size(self, size):
        doc = bytearray((random.getrandbits(8) for i in range(size)))
        return doc.hex()

    async def _get_scope(self, cm, scope_name):
        scopes = await cm.get_all_scopes()
        return next((s for s in scopes if s.name == scope_name), None)

    async def _get_collection(self, cm, scope_name, coll_name):
        scope = await self._get_scope(cm, scope_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    @async_test
    async def test_rate_limits(self):
        await self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 10,
                "num_ops_per_min": 10,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        await cluster.on_connect()
        bucket = cluster.bucket("default")
        await bucket.on_connect()
        collection = bucket.default_collection()

        with self.assertRaises(RateLimitedException):
            await self._try_until_timeout(
                5, 10, collection.upsert, "ratelimit", "test")

        cluster._close()
        cluster.disconnect()

    @async_test
    async def test_rate_limits_ingress(self):
        await self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 10,
                "num_ops_per_min": 100,
                "ingress_mib_per_min": 1,
                "egress_mib_per_min": 10
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        await cluster.on_connect()
        bucket = cluster.bucket("default")
        await bucket.on_connect()
        collection = bucket.default_collection()

        with self.assertRaises(RateLimitedException):
            doc = self._random_doc_by_size(1024*512)
            for _ in range(3):
                await collection.upsert("ratelimit-ingress", doc)

        cluster._close()
        cluster.disconnect()

    @async_test
    async def test_rate_limits_egress(self):
        await self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 10,
                "num_ops_per_min": 100,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 1
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        await cluster.on_connect()
        bucket = cluster.bucket("default")
        await bucket.on_connect()
        collection = bucket.default_collection()

        doc = self._random_doc_by_size(1024*512)
        key = "ratelimit-egress"
        await collection.upsert(key, doc)
        with self.assertRaises(RateLimitedException):
            for _ in range(3):
                await collection.get(key, GetOptions(timeout=timedelta(seconds=10)))

        cluster._close()
        cluster.disconnect()

    @async_test
    async def test_rate_limits_max_conns(self):
        await self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 1,
                "num_ops_per_min": 100,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        await cluster.on_connect()
        bucket = cluster.bucket("default")
        await bucket.on_connect()
        collection1 = bucket.default_collection()
        try:
            await collection1.get("some-key")
        except:
            pass  # don't care about the failure, just need a KV op

        with self.assertRaises((RateLimitedException)):
            cluster1 = Cluster("couchbase://{}".format(self.cluster_info.host),
                               ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            await cluster1.on_connect()
            bucket1 = cluster1.bucket("default")
            await bucket1.on_connect()
            collection1 = bucket1.default_collection()
            await collection1.get("some-key")

        cluster._close()
        cluster.disconnect()
        cluster1._close()
        cluster1.disconnect()

    @async_test
    async def test_rate_limits_query(self):
        await self._create_rate_limit_user(self.USERNAME, {
            "query_limits": {
                "num_queries_per_min": 1,
                "num_concurrent_requests": 10,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })

        qm = self.cluster.query_indexes()
        await qm.create_primary_index("default", ignore_if_exists=True)

        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        await cluster.on_connect()

        with self.assertRaises(RateLimitedException):
            await self._try_until_timeout(
                5, 50, cluster.query, "SELECT * FROM `{}` LIMIT 1".format("default"), query=True)

        await qm.drop_primary_index("default")
        cluster._close()
        cluster.disconnect()

    # TODO:  fts idx not available via acouchbase API yet
    # @async_test
    # async def test_rate_limits_fts(self):
    #     pass

    @async_test
    async def test_rate_limits_kv_scopes_data_size(self):
        scope_name = "rate-limit-scope"
        cm = self.bucket.collections()
        await self._create_rate_limit_scope(cm, scope_name, "default", {
            "kv_limits": {"data_size": 1024*1024}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        await cm.create_collection(collection_spec)

        # verify collection exists
        created = await self.try_n_times_async(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name)
        self.assertIsNotNone(created)

        scope = self.bucket.scope(scope_name)
        collection = scope.collection(collection_spec.name)

        doc = self._random_doc_by_size(1024*512)
        with self.assertRaises(QuotaLimitedException):
            for _ in range(5):
                await collection.upsert("ratelimit-datasize", doc)

        await cm.drop_collection(collection_spec)
        await self.try_n_times_till_exception_async(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name, (CollectionNotFoundException,))
        await cm.drop_scope(scope_name)
        await self.try_n_times_till_exception_async(
            10, 3, self._get_scope, cm, scope_name, (ScopeNotFoundException,))

    @async_test
    async def test_rate_limits_index_scopes(self):
        scope_name = "rate-limit-scope"
        cm = self.bucket.collections()
        await self._create_rate_limit_scope(cm, scope_name, "default", {
            "index_limits": {"num_indexes": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        await cm.create_collection(collection_spec)

        # verify collection exists
        created = await self.try_n_times_async(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name)
        self.assertIsNotNone(created)

        # make sure query service sees the new keyspace
        fqdn = "`{}`.`{}`.`{}`".format(
            "default", scope_name, collection_spec.name)
        num_tries = 10
        for i in range(num_tries):
            try:
                q_iter = self.cluster.query(
                    "CREATE PRIMARY INDEX ON {}".format(fqdn))
                [r async for r in q_iter]
            except KeyspaceNotFoundException:
                if i < (num_tries - 1):
                    time.sleep(3)
            except Exception:
                raise

            # if the keyspace doesn't exist, the exception will be raised here
            # if the keyspace does exist, lets remove the previously created idx
            #   it will be created again shortly
            q_iter = self.cluster.query(
                "DROP PRIMARY INDEX ON {}".format(fqdn))
            [r async for r in q_iter]
            break

        ixm = self.cluster.query_indexes()
        q_context = '{}.{}'.format(
            self.bucket_name, scope_name)
        with self.assertRaises(QuotaLimitedException):
            q_iter = self.cluster.query("CREATE PRIMARY INDEX ON `{}`".format(
                collection_spec.name), query_context=q_context)
            [r async for r in q_iter]
            indexes = await ixm.get_all_indexes("default")
            filtered_idxs = [
                i for i in indexes if i.keyspace == collection_spec.name]
            self.assertGreaterEqual(len(filtered_idxs), 1)
            self.assertTrue(filtered_idxs[0].is_primary)
            self.assertEqual('#primary', filtered_idxs[0].name)
            self.assertEqual(collection_spec.name, filtered_idxs[0].keyspace)
            # helps to avoid "Index already exist" failure
            idx_name = "rate-limit-idx-{}".format(random.randrange(0, 100))
            q_iter = self.cluster.query("CREATE INDEX `{}` ON `{}`(testField)".format(
                idx_name, collection_spec.name), query_context=q_context)
            [r async for r in q_iter]

        await cm.drop_collection(collection_spec)
        await self.try_n_times_till_exception_async(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name, (CollectionNotFoundException,))
        await cm.drop_scope(scope_name)
        await self.try_n_times_till_exception_async(
            10, 3, self._get_scope, cm, scope_name, (ScopeNotFoundException,))

    # TODO:  fts idx not available via acouchbase API yet
    # @async_test
    # async def test_rate_limits_fts_scopes(self):
    #     pass

    @async_test
    async def test_rate_limits_collections_scopes_limits(self):
        scope_name = "rate-limit-scope"
        cm = self.bucket.collections()
        await self._create_rate_limit_scope(cm, scope_name, "default", {
            "cluster_mgr_limits": {"num_collections": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        await cm.create_collection(collection_spec)

        # verify collection exists
        created = await self.try_n_times_async(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name)
        self.assertIsNotNone(created)

        with self.assertRaises(QuotaLimitedException):
            collection_spec = CollectionSpec(
                'rate-limit-collection-1', scope_name=scope_name)
            await cm.create_collection(collection_spec)

    @async_test
    async def test_rate_limits_cluster_mgr_concurrency(self):
        await self._create_rate_limit_user(self.USERNAME, {
            "cluster_mgr_limits": {
                "num_concurrent_requests": 5,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })

        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        await cluster.on_connect()
        bucket = cluster.bucket("default")
        await bucket.on_connect()

        cm = bucket.collections()

        async def create_collection(scope_name):
            await cm.create_scope(scope_name)

        scope_names = ["concurrent-scope-{}".format(i) for i in range(10)]

        with self.assertRaises(RateLimitedException):
            await asyncio.gather(*[create_collection(s) for s in scope_names])
