from datetime import timedelta
from unittest import SkipTest
import time
import random
import json
from flaky import flaky

from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.collection import GetOptions
from couchbase_core import mk_formstr
from couchbase_tests.base import CollectionTestCase
from couchbase.exceptions import (CollectionNotFoundException, CouchbaseException, HTTPException, KeyspaceNotFoundException,
                                  RateLimitedException, ScopeNotFoundException, QuotaLimitedException)
from couchbase.management.search import SearchIndex
import couchbase.search as search
from couchbase.management.collections import CollectionSpec
from couchbase.management.users import GetUserOptions
from couchbase.management.queries import (CreatePrimaryQueryIndexOptions,
                                          DropPrimaryQueryIndexOptions,
                                          GetAllQueryIndexOptions,
                                          CreateQueryIndexOptions)


@flaky(5, 1)
class RateLimitTests(CollectionTestCase):
    """
    These tests should just test the collection interface, as simply
    as possible.  We have the Scenario tests for more complicated
    stuff.
    """
    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"
    USERNAME = "rate-limit-user"
    MIN_VERSION = 7.1
    MIN_BUILD = 1621

    def setUp(self):
        super(RateLimitTests, self).setUp()

        if(self.is_mock):
            raise SkipTest("Rate limit testing not available on mock.")

        self._check_version()
        self._enforce_rate_limits()
        self._fts_indexes = []

    def tearDown(self):

        self._drop_rate_limit_user()
        cm = self.bucket.collections()
        for scope in cm.get_all_scopes():
            if "_default" not in scope.name:
                cm.drop_scope(scope.name)

        if len(self._fts_indexes) > 0:
            ixm = self.cluster.search_indexes()
            for idx in self._fts_indexes:
                self.try_n_times_till_exception(10, 3, ixm.drop_index, idx)

        return super(RateLimitTests, self).tearDown()

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

    def _enforce_rate_limits(self):
        path = "/internalSettings"
        form = mk_formstr({"enforceLimits": "true"})
        self.cluster._admin.http_request(path=path,
                                         method="POST",
                                         content_type='application/x-www-form-urlencoded',
                                         content=form)

    def _create_rate_limit_user(self, username, limits):
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

        if user_limits:
            #params["limits"] = "{{\"kv\":{{\"num_connections\":{}, \"num_ops_per_min\":{}, \"ingress_mib_per_min\":{}, \"egress_mib_per_min\":{}}}}}".format(10, 10, 10, 10)
            params["limits"] = json.dumps(user_limits)

        path = '/settings/rbac/users/local/{}'.format(username)
        form = mk_formstr(params)
        self.cluster._admin.http_request(path=path,
                                         method='PUT',
                                         content_type='application/x-www-form-urlencoded',
                                         content=form)

        # lets verify user exists
        um = self.cluster.users()
        user_metadata = self.try_n_times(10, 1, um.get_user, username,
                                         GetUserOptions(domain_name="local"))

        self.assertIsNotNone(user_metadata)
        self.assertEqual(username, user_metadata.user.username)

    def _drop_rate_limit_user(self):
        um = self.cluster.users()
        self.try_n_times_till_exception(10, 3, um.drop_user, self.USERNAME)

    def _create_rate_limit_scope(self, cm, scope_name, bucket_name, limits):
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
        self.cluster._admin.http_request(path=path,
                                         method='POST',
                                         content_type='application/x-www-form-urlencoded',
                                         content=form)

        # verify the scope exists
        all_scopes = cm.get_all_scopes()
        self.assertIn(scope_name, map(lambda s: s.name, all_scopes))

    def _random_doc_by_size(self, size):
        doc = bytearray((random.getrandbits(8) for i in range(size)))
        return doc.hex()

    def _get_scope(self, cm, scope_name):
        return next((s for s in cm.get_all_scopes() if s.name == scope_name), None)

    def _get_collection(self, cm, scope_name, coll_name):
        scope = self._get_scope(cm, scope_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    def _try_until_timeout(self, timeout, interval, func, *args, **kwargs):
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
                func(*args, **kwargs).execute()
            elif is_fts is True:
                res = func(*args, **kwargs)
                res.rows()
            else:
                func(*args, **kwargs)
            time_left = timeout_ms - ((time.perf_counter() - start) * 1000)
            if time_left <= 0:
                break

            time.sleep(interval_ms)

    def test_rate_limits(self):
        self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 10,
                "num_ops_per_min": 10,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        bucket = cluster.bucket("default")
        collection = bucket.default_collection()

        with self.assertRaises(RateLimitedException):
            self._try_until_timeout(
                5, 10, collection.upsert, "ratelimit", "test")

        cluster._close()
        cluster.disconnect()

    def test_rate_limits_ingress(self):
        self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 10,
                "num_ops_per_min": 100,
                "ingress_mib_per_min": 1,
                "egress_mib_per_min": 10
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        bucket = cluster.bucket("default")
        collection = bucket.default_collection()

        with self.assertRaises(RateLimitedException):
            doc = self._random_doc_by_size(1024*512)
            for _ in range(3):
                collection.upsert("ratelimit-ingress", doc)

        cluster._close()
        cluster.disconnect()

    def test_rate_limits_egress(self):
        self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 10,
                "num_ops_per_min": 100,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 1
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        bucket = cluster.bucket("default")
        collection = bucket.default_collection()

        doc = self._random_doc_by_size(1024*512)
        key = "ratelimit-egress"
        collection.upsert(key, doc)
        with self.assertRaises(RateLimitedException):
            for _ in range(3):
                collection.get(key, GetOptions(timeout=timedelta(seconds=10)))

        cluster._close()
        cluster.disconnect()

    def test_rate_limits_max_conns(self):
        self._create_rate_limit_user(self.USERNAME, {
            "kv_limits": {
                "num_connections": 1,
                "num_ops_per_min": 100,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })
        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        bucket = cluster.bucket("default")
        collection = bucket.default_collection()
        collection.exists("some-key")

        with self.assertRaises((RateLimitedException)):
            cluster1 = Cluster("couchbase://{}".format(self.cluster_info.host),
                               ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket1 = cluster1.bucket("default")
            collection1 = bucket1.default_collection()
            collection1.exists("some-key")

        cluster._close()
        cluster.disconnect()
        cluster1._close()
        cluster1.disconnect()

    def test_rate_limits_query(self):
        self._create_rate_limit_user(self.USERNAME, {
            "query_limits": {
                "num_queries_per_min": 1,
                "num_concurrent_requests": 10,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })

        qm = self.cluster.query_indexes()
        qm.create_primary_index("default", ignore_if_exists=True)

        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))

        with self.assertRaises(RateLimitedException):
            self._try_until_timeout(
                5, 50, cluster.query, "SELECT * FROM `{}` LIMIT 1".format("default"), query=True)

        qm.drop_primary_index("default")
        cluster._close()
        cluster.disconnect()

    def test_rate_limits_fts(self):
        self._create_rate_limit_user(self.USERNAME, {
            "fts_limits": {
                "num_queries_per_min": 1,
                "num_concurrent_requests": 10,
                "ingress_mib_per_min": 10,
                "egress_mib_per_min": 10
            }
        })

        sm = self.cluster.search_indexes()
        sm.upsert_index(SearchIndex(
            name="ratelimit-idx", source_name="default"))
        self._fts_indexes.append("ratelimit-idx")

        cluster = Cluster("couchbase://{}".format(self.cluster_info.host),
                          ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))

        # TODO:  see why lcb raises HTTPException on Jenkins (seems to work locally)
        with self.assertRaises((RateLimitedException, HTTPException,)):
            self._try_until_timeout(
                5, 50, cluster.search_query, "ratelimit-idx", search.TermQuery("north"), search.SearchOptions(limit=1), fts=True)

        sm.drop_index("ratelimit-idx")
        cluster._close()
        cluster.disconnect()

    def test_rate_limits_kv_scopes_data_size(self):
        scope_name = "rate-limit-scope"
        cm = self.bucket.collections()
        self._create_rate_limit_scope(cm, scope_name, "default", {
            "kv_limits": {"data_size": 1024*1024}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cm.create_collection(collection_spec)

        # verify collection exists
        created = self.try_n_times(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name)
        self.assertIsNotNone(created)

        scope = self.bucket.scope(scope_name)
        collection = scope.collection(collection_spec.name)

        doc = self._random_doc_by_size(1024*512)
        with self.assertRaises(QuotaLimitedException):
            for _ in range(5):
                collection.upsert("ratelimit-datasize", doc)

        cm.drop_collection(collection_spec)
        self.try_n_times_till_exception(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name, (CollectionNotFoundException,))
        cm.drop_scope(scope_name)
        self.try_n_times_till_exception(
            10, 3, self._get_scope, cm, scope_name, (ScopeNotFoundException,))

    def test_rate_limits_index_scopes(self):
        scope_name = "rate-limit-scope"
        cm = self.bucket.collections()
        self._create_rate_limit_scope(cm, scope_name, "default", {
            "index_limits": {"num_indexes": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cm.create_collection(collection_spec)

        # verify collection exists
        created = self.try_n_times(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name)
        self.assertIsNotNone(created)

        # make sure query service sees the new keyspace
        # drop the index and then re-create
        ixm = self.cluster.query_indexes()
        self.try_n_times(
            10, 3, ixm.create_primary_index, "default",
            CreatePrimaryQueryIndexOptions(scope_name=scope_name,
                                           collection_name=collection_spec.name))
        self.try_n_times(
            10, 3, ixm.drop_primary_index, "default",
            DropPrimaryQueryIndexOptions(scope_name=scope_name,
                                         collection_name=collection_spec.name))

        scope = self.bucket.scope(scope_name)

        with self.assertRaises(QuotaLimitedException):
            self.try_n_times(
                10, 3, ixm.create_primary_index, "default",
                CreatePrimaryQueryIndexOptions(scope_name=scope_name,
                                               collection_name=collection_spec.name))
            indexes = ixm.get_all_indexes("default", GetAllQueryIndexOptions(scope_name=scope_name,
                                                                             collection_name=collection_spec.name))
            self.assertGreaterEqual(len(indexes), 1)
            self.assertTrue(indexes[0].is_primary)
            self.assertEqual('#primary', indexes[0].name)
            self.assertEqual(collection_spec.name, indexes[0].collection_name)
            # helps to avoid "Index already exist" failure
            idx_name = "rate-limit-idx-{}".format(random.randrange(0, 100))
            scope.query("CREATE INDEX `{}` ON `{}`(testField)".format(
                idx_name, collection_spec.name)).execute()

        cm.drop_collection(collection_spec)
        self.try_n_times_till_exception(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name, (CollectionNotFoundException,))
        cm.drop_scope(scope_name)
        self.try_n_times_till_exception(
            10, 3, self._get_scope, cm, scope_name, (ScopeNotFoundException,))

    def test_rate_limits_fts_scopes(self):
        scope_name = "rate-limit-scope"
        cm = self.bucket.collections()
        self._create_rate_limit_scope(cm, scope_name, "default", {
            "fts_limits": {"num_fts_indexes": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cm.create_collection(collection_spec)

        # verify collection exists
        created = self.try_n_times(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name)
        self.assertIsNotNone(created)

        # see beer-search-coll-index-params.json for ref
        idx_name = "{}.{}".format(scope_name, collection_spec.name)
        idx_params = {
            "doc_config": {
                "mode": "scope.collection.type_field",
                "type_field": "type"
            },
            "mapping": {
                "default_analyzer": "standard",
                "default_datetime_parser": "dateTimeOptional",
                "default_field": "_all",
                "default_mapping": {
                    "dynamic": True,
                    "enabled": False
                },
                "default_type": "_default",
                "docvalues_dynamic": True,
                "index_dynamic": True,
                "store_dynamic": False,
                "type_field": "_type",
                "types": {
                    idx_name: {
                        "dynamic": False,
                        "enabled": True
                    }
                }
            }
        }

        ixm = self.cluster.search_indexes()
        indexes = []
        with self.assertRaises(QuotaLimitedException):
            # random helps to avoid "Index already exist" failure
            new_idx = SearchIndex(name="rate-limit-idx-{}".format(random.randrange(0, 50)),
                                  idx_type="fulltext-index",
                                  source_name="default",
                                  source_type="couchbase",
                                  params=json.loads(json.dumps(idx_params)))
            self._fts_indexes.append(new_idx.name)
            # try multiple times to avoid scope not w/in bucket failure
            num_tries = 10
            for i in range(num_tries):
                try:
                    ixm.upsert_index(new_idx)
                except CouchbaseException:
                    if i < (num_tries - 1):
                        time.sleep(3)
                except Exception:
                    raise
            ixm.upsert_index(new_idx)

            # random helps to avoid "Index already exist" failure
            new_idx = SearchIndex(name="rate-limit-idx-{}".format(random.randrange(51, 100)),
                                  idx_type="fulltext-index",
                                  source_name="default",
                                  source_type="couchbase",
                                  params=json.loads(json.dumps(idx_params)))
            self._fts_indexes.append(new_idx.name)
            ixm.upsert_index(new_idx)

        cm.drop_collection(collection_spec)
        self.try_n_times_till_exception(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name, (CollectionNotFoundException,))
        cm.drop_scope(scope_name)
        self.try_n_times_till_exception(
            10, 3, self._get_scope, cm, scope_name, (ScopeNotFoundException,))

    def test_rate_limits_collections_scopes_limits(self):
        scope_name = "rate-limit-scope"
        cm = self.bucket.collections()
        self._create_rate_limit_scope(cm, scope_name, "default", {
            "cluster_mgr_limits": {"num_collections": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cm.create_collection(collection_spec)

        # verify collection exists
        created = self.try_n_times(
            10, 3, self._get_collection, cm, scope_name, collection_spec.name)
        self.assertIsNotNone(created)

        with self.assertRaises(QuotaLimitedException):
            collection_spec = CollectionSpec(
                'rate-limit-collection-1', scope_name=scope_name)
            cm.create_collection(collection_spec)

    # Handled via acouchbase API
    # def test_rate_limits_cluster_mgr_concurrency(self):
    #     pass
