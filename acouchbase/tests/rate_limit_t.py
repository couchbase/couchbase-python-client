#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio
import json
import random
import time
from datetime import timedelta

import pytest
import pytest_asyncio
import requests

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (CouchbaseException,
                                  DocumentNotFoundException,
                                  QuotaLimitedException,
                                  RateLimitedException,
                                  ScopeNotFoundException)
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.collections import CollectionSpec
from couchbase.management.options import GetUserOptions
from couchbase.management.users import Role, User
from couchbase.options import ClusterOptions, GetOptions
from tests.helpers import CouchbaseTestEnvironmentException

from ._test_utils import TestEnvironment


@pytest.mark.flaky(reruns=5, reruns_delay=1)
class RateLimitTests:
    """
    These tests should just test the collection interface, as simply
    as possible.  We have the Scenario tests for more complicated
    stuff.
    """
    CONTENT = {"some": "content"}
    KEY = "imakey"
    NOKEY = "somerandomkey"
    USERNAME = "rate-limit-user"
    RATE_LIMIT_SCOPE_NAME = 'rate-limit-scope'

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True,
                                                       manage_collections=True,
                                                       manage_users=True,
                                                       manage_rate_limit=True)
        # if couchbase_config.is_mock_server:
        #     pytest.skip('Mock server does not support feature: rate limit testing.')
        # conn_string = couchbase_config.get_connection_string()
        # username, pw = couchbase_config.get_username_and_pw()
        # opts = ClusterOptions(PasswordAuthenticator(username, pw))
        # cluster = await Cluster.connect(conn_string, opts)
        # bucket = cluster.bucket(f"{couchbase_config.bucket_name}")
        # await bucket.on_connect()
        # await cluster.cluster_info()

        # coll = bucket.default_collection()

        # self._conn_str = conn_string
        # parsed_conn = urlparse(conn_string)
        # url = f'http://{parsed_conn.netloc}:8091'

        # cb_env = TestEnvironment(cluster,
        #                          bucket,
        #                          coll,
        #                          couchbase_config,
        #                          manage_buckets=True,
        #                          manage_collections=True,
        #                          manage_users=True,
        #                          rate_limit_params=RateLimitData(url, username, pw))

        self._fts_indexes = []
        self._enforce_rate_limits(cb_env, True)

        yield cb_env
        await self.tear_down(cb_env)

    def _enforce_rate_limits(self, cb_env, enforce=True):
        url = f'{cb_env.rate_limit_params.url}/internalSettings'
        payload = {'enforceLimits': f'{"true" if enforce is True else "false"}'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.post(url,
                          headers=headers,
                          data=payload,
                          auth=(cb_env.rate_limit_params.username,
                                cb_env.rate_limit_params.pw))

        if r.status_code != 200:
            raise CouchbaseTestEnvironmentException('Unable to enforce rate limits.')

    async def tear_down(self, cb_env):
        await self._drop_rate_limit_user(cb_env)
        scopes = await cb_env.cm.get_all_scopes()
        scope_names = ["concurrent-scope-{}".format(i) for i in range(10)]
        scope_names.append(self.RATE_LIMIT_SCOPE_NAME)
        for scope in scopes:
            if scope.name in scope_names:
                await cb_env.cm.drop_scope(scope.name)

        num_indexes = 0
        if cb_env.rate_limit_params.fts_indexes:
            num_indexes = len(cb_env.rate_limit_params.fts_indexes)
        if num_indexes > 0:
            ixm = cb_env.cluster.search_indexes()
            for idx in cb_env.rate_limit_params.fts_indexes:
                await cb_env.try_n_times_till_exception(10, 3, ixm.drop_index, idx)

        qm = cb_env.cluster.query_indexes()
        await qm.drop_primary_index(cb_env.bucket.name, ignore_if_not_exists=True)

        self._enforce_rate_limits(cb_env, False)

    @pytest_asyncio.fixture()
    async def remove_docs(self, cb_env):
        try:
            await cb_env.collection.remove('ratelimit-ingress')
            await cb_env.try_n_times_till_exception(10, 3, cb_env.collection.get,
                                                    'ratelimit-ingress', (DocumentNotFoundException,))
        except CouchbaseException:
            pass

        try:
            await cb_env.collection.remove('ratelimit-egress')
            await cb_env.try_n_times_till_exception(10, 3, cb_env.collection.get,
                                                    'ratelimit-egress', (DocumentNotFoundException,))
        except CouchbaseException:
            pass

    @pytest_asyncio.fixture()
    async def cleanup_scope_and_collection(self, cb_env):
        await cb_env.try_n_times_till_exception(5, 1,
                                                cb_env.cm.drop_scope,
                                                self.RATE_LIMIT_SCOPE_NAME,
                                                expected_exceptions=(ScopeNotFoundException,))
        yield
        await cb_env.try_n_times_till_exception(5, 1,
                                                cb_env.cm.drop_scope,
                                                self.RATE_LIMIT_SCOPE_NAME,
                                                expected_exceptions=(ScopeNotFoundException,))

    async def _create_rate_limit_user(self, cb_env, username, limits):
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
            params["limits"] = json.dumps(user_limits)

        path = f'/settings/rbac/users/local/{username}'

        url = f'{cb_env.rate_limit_params.url}/{path}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.put(url,
                         headers=headers,
                         data=params,
                         auth=(cb_env.rate_limit_params.username,
                               cb_env.rate_limit_params.pw))

        if r.status_code != 200:
            raise CouchbaseTestEnvironmentException('Unable to create rate-limit-user.')

        # lets verify user exists
        user_metadata = await cb_env.try_n_times(10, 1, cb_env.um.get_user, username,
                                                 GetUserOptions(domain_name="local"))

        assert user_metadata is not None
        assert username == user_metadata.user.username

    async def _drop_rate_limit_user(self, cb_env):
        await cb_env.try_n_times_till_exception(10, 3, cb_env.um.drop_user, self.USERNAME)

    async def _create_rate_limit_scope(self, cb_env, scope_name, limits):
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

        path = f'/pools/default/buckets/{cb_env.bucket.name}/scopes'
        url = f'{cb_env.rate_limit_params.url}/{path}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        requests.post(url,
                      headers=headers,
                      data=params,
                      auth=(cb_env.rate_limit_params.username,
                            cb_env.rate_limit_params.pw))
        # verify the scope exists
        all_scopes = await cb_env.cm.get_all_scopes()
        assert scope_name in map(lambda s: s.name, all_scopes)

    def _random_doc_by_size(self, size):
        doc = bytearray((random.getrandbits(8) for i in range(size)))
        return doc.hex()

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
                await func(*args, **kwargs).execute()
            elif is_fts is True:
                res = func(*args, **kwargs)
                [r async for r in res.rows()]
            else:
                await func(*args, **kwargs)
            time_left = timeout_ms - ((time.perf_counter() - start) * 1000)
            if time_left <= 0:
                break

            await asyncio.sleep(interval_ms)

    @pytest.mark.asyncio
    async def test_rate_limits(self, couchbase_config, cb_env):
        await self._create_rate_limit_user(cb_env,
                                           self.USERNAME, {
                                               "kv_limits": {
                                                   "num_connections": 10,
                                                   "num_ops_per_min": 10,
                                                   "ingress_mib_per_min": 1,
                                                   "egress_mib_per_min": 10
                                               }
                                           })
        conn_string = couchbase_config.get_connection_string()
        cluster = None
        try:
            cluster = await Cluster.connect(conn_string,
                                            ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket('default')
            await bucket.on_connect()
            collection = bucket.default_collection()

            await self._try_until_timeout(
                5, 10, collection.upsert, "ratelimit", "test")
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.usefixtures('remove_docs')
    @pytest.mark.asyncio
    async def test_rate_limits_ingress(self, couchbase_config, cb_env):
        await self._create_rate_limit_user(cb_env,
                                           self.USERNAME, {
                                               "kv_limits": {
                                                   "num_connections": 10,
                                                   "num_ops_per_min": 100,
                                                   "ingress_mib_per_min": 1,
                                                   "egress_mib_per_min": 10
                                               }
                                           })
        conn_string = couchbase_config.get_connection_string()
        cluster = None
        try:
            cluster = await Cluster.connect(conn_string,
                                            ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket("default")
            await bucket.on_connect()
            collection = bucket.default_collection()

            doc = self._random_doc_by_size(1024*512)
            for _ in range(3):
                await collection.upsert("ratelimit-ingress", doc)

        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.usefixtures('remove_docs')
    @pytest.mark.asyncio
    async def test_rate_limits_egress(self, couchbase_config, cb_env):
        await self._create_rate_limit_user(cb_env,
                                           self.USERNAME, {"kv_limits": {
                                               "num_connections": 10,
                                               "num_ops_per_min": 100,
                                               "ingress_mib_per_min": 10,
                                               "egress_mib_per_min": 2
                                           }
                                           })
        conn_string = couchbase_config.get_connection_string()
        cluster = None
        try:
            cluster = await Cluster.connect(conn_string,
                                            ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket("default")
            await bucket.on_connect()
            collection = bucket.default_collection()

            doc = self._random_doc_by_size(1024*512)
            key = "ratelimit-egress"
            await collection.upsert(key, doc)
            for _ in range(3):
                await collection.get(key, GetOptions(timeout=timedelta(seconds=10)))
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.asyncio
    async def test_rate_limits_max_conns(self, couchbase_config, cb_env):
        await self._create_rate_limit_user(cb_env,
                                           self.USERNAME, {
                                               "kv_limits": {
                                                   "num_connections": 1,
                                                   "num_ops_per_min": 100,
                                                   "ingress_mib_per_min": 10,
                                                   "egress_mib_per_min": 10
                                               }
                                           })
        cluster = None
        cluster1 = None
        conn_string = couchbase_config.get_connection_string()
        try:
            cluster = await Cluster.connect(conn_string,
                                            ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket("default")
            await bucket.on_connect()
            collection = bucket.default_collection()
            collection.exists("some-key")

            cluster1 = await Cluster.connect(conn_string,
                                             ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket1 = cluster1.bucket("default")
            await bucket.on_connect()
            collection1 = bucket1.default_collection()
            await collection1.exists("some-key")
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.usefixtures('cleanup_scope_and_collection')
    @pytest.mark.asyncio
    async def test_rate_limits_kv_scopes_data_size(self, cb_env):
        scope_name = self.RATE_LIMIT_SCOPE_NAME
        await self._create_rate_limit_scope(cb_env, scope_name, {
            "kv_limits": {"data_size": 1024*1024}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        await cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        created = await cb_env.try_n_times(
            10, 3, cb_env.get_collection, scope_name, collection_spec.name, cb_env.bucket.name)
        assert created is not None

        scope = cb_env.bucket.scope(scope_name)
        collection = scope.collection(collection_spec.name)

        doc = self._random_doc_by_size(1024*512)
        with pytest.raises(QuotaLimitedException):
            for _ in range(5):
                await collection.upsert("ratelimit-datasize", doc)

        # await cb_env.cm.drop_collection(collection_spec)
        # for _ in range(5):
        #     res = await cb_env.get_collection(scope_name,
        #                                       collection_spec.name,
        #                                       cb_env.bucket.name)
        #     if not res:
        #         break
        # await cb_env.cm.drop_scope(scope_name)
        # for _ in range(5):
        #     res = await cb_env.get_scope(scope_name, cb_env.bucket.name)
        #     if not res:
        #         break

    @pytest.mark.usefixtures('cleanup_scope_and_collection')
    @pytest.mark.asyncio
    async def test_rate_limits_collections_scopes_limits(self, cb_env):
        scope_name = self.RATE_LIMIT_SCOPE_NAME
        await self._create_rate_limit_scope(cb_env, scope_name, {
            "cluster_mgr_limits": {"num_collections": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        await cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        created = await cb_env.try_n_times(10,
                                           3,
                                           cb_env.get_collection,
                                           scope_name,
                                           collection_spec.name,
                                           cb_env.bucket.name)
        assert created is not None

        with pytest.raises(QuotaLimitedException):
            collection_spec = CollectionSpec(
                'rate-limit-collection-1', scope_name=scope_name)
            await cb_env.cm.create_collection(collection_spec)

    @pytest.mark.asyncio
    async def test_rate_limits_cluster_mgr_concurrency(self, couchbase_config, cb_env):
        pytest.skip('Needs some work, not raising RateLimitedExceptions for some reason...')
        await self._create_rate_limit_user(cb_env,
                                           self.USERNAME, {
                                               "cluster_mgr_limits": {
                                                   "num_concurrent_requests": 3,
                                                   "ingress_mib_per_min": 10,
                                                   "egress_mib_per_min": 10
                                               }
                                           })

        conn_string = couchbase_config.get_connection_string()
        cluster = await Cluster.connect(conn_string,
                                        ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
        bucket = cluster.bucket("default")
        await bucket.on_connect()

        cm = bucket.collections()

        async def create_collection(scope_name):
            await cm.create_scope(scope_name)

        scope_names = ["concurrent-scope-{}".format(i) for i in range(10)]

        with pytest.raises(RateLimitedException):
            await asyncio.gather(*[create_collection(s) for s in scope_names])

        bm = cluster.buckets()

        async def create_bucket(bucket_settings):
            await bm.create_bucket(bucket_settings)

        bucket_list = [CreateBucketSettings(
            name="testBucket{}".format(i),
            bucket_type="couchbase",
            ram_quota_mb=100) for i in range(10)]

        with pytest.raises(RateLimitedException):
            await asyncio.gather(*[create_bucket(b) for b in bucket_list])

        um = cluster.users()

        async def create_user(user):
            await um.upsert_user(user, domain_name="local")

        roles = [
            Role(name='data_reader', bucket='default'),
            Role(name='data_writer', bucket='default')
        ]
        user_list = [User(username="user{}".format(
            i), roles=roles, password="password{}".format(i)) for i in range(10)]

        with pytest.raises(RateLimitedException):
            await asyncio.gather(*[create_user(u) for u in user_list])
