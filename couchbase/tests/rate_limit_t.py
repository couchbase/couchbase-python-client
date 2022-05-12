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

import json
import random
import time
from datetime import timedelta

import pytest
import requests

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (CouchbaseException,
                                  DocumentNotFoundException,
                                  QuotaLimitedException,
                                  RateLimitedException,
                                  ScopeNotFoundException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.options import GetUserOptions
from couchbase.management.search import SearchIndex
from couchbase.options import ClusterOptions, GetOptions
from couchbase.search import SearchOptions, TermQuery
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

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_buckets=True,
                                                 manage_collections=True,
                                                 manage_users=True,
                                                 manage_rate_limit=True)

        self._fts_indexes = []
        self._enforce_rate_limits(cb_env, True)

        yield cb_env
        self.tear_down(cb_env)

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

    def tear_down(self, cb_env):
        self._drop_rate_limit_user(cb_env)
        for scope in cb_env.cm.get_all_scopes():
            if scope.name == self.RATE_LIMIT_SCOPE_NAME:
                cb_env.cm.drop_scope(scope.name)

        num_indexes = 0
        if cb_env.rate_limit_params.fts_indexes:
            num_indexes = len(cb_env.rate_limit_params.fts_indexes)
        if num_indexes > 0:
            ixm = cb_env.cluster.search_indexes()
            for idx in cb_env.rate_limit_params.fts_indexes:
                cb_env.try_n_times_till_exception(10, 3, ixm.drop_index, idx)

        qm = cb_env.cluster.query_indexes()
        qm.drop_primary_index(cb_env.bucket.name, ignore_if_not_exists=True)

        self._enforce_rate_limits(cb_env, False)

    @pytest.fixture()
    def remove_docs(self, cb_env):
        try:
            cb_env.collection.remove('ratelimit-ingress')
            cb_env.try_n_times_till_exception(10, 3, cb_env.collection.get,
                                              'ratelimit-ingress', (DocumentNotFoundException,))
        except CouchbaseException:
            pass

        try:
            cb_env.collection.remove('ratelimit-egress')
            cb_env.try_n_times_till_exception(10, 3, cb_env.collection.get,
                                              'ratelimit-egress', (DocumentNotFoundException,))
        except CouchbaseException:
            pass

    @pytest.fixture()
    def cleanup_scope_and_collection(self, cb_env):
        cb_env.try_n_times_till_exception(5, 1,
                                          cb_env.cm.drop_scope,
                                          self.RATE_LIMIT_SCOPE_NAME,
                                          expected_exceptions=(ScopeNotFoundException,))
        yield
        cb_env.try_n_times_till_exception(5, 1,
                                          cb_env.cm.drop_scope,
                                          self.RATE_LIMIT_SCOPE_NAME,
                                          expected_exceptions=(ScopeNotFoundException,))

    def _create_rate_limit_user(self, cb_env, username, limits):
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
        user_metadata = cb_env.try_n_times(10, 1, cb_env.um.get_user, username,
                                           GetUserOptions(domain_name="local"))

        assert user_metadata is not None
        assert username == user_metadata.user.username

    def _drop_rate_limit_user(self, cb_env):
        cb_env.try_n_times_till_exception(10, 3, cb_env.um.drop_user, self.USERNAME)

    def _create_rate_limit_scope(self, cb_env, scope_name, limits):
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
        all_scopes = cb_env.cm.get_all_scopes()
        assert scope_name in map(lambda s: s.name, all_scopes)

    def _random_doc_by_size(self, size):
        doc = bytearray((random.getrandbits(8) for i in range(size)))
        return doc.hex()

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

    def test_rate_limits(self, couchbase_config, cb_env):
        self._create_rate_limit_user(cb_env,
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
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket('default')
            collection = bucket.default_collection()

            self._try_until_timeout(
                5, 10, collection.upsert, "ratelimit", "test")
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.usefixtures('remove_docs')
    def test_rate_limits_ingress(self, couchbase_config, cb_env):
        self._create_rate_limit_user(cb_env,
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
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket("default")
            collection = bucket.default_collection()

            doc = self._random_doc_by_size(1024*512)
            for _ in range(3):
                collection.upsert("ratelimit-ingress", doc)

        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.usefixtures('remove_docs')
    def test_rate_limits_egress(self, couchbase_config, cb_env):
        self._create_rate_limit_user(cb_env,
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
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket("default")
            collection = bucket.default_collection()

            doc = self._random_doc_by_size(1024*512)
            key = "ratelimit-egress"
            collection.upsert(key, doc)
            for _ in range(3):
                collection.get(key, GetOptions(timeout=timedelta(seconds=10)))
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    def test_rate_limits_max_conns(self, couchbase_config, cb_env):
        self._create_rate_limit_user(cb_env,
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
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket = cluster.bucket("default")
            collection = bucket.default_collection()
            collection.exists("some-key")

            cluster1 = Cluster(conn_string,
                               ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))
            bucket1 = cluster1.bucket("default")
            collection1 = bucket1.default_collection()
            collection1.exists("some-key")
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    def test_rate_limits_query(self, couchbase_config, cb_env):
        self._create_rate_limit_user(cb_env,
                                     self.USERNAME, {
                                         "query_limits": {
                                             "num_queries_per_min": 1,
                                             "num_concurrent_requests": 10,
                                             "ingress_mib_per_min": 10,
                                             "egress_mib_per_min": 10
                                         }
                                     })

        conn_string = couchbase_config.get_connection_string()
        cluster = None
        qm = None
        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))

            qm = cluster.query_indexes()
            qm.create_primary_index("default", ignore_if_exists=True)
            self._try_until_timeout(
                5, 50, cluster.query, "SELECT 'Hi there!'", query=True)

        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    def test_rate_limits_fts(self, couchbase_config, cb_env):
        self._create_rate_limit_user(cb_env,
                                     self.USERNAME, {
                                         "fts_limits": {
                                             "num_queries_per_min": 1,
                                             "num_concurrent_requests": 10,
                                             "ingress_mib_per_min": 10,
                                             "egress_mib_per_min": 10
                                         }
                                     })

        conn_string = couchbase_config.get_connection_string()
        sixm = cb_env.cluster.search_indexes()
        sixm.upsert_index(SearchIndex(
            name="ratelimit-idx", source_name="default"))
        if not cb_env.rate_limit_params.fts_indexes:
            cb_env.rate_limit_params.fts_indexes = []
        cb_env.rate_limit_params.fts_indexes.append("ratelimit-idx")

        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(self.USERNAME, "password")))

            self._try_until_timeout(
                5, 50, cluster.search_query, "ratelimit-idx", TermQuery("north"), SearchOptions(limit=1), fts=True)

        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')
        finally:
            sixm.drop_index("ratelimit-idx")

    @pytest.mark.usefixtures('cleanup_scope_and_collection')
    def test_rate_limits_kv_scopes_data_size(self, cb_env):
        scope_name = self.RATE_LIMIT_SCOPE_NAME
        self._create_rate_limit_scope(cb_env, scope_name, {
            "kv_limits": {"data_size": 1024*1024}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        created = cb_env.try_n_times(
            10, 3, cb_env.get_collection, scope_name, collection_spec.name, cb_env.bucket.name)
        assert created is not None

        scope = cb_env.bucket.scope(scope_name)
        collection = scope.collection(collection_spec.name)

        doc = self._random_doc_by_size(1024*512)
        with pytest.raises(QuotaLimitedException):
            for _ in range(5):
                collection.upsert("ratelimit-datasize", doc)

    @pytest.mark.usefixtures('cleanup_scope_and_collection')  # noqa: C901
    def test_rate_limits_index_scopes(self, cb_env):  # noqa: C901
        scope_name = self.RATE_LIMIT_SCOPE_NAME
        self._create_rate_limit_scope(cb_env, scope_name, {
            "index_limits": {"num_indexes": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        created = cb_env.try_n_times(
            10, 3, cb_env.get_collection, scope_name, collection_spec.name, cb_env.bucket.name)
        assert created is not None

        # make sure query service sees the new keyspace
        # drop the index and then re-create
        ixm = cb_env.cluster.query_indexes()

        def create_primary_index():
            try:
                ixm.create_primary_index("default", scope_name=scope_name,
                                         collection_name=collection_spec.name)
                indexes = ixm.get_all_indexes("default",
                                              scope_name=scope_name,
                                              collection_name=collection_spec.name)
                if len(indexes) == 0:
                    return False
            except CouchbaseException:
                return False

            return True

        count = 1
        while not create_primary_index():
            if count == 5:
                raise pytest.skip("Unable to create primary index.")

            count += 1
            indexes = ixm.get_all_indexes("default",
                                          scope_name=scope_name,
                                          collection_name=collection_spec.name)
            cb_env.sleep(1)
            if len(indexes) > 0:
                break

        cb_env.try_n_times(10,
                           3,
                           ixm.drop_primary_index,
                           "default",
                           scope_name=scope_name,
                           collection_name=collection_spec.name)

        scope = cb_env.bucket.scope(scope_name)

        with pytest.raises(QuotaLimitedException):
            cb_env.try_n_times(10,
                               3,
                               ixm.create_primary_index,
                               "default",
                               scope_name=scope_name,
                               collection_name=collection_spec.name)

            indexes = ixm.get_all_indexes("default",
                                          scope_name=scope_name,
                                          collection_name=collection_spec.name)
            assert len(indexes) >= 1
            assert indexes[0].is_primary is True
            assert '#primary' == indexes[0].name
            assert collection_spec.name == indexes[0].collection_name
            # helps to avoid "Index already exist" failure
            idx_name = "rate-limit-idx-{}".format(random.randrange(0, 100))
            scope.query("CREATE INDEX `{}` ON `{}`(testField)".format(
                idx_name, collection_spec.name)).execute()

    @pytest.mark.usefixtures('cleanup_scope_and_collection')  # noqa: C901
    def test_rate_limits_fts_scopes(self, cb_env):  # noqa: C901
        scope_name = self.RATE_LIMIT_SCOPE_NAME
        self._create_rate_limit_scope(cb_env, scope_name, {
            "fts_limits": {"num_fts_indexes": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        created = cb_env.try_n_times(10,
                                     3,
                                     cb_env.get_collection,
                                     scope_name,
                                     collection_spec.name,
                                     cb_env.bucket.name)
        assert created is not None

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

        ixm = cb_env.cluster.search_indexes()
        if not cb_env.rate_limit_params.fts_indexes:
            cb_env.rate_limit_params.fts_indexes = []
        with pytest.raises(QuotaLimitedException):
            # random helps to avoid "Index already exist" failure
            new_idx = SearchIndex(name="rate-limit-idx-{}".format(random.randrange(0, 50)),
                                  idx_type="fulltext-index",
                                  source_name="default",
                                  source_type="couchbase",
                                  params=json.loads(json.dumps(idx_params)))
            cb_env.rate_limit_params.fts_indexes.append(new_idx.name)
            # try multiple times to avoid scope not w/in bucket failure
            num_tries = 10
            success = False
            for i in range(num_tries):
                try:
                    ixm.upsert_index(new_idx)
                    success = True
                except CouchbaseException:
                    if i < (num_tries - 1):
                        cb_env.sleep(3)
                except Exception:
                    raise
            if not success:
                ixm.upsert_index(new_idx)

            # random helps to avoid "Index already exist" failure
            new_idx = SearchIndex(name="rate-limit-idx-{}".format(random.randrange(51, 100)),
                                  idx_type="fulltext-index",
                                  source_name="default",
                                  source_type="couchbase",
                                  params=json.loads(json.dumps(idx_params)))
            cb_env.rate_limit_params.fts_indexes.append(new_idx.name)
            ixm.upsert_index(new_idx)

    @pytest.mark.usefixtures('cleanup_scope_and_collection')
    def test_rate_limits_collections_scopes_limits(self, cb_env):
        scope_name = self.RATE_LIMIT_SCOPE_NAME
        self._create_rate_limit_scope(cb_env, scope_name, {
            "cluster_mgr_limits": {"num_collections": 1}
        })

        collection_spec = CollectionSpec(
            'rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        created = cb_env.try_n_times(10,
                                     3,
                                     cb_env.get_collection,
                                     scope_name,
                                     collection_spec.name,
                                     cb_env.bucket.name)
        assert created is not None

        with pytest.raises(QuotaLimitedException):
            collection_spec = CollectionSpec(
                'rate-limit-collection-1', scope_name=scope_name)
            cb_env.cm.create_collection(collection_spec)
