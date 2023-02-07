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
from datetime import timedelta

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  CouchbaseException,
                                  QuotaLimitedException,
                                  RateLimitedException)
from couchbase.management.collections import CollectionSpec
from couchbase.management.search import SearchIndex
from couchbase.options import ClusterOptions, GetOptions
from couchbase.search import SearchOptions, TermQuery
from tests.environments.rate_limit_environment import RateLimitTestEnvironment
from tests.environments.test_environment import TestEnvironment


class RateLimitTestSuite:
    TEST_MANIFEST = [
        'test_rate_limits',
        'test_rate_limits_collections_scopes_limits',
        'test_rate_limits_egress',
        'test_rate_limits_fts',
        'test_rate_limits_fts_scopes',
        'test_rate_limits_index_scopes',
        'test_rate_limits_ingress',
        'test_rate_limits_kv_scopes_data_size',
        'test_rate_limits_max_conns',
        'test_rate_limits_query',
    ]

    @pytest.fixture()
    def remove_docs(self, cb_env):
        cb_env.remove_docs()

    @pytest.fixture()
    def cleanup_scope_and_collection(self, cb_env):
        cb_env.drop_scope()
        yield
        cb_env.drop_scope()

    def test_rate_limits(self, couchbase_config, cb_env):
        cb_env.create_rate_limit_user(cb_env.USERNAME,
                                      {'kv_limits': {'num_connections': 10,
                                                     'num_ops_per_min': 10,
                                                     'ingress_mib_per_min': 1,
                                                     'egress_mib_per_min': 10
                                                     }
                                       })
        conn_string = couchbase_config.get_connection_string()
        cluster = None
        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(cb_env.USERNAME, 'password')))
            bucket = cluster.bucket('default')
            collection = bucket.default_collection()

            cb_env.try_until_timeout(5, 10, collection.upsert, 'ratelimit', "test")
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.usefixtures('cleanup_scope_and_collection')
    def test_rate_limits_collections_scopes_limits(self, cb_env):
        scope_name = cb_env.RATE_LIMIT_SCOPE_NAME
        cb_env.create_rate_limit_scope(scope_name, {'cluster_mgr_limits': {'num_collections': 1}})

        collection_spec = CollectionSpec('rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        TestEnvironment.try_n_times_till_exception(5,
                                                   3,
                                                   cb_env.cm.create_collection,
                                                   collection_spec,
                                                   expected_exceptions=(CollectionAlreadyExistsException,))

        with pytest.raises(QuotaLimitedException):
            collection_spec = CollectionSpec('rate-limit-collection-1', scope_name=scope_name)
            cb_env.cm.create_collection(collection_spec)

    @pytest.mark.usefixtures('remove_docs')
    def test_rate_limits_egress(self, couchbase_config, cb_env):
        cb_env.create_rate_limit_user(cb_env.USERNAME, {'kv_limits': {'num_connections': 10,
                                                                      'num_ops_per_min': 100,
                                                                      'ingress_mib_per_min': 10,
                                                                      'egress_mib_per_min': 2}
                                                        })
        conn_string = couchbase_config.get_connection_string()
        cluster = None
        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(cb_env.USERNAME, 'password')))
            bucket = cluster.bucket('default')
            collection = bucket.default_collection()

            doc = cb_env.random_doc_by_size(1024*512)
            key = 'ratelimit-egress'
            collection.upsert(key, doc)
            for _ in range(3):
                collection.get(key, GetOptions(timeout=timedelta(seconds=10)))
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    def test_rate_limits_fts(self, couchbase_config, cb_env):
        cb_env.create_rate_limit_user(cb_env.USERNAME, {
            'fts_limits': {
                'num_queries_per_min': 1,
                'num_concurrent_requests': 10,
                'ingress_mib_per_min': 10,
                'egress_mib_per_min': 10
            }
        })

        conn_string = couchbase_config.get_connection_string()
        sixm = cb_env.cluster.search_indexes()
        sixm.upsert_index(SearchIndex(name='ratelimit-idx', source_name='default'))
        if not cb_env.rate_limit_params.fts_indexes:
            cb_env.rate_limit_params.fts_indexes = []
        cb_env.rate_limit_params.fts_indexes.append('ratelimit-idx')

        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(cb_env.USERNAME, 'password')))

            cb_env.try_until_timeout(5,
                                     50,
                                     cluster.search_query,
                                     'ratelimit-idx',
                                     TermQuery('auto'),
                                     SearchOptions(limit=1),
                                     fts=True)

        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')
        finally:
            sixm.drop_index('ratelimit-idx')

    @pytest.mark.usefixtures('cleanup_scope_and_collection')  # noqa: C901
    def test_rate_limits_fts_scopes(self, cb_env):  # noqa: C901
        scope_name = cb_env.RATE_LIMIT_SCOPE_NAME
        cb_env.create_rate_limit_scope(scope_name, {'fts_limits': {'num_fts_indexes': 1}})

        collection_spec = CollectionSpec('rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        TestEnvironment.try_n_times_till_exception(5,
                                                   3,
                                                   cb_env.cm.create_collection,
                                                   collection_spec,
                                                   expected_exceptions=(CollectionAlreadyExistsException,))

        # see beer-search-coll-index-params.json for ref
        idx_name = "{}.{}".format(scope_name, collection_spec.name)
        idx_params = {
            'doc_config': {
                'mode': 'scope.collection.type_field',
                'type_field': 'type'
            },
            'mapping': {
                'default_analyzer': 'standard',
                'default_datetime_parser': 'dateTimeOptional',
                'default_field': '_all',
                'default_mapping': {
                    'dynamic': True,
                    'enabled': False
                },
                'default_type': '_default',
                'docvalues_dynamic': True,
                'index_dynamic': True,
                'store_dynamic': False,
                'type_field': '_type',
                'types': {
                    idx_name: {
                        'dynamic': False,
                        'enabled': True
                    }
                }
            }
        }

        ixm = cb_env.cluster.search_indexes()
        if not cb_env.rate_limit_params.fts_indexes:
            cb_env.rate_limit_params.fts_indexes = []
        with pytest.raises(QuotaLimitedException):
            # random helps to avoid "Index already exist" failure
            new_idx = SearchIndex(name='rate-limit-idx-{}'.format(random.randrange(0, 50)),
                                  idx_type='fulltext-index',
                                  source_name='default',
                                  source_type='couchbase',
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
            new_idx = SearchIndex(name='rate-limit-idx-{}'.format(random.randrange(51, 100)),
                                  idx_type='fulltext-index',
                                  source_name='default',
                                  source_type='couchbase',
                                  params=json.loads(json.dumps(idx_params)))
            cb_env.rate_limit_params.fts_indexes.append(new_idx.name)
            ixm.upsert_index(new_idx)

    @pytest.mark.usefixtures('cleanup_scope_and_collection')  # noqa: C901
    def test_rate_limits_index_scopes(self, cb_env):  # noqa: C901
        scope_name = cb_env.RATE_LIMIT_SCOPE_NAME
        cb_env.create_rate_limit_scope(scope_name, {'index_limits': {'num_indexes': 1}})

        collection_spec = CollectionSpec('rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        TestEnvironment.try_n_times_till_exception(5,
                                                   3,
                                                   cb_env.cm.create_collection,
                                                   collection_spec,
                                                   expected_exceptions=(CollectionAlreadyExistsException,))

        # make sure query service sees the new keyspace
        # drop the index and then re-create
        ixm = cb_env.cluster.query_indexes()

        def create_primary_index():
            try:
                ixm.create_primary_index('default', scope_name=scope_name, collection_name=collection_spec.name)
                indexes = ixm.get_all_indexes('default', scope_name=scope_name, collection_name=collection_spec.name)
                if len(indexes) == 0:
                    return False
            except CouchbaseException:
                return False

            return True

        count = 1
        while not create_primary_index():
            if count == 5:
                raise pytest.skip('Unable to create primary index.')

            count += 1
            indexes = ixm.get_all_indexes('default', scope_name=scope_name, collection_name=collection_spec.name)
            TestEnvironment.sleep(1)
            if len(indexes) > 0:
                break

        TestEnvironment.try_n_times(10,
                                    3,
                                    ixm.drop_primary_index,
                                    'default',
                                    scope_name=scope_name,
                                    collection_name=collection_spec.name)

        scope = cb_env.bucket.scope(scope_name)

        with pytest.raises(QuotaLimitedException):
            TestEnvironment.try_n_times(10,
                                        3,
                                        ixm.create_primary_index,
                                        'default',
                                        scope_name=scope_name,
                                        collection_name=collection_spec.name)

            indexes = ixm.get_all_indexes('default', scope_name=scope_name, collection_name=collection_spec.name)
            assert len(indexes) >= 1
            assert indexes[0].is_primary is True
            assert '#primary' == indexes[0].name
            assert collection_spec.name == indexes[0].collection_name
            # helps to avoid "Index already exist" failure
            idx_name = 'rate-limit-idx-{}'.format(random.randrange(0, 100))
            scope.query("CREATE INDEX `{}` ON `{}`(testField)".format(idx_name, collection_spec.name)).execute()

    @pytest.mark.usefixtures('remove_docs')
    def test_rate_limits_ingress(self, couchbase_config, cb_env):
        cb_env.create_rate_limit_user(cb_env.USERNAME, {
            'kv_limits': {
                'num_connections': 10,
                'num_ops_per_min': 100,
                'ingress_mib_per_min': 1,
                'egress_mib_per_min': 10
            }
        })
        conn_string = couchbase_config.get_connection_string()
        cluster = None
        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(cb_env.USERNAME, 'password')))
            bucket = cluster.bucket('default')
            collection = bucket.default_collection()

            doc = cb_env.random_doc_by_size(1024*512)
            for _ in range(3):
                collection.upsert('ratelimit-ingress', doc)

        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    @pytest.mark.usefixtures('cleanup_scope_and_collection')
    def test_rate_limits_kv_scopes_data_size(self, cb_env):
        scope_name = cb_env.RATE_LIMIT_SCOPE_NAME
        cb_env.create_rate_limit_scope(scope_name, {'kv_limits': {'data_size': 1024*1024}})

        collection_spec = CollectionSpec('rate-limit-collection', scope_name=scope_name)
        cb_env.cm.create_collection(collection_spec)

        # verify collection exists
        TestEnvironment.try_n_times_till_exception(5,
                                                   3,
                                                   cb_env.cm.create_collection,
                                                   collection_spec,
                                                   expected_exceptions=(CollectionAlreadyExistsException,))

        scope = cb_env.bucket.scope(scope_name)
        collection = scope.collection(collection_spec.name)

        doc = cb_env.random_doc_by_size(1024*512)
        with pytest.raises(QuotaLimitedException):
            for _ in range(5):
                collection.upsert('ratelimit-datasize', doc)

    def test_rate_limits_max_conns(self, couchbase_config, cb_env):
        cb_env.create_rate_limit_user(cb_env.USERNAME, {
            'kv_limits': {
                'num_connections': 1,
                'num_ops_per_min': 100,
                'ingress_mib_per_min': 10,
                'egress_mib_per_min': 10
            }
        })
        cluster = None
        cluster1 = None
        conn_string = couchbase_config.get_connection_string()
        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(cb_env.USERNAME, 'password')))
            bucket = cluster.bucket('default')
            collection = bucket.default_collection()
            collection.exists('some-key')

            cluster1 = Cluster(conn_string,
                               ClusterOptions(PasswordAuthenticator(cb_env.USERNAME, 'password')))
            bucket1 = cluster1.bucket('default')
            collection1 = bucket1.default_collection()
            collection1.exists('some-key')
        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')

    def test_rate_limits_query(self, couchbase_config, cb_env):
        cb_env.create_rate_limit_user(cb_env.USERNAME, {
            'query_limits': {
                'num_queries_per_min': 1,
                'num_concurrent_requests': 10,
                'ingress_mib_per_min': 10,
                'egress_mib_per_min': 10
            }
        })

        conn_string = couchbase_config.get_connection_string()
        cluster = None
        qm = None
        try:
            cluster = Cluster.connect(conn_string,
                                      ClusterOptions(PasswordAuthenticator(cb_env.USERNAME, 'password')))

            qm = cluster.query_indexes()
            qm.create_primary_index('default', ignore_if_exists=True)
            cb_env.try_until_timeout(5, 50, cluster.query, "SELECT 'Hi there!'", query=True)

        except RateLimitedException:
            pass
        except Exception:
            pytest.fail('Expected RateLimitedException')


@pytest.mark.flaky(reruns=5, reruns_delay=1)
class ClassicRateLimitTests(RateLimitTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicRateLimitTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicRateLimitTests) if valid_test_method(meth)]
        compare = set(RateLimitTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = RateLimitTestEnvironment.from_environment(cb_base_env)
        cb_env.setup()
        yield cb_env
        cb_env.teardown()
