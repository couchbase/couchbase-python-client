#  Copyright 2016-2023. Couchbase, Inc.
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


from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlparse

import requests

from couchbase.exceptions import (CouchbaseException,
                                  DocumentNotFoundException,
                                  ScopeNotFoundException)
from couchbase.management.options import GetUserOptions
from tests.environments import CollectionType, CouchbaseTestEnvironmentException
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


@dataclass
class RateLimitData:
    url: str = None
    username: str = None
    pw: str = None
    fts_indexes: List[str] = None


class RateLimitTestEnvironment(TestEnvironment):
    CONTENT = {'some': 'content'}
    KEY = 'imakey'
    NOKEY = 'somerandomkey'
    USERNAME = 'rate-limit-user'
    RATE_LIMIT_SCOPE_NAME = 'rate-limit-scope'

    # def __init__(self,
    #              **kwargs # type: Dict[str, Any]
    #              ):
    #     self._tracer = kwargs.pop('tracer', None)
    #     self._meter = kwargs.pop('meter', None)
    #     super().__init__(**kwargs)

    @property
    def rate_limit_params(self) -> Optional[RateLimitData]:
        """Returns the rate limit testing data"""
        return self._rate_limit_params if hasattr(self, '_rate_limit_params') else None

    def create_rate_limit_scope(self, scope_name, limits):
        params = {
            'name': scope_name
        }

        scope_limits = {}
        kv_limits = limits.get('kv_limits', None)
        if kv_limits:
            scope_limits['kv'] = {
                'data_size': kv_limits['data_size']
            }

        index_limits = limits.get('index_limits', None)
        if index_limits:
            scope_limits['index'] = {
                'num_indexes': index_limits['num_indexes']
            }

        fts_limits = limits.get('fts_limits', None)
        if fts_limits:
            scope_limits['fts'] = {
                'num_fts_indexes': fts_limits['num_fts_indexes']
            }

        cluster_mgr_limits = limits.get('cluster_mgr_limits', None)
        if cluster_mgr_limits:
            scope_limits['clusterManager'] = {
                'num_collections': cluster_mgr_limits['num_collections']
            }

        if scope_limits:
            params['limits'] = json.dumps(scope_limits)

        path = f'/pools/default/buckets/{self.bucket.name}/scopes'
        url = f'{self.rate_limit_params.url}/{path}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        requests.post(url,
                      headers=headers,
                      data=params,
                      auth=(self.rate_limit_params.username,
                            self.rate_limit_params.pw))
        # verify the scope exists
        all_scopes = self.cm.get_all_scopes()
        assert scope_name in map(lambda s: s.name, all_scopes)

    def create_rate_limit_user(self, username, limits):
        params = {
            'password': 'password',
            'roles': 'admin'
        }

        user_limits = {}
        kv_limits = limits.get('kv_limits', None)
        if kv_limits:
            user_limits['kv'] = {
                'num_connections': kv_limits['num_connections'],
                'num_ops_per_min': kv_limits['num_ops_per_min'],
                'ingress_mib_per_min': kv_limits['ingress_mib_per_min'],
                'egress_mib_per_min': kv_limits['egress_mib_per_min']
            }

        query_limits = limits.get('query_limits', None)
        if query_limits:
            user_limits['query'] = {
                'num_queries_per_min': query_limits['num_queries_per_min'],
                'num_concurrent_requests': query_limits['num_concurrent_requests'],
                'ingress_mib_per_min': query_limits['ingress_mib_per_min'],
                'egress_mib_per_min': query_limits['egress_mib_per_min']
            }

        fts_limits = limits.get('fts_limits', None)
        if fts_limits:
            user_limits['fts'] = {
                'num_queries_per_min': fts_limits['num_queries_per_min'],
                'num_concurrent_requests': fts_limits['num_concurrent_requests'],
                'ingress_mib_per_min': fts_limits['ingress_mib_per_min'],
                'egress_mib_per_min': fts_limits['egress_mib_per_min']
            }

        if user_limits:
            params['limits'] = json.dumps(user_limits)

        path = f'/settings/rbac/users/local/{username}'

        url = f'{self.rate_limit_params.url}/{path}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.put(url,
                         headers=headers,
                         data=params,
                         auth=(self.rate_limit_params.username,
                               self.rate_limit_params.pw))

        if r.status_code != 200:
            raise CouchbaseTestEnvironmentException('Unable to create rate-limit-user.')

        # lets verify user exists
        user_metadata = TestEnvironment.try_n_times(10,
                                                    1,
                                                    self.um.get_user,
                                                    username,
                                                    GetUserOptions(domain_name='local'))

        assert user_metadata is not None
        assert username == user_metadata.user.username

    def disable_rate_limiting(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('rate_limiting',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if hasattr(self, '_rate_limit_params'):
            del self._rate_limit_params
        return self

    def drop_rate_limit_user(self):
        TestEnvironment.try_n_times_till_exception(10, 3, self.um.drop_user, self.USERNAME)

    def drop_scope(self):
        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   self.cm.drop_scope,
                                                   self.RATE_LIMIT_SCOPE_NAME,
                                                   expected_exceptions=(ScopeNotFoundException,))

    def enable_rate_limiting(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('rate_limiting',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        parsed_conn = urlparse(self._config.get_connection_string())
        url = f'http://{parsed_conn.netloc}:8091'
        u, p = self.config.get_username_and_pw()
        self._rate_limit_params = RateLimitData(url, u, p)
        return self

    def enforce_rate_limits(self, enforce=True):
        url = f'{self.rate_limit_params.url}/internalSettings'
        payload = {'enforceLimits': f'{"true" if enforce is True else "false"}'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.post(url,
                          headers=headers,
                          data=payload,
                          auth=(self.rate_limit_params.username,
                                self.rate_limit_params.pw))

        if r.status_code != 200:
            raise CouchbaseTestEnvironmentException('Unable to enforce rate limits.')

    def random_doc_by_size(self, size):
        doc = bytearray((random.getrandbits(8) for i in range(size)))
        return doc.hex()

    def remove_docs(self):
        try:
            self.collection.remove('ratelimit-ingress')
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.collection.get,
                                                       'ratelimit-ingress',
                                                       (DocumentNotFoundException,))
        except CouchbaseException:
            pass

        try:
            self.collection.remove('ratelimit-egress')
            TestEnvironment.try_n_times_till_exception(10,
                                                       3,
                                                       self.collection.get,
                                                       'ratelimit-egress',
                                                       (DocumentNotFoundException,))
        except CouchbaseException:
            pass

    def setup(self,
              collection_type=None,  # type: Optional[CollectionType]
              num_docs=None,  # type: Optional[int]
              ):
        self.enable_rate_limiting().enable_bucket_mgmt().enable_collection_mgmt().enable_user_mgmt()
        self._fts_indexes = []
        self.enforce_rate_limits(True)
        TestEnvironment.try_n_times(5, 3, self.load_data)

    def teardown(self):
        self.enforce_rate_limits(False)
        self.disable_rate_limiting().disable_bucket_mgmt().disable_collection_mgmt().disable_user_mgmt()
        TestEnvironment.try_n_times(5, 3, self.purge_data)

    def try_until_timeout(self, timeout, interval, func, *args, **kwargs):
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
        is_query = kwargs.pop('query', False)
        is_fts = kwargs.pop('fts', False)

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

    @classmethod
    def from_environment(cls,
                         env,  # type: TestEnvironment
                         ) -> RateLimitTestEnvironment:

        base_env_args = {
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        # we have to create a new environment b/c we need a new cluster in order to set the tracer
        cb_env = TestEnvironment.get_environment(**base_env_args)
        env_args = {
            'bucket': cb_env.bucket,
            'cluster': cb_env.cluster,
            'default_collection': cb_env.default_collection,
            'couchbase_config': cb_env.config,
            'data_provider': cb_env.data_provider,
        }

        return cls(**env_args)
