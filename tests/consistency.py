from datetime import datetime, timedelta
from time import sleep
from typing import Iterable

import requests

from tests.couchbase_config import CouchbaseConfig


class ConsistencyChecker:
    RETRY_DELAY_SECS = 1
    DEFAULT_TIMEOUT = timedelta(seconds=10)

    def __init__(self, **kwargs):
        self._hostnames: Iterable[str] = kwargs.get('hostnames', [])
        self._config: CouchbaseConfig = kwargs.get('config')

    def fetch_hostnames(self):
        path = f'http://{self._config.host}:{self._config.port}/pools/nodes'
        resp = requests.get(path, auth=self._config.get_username_and_pw())
        self._hostnames = [node['configuredHostname'] for node in resp.json()['nodes']]

    def wait_until_user_present(self, name, domain='local', timeout=DEFAULT_TIMEOUT):
        path = f'settings/rbac/users/{domain}/{name}'
        error_msg = f'User {name} in the {domain} domain is not present in all nodes'
        self._wait_until_resource_present(path, error_msg, timeout)

    def wait_until_user_dropped(self, name, domain='local', timeout=DEFAULT_TIMEOUT):
        path = f'settings/rbac/users/{domain}/{name}'
        error_msg = f'User {name} in the {domain} domain has not been dropped from all nodes'
        self._wait_until_resource_dropped(path, error_msg, timeout)

    def wait_until_group_present(self, name, timeout=DEFAULT_TIMEOUT):
        path = f'settings/rbac/groups/{name}'
        error_msg = f'Group {name} is not present in all nodes'
        self._wait_until_resource_present(path, error_msg, timeout)

    def wait_until_group_dropped(self, name, timeout=DEFAULT_TIMEOUT):
        path = f'settings/rbac/groups/{name}'
        error_msg = f'Group {name} has not been dropped from all nodes'
        self._wait_until_resource_dropped(path, error_msg, timeout)

    def wait_until_bucket_present(self, name, timeout=DEFAULT_TIMEOUT):
        path = f'pools/default/buckets/{name}'
        error_msg = f'Bucket {name} is not present in all nodes'
        self._wait_until_resource_present(path, error_msg, timeout)

    def wait_until_bucket_dropped(self, name, timeout=DEFAULT_TIMEOUT):
        path = f'pools/default/buckets/{name}'
        error_msg = f'Bucket {name} has not been dropped from all nodes'
        self._wait_until_resource_dropped(path, error_msg, timeout)

    def wait_until_scope_present(self, bucket_name, scope_name, timeout=DEFAULT_TIMEOUT):
        def predicate(resp):
            return any(scope['name'] == scope_name for scope in resp['scopes'])

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = f'Scope {scope_name} in bucket {bucket_name} is not present in all nodes'
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_scope_dropped(self, bucket_name, scope_name, timeout=DEFAULT_TIMEOUT):
        def predicate(resp):
            return all(scope['name'] != scope_name for scope in resp['scopes'])

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = f'Scope {scope_name} in bucket {bucket_name} has not been dropped in all nodes'
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_collection_present(self, bucket_name, scope_name, collection_name, timeout=DEFAULT_TIMEOUT):
        def predicate(resp):
            return any(
                scope['name'] == scope_name and any(coll['name'] == collection_name for coll in scope['collections'])
                for scope in resp['scopes']
            )

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} is not present in all ' \
                    f'nodes'
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_collection_dropped(self, bucket_name, scope_name, collection_name, timeout=DEFAULT_TIMEOUT):
        def predicate(resp):
            return all(
                scope['name'] != scope_name or all(coll['name'] != collection_name for coll in scope['collections'])
                for scope in resp['scopes']
            )

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} has not been dropped ' \
                    f'in all nodes'
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def _wait_until_resource_present(self, path, error_msg, timeout=DEFAULT_TIMEOUT):
        deadline = datetime.now() + timeout
        while datetime.now() < deadline:
            if self._resource_is_present(path):
                return
            else:
                sleep(self.RETRY_DELAY_SECS)
        raise RuntimeError(error_msg)

    def _wait_until_resource_dropped(self, path, error_msg, timeout=DEFAULT_TIMEOUT):
        deadline = datetime.now() + timeout
        while datetime.now() < deadline:
            if self._resource_is_dropped(path):
                return
            else:
                sleep(self.RETRY_DELAY_SECS)
        raise RuntimeError(error_msg)

    def _wait_until_resource_satisfies_predicate(self, path, predicate, error_msg, timeout=DEFAULT_TIMEOUT):
        deadline = datetime.now() + timeout
        while datetime.now() < deadline:
            if self._resource_satisfies_predicate(path, predicate):
                return
            else:
                sleep(self.RETRY_DELAY_SECS)
        raise RuntimeError(error_msg)

    def _resource_is_present(self, path):
        for hostname in self._hostnames:
            url = f'http://{hostname}/{path}'
            resp = requests.get(url, auth=self._config.get_username_and_pw())
            if resp.status_code != 200:
                return False
        return True

    def _resource_is_dropped(self, path):
        for hostname in self._hostnames:
            url = f'http://{hostname}/{path}'
            resp = requests.get(url, auth=self._config.get_username_and_pw())
            if resp.status_code != 404:
                return False
        return True

    def _resource_satisfies_predicate(self, path, predicate):
        for hostname in self._hostnames:
            url = f'http://{hostname}/{path}'
            resp = requests.get(url, auth=self._config.get_username_and_pw())
            if resp.status_code != 200 or not predicate(resp.json()):
                return False
        return True

    @classmethod
    def from_test_environment(cls, env):
        checker = cls(config=env.config)
        if not (env.config.is_protostellar or env.config.is_mock_server):
            checker.fetch_hostnames()
        return checker
