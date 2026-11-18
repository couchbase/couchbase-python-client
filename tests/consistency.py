#  Copyright 2016-2026. Couchbase, Inc.
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

from collections.abc import Callable, Iterable
from datetime import datetime, timedelta
from time import sleep
from typing import (TYPE_CHECKING,
                    Any,
                    NoReturn)

import requests

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import (BucketDoesNotExistException,
                                  CouchbaseException,
                                  DocumentNotFoundException)
from couchbase.options import ClusterOptions
from tests.couchbase_config import CouchbaseConfig
from tests.skips import CouchbaseTestEnvironmentException, env_degraded
from tests.test_features import TestProtocol

if TYPE_CHECKING:
    from couchbase.management.collections import CollectionSpec, ScopeSpec
    from tests.environments.test_environment import TestEnvironment


class ConsistencyChecker:
    RETRY_DELAY_SECS = 1
    DEFAULT_TIMEOUT = timedelta(seconds=10)

    # A magma bucket takes 1024MB and warms up far slower than a 100MB couchstore one, and it
    # is slower still while another worker is creating or dropping buckets.
    BUCKET_WARMUP_TIMEOUT = timedelta(seconds=60)

    # Per-request ceiling for every management GET below.  `requests` has no default
    # timeout, so without this a cluster that accepts the connection and then stops
    # answering hangs the run rather than failing it: the wait loops check their deadline
    # only between requests, so a single hung GET outlives any deadline they set.  Five
    # seconds is generous for a management endpoint and stays under DEFAULT_TIMEOUT, so a
    # wait that does blow its budget overruns by one request rather than by several.
    HTTP_TIMEOUT_SECS = 5

    # The cluster manager can answer one request with a 500 and the next with a 200, and every
    # environment build runs these fetchers, so a single bad answer fails the whole class.  A
    # 5xx or a dropped connection is asked again; a 4xx is not, since bad credentials or a
    # wrong path will not clear.
    HTTP_ATTEMPTS = 3

    # What the cluster calls each service in `/pools/default`, mapped to the SDK's public
    # vocabulary.  Only the codes that correspond to a ServiceType member are listed: `index`
    # is the GSI service, which has no member (see FEATURE_SERVICES), and `backup` likewise.
    # An unrecognised code is ignored rather than rejected, so a service added by a future
    # server release cannot break the probe.
    SERVICE_CODES: dict[str, ServiceType] = {
        'kv': ServiceType.KeyValue,
        'n1ql': ServiceType.Query,
        'cbas': ServiceType.Analytics,
        'fts': ServiceType.Search,
        'eventing': ServiceType.Eventing,
    }

    def __init__(self, config: CouchbaseConfig, hostnames: Iterable[str] | None = None) -> None:
        self._config = config
        self._hostnames: Iterable[str] = hostnames if hostnames is not None else []

    def _get_json(self, path: str, *, stage: str) -> dict[str, Any]:
        """GET a management endpoint, or fail as a degraded environment.

        The three fetchers below run before any test does, and each one's answer decides
        something the whole session depends on: which hosts to poll, whether cluster labels
        work, which services can be tested.  A bare `resp.json()[key]` reports every way
        those can fail as a KeyError or a JSONDecodeError naming neither the call nor the
        URL, which is a poor first thing to see when a cluster is misconfigured.

        Every failure lands as `ENV[env-degraded]` carrying the stage and the URL: an
        unreachable host, a refused connection, a 401 from wrong credentials, a proxy
        returning HTML.  None of them is a skip, because none of them says anything about
        what the cluster supports.
        """
        for attempt in range(1, self.HTTP_ATTEMPTS + 1):
            try:
                resp = requests.get(path,
                                    auth=self._config.get_username_and_pw(),
                                    timeout=self.HTTP_TIMEOUT_SECS)
            except requests.RequestException as ex:
                if attempt < self.HTTP_ATTEMPTS:
                    sleep(self.RETRY_DELAY_SECS)
                    continue
                env_degraded(stage=stage,
                             url=path,
                             error=type(ex).__name__,
                             attempts=self.HTTP_ATTEMPTS,
                             note=f'management endpoint unreachable: {ex}')
            if resp.status_code < 500 or attempt == self.HTTP_ATTEMPTS:
                break
            sleep(self.RETRY_DELAY_SECS)
        if resp.status_code != 200:
            env_degraded(stage=stage,
                         url=path,
                         status=resp.status_code,
                         note='management endpoint did not return 200')
        try:
            return resp.json()
        except ValueError:
            env_degraded(stage=stage,
                         url=path,
                         note='management endpoint returned a non-JSON body')

    def fetch_hostnames(self) -> None:
        """Work out an address to poll each node at, and prove every one of them answers.

        `hostname` is the address the cluster answers on; `configuredHostname` is only what a
        node was told to call itself at initialisation, which on a single node set up without
        one is `127.0.0.1:8091` and unusable from anywhere else.

        Probing here, once, rather than leaving a bad address to surface in whichever
        consistency wait a suite happens to make first: that failure is a ten-second timeout
        reporting "not present in all nodes", which blames slow propagation for what is really
        an unroutable host.

        Neither field covers a runner that reaches the cluster by a different route than the
        nodes use for each other (a port mapping, NAT, a load balancer).  The server's answer
        for that is `alternateAddresses`, read from this same `/pools/nodes` entry and belonging
        in the precedence directly above `hostname`; the probe below is what will report the
        need.
        """
        path = f'http://{self._config.host}:{self._config.port}/pools/nodes'
        nodes = self._get_json(path, stage='fetch-hostnames').get('nodes')
        if not nodes:
            env_degraded(stage='fetch-hostnames',
                         url=path,
                         note='/pools/nodes returned no nodes; there is nothing to poll for consistency')
        self._hostnames = [node['hostname'] for node in nodes]
        for hostname in self._hostnames:
            self._get_json(f'http://{hostname}/pools/default', stage='probe-hostnames')

    def fetch_cluster_name(self) -> str:
        path = f'http://{self._config.host}:{self._config.port}/pools/default'
        name = self._get_json(path, stage='fetch-cluster-name').get('clusterName')
        if name is None:
            env_degraded(stage='fetch-cluster-name',
                         url=path,
                         note='/pools/default carried no clusterName')
        return name

    def fetch_services(self) -> frozenset[ServiceType]:
        """Which services this cluster actually runs, as the union across its nodes.

        Read from the cluster's own configuration rather than inferred from a ping.  A ping
        reports the ports a node advertises, which is not the same claim: GoCAVES advertises
        query, search and analytics ports and implements none of them, so a ping-based probe
        calls a mock fully featured.  `/pools/default` lists what each node was actually
        asked to run, which is the question the service gate is asking.

        The union across nodes is the right reduction because the SDK routes an operation to
        whichever node runs its service: on a four-node cluster with analytics on one node,
        analytics is available to every client.  A per-node view would be the wrong shape for
        anything a test can observe.

        Management is added unconditionally: every node serves the management API, and it is
        the very endpoint this request was answered by.  No node advertises it as a service.

        Not called for the mock; see the caller.  A failure here is a broken environment
        rather than an absent service, and propagates as one instead of being read as an
        empty service list, which would skip every non-kv suite in the run and look clean.
        """
        path = f'http://{self._config.host}:{self._config.port}/pools/default'
        nodes = self._get_json(path, stage='service-probe').get('nodes')
        if not nodes:
            env_degraded(stage='service-probe',
                         url=path,
                         note='/pools/default returned no nodes; cannot tell which services are deployed')
        found = {ServiceType.Management}
        for node in nodes:
            for code in node.get('services') or []:
                service = self.SERVICE_CODES.get(code)
                if service is not None:
                    found.add(service)
        return frozenset(found)

    def wait_until_user_present(self, name: str, domain: str = 'local', timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        path = f'settings/rbac/users/{domain}/{name}'
        error_msg = f'User {name} in the {domain} domain is not present in all nodes'
        self._wait_until_resource_present(path, error_msg, timeout)

    def wait_until_user_dropped(self, name: str, domain: str = 'local', timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        path = f'settings/rbac/users/{domain}/{name}'
        error_msg = f'User {name} in the {domain} domain has not been dropped from all nodes'
        self._wait_until_resource_dropped(path, error_msg, timeout)

    def wait_until_group_present(self, name: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        path = f'settings/rbac/groups/{name}'
        error_msg = f'Group {name} is not present in all nodes'
        self._wait_until_resource_present(path, error_msg, timeout)

    def wait_until_group_dropped(self, name: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        path = f'settings/rbac/groups/{name}'
        error_msg = f'Group {name} has not been dropped from all nodes'
        self._wait_until_resource_dropped(path, error_msg, timeout)

    def wait_until_bucket_present(self, name: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        path = f'pools/default/buckets/{name}'
        error_msg = f'Bucket {name} is not present in all nodes'
        self._wait_until_resource_present(path, error_msg, timeout)

    def wait_until_bucket_healthy(self, name: str, timeout: timedelta = BUCKET_WARMUP_TIMEOUT) -> None:
        """Wait until every node reports the bucket healthy, not merely configured.

        A bucket is present in the config, and `get_bucket` succeeds, before its data service
        is warm.  Opening it in that window times out in `open_bucket`, so a create that is
        followed by any KV or collection work waits for this instead of for presence.
        """
        def predicate(resp: dict) -> bool:
            nodes = resp.get('nodes') or []
            return bool(nodes) and all(node.get('status') == 'healthy' for node in nodes)

        path = f'pools/default/buckets/{name}'
        error_msg = f'Bucket {name} is not healthy on all nodes'
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_bucket_dropped(self, name: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        path = f'pools/default/buckets/{name}'
        error_msg = f'Bucket {name} has not been dropped from all nodes'
        self._wait_until_resource_dropped(path, error_msg, timeout)

    def wait_until_scope_present(self, bucket_name: str, scope_name: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        def predicate(resp: dict) -> bool:
            return any(scope['name'] == scope_name for scope in resp['scopes'])

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = f'Scope {scope_name} in bucket {bucket_name} is not present in all nodes'
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_scope_dropped(self, bucket_name: str, scope_name: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        def predicate(resp: dict) -> bool:
            return all(scope['name'] != scope_name for scope in resp['scopes'])

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = f'Scope {scope_name} in bucket {bucket_name} has not been dropped in all nodes'
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_collection_present(self, bucket_name: str, scope_name: str, collection_name: str,
                                      timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        def predicate(resp: dict) -> bool:
            return any(
                scope['name'] == scope_name and any(coll['name'] == collection_name for coll in scope['collections'])
                for scope in resp['scopes']
            )

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = (
            f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} is not present in all nodes'
        )
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_collection_dropped(self, bucket_name: str, scope_name: str, collection_name: str,
                                      timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        def predicate(resp: dict) -> bool:
            return all(
                scope['name'] != scope_name or all(coll['name'] != collection_name for coll in scope['collections'])
                for scope in resp['scopes']
            )

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = (
            f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} '
            f'has not been dropped in all nodes'
        )
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    def wait_until_collection_has_settings(self, bucket_name: str, scope_name: str, collection_name: str,
                                           settings: dict, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        def satisfies_settings(coll: dict) -> bool:
            return all(coll[k] == v for k, v in settings.items())

        def predicate(resp: dict) -> bool:
            return any(
                scope['name'] == scope_name
                and any(coll['name'] == collection_name and satisfies_settings(coll) for coll in scope['collections'])
                for scope in resp['scopes']
            )

        path = f'pools/default/buckets/{bucket_name}/scopes'
        error_msg = (
            f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} '
            f'does not have the expected settings in all nodes ({settings})'
        )
        self._wait_until_resource_satisfies_predicate(path, predicate, error_msg, timeout)

    # These raise through env_degraded rather than a bare RuntimeError.  A cluster
    # resource that never propagates to every node is the definition of env-degraded, and a
    # RuntimeError carries neither the category nor anything a CI run can parse.  The prose
    # the callers build becomes the note; the deadline and the polled path become fields, so
    # a timeout says how long it actually waited rather than leaving that to be inferred
    # from the source.

    def _wait_until_resource_present(self, path: str, error_msg: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._resource_is_present(path),
                         error_msg,
                         timeout,
                         condition='resource-present',
                         path=path)

    def _wait_until_resource_dropped(self, path: str, error_msg: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._resource_is_dropped(path),
                         error_msg,
                         timeout,
                         condition='resource-dropped',
                         path=path)

    def _wait_until_resource_satisfies_predicate(self, path: str, predicate: Callable[[dict], bool],
                                                 error_msg: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._resource_satisfies_predicate(path, predicate),
                         error_msg,
                         timeout,
                         condition='resource-satisfies-predicate',
                         path=path)

    def _wait_until(self, check: Callable[[], bool], error_msg: str, timeout: timedelta, **fields: Any) -> None:
        deadline = datetime.now() + timeout
        while datetime.now() < deadline:
            if check():
                return
            sleep(self.RETRY_DELAY_SECS)
        env_degraded(stage='consistency-wait',
                     timeout_s=timeout.total_seconds(),
                     note=error_msg,
                     **fields)

    def _resource_is_present(self, path: str) -> bool:
        for hostname in self._hostnames:
            url = f'http://{hostname}/{path}'
            try:
                resp = requests.get(url,
                                    auth=self._config.get_username_and_pw(),
                                    timeout=self.HTTP_TIMEOUT_SECS)
            except requests.RequestException:
                # Not yet, rather than degraded: these run inside a retry loop whose own
                # deadline decides when to give up, and a node that blips mid-wait is the
                # case the loop exists for.  The timeout is what keeps "not yet" bounded.
                return False
            if resp.status_code != 200:
                return False
        return True

    def _resource_is_dropped(self, path: str) -> bool:
        for hostname in self._hostnames:
            url = f'http://{hostname}/{path}'
            try:
                resp = requests.get(url,
                                    auth=self._config.get_username_and_pw(),
                                    timeout=self.HTTP_TIMEOUT_SECS)
            except requests.RequestException:
                # Not yet, rather than degraded: these run inside a retry loop whose own
                # deadline decides when to give up, and a node that blips mid-wait is the
                # case the loop exists for.  The timeout is what keeps "not yet" bounded.
                return False
            if resp.status_code != 404:
                return False
        return True

    def _resource_satisfies_predicate(self, path: str, predicate: Callable[[dict], bool]) -> bool:
        for hostname in self._hostnames:
            url = f'http://{hostname}/{path}'
            try:
                resp = requests.get(url,
                                    auth=self._config.get_username_and_pw(),
                                    timeout=self.HTTP_TIMEOUT_SECS)
            except requests.RequestException:
                return False
            if resp.status_code != 200 or not predicate(resp.json()):
                return False
        return True

    @classmethod
    def from_test_environment(cls, env: TestEnvironment) -> ConsistencyChecker:
        if env.config.protocol is TestProtocol.COUCHBASE2:
            return GatewayConsistencyChecker(config=env.config)
        # The mock serves no management API, so its checker polls no hosts and every wait
        # passes at once.
        checker = cls(config=env.config)
        if env.config.has_management_api:
            checker.fetch_hostnames()
        return checker


class GatewayConsistencyChecker(ConsistencyChecker):
    """The waits for a couchbase2 run, asked of the gateway the run connects through.

    The cluster behind the gateway is not polled directly, for the reason
    CouchbaseConfig.has_management_api gives.  Each wait asks its question of the SDK instead,
    over a sync cluster of its own, so the sync, asyncio and Twisted environments share it just
    as they share the REST checker.  A wait here proves the change is visible through the
    gateway, which is the only view a couchbase2 test has; it cannot say whether every node
    behind the gateway agrees.

    Any error other than the one that answers the question is read as "not yet", and the wait's
    deadline decides when to give up, as it does for a node that blips mid-wait under REST.
    """

    # The same per-request ceiling as the REST checker's, for the same reason.
    REQUEST_TIMEOUT = timedelta(seconds=ConsistencyChecker.HTTP_TIMEOUT_SECS)

    PROBE_KEY = 'tests-consistency-probe'

    # The REST names wait_until_collection_has_settings is called with, read off a CollectionSpec.
    _SPEC_SETTINGS: dict[str, Callable[[CollectionSpec], Any]] = {
        'maxTTL': lambda spec: None if spec.max_expiry is None else int(spec.max_expiry.total_seconds()),
        'history': lambda spec: spec.history,
    }

    def __init__(self, config: CouchbaseConfig) -> None:
        super().__init__(config)
        self._cluster: Cluster | None = None

    def _gateway(self) -> Cluster:
        # Connected on first use, since most suites never wait.
        if self._cluster is None:
            username, pw = self._config.get_username_and_pw()
            opts = ClusterOptions(PasswordAuthenticator(username, pw))
            opts['enable_metrics'] = False
            opts['enable_tracing'] = False
            self._cluster = Cluster.connect(self._config.get_connection_string(), opts)
        return self._cluster

    def _bucket_present(self, name: str) -> bool | None:
        """Whether the bucket exists, or None when the gateway gave no answer."""
        try:
            self._gateway().buckets().get_bucket(name, timeout=self.REQUEST_TIMEOUT)
        except BucketDoesNotExistException:
            return False
        except CouchbaseException:
            return None
        return True

    def _bucket_warm(self, name: str) -> bool:
        try:
            self._gateway().bucket(name).default_collection().get(self.PROBE_KEY, timeout=self.REQUEST_TIMEOUT)
        except DocumentNotFoundException:
            return True
        except CouchbaseException:
            return False
        return True

    def _scopes(self, bucket_name: str) -> list[ScopeSpec] | None:
        try:
            return list(self._gateway().bucket(bucket_name).collections().get_all_scopes(timeout=self.REQUEST_TIMEOUT))
        except CouchbaseException:
            return None

    @staticmethod
    def _find_collection(scopes: list[ScopeSpec], scope_name: str, collection_name: str) -> CollectionSpec | None:
        for scope in scopes:
            if scope.name == scope_name:
                for coll in scope.collections:
                    if coll.name == collection_name:
                        return coll
        return None

    def _scope_present(self, bucket_name: str, scope_name: str) -> bool | None:
        """Whether the scope exists, or None when the gateway gave no answer."""
        scopes = self._scopes(bucket_name)
        if scopes is None:
            return None
        return any(scope.name == scope_name for scope in scopes)

    def _collection_present(self, bucket_name: str, scope_name: str, collection_name: str) -> bool | None:
        """Whether the collection exists, or None when the gateway gave no answer."""
        scopes = self._scopes(bucket_name)
        if scopes is None:
            return None
        return self._find_collection(scopes, scope_name, collection_name) is not None

    def wait_until_bucket_present(self, name: str, timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._bucket_present(name) is True,
                         f'Bucket {name} is not present through the gateway',
                         timeout,
                         condition='bucket-present',
                         bucket=name)

    def wait_until_bucket_healthy(self, name: str,
                                  timeout: timedelta = ConsistencyChecker.BUCKET_WARMUP_TIMEOUT) -> None:
        """Wait until a KV read on the bucket is answered.

        The gateway reports no per-node bucket health, so this asks the thing the health check
        guards: a read that returns, a missing document included, means the data service is
        warm enough to open the bucket.
        """
        self._wait_until(lambda: self._bucket_warm(name),
                         f'Bucket {name} does not answer a KV read through the gateway',
                         timeout,
                         condition='bucket-healthy',
                         bucket=name)

    def wait_until_bucket_dropped(self, name: str, timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._bucket_present(name) is False,
                         f'Bucket {name} has not been dropped through the gateway',
                         timeout,
                         condition='bucket-dropped',
                         bucket=name)

    def wait_until_scope_present(self, bucket_name: str, scope_name: str,
                                 timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._scope_present(bucket_name, scope_name) is True,
                         f'Scope {scope_name} in bucket {bucket_name} is not present through the gateway',
                         timeout,
                         condition='scope-present',
                         bucket=bucket_name,
                         scope=scope_name)

    def wait_until_scope_dropped(self, bucket_name: str, scope_name: str,
                                 timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._scope_present(bucket_name, scope_name) is False,
                         f'Scope {scope_name} in bucket {bucket_name} has not been dropped through the gateway',
                         timeout,
                         condition='scope-dropped',
                         bucket=bucket_name,
                         scope=scope_name)

    def wait_until_collection_present(self, bucket_name: str, scope_name: str, collection_name: str,
                                      timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._collection_present(bucket_name, scope_name, collection_name) is True,
                         (f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} '
                          f'is not present through the gateway'),
                         timeout,
                         condition='collection-present',
                         bucket=bucket_name,
                         scope=scope_name,
                         collection=collection_name)

    def wait_until_collection_dropped(self, bucket_name: str, scope_name: str, collection_name: str,
                                      timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._wait_until(lambda: self._collection_present(bucket_name, scope_name, collection_name) is False,
                         (f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} '
                          f'has not been dropped through the gateway'),
                         timeout,
                         condition='collection-dropped',
                         bucket=bucket_name,
                         scope=scope_name,
                         collection=collection_name)

    def wait_until_collection_has_settings(self, bucket_name: str, scope_name: str, collection_name: str,
                                           settings: dict, timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT
                                           ) -> None:
        unknown = set(settings).difference(self._SPEC_SETTINGS)
        if unknown:
            raise ValueError(f'No CollectionSpec reading for the settings {sorted(unknown)}.')

        def satisfied() -> bool:
            scopes = self._scopes(bucket_name)
            coll = None if scopes is None else self._find_collection(scopes, scope_name, collection_name)
            if coll is None:
                return False
            return all(self._SPEC_SETTINGS[k](coll) == v for k, v in settings.items())

        self._wait_until(satisfied,
                         (f'Collection {collection_name} in scope {scope_name}, bucket {bucket_name} '
                          f'does not have the expected settings through the gateway ({settings})'),
                         timeout,
                         condition='collection-has-settings',
                         bucket=bucket_name,
                         scope=scope_name,
                         collection=collection_name)

    # User and group management are not in the protocol, so nothing under couchbase2 should
    # reach these.  One that does raises, rather than polling no hosts and passing.

    def wait_until_user_present(self, name: str, domain: str = 'local',
                                timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._unsupported('wait_until_user_present')

    def wait_until_user_dropped(self, name: str, domain: str = 'local',
                                timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._unsupported('wait_until_user_dropped')

    def wait_until_group_present(self, name: str, timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._unsupported('wait_until_group_present')

    def wait_until_group_dropped(self, name: str, timeout: timedelta = ConsistencyChecker.DEFAULT_TIMEOUT) -> None:
        self._unsupported('wait_until_group_dropped')

    @staticmethod
    def _unsupported(wait: str) -> NoReturn:
        raise CouchbaseTestEnvironmentException(f'{wait} has no couchbase2 equivalent.')
