from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime, timedelta
from time import sleep
from typing import TYPE_CHECKING, Any

import requests

from couchbase.diagnostics import ServiceType
from tests_v2.couchbase_config import CouchbaseConfig
from tests_v2.skips import env_degraded

if TYPE_CHECKING:
    from tests_v2.environments.test_environment import TestEnvironment


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

    # These three raise through env_degraded rather than a bare RuntimeError.  A cluster
    # resource that never propagates to every node is the definition of env-degraded, and a
    # RuntimeError carries neither the category nor anything a CI run can parse.  The prose
    # the callers build becomes the note; the deadline and the polled path become fields, so
    # a timeout says how long it actually waited rather than leaving that to be inferred
    # from the source.

    def _wait_until_resource_present(self, path: str, error_msg: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        deadline = datetime.now() + timeout
        while datetime.now() < deadline:
            if self._resource_is_present(path):
                return
            else:
                sleep(self.RETRY_DELAY_SECS)
        env_degraded(stage='consistency-wait',
                     condition='resource-present',
                     path=path,
                     timeout_s=timeout.total_seconds(),
                     note=error_msg)

    def _wait_until_resource_dropped(self, path: str, error_msg: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        deadline = datetime.now() + timeout
        while datetime.now() < deadline:
            if self._resource_is_dropped(path):
                return
            else:
                sleep(self.RETRY_DELAY_SECS)
        env_degraded(stage='consistency-wait',
                     condition='resource-dropped',
                     path=path,
                     timeout_s=timeout.total_seconds(),
                     note=error_msg)

    def _wait_until_resource_satisfies_predicate(self, path: str, predicate: Callable[[dict], bool],
                                                 error_msg: str, timeout: timedelta = DEFAULT_TIMEOUT) -> None:
        deadline = datetime.now() + timeout
        while datetime.now() < deadline:
            if self._resource_satisfies_predicate(path, predicate):
                return
            else:
                sleep(self.RETRY_DELAY_SECS)
        env_degraded(stage='consistency-wait',
                     condition='resource-satisfies-predicate',
                     path=path,
                     timeout_s=timeout.total_seconds(),
                     note=error_msg)

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
        checker = cls(config=env.config)
        if env.config.has_management_api:
            checker.fetch_hostnames()
        return checker
