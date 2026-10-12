#  Copyright 2016-2024. Couchbase, Inc.
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

"""Rate-limit enforcement, limited users and limited scopes, for the rate limit suite.

**One module, not one per api style, and that is a property of what it does rather than a
shortcut.**  Every other environment module has a sync twin and an asyncio twin because it
drives the SDK, and the SDK's shape differs per api.  Nothing here does: user limits, scope
limits and the enforcement switch have no SDK surface at all, so all three are management
REST calls, and the verification that follows each one goes through `cb_env.consistency`,
which is the same synchronous object polling the same endpoints whichever api is running.
The suite reaches the SDK; this module never does.

**Limits are passed in the server's own shape.**  `{'kv': {...}}`, `{'clusterManager': {...}}`
and so on, exactly as `/settings/rbac/users` and `/pools/default/buckets/*/scopes` document
them.  Accepting a differently-spelled dict and copying it key by key into the server's reads
as validation and is the opposite: an unrecognised key goes in silence, so a misspelled limit
produces a user with no limits at all.  These tests pass when no limit is hit, so a typo could
not fail.

**Every request's status is checked**, because a rejected scope creation otherwise surfaces as
a confusing assertion two lines later.
"""

from __future__ import annotations

import json
import random
import uuid
from typing import TYPE_CHECKING, Any

import requests

from tests.skips import CouchbaseTestEnvironmentException, env_reason

if TYPE_CHECKING:
    from tests.environments.test_environment import TestEnvironment


class RateLimitEnvironment:
    """Management REST calls the rate limit suite needs and the SDK does not expose."""

    # Prefix for every limited user, and the only part of the name that is fixed.
    #
    # **Each call to create_rate_limit_user mints a new name, and that is what makes the suite
    # repeatable.**  A limit is spent against a *username*, and neither replacing the user's
    # limits nor dropping and recreating the user under the same name resets what it has
    # already spent inside the current minute.  So a name that has run one test carries that
    # test's spending into the next one to use it.
    #
    # Measured 2026-08-26 on 7.2.5, with all ten tests sharing one name: a single run passes,
    # and three runs back to back fail 4, 3 and 5 tests.  The failures are not in the
    # assertions; they are in the *setup* of the tests that have to succeed at something before
    # they can be refused at it, which is the shape a budget already spent produces.  A fresh
    # name has spent nothing by construction, so nothing has to be waited out.
    #
    # The batch id makes names unique per process as well as per call, so runs back to back
    # cannot collide either.
    USERNAME_PREFIX = 'rate-limit-user'
    PASSWORD = 'password'

    _BATCH_ID = uuid.uuid4().hex[:8]
    _MINTED: list[str] = []

    @staticmethod
    def new_username() -> str:
        """Mint a username nothing in this run or any other has spent a budget against."""
        name = f'{RateLimitEnvironment.USERNAME_PREFIX}-{RateLimitEnvironment._BATCH_ID}-{len(RateLimitEnvironment._MINTED)}'  # noqa: E501
        RateLimitEnvironment._MINTED.append(name)
        return name

    @staticmethod
    def minted_usernames() -> list[str]:
        """Every username minted so far, for the suite's cleanup fixture to drop."""
        return list(RateLimitEnvironment._MINTED)

    @staticmethod
    def forget_usernames() -> None:
        """Drop the record of minted names, once the caller has dropped the users themselves."""
        RateLimitEnvironment._MINTED.clear()

    SCOPE_NAME = 'rate-limit-scope'
    COLLECTION_NAME = 'rate-limit-collection'

    # Matches ConsistencyChecker.HTTP_TIMEOUT_SECS, and for the same reason: `requests` has
    # no default timeout, so a cluster that accepts the connection and then stops answering
    # would hang the run rather than fail it.
    HTTP_TIMEOUT_SECS = 5

    @staticmethod
    def _mgmt_url(cb_env: TestEnvironment, path: str) -> str:
        """Build a management URL from the config the run was given.

        From `config.host` and `config.port`, which is what the rest of the framework
        already polls.  Parsing the connection string and appending a hard-coded 8091 is
        right only for the default port, and only for a connection string carrying no port
        of its own.
        """
        return f'http://{cb_env.config.host}:{cb_env.config.port}/{path.lstrip("/")}'

    @staticmethod
    def _post_form(cb_env: TestEnvironment, path: str, data: dict[str, Any], *, stage: str) -> None:
        RateLimitEnvironment._send_form(cb_env, requests.post, path, data, stage=stage)

    @staticmethod
    def _put_form(cb_env: TestEnvironment, path: str, data: dict[str, Any], *, stage: str) -> None:
        RateLimitEnvironment._send_form(cb_env, requests.put, path, data, stage=stage)

    @staticmethod
    def _send_form(cb_env: TestEnvironment,
                   method: Any,
                   path: str,
                   data: dict[str, Any],
                   *,
                   stage: str) -> None:
        """Send a form-encoded management request, or raise saying which one failed and why.

        Raises rather than skips: every caller is setting up state a test is about to assert
        on, so a refusal here says the environment is broken, not that the server lacks the
        feature.  The feature question is answered by REQUIRED_FEATURES before any of this runs.
        """
        url = RateLimitEnvironment._mgmt_url(cb_env, path)
        try:
            resp = method(url,
                          headers={'Content-Type': 'application/x-www-form-urlencoded'},
                          data=data,
                          auth=cb_env.config.get_username_and_pw(),
                          timeout=RateLimitEnvironment.HTTP_TIMEOUT_SECS)
        except requests.RequestException as ex:
            raise CouchbaseTestEnvironmentException(
                env_reason(stage=stage, url=url, error=type(ex).__name__,
                           note=f'management endpoint unreachable: {ex}'))
        if resp.status_code != 200:
            raise CouchbaseTestEnvironmentException(
                env_reason(stage=stage, url=url, status=resp.status_code, body=resp.text[:200],
                           note='management endpoint did not accept the request'))

    # ------------------------------------------------------------------
    # Enforcement
    # ------------------------------------------------------------------

    @staticmethod
    def enforce_rate_limits(cb_env: TestEnvironment, enforce: bool) -> None:
        """Turn limit enforcement on or off.

        Cluster-global, which is the one thing about this suite that reaches outside itself:
        while it is on, every limited user on the cluster is enforced, not only this suite's.
        Nothing else in the tree creates a limited user, so in practice that is this suite
        alone, but it is why the class-scoped fixture turns it back off in teardown even
        when the suite failed.
        """
        RateLimitEnvironment._post_form(cb_env,
                                        '/internalSettings',
                                        {'enforceLimits': 'true' if enforce else 'false'},
                                        stage='rate-limit-enforce')

    # ------------------------------------------------------------------
    # Limited users and scopes
    # ------------------------------------------------------------------

    @staticmethod
    def create_rate_limit_user(cb_env: TestEnvironment,
                               limits: dict[str, Any],
                               username: str | None = None) -> str:
        """Create or replace *username* with *limits*, and wait until it is visible.

        *limits* is the server's own shape, keyed by service: `kv`, `query`, `fts`.  Sent as a
        PUT, so calling it again replaces the limits rather than adding to them, which is what
        lets one test tighten a budget it has already used once.

        Returns the username, which is normally a freshly minted one; see USERNAME_PREFIX for
        why every call gets its own.  Pass *username* to re-PUT limits over a name this test
        already holds, which is what the connection-limit test does when it tightens its budget.
        """
        name = username if username is not None else RateLimitEnvironment.new_username()
        params: dict[str, Any] = {'password': RateLimitEnvironment.PASSWORD, 'roles': 'admin'}
        if limits:
            params['limits'] = json.dumps(limits)
        RateLimitEnvironment._put_form(cb_env,
                                       f'/settings/rbac/users/local/{name}',
                                       params,
                                       stage='rate-limit-user')
        cb_env.consistency.wait_until_user_present(name)
        return name

    @staticmethod
    def create_rate_limit_scope(cb_env: TestEnvironment, limits: dict[str, Any]) -> None:
        """Create the limited scope with *limits*, and wait until it is visible.

        *limits* is again the server's shape: `kv`, `index`, `fts`, `clusterManager`.  Note
        that the scope endpoint is the only place the cluster manager's own limits can be
        set, and that it spells that service `clusterManager` where the user endpoint has no
        equivalent at all.
        """
        params: dict[str, Any] = {'name': RateLimitEnvironment.SCOPE_NAME}
        if limits:
            params['limits'] = json.dumps(limits)
        bucket_name = cb_env.bucket.name
        RateLimitEnvironment._post_form(cb_env,
                                        f'/pools/default/buckets/{bucket_name}/scopes',
                                        params,
                                        stage='rate-limit-scope')
        cb_env.consistency.wait_until_scope_present(bucket_name, RateLimitEnvironment.SCOPE_NAME)

    # ------------------------------------------------------------------
    # Documents
    # ------------------------------------------------------------------

    @staticmethod
    def random_doc_by_size(size: int) -> str:
        """A hex string of *size* bytes of noise, for the ingress and egress limits.

        Random rather than repeated, because the limits count bytes on the wire and the
        server compresses; a run of identical bytes would cross a megabyte limit at a size
        nothing here could predict.
        """
        return bytearray(random.getrandbits(8) for _ in range(size)).hex()
