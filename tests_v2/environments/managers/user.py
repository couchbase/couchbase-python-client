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

"""User management test-infrastructure managers, one per api style.

  TestEnvironmentUserManager        - sync (couchbase/)
  AsyncTestEnvironmentUserManager   - asyncio (acouchbase/)
  TwistedTestEnvironmentUserManager - Twisted (txcouchbase/)

All three mirror ``cluster.users()``: nine of the SDK ``UserManager``'s ten methods, in the same
order, each forwarding ``*options`` and ``**kwargs`` because the suite drives both the
options-object and the keyword form of every one of them.

**The tenth, ``change_password``, is deliberately absent.**  It changes the password of whoever
the *connection* is authenticated as, not of a user named in an argument, and the environment's
connection is the administrator the whole run uses.  A wrapper forwarding it to ``self._um``
would offer one call that locks every later test out of the cluster.  The suite covers it by
opening a cluster of its own, authenticated as the user it just made, which is the `query_t`
shape: a second cluster built inside the test and a `testgen_exclude` for txcouchbase, whose
`Cluster` has no `connect` classmethod.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tests_v2.environments.managers.base import TestEnvironmentManager
from tests_v2.test_features import ServerFeatures

if TYPE_CHECKING:
    from collections.abc import Iterable

    from couchbase.management.users import (Group,
                                            RoleAndDescription,
                                            User,
                                            UserAndMetadata)
    from tests_v2.environments.test_environment import TestEnvironment


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

class TestEnvironmentUserManager(TestEnvironmentManager):
    """Mirrors couchbase.management.users.UserManager."""
    required_feature = ServerFeatures.UserManagement

    def setup(self, env: TestEnvironment) -> None:
        self._um = env.cluster.users()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_user(self, username: str, *options: Any, **kwargs: Any) -> UserAndMetadata:
        return self._um.get_user(username, *options, **kwargs)

    def get_all_users(self, *options: Any, **kwargs: Any) -> Iterable[UserAndMetadata]:
        return self._um.get_all_users(*options, **kwargs)

    def upsert_user(self, user: User, *options: Any, **kwargs: Any) -> None:
        self._um.upsert_user(user, *options, **kwargs)

    def drop_user(self, username: str, *options: Any, **kwargs: Any) -> None:
        self._um.drop_user(username, *options, **kwargs)

    def get_roles(self, *options: Any, **kwargs: Any) -> Iterable[RoleAndDescription]:
        return self._um.get_roles(*options, **kwargs)

    def get_group(self, group_name: str, *options: Any, **kwargs: Any) -> Group:
        return self._um.get_group(group_name, *options, **kwargs)

    def get_all_groups(self, *options: Any, **kwargs: Any) -> Iterable[Group]:
        return self._um.get_all_groups(*options, **kwargs)

    def upsert_group(self, group: Group, *options: Any, **kwargs: Any) -> None:
        self._um.upsert_group(group, *options, **kwargs)

    def drop_group(self, group_name: str, *options: Any, **kwargs: Any) -> None:
        self._um.drop_group(group_name, *options, **kwargs)


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentUserManager(TestEnvironmentManager):
    """Mirrors acouchbase.management.users.UserManager.

    setup() stays synchronous, matching the base class and every other manager in the package:
    TestEnvironment._setup_managers() calls it through an unawaited loop for all three api
    styles, and cluster.users() performs no I/O.  Only the forwarded calls are coroutines.
    """
    required_feature = ServerFeatures.UserManagement

    def setup(self, env: TestEnvironment) -> None:
        self._um = env.cluster.users()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def get_user(self, username: str, *options: Any, **kwargs: Any) -> UserAndMetadata:
        return await self._um.get_user(username, *options, **kwargs)

    async def get_all_users(self, *options: Any, **kwargs: Any) -> Iterable[UserAndMetadata]:
        return await self._um.get_all_users(*options, **kwargs)

    async def upsert_user(self, user: User, *options: Any, **kwargs: Any) -> None:
        await self._um.upsert_user(user, *options, **kwargs)

    async def drop_user(self, username: str, *options: Any, **kwargs: Any) -> None:
        await self._um.drop_user(username, *options, **kwargs)

    async def get_roles(self, *options: Any, **kwargs: Any) -> Iterable[RoleAndDescription]:
        return await self._um.get_roles(*options, **kwargs)

    async def get_group(self, group_name: str, *options: Any, **kwargs: Any) -> Group:
        return await self._um.get_group(group_name, *options, **kwargs)

    async def get_all_groups(self, *options: Any, **kwargs: Any) -> Iterable[Group]:
        return await self._um.get_all_groups(*options, **kwargs)

    async def upsert_group(self, group: Group, *options: Any, **kwargs: Any) -> None:
        await self._um.upsert_group(group, *options, **kwargs)

    async def drop_group(self, group_name: str, *options: Any, **kwargs: Any) -> None:
        await self._um.drop_group(group_name, *options, **kwargs)


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------

class TwistedTestEnvironmentUserManager(TestEnvironmentManager):
    """Mirrors txcouchbase.management.users.UserManager, via the reactor thread.

    Every txcouchbase manager call returns a Deferred, so each one goes through
    run_in_reactor_thread(), which blocks the pytest thread until it resolves.  That is what
    lets these keep the sync signatures while the async variant above does not.
    """
    required_feature = ServerFeatures.UserManagement

    def setup(self, env: TestEnvironment) -> None:
        from tests_v2.environments.twisted_environment import _get_run_in_reactor_thread
        self._um = env.cluster.users()
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_user(self, username: str, *options: Any, **kwargs: Any) -> UserAndMetadata:
        return self._rit(self._um.get_user, username, *options, **kwargs)

    def get_all_users(self, *options: Any, **kwargs: Any) -> Iterable[UserAndMetadata]:
        return self._rit(self._um.get_all_users, *options, **kwargs)

    def upsert_user(self, user: User, *options: Any, **kwargs: Any) -> None:
        self._rit(self._um.upsert_user, user, *options, **kwargs)

    def drop_user(self, username: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._um.drop_user, username, *options, **kwargs)

    def get_roles(self, *options: Any, **kwargs: Any) -> Iterable[RoleAndDescription]:
        return self._rit(self._um.get_roles, *options, **kwargs)

    def get_group(self, group_name: str, *options: Any, **kwargs: Any) -> Group:
        return self._rit(self._um.get_group, group_name, *options, **kwargs)

    def get_all_groups(self, *options: Any, **kwargs: Any) -> Iterable[Group]:
        return self._rit(self._um.get_all_groups, *options, **kwargs)

    def upsert_group(self, group: Group, *options: Any, **kwargs: Any) -> None:
        self._rit(self._um.upsert_group, group, *options, **kwargs)

    def drop_group(self, group_name: str, *options: Any, **kwargs: Any) -> None:
        self._rit(self._um.drop_group, group_name, *options, **kwargs)
