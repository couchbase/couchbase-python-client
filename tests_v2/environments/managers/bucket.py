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

"""Bucket management test-infrastructure manager."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tests_v2.environments.managers.base import TestEnvironmentManager
from tests_v2.test_features import ServerFeatures

if TYPE_CHECKING:
    from couchbase.management.buckets import (BucketDescribeResult,
                                              BucketSettings,
                                              CreateBucketSettings)
    from tests_v2.environments.test_environment import TestEnvironment


class TestEnvironmentBucketManager(TestEnvironmentManager):
    """Wraps couchbase.cluster.Cluster.buckets(), cluster-level bucket management."""
    required_feature = ServerFeatures.BasicBucketManagement

    def setup(self, env: TestEnvironment) -> None:
        self._bm = env.cluster.buckets()

    def teardown(self, env: TestEnvironment) -> None:
        pass  # no teardown needed; bucket state managed by individual tests

    def get_bucket(self, name: str) -> BucketSettings:
        return self._bm.get_bucket(name)

    def get_all_buckets(self, *options: Any, **kwargs: Any) -> list[BucketSettings]:
        return self._bm.get_all_buckets(*options, **kwargs)

    def create_bucket(self, settings: CreateBucketSettings) -> None:
        self._bm.create_bucket(settings)

    def update_bucket(self, settings: BucketSettings) -> None:
        self._bm.update_bucket(settings)

    def drop_bucket(self, name: str) -> None:
        self._bm.drop_bucket(name)

    def flush_bucket(self, name: str) -> None:
        self._bm.flush_bucket(name)

    def bucket_describe(self, name: str) -> BucketDescribeResult:
        return self._bm.bucket_describe(name)


# ---------------------------------------------------------------------------
# Async (acouchbase/)
# ---------------------------------------------------------------------------

class AsyncTestEnvironmentBucketManager(TestEnvironmentManager):
    """Wraps acouchbase.cluster.AsyncCluster.buckets().

    setup()/teardown() stay synchronous, matching the base class and the collection
    manager above: TestEnvironment._setup_managers() calls them through an unawaited
    loop for every api style.  cluster.buckets() itself performs no I/O.

    Every wrapped method returns the delegate's awaitable rather than swallowing it.
    The sync wrapper can drop `create_bucket`'s return value because there isn't one;
    the async delegate returns a coroutine, and a wrapper that discards it leaves the
    bucket uncreated while the test proceeds as though it exists.
    """
    required_feature = ServerFeatures.BasicBucketManagement

    def setup(self, env: TestEnvironment) -> None:
        self._bm = env.cluster.buckets()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    async def get_bucket(self, name: str) -> BucketSettings:
        return await self._bm.get_bucket(name)

    async def get_all_buckets(self, *options: Any, **kwargs: Any) -> list[BucketSettings]:
        return await self._bm.get_all_buckets(*options, **kwargs)

    async def create_bucket(self, settings: CreateBucketSettings) -> None:
        await self._bm.create_bucket(settings)

    async def update_bucket(self, settings: BucketSettings) -> None:
        await self._bm.update_bucket(settings)

    async def drop_bucket(self, name: str) -> None:
        await self._bm.drop_bucket(name)

    async def flush_bucket(self, name: str) -> None:
        await self._bm.flush_bucket(name)

    async def bucket_describe(self, name: str) -> BucketDescribeResult:
        return await self._bm.bucket_describe(name)


# ---------------------------------------------------------------------------
# Twisted (txcouchbase/)
# ---------------------------------------------------------------------------

class TwistedTestEnvironmentBucketManager(TestEnvironmentManager):
    """Wraps txcouchbase.cluster.Cluster.buckets(), dispatching via the reactor thread.

    The txcouchbase manager methods return Deferreds, so each call goes through
    run_in_reactor_thread(), which blocks the calling (pytest) thread until the Deferred
    resolves.  That is what lets a synchronous test body drive them, and it is why these
    methods keep the sync signatures while the async ones above do not.
    """
    required_feature = ServerFeatures.BasicBucketManagement

    def setup(self, env: TestEnvironment) -> None:
        from tests_v2.environments.twisted_environment import _get_run_in_reactor_thread
        self._bm = env.cluster.buckets()
        self._rit = _get_run_in_reactor_thread()

    def teardown(self, env: TestEnvironment) -> None:
        pass

    def get_bucket(self, name: str) -> BucketSettings:
        return self._rit(self._bm.get_bucket, name)

    def get_all_buckets(self, *options: Any, **kwargs: Any) -> list[BucketSettings]:
        return self._rit(self._bm.get_all_buckets, *options, **kwargs)

    def create_bucket(self, settings: CreateBucketSettings) -> None:
        self._rit(self._bm.create_bucket, settings)

    def update_bucket(self, settings: BucketSettings) -> None:
        self._rit(self._bm.update_bucket, settings)

    def drop_bucket(self, name: str) -> None:
        self._rit(self._bm.drop_bucket, name)

    def flush_bucket(self, name: str) -> None:
        self._rit(self._bm.flush_bucket, name)

    def bucket_describe(self, name: str) -> BucketDescribeResult:
        return self._rit(self._bm.bucket_describe, name)
