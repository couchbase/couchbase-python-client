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

"""Eventing bucket setup, teardown and function-state polling for the acouchbase suites.

The asyncio twin of eventing_environment.py; that module's docstring has the reasoning, and
this one holds only what differs.  There is no Twisted twin: txcouchbase has no eventing
surface to wrap.

Three things differ, and only three.  `cluster.bucket()` hands back a Bucket that has not
connected yet, so `on_connect()` is awaited before anything reads a scope off it.  The
try_n_times and sleep helpers come from AsyncTestEnvironment.  And `consistency` stays
synchronous throughout: it polls management REST endpoints rather than going through the
SDK, so it is the same object doing the same blocking GETs in both api styles.
"""

from __future__ import annotations

from typing import Any

from couchbase.exceptions import BucketDoesNotExistException, EventingFunctionNotFoundException
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.eventing import (EventingFunction,
                                           EventingFunctionKeyspace,
                                           EventingFunctionState)
from tests_v2.environments.async_environment import AsyncTestEnvironment
from tests_v2.environments.managers import AsyncTestEnvironmentBucketManager
from tests_v2.eventing_defs import (FUNCTION_SCOPE_NAME,
                                    META_COLLECTION_NAME,
                                    SIMPLE_EVT_CODE,
                                    SOURCE_COLLECTION_NAME,
                                    function_name,
                                    function_version)
from tests_v2.skips import CouchbaseTestEnvironmentException, env_degraded
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class AsyncEventingEnvironment:
    """Asyncio eventing bucket setup, teardown and function-state polling."""

    EVENTING_BUCKET_RAM_MB = 256
    STATUS_WAIT_ROUNDS = 40
    STATUS_WAIT_DELAY = 3

    # ------------------------------------------------------------------
    # Bucket lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    async def _drop_stranded_bucket(cb_env: AsyncTestEnvironment, bucket_name: str) -> None:
        """Remove an eventing bucket an earlier run left behind; see the sync twin."""
        bm = cb_env.get_manager(AsyncTestEnvironmentBucketManager)
        try:
            await bm.get_bucket(bucket_name)
        except BucketDoesNotExistException:
            return
        print(f'Bucket {bucket_name} was left behind by an earlier run; dropping it.')
        await bm.drop_bucket(bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

    @staticmethod
    async def _provision_keyspaces(cb_env: AsyncTestEnvironment, bucket: Any, bucket_name: str) -> None:
        """Create the scope and the two collections every function in the suite names."""
        cm = bucket.collections()
        await AsyncTestEnvironment.try_n_times(5, 3, cm.create_scope, FUNCTION_SCOPE_NAME)
        cb_env.consistency.wait_until_scope_present(bucket_name, FUNCTION_SCOPE_NAME)
        for collection_name in (SOURCE_COLLECTION_NAME, META_COLLECTION_NAME):
            await AsyncTestEnvironment.try_n_times(5, 3, cm.create_collection,
                                                   FUNCTION_SCOPE_NAME, collection_name)
            cb_env.consistency.wait_until_collection_present(bucket_name, FUNCTION_SCOPE_NAME, collection_name)

    @staticmethod
    async def _await_eventing_keyspaces(cb_env: AsyncTestEnvironment, efm: Any, bucket_name: str) -> None:
        """Upsert and drop a throwaway function until the eventing service accepts one."""
        full = cb_env.server_version_full
        if full is None:
            env_degraded(stage='eventing-setup',
                         bucket=bucket_name,
                         note='the cluster reported no server version, so the version an '
                              'eventing function has to be stamped with cannot be built')
        probe = EventingFunction(
            function_name('probe'),
            SIMPLE_EVT_CODE,
            function_version(full),
            metadata_keyspace=EventingFunctionKeyspace(bucket_name,
                                                       FUNCTION_SCOPE_NAME,
                                                       META_COLLECTION_NAME),
            source_keyspace=EventingFunctionKeyspace(bucket_name,
                                                     FUNCTION_SCOPE_NAME,
                                                     SOURCE_COLLECTION_NAME))
        await AsyncTestEnvironment.try_n_times(10, 3, efm.upsert_function, probe)
        await AsyncEventingEnvironment.ensure_function_dropped(efm, probe.name)

    @staticmethod
    async def setup_eventing(cb_env: AsyncTestEnvironment,
                             bucket_name: str,
                             required_feature: ServerFeatures,
                             function_manager_cls: type[Any],
                             *,
                             scoped: bool = False) -> None:
        """Create the suite's bucket and keyspaces, and leave the function manager ready to use.

        See the sync environment for why the manager arrives as a class and is resolved here
        rather than by the caller.
        """
        EnvironmentFeatures.check_if_feature_supported(
            [ServerFeatures.BucketManagement, required_feature], cb_env.server_context)

        function_manager = cb_env.get_manager(function_manager_cls)
        await AsyncEventingEnvironment._drop_stranded_bucket(cb_env, bucket_name)
        bm = cb_env.get_manager(AsyncTestEnvironmentBucketManager)
        await bm.create_bucket(CreateBucketSettings(name=bucket_name,
                                                    ram_quota_mb=AsyncEventingEnvironment.EVENTING_BUCKET_RAM_MB,
                                                    flush_enabled=False))
        cb_env.consistency.wait_until_bucket_healthy(bucket_name)

        # The bucket exists from here on; see the sync environment for why every failure below
        # has to take it with it.
        try:
            bucket = cb_env.cluster.bucket(bucket_name)
            await bucket.on_connect()
            await AsyncEventingEnvironment._provision_keyspaces(cb_env, bucket, bucket_name)
            if scoped:
                function_manager.use_scope(bucket.scope(FUNCTION_SCOPE_NAME))
            await AsyncEventingEnvironment._await_eventing_keyspaces(cb_env, function_manager, bucket_name)
        except Exception:
            try:
                await AsyncEventingEnvironment.teardown_eventing(cb_env, bucket_name)
            except Exception as drop_exc:
                print(f'Could not drop {bucket_name} after a failed setup: {drop_exc}')
            raise

    @staticmethod
    async def teardown_eventing(cb_env: AsyncTestEnvironment, bucket_name: str) -> None:
        """Drop the bucket, which takes the scope, the collections and any metadata with it."""
        bm = cb_env.get_manager(AsyncTestEnvironmentBucketManager)
        await AsyncTestEnvironment.try_n_times(5, 3, bm.drop_bucket, bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

    # ------------------------------------------------------------------
    # Function state
    # ------------------------------------------------------------------

    @staticmethod
    async def wait_until_status(efm: Any, state: EventingFunctionState, name: str) -> None:
        """Poll until *name* reports *state*, or raise saying what it reported instead."""
        status = None
        for _ in range(AsyncEventingEnvironment.STATUS_WAIT_ROUNDS):
            status = await efm.get_function_status(name)
            if status is not None and status.state == state:
                return
            await AsyncTestEnvironment.sleep(AsyncEventingEnvironment.STATUS_WAIT_DELAY)

        if status is None:
            raise CouchbaseTestEnvironmentException(
                f'Eventing function {name} reported no status at all after '
                f'{AsyncEventingEnvironment.STATUS_WAIT_ROUNDS} rounds; this manager cannot see it')
        raise CouchbaseTestEnvironmentException(
            f'Eventing function {name} is {status.state.value} and did not reach {state.value}')

    @staticmethod
    async def ensure_function_dropped(efm: Any, name: str) -> None:
        """Leave no function called *name* behind, whatever state this one is in.

        See the sync environment for why the status read and the undeploy are retried.
        """
        status = await AsyncTestEnvironment.try_n_times(5, 3, efm.get_function_status, name)
        if status is None:
            return
        if status.state != EventingFunctionState.Undeployed:
            await AsyncTestEnvironment.try_n_times(5, 3, efm.undeploy_function, name)
            await AsyncEventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, name)
        try:
            await efm.drop_function(name)
        except EventingFunctionNotFoundException:
            return
        await AsyncTestEnvironment.try_n_times_till_exception(
            10, 1, efm.get_function, name,
            expected_exceptions=(EventingFunctionNotFoundException,))
