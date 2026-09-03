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

"""Eventing bucket setup, teardown and function-state polling for the sync suites.

One module per api style, one class of static helpers, no TestEnvironment subclass.  Two
api styles rather than three, because txcouchbase does not support eventing function
management; see managers/eventing.py.

**The suites own a bucket.**  An eventing function streams a source keyspace and writes
metadata documents into a metadata keyspace, so pointing either at the shared test bucket
puts documents nothing asserted on in front of every other suite in the run.  Each suite
therefore creates a bucket, provisions one scope and two collections inside it, and drops the
whole bucket afterwards, which is also why both are gated on BucketManagement as well as on
their eventing feature.

**wait_until_status is the third thing here.**  The framework manager offers
`get_function_status()` over the public `functions_status()`, and this wraps it in the bounded
poll every state transition in the suites needs.
"""

from __future__ import annotations

from typing import Any

from couchbase.exceptions import BucketDoesNotExistException, EventingFunctionNotFoundException
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.eventing import (EventingFunction,
                                           EventingFunctionKeyspace,
                                           EventingFunctionState)
from tests_v2.environments.managers import TestEnvironmentBucketManager
from tests_v2.environments.test_environment import SyncTestEnvironment, TestEnvironment
from tests_v2.eventing_defs import (FUNCTION_SCOPE_NAME,
                                    META_COLLECTION_NAME,
                                    SIMPLE_EVT_CODE,
                                    SOURCE_COLLECTION_NAME,
                                    function_name,
                                    function_version)
from tests_v2.skips import CouchbaseTestEnvironmentException, env_degraded
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class EventingEnvironment:
    """Sync eventing bucket setup, teardown and function-state polling."""

    # Sized for `evt-meta` rather than the source keyspace: the suites write nothing, and a
    # deployed function writes one checkpoint document per vbucket.
    EVENTING_BUCKET_RAM_MB = 256

    # Rounds and seconds between them while polling for a function state, so two minutes.
    # One budget for every transition rather than a per-call-site pair: nothing would record
    # why one transition had been given six times another's patience.
    #
    # Two minutes because the transitions are slow and unevenly so.  A deploy on an idle
    # single eventing node settles in a few seconds; an undeploy on the same node with both
    # suites' worth of work behind it can take longer than a minute.
    STATUS_WAIT_ROUNDS = 40
    STATUS_WAIT_DELAY = 3

    # ------------------------------------------------------------------
    # Bucket lifecycle
    # ------------------------------------------------------------------

    @staticmethod
    def _drop_stranded_bucket(cb_env: SyncTestEnvironment, bucket_name: str) -> None:
        """Remove an eventing bucket an earlier run left behind, if there is one.

        Nothing else will drop it.  A yield fixture only runs its teardown half if it got as
        far as yielding, so a setup that raises leaves the bucket in place, and a process that
        is killed outright runs neither half.  Without this the next run fails at create_bucket
        with BucketAlreadyExistsException, which says nothing about why the run before stopped.
        """
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        try:
            bm.get_bucket(bucket_name)
        except BucketDoesNotExistException:
            return
        print(f'Bucket {bucket_name} was left behind by an earlier run; dropping it.')
        bm.drop_bucket(bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

    @staticmethod
    def _provision_keyspaces(cb_env: SyncTestEnvironment, bucket: Any, bucket_name: str) -> None:
        """Create the scope and the two collections every function in the suite names.

        Provisioned here rather than per test because no test asserts anything about creating
        them.  A test that creates its own keyspace has to tolerate one already being there,
        and a tolerate-already-exists retry inside a test body is provisioning wearing a test's
        clothes: what the collection-qualified test is about is whether a qualified keyspace
        round-trips.
        """
        cm = bucket.collections()
        TestEnvironment.try_n_times(5, 3, cm.create_scope, FUNCTION_SCOPE_NAME)
        cb_env.consistency.wait_until_scope_present(bucket_name, FUNCTION_SCOPE_NAME)
        for collection_name in (SOURCE_COLLECTION_NAME, META_COLLECTION_NAME):
            TestEnvironment.try_n_times(5, 3, cm.create_collection, FUNCTION_SCOPE_NAME, collection_name)
            cb_env.consistency.wait_until_collection_present(bucket_name, FUNCTION_SCOPE_NAME, collection_name)

    @staticmethod
    def _await_eventing_keyspaces(cb_env: SyncTestEnvironment, efm: Any, bucket_name: str) -> None:
        """Upsert and drop a throwaway function until the eventing service accepts one.

        `wait_until_collection_present` asks the cluster manager, and the eventing service
        keeps its own view of the manifest that catches up a moment behind it.  Measured on
        7.6.10: the first upsert a suite made raised ERR_COLLECTION_MISSING for `evt-meta`,
        which the collection manager had been reporting present for several seconds by then,
        and every test after it passed.  Retrying here rather than in the suites is what keeps
        that retry out of twenty test bodies, where it would read as an assertion about upsert.

        There is no lighter probe: the keyspaces are validated at upsert and nowhere earlier.
        """
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
        TestEnvironment.try_n_times(10, 3, efm.upsert_function, probe)
        EventingEnvironment.ensure_function_dropped(efm, probe.name)

    @staticmethod
    def setup_eventing(cb_env: SyncTestEnvironment,
                       bucket_name: str,
                       required_feature: ServerFeatures,
                       function_manager_cls: type[Any],
                       *,
                       scoped: bool = False) -> None:
        """Create the suite's bucket and keyspaces, and leave the function manager ready to use.

        The feature check runs here rather than in the suite so that it takes the whole class
        with it on a mock or a server without bucket management.

        The manager arrives as a class and is resolved below rather than being passed in.
        Resolving it in the caller puts get_manager in an argument expression, so it runs
        before this function does: on a server without eventing the registry has already
        dropped the manager, and get_manager raises env-degraded before the check below can
        skip.  REQUIRED_FEATURES does not cover it either, since a class-scoped skip fires
        only once cb_env has been built.

        *scoped* says the manager addresses its functions through a scope, which is the one
        that has to be re-pointed: the scope it manages through lives in the bucket created
        below, so it does not exist when the registry builds the manager.  A cluster-level
        manager hangs off the cluster, which exists before any suite runs.
        """
        EnvironmentFeatures.check_if_feature_supported(
            [ServerFeatures.BucketManagement, required_feature], cb_env.server_context)

        function_manager = cb_env.get_manager(function_manager_cls)
        EventingEnvironment._drop_stranded_bucket(cb_env, bucket_name)
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        bm.create_bucket(CreateBucketSettings(name=bucket_name,
                                              ram_quota_mb=EventingEnvironment.EVENTING_BUCKET_RAM_MB,
                                              flush_enabled=False))
        cb_env.consistency.wait_until_bucket_healthy(bucket_name)

        # The bucket exists from here on, and every remaining step can fail.  Anything that
        # does has to take the bucket with it: this is a fixture setup, so a raise from here
        # never reaches the teardown half that would otherwise drop it.
        try:
            bucket = cb_env.cluster.bucket(bucket_name)
            EventingEnvironment._provision_keyspaces(cb_env, bucket, bucket_name)
            if scoped:
                function_manager.use_scope(bucket.scope(FUNCTION_SCOPE_NAME))
            EventingEnvironment._await_eventing_keyspaces(cb_env, function_manager, bucket_name)
        except Exception:
            # A failure to drop is printed rather than raised, so the exception that reaches
            # the run is the one worth debugging.
            try:
                EventingEnvironment.teardown_eventing(cb_env, bucket_name)
            except Exception as drop_exc:
                print(f'Could not drop {bucket_name} after a failed setup: {drop_exc}')
            raise

    @staticmethod
    def teardown_eventing(cb_env: SyncTestEnvironment, bucket_name: str) -> None:
        """Drop the bucket, which takes the scope, the collections and any metadata with it."""
        bm = cb_env.get_manager(TestEnvironmentBucketManager)
        TestEnvironment.try_n_times(5, 3, bm.drop_bucket, bucket_name)
        cb_env.consistency.wait_until_bucket_dropped(bucket_name)

    # ------------------------------------------------------------------
    # Function state
    # ------------------------------------------------------------------

    @staticmethod
    def wait_until_status(efm: Any, state: EventingFunctionState, name: str) -> None:
        """Poll until *name* reports *state*, or raise saying what it reported instead.

        Takes the manager rather than reading one off an environment, which is what lets the
        two suites share it while each drives a different manager.  It also keeps the poll
        off `efm._get_status`, which is private.

        Raises rather than returning a flag, because every caller treats anything else as a
        failed transition, and keeps the two failures apart: a function this manager cannot
        see at all is a different problem from one that is visible and stuck part-way.
        """
        status = None
        for _ in range(EventingEnvironment.STATUS_WAIT_ROUNDS):
            status = efm.get_function_status(name)
            if status is not None and status.state == state:
                return
            TestEnvironment.sleep(EventingEnvironment.STATUS_WAIT_DELAY)

        if status is None:
            raise CouchbaseTestEnvironmentException(
                f'Eventing function {name} reported no status at all after '
                f'{EventingEnvironment.STATUS_WAIT_ROUNDS} rounds; this manager cannot see it')
        raise CouchbaseTestEnvironmentException(
            f'Eventing function {name} is {status.state.value} and did not reach {state.value}')

    @staticmethod
    def ensure_function_dropped(efm: Any, name: str) -> None:
        """Leave no function called *name* behind, whatever state this one is in.

        A teardown cannot know which state its test stopped in, and drop_function refuses a
        function that is not undeployed.  One teardown per end state would make every test name
        the one matching the state it expected to reach, and the wrong choice strands a deployed
        function for the rest of the run.  Asking for the state first answers it once.

        The status read and the undeploy are retried because the eventing service answers for
        a node it cannot reach with INTERNAL_SERVER_ERROR carrying a "retry" attribute, and a
        single unretried failure here strands a deployed function for every test after it.
        """
        status = TestEnvironment.try_n_times(5, 3, efm.get_function_status, name)
        if status is None:
            return
        if status.state != EventingFunctionState.Undeployed:
            TestEnvironment.try_n_times(5, 3, efm.undeploy_function, name)
            EventingEnvironment.wait_until_status(efm, EventingFunctionState.Undeployed, name)
        try:
            efm.drop_function(name)
        except EventingFunctionNotFoundException:
            return
        TestEnvironment.try_n_times_till_exception(
            10, 1, efm.get_function, name,
            expected_exceptions=(EventingFunctionNotFoundException,))
