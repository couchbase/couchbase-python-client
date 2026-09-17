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

from collections.abc import Generator
from datetime import timedelta
from typing import Any, Optional

import pytest

import couchbase.subdocument as SD
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (DocumentExistsException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException)
from couchbase.logic.observability import OpName
from couchbase.options import (ExistsOptions,
                               GetAllReplicasOptions,
                               GetAndLockOptions,
                               GetAndTouchOptions,
                               GetAnyReplicaOptions,
                               GetOptions,
                               InsertOptions,
                               LookupInAllReplicasOptions,
                               LookupInAnyReplicaOptions,
                               LookupInOptions,
                               MutateInOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               TouchOptions,
                               UnlockOptions,
                               UpsertOptions)
from couchbase.transcoder import Transcoder
from tests_v2.environments.managers import TestEnvironmentBucketManager
from tests_v2.environments.test_environment import (ConnectionStrategy,
                                                    SyncTestEnvironment,
                                                    TestEnvironment,
                                                    TestProtocol,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.environments.tracing_environment import TracingEnvironment, TracingType
from tests_v2.manifest import validate_test_manifest
from tests_v2.suite_fixtures import TopologySuite
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures
from tests_v2.tracing_validators import KeyValueSpanValidator

ONE_SECOND = timedelta(seconds=1)
FIVE_SECONDS = timedelta(seconds=5)
# Rejected by the client rather than by the server, which is the point of the two tests that
# use it: the operation dies before a request span is ever created.
NEGATIVE_EXPIRY = timedelta(seconds=-1)
# A cas that belongs to no document.  Paired with a key that does not exist either, so what the
# server refuses is the key; the value only has to be a well-formed cas.
UNOWNED_CAS = 12345
# `project` takes field paths.  Annotated Any so that handing it integers is the test rather
# than something the type checker has to be told to ignore.
INVALID_PROJECT: Any = [1, 2, 3]
# The replica reads run the active copy and the replicas as separate operations, each with its
# own span underneath the one the caller asked for.
REPLICA_READ_OPS = [OpName.Get, OpName.GetReplica]
SUBDOC_REPLICA_READ_OPS = [OpName.LookupIn, OpName.LookupInReplica]
# MAJORITY rather than PERSIST_TO_MAJORITY: the span carries whichever level was asked for, and
# a persisted write waits on disk on more than one node, which a loaded cluster has been measured
# taking past 9s.  The persisted levels are covered by the durability suites.
MAJORITY = ServerDurability(level=DurabilityLevel.MAJORITY)
MAJORITY_LABEL = DurabilityLevel.to_server_str(DurabilityLevel.MAJORITY)
# The server gives up on a durable write within the request's own timeout and reports it
# ambiguous, and a loaded cluster has been measured taking past 9s over a MAJORITY write alone.
SERVER_DURABLE_TIMEOUT = timedelta(seconds=30)
# The server reports a durable write ambiguous when it cannot get a majority to acknowledge it within
# its own limit, and a single write can stall for that whole limit while the writes around it finish
# in seconds, so a longer timeout only waits longer.  These tests assert the span rather than the
# durability, so a stalled write is retried; any other failure still fails the test.
SYNC_WRITE_AMBIGUOUS_RERUN = pytest.mark.flaky(reruns=2,
                                               reruns_delay=1,
                                               only_rerun=['DurabilitySyncWriteAmbiguous'])
# PYCBC-1973, CXXCBC-1058: an any-replica read completes on the first response while the other
# replica reads are still open, so their spans arrive unfinished, with no peer socket and an end
# time of 0.  Reruns only on the assertions such a span trips; drop this once either lands.
ANY_REPLICA_OPEN_SIBLING_RERUN = pytest.mark.flaky(reruns=3,
                                                   reruns_delay=1,
                                                   only_rerun=['remote_socket: expected',
                                                               'span end_time=',
                                                               "span attribute '(network\\.peer|server)\\."])
MUTATE_IN_SPECS = (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model'))
LOOKUP_IN_SPECS = (SD.get('batch'),)


# Named so the test can catch this instead of Exception.
class EncodingFailure(Exception):
    pass


class ErrorTranscoder(Transcoder):
    # Only `encode_value` is ever reached: the suite drives one upsert, and the point is that
    # an operation dying inside the transcoder still gets a span, with an error status and
    # nothing underneath it.

    def encode_value(self, value: Any) -> tuple[bytes, int]:
        raise EncodingFailure('transcoder error')

    def decode_value(self, value: bytes, flags: Optional[int]) -> Any:
        raise EncodingFailure('transcoder error')


class KeyValueTracingTestSuite(TopologySuite):
    # Every test is the same three steps: clear the tracer, run one operation, assert on the
    # span tree it produced.  The suite runs four times, once per `TracingType`, and what the
    # assertion means changes with it: `Basic` and `Legacy` walk the tree, `ThresholdLogging`
    # checks the SDK's own roll-ups against the child spans they were rolled up from, and `NoOp`
    # asserts the SDK never asked for a span at all.
    #
    # Named tests rather than one parametrized over `OpName`.  An operation reached through
    # `getattr` has no type for the generator to read, so the generated suites would call it
    # without awaiting it, and an unawaited coroutine produces no spans: the assertion would
    # fail somewhere far from the cause, or pass under `NoOp` where empty is the expectation.
    #
    # Two gates worth naming, because failing them looks like a broken assertion rather than a
    # missing prerequisite.  The replica reads assert a span for the active copy and for each
    # replica, so on a bucket configured with no replicas the assertion fails rather than
    # skipping, and they are gated on the replica fixtures.  The subdocument replica reads are
    # gated on the server feature at the class as well, where an inline `is_feature_supported`
    # per test would put the same condition in eight places.

    # Observability support in CNG is pending.
    UNSUPPORTED_PROTOCOLS = [TestProtocol.COUCHBASE2]
    # A tracer is a cluster option, so the suite needs a cluster of its own.
    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION
    # Read by the topology fixtures the durability tests are gated on.
    MANAGERS = [TestEnvironmentBucketManager]
    # `get_existing_doc` marks each key used and never hands it back.  Most of the fifty-odd
    # tests take one and the parent-span tests take two, so the seeded count is a real floor
    # here rather than a default worth inheriting silently.
    NUM_DOCS = 125

    TEST_MANIFEST = [
        'test_exists',
        'test_exists_with_parent',
        'test_get',
        'test_get_all_replicas',
        'test_get_all_replicas_missing_key',
        'test_get_all_replicas_with_parent',
        'test_get_and_lock',
        'test_get_and_lock_missing_key',
        'test_get_and_lock_with_parent',
        'test_get_and_touch',
        'test_get_and_touch_missing_key',
        'test_get_and_touch_with_parent',
        'test_get_any_replica',
        'test_get_any_replica_missing_key',
        'test_get_any_replica_with_parent',
        'test_get_invalid_project',
        'test_get_missing_key',
        'test_get_with_parent',
        'test_insert',
        'test_insert_existing_key',
        'test_insert_negative_expiry',
        'test_insert_with_durability',
        'test_insert_with_parent',
        'test_lookup_in',
        'test_lookup_in_all_replicas',
        'test_lookup_in_all_replicas_missing_key',
        'test_lookup_in_all_replicas_with_parent',
        'test_lookup_in_any_replica',
        'test_lookup_in_any_replica_missing_key',
        'test_lookup_in_any_replica_with_parent',
        'test_lookup_in_missing_key',
        'test_lookup_in_with_parent',
        'test_mutate_in',
        'test_mutate_in_missing_key',
        'test_mutate_in_preserve_expiry_conflict',
        'test_mutate_in_with_durability',
        'test_mutate_in_with_parent',
        'test_remove',
        'test_remove_missing_key',
        'test_remove_with_durability',
        'test_remove_with_parent',
        'test_replace',
        'test_replace_missing_key',
        'test_replace_preserve_expiry_conflict',
        'test_replace_with_durability',
        'test_replace_with_parent',
        'test_touch',
        'test_touch_missing_key',
        'test_touch_with_parent',
        'test_unlock',
        'test_unlock_invalid_cas',
        'test_unlock_with_parent',
        'test_upsert',
        'test_upsert_encoding_error',
        'test_upsert_negative_expiry',
        'test_upsert_with_durability',
        'test_upsert_with_parent',
        'test_upsert_with_retries',
    ]

    @pytest.fixture(name='kv_validator')
    def kv_validator_fixture(self, cb_env: SyncTestEnvironment) -> KeyValueSpanValidator:
        # Cleared here rather than in a teardown so that each test's assertion is about its
        # own operation: the tracer is class-scoped and has already traced every upsert the
        # seeding performed by the time the first test runs.
        validator = TracingEnvironment.kv_span_validator(cb_env)
        validator.reset()
        return validator

    @pytest.fixture(scope='class')
    def check_subdoc_replica_read_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.SubdocReplicaRead, cb_env.server_context)

    # ------------------------------------------------------------------
    # Operations that succeed
    # ------------------------------------------------------------------

    def test_insert(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_new_doc()
        cb_env.collection.insert(key, value)
        kv_validator.validate_kv_op(OpName.Insert)

    def test_replace(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        cb_env.collection.replace(key, value)
        kv_validator.validate_kv_op(OpName.Replace)

    def test_upsert(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        cb_env.collection.upsert(key, value)
        kv_validator.validate_kv_op(OpName.Upsert)

    def test_exists(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.exists(key)
        kv_validator.validate_kv_op(OpName.Exists)

    def test_get(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.get(key)
        kv_validator.validate_kv_op(OpName.Get)

    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_get_all_replicas(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.get_all_replicas(key)
        kv_validator.validate_kv_op(OpName.GetAllReplicas, nested_ops=REPLICA_READ_OPS)

    @ANY_REPLICA_OPEN_SIBLING_RERUN
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_get_any_replica(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.get_any_replica(key)
        # Whichever copy answers first wins, and a copy that answers out of a local cache can
        # report no server time at all, so what is asserted is that one of them reached a
        # server rather than that all of them did.
        kv_validator.validate_kv_op(OpName.GetAnyReplica,
                                    nested_ops=REPLICA_READ_OPS,
                                    server_duration_gt_zero=False)

    def test_get_and_lock(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.get_and_lock(key, ONE_SECOND)
        kv_validator.validate_kv_op(OpName.GetAndLock)

    def test_get_and_touch(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.get_and_touch(key, ONE_SECOND)
        kv_validator.validate_kv_op(OpName.GetAndTouch)

    def test_touch(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.touch(key, ONE_SECOND)
        kv_validator.validate_kv_op(OpName.Touch)

    def test_lookup_in(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.lookup_in(key, LOOKUP_IN_SPECS)
        kv_validator.validate_kv_op(OpName.LookupIn)

    @pytest.mark.usefixtures('check_subdoc_replica_read_supported')
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_lookup_in_all_replicas(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.lookup_in_all_replicas(key, LOOKUP_IN_SPECS)
        kv_validator.validate_kv_op(OpName.LookupInAllReplicas, nested_ops=SUBDOC_REPLICA_READ_OPS)

    @ANY_REPLICA_OPEN_SIBLING_RERUN
    @pytest.mark.usefixtures('check_subdoc_replica_read_supported')
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_lookup_in_any_replica(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.lookup_in_any_replica(key, LOOKUP_IN_SPECS)
        kv_validator.validate_kv_op(OpName.LookupInAnyReplica,
                                    nested_ops=SUBDOC_REPLICA_READ_OPS,
                                    server_duration_gt_zero=False)

    def test_mutate_in(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.mutate_in(key, MUTATE_IN_SPECS)
        kv_validator.validate_kv_op(OpName.MutateIn)

    def test_remove(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.remove(key)
        kv_validator.validate_kv_op(OpName.Remove)

    def test_unlock(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.get_and_lock(key, FIVE_SECONDS)
        kv_validator.reset()
        cb_env.collection.unlock(key, result.cas)
        kv_validator.validate_kv_op(OpName.Unlock)

    def test_upsert_with_retries(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        # The lock is held for a second and the upsert is given three, so it retries until the
        # lock lapses and then succeeds.  A `RetryCount` above zero is the only thing in the
        # span tree that says the SDK re-sent a request the caller made once.
        key = cb_env.get_existing_doc(key_only=True)
        cb_env.collection.get_and_lock(key, ONE_SECOND)
        kv_validator.reset()
        cb_env.collection.upsert(key, {'foo': 'bar'}, UpsertOptions(timeout=timedelta(seconds=3)))
        kv_validator.validate_kv_op(OpName.Upsert, retry_count_gt_zero=True)

    # ------------------------------------------------------------------
    # Operations that carry a durability requirement
    # ------------------------------------------------------------------

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_insert_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_new_doc()
        opts = InsertOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.insert(key, value, opts)
        kv_validator.validate_kv_op(OpName.Insert, durability=MAJORITY_LABEL)

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_replace_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        opts = ReplaceOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.replace(key, value, opts)
        kv_validator.validate_kv_op(OpName.Replace, durability=MAJORITY_LABEL)

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_upsert_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        opts = UpsertOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.upsert(key, value, opts)
        kv_validator.validate_kv_op(OpName.Upsert, durability=MAJORITY_LABEL)

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_mutate_in_with_durability(self, cb_env: SyncTestEnvironment,
                                       kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        opts = MutateInOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.mutate_in(key, MUTATE_IN_SPECS, opts)
        kv_validator.validate_kv_op(OpName.MutateIn, durability=MAJORITY_LABEL)

    @SYNC_WRITE_AMBIGUOUS_RERUN
    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_remove_with_durability(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        opts = RemoveOptions(durability=MAJORITY, timeout=SERVER_DURABLE_TIMEOUT)
        cb_env.collection.remove(key, opts)
        kv_validator.validate_kv_op(OpName.Remove, durability=MAJORITY_LABEL)

    # ------------------------------------------------------------------
    # Operations the server refuses, which still produce a full span tree
    # ------------------------------------------------------------------

    def test_insert_existing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        with pytest.raises(DocumentExistsException):
            cb_env.collection.insert(key, value)
        kv_validator.validate_kv_op(OpName.Insert, validate_error=True)

    def test_replace_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        _, value = cb_env.get_new_doc()
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace(TestEnvironment.NOT_A_KEY, value)
        kv_validator.validate_kv_op(OpName.Replace, validate_error=True)

    def test_get_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(TestEnvironment.NOT_A_KEY)
        kv_validator.validate_kv_op(OpName.Get, validate_error=True)

    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_get_all_replicas_missing_key(self, cb_env: SyncTestEnvironment,
                                          kv_validator: KeyValueSpanValidator) -> None:
        # `get_all_replicas` reports the key, where `get_any_replica` reports that no copy
        # could be retrieved.
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_all_replicas(TestEnvironment.NOT_A_KEY)
        kv_validator.validate_kv_op(OpName.GetAllReplicas, nested_ops=REPLICA_READ_OPS, validate_error=True)

    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_get_any_replica_missing_key(self, cb_env: SyncTestEnvironment,
                                         kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.get_any_replica(TestEnvironment.NOT_A_KEY)
        kv_validator.validate_kv_op(OpName.GetAnyReplica,
                                    nested_ops=REPLICA_READ_OPS,
                                    validate_error=True,
                                    server_duration_gt_zero=False)

    def test_get_and_lock_missing_key(self, cb_env: SyncTestEnvironment,
                                      kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_and_lock(TestEnvironment.NOT_A_KEY, ONE_SECOND)
        kv_validator.validate_kv_op(OpName.GetAndLock, validate_error=True)

    def test_get_and_touch_missing_key(self, cb_env: SyncTestEnvironment,
                                       kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_and_touch(TestEnvironment.NOT_A_KEY, ONE_SECOND)
        kv_validator.validate_kv_op(OpName.GetAndTouch, validate_error=True)

    def test_touch_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.touch(TestEnvironment.NOT_A_KEY, ONE_SECOND)
        kv_validator.validate_kv_op(OpName.Touch, validate_error=True)

    def test_lookup_in_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.lookup_in(TestEnvironment.NOT_A_KEY, LOOKUP_IN_SPECS)
        kv_validator.validate_kv_op(OpName.LookupIn, validate_error=True)

    @pytest.mark.usefixtures('check_subdoc_replica_read_supported')
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_lookup_in_all_replicas_missing_key(self, cb_env: SyncTestEnvironment,
                                                kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.lookup_in_all_replicas(TestEnvironment.NOT_A_KEY, LOOKUP_IN_SPECS)
        kv_validator.validate_kv_op(OpName.LookupInAllReplicas,
                                    nested_ops=SUBDOC_REPLICA_READ_OPS,
                                    validate_error=True)

    @pytest.mark.usefixtures('check_subdoc_replica_read_supported')
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_lookup_in_any_replica_missing_key(self, cb_env: SyncTestEnvironment,
                                               kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.lookup_in_any_replica(TestEnvironment.NOT_A_KEY, LOOKUP_IN_SPECS)
        kv_validator.validate_kv_op(OpName.LookupInAnyReplica,
                                    nested_ops=SUBDOC_REPLICA_READ_OPS,
                                    validate_error=True,
                                    server_duration_gt_zero=False)

    def test_mutate_in_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.mutate_in(TestEnvironment.NOT_A_KEY, MUTATE_IN_SPECS)
        kv_validator.validate_kv_op(OpName.MutateIn, validate_error=True)

    def test_remove_missing_key(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.remove(TestEnvironment.NOT_A_KEY)
        kv_validator.validate_kv_op(OpName.Remove, validate_error=True)

    def test_unlock_invalid_cas(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.unlock(TestEnvironment.NOT_A_KEY, UNOWNED_CAS)
        kv_validator.validate_kv_op(OpName.Unlock, validate_error=True)

    # ------------------------------------------------------------------
    # Operations the client refuses, which never reach the wire
    # ------------------------------------------------------------------

    def test_get_invalid_project(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get(key, GetOptions(project=INVALID_PROJECT))
        kv_validator.validate_kv_op(OpName.Get, validate_error=True, error_before_dispatch=True)

    def test_mutate_in_preserve_expiry_conflict(self, cb_env: SyncTestEnvironment,
                                                kv_validator: KeyValueSpanValidator) -> None:
        # `preserve_expiry` cannot be honoured by a spec that creates the field.
        key = cb_env.get_existing_doc(key_only=True)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.mutate_in(key,
                                        (SD.insert('make', 'New Make'),),
                                        MutateInOptions(preserve_expiry=True))
        kv_validator.validate_kv_op(OpName.MutateIn, validate_error=True, error_before_dispatch=True)

    def test_replace_preserve_expiry_conflict(self, cb_env: SyncTestEnvironment,
                                              kv_validator: KeyValueSpanValidator) -> None:
        # Preserving the expiry and setting one at the same time is a contradiction.
        key, value = cb_env.get_existing_doc()
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.replace(key, value, ReplaceOptions(preserve_expiry=True, expiry=FIVE_SECONDS))
        kv_validator.validate_kv_op(OpName.Replace, validate_error=True, error_before_dispatch=True)

    def test_upsert_encoding_error(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_new_doc()
        with pytest.raises(EncodingFailure):
            cb_env.collection.upsert(key, value, UpsertOptions(transcoder=ErrorTranscoder()))
        kv_validator.validate_kv_op(OpName.Upsert, validate_error=True, error_before_dispatch=True)

    def test_insert_negative_expiry(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        # The expiry is rejected while the options are still being read, before any span exists.
        key, value = cb_env.get_new_doc()
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.insert(key, value, InsertOptions(expiry=NEGATIVE_EXPIRY))
        kv_validator.validate_kv_op(OpName.Insert, expect_request_span=False)

    def test_upsert_negative_expiry(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.upsert(key, value, UpsertOptions(expiry=NEGATIVE_EXPIRY))
        kv_validator.validate_kv_op(OpName.Upsert, expect_request_span=False)

    # ------------------------------------------------------------------
    # Operations handed a parent span
    # ------------------------------------------------------------------
    # Each of these runs the operation twice, and the two calls are not a repetition.  The
    # option is `parent_span`; `span` is the name it used to have, still accepted and now
    # deprecated in favour of it.  So the first call passes `parent_span` through an options
    # object and the second passes `span` as a keyword, which covers both names and both ways
    # of delivering them.  Passing `span` both times exercises the deprecated name twice and
    # the current one not at all.

    def test_insert_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_new_doc()
        parent = kv_validator.new_parent_span(OpName.Insert)
        cb_env.collection.insert(key, value, InsertOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Insert, parent_span=parent)

        key, value = cb_env.get_new_doc()
        parent = kv_validator.new_parent_span(OpName.Insert)
        cb_env.collection.insert(key, value, span=parent)
        kv_validator.validate_kv_op(OpName.Insert, parent_span=parent)

    def test_replace_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        parent = kv_validator.new_parent_span(OpName.Replace)
        cb_env.collection.replace(key, value, ReplaceOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Replace, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Replace)
        cb_env.collection.replace(key, value, span=parent)
        kv_validator.validate_kv_op(OpName.Replace, parent_span=parent)

    def test_upsert_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key, value = cb_env.get_existing_doc()
        parent = kv_validator.new_parent_span(OpName.Upsert)
        cb_env.collection.upsert(key, value, UpsertOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Upsert, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Upsert)
        cb_env.collection.upsert(key, value, span=parent)
        kv_validator.validate_kv_op(OpName.Upsert, parent_span=parent)

    def test_exists_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.Exists)
        cb_env.collection.exists(key, ExistsOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Exists, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Exists)
        cb_env.collection.exists(key, span=parent)
        kv_validator.validate_kv_op(OpName.Exists, parent_span=parent)

    def test_get_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.Get)
        cb_env.collection.get(key, GetOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Get, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.Get)
        cb_env.collection.get(key, span=parent)
        kv_validator.validate_kv_op(OpName.Get, parent_span=parent)

    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_get_all_replicas_with_parent(self, cb_env: SyncTestEnvironment,
                                          kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.GetAllReplicas)
        cb_env.collection.get_all_replicas(key, GetAllReplicasOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.GetAllReplicas, parent_span=parent, nested_ops=REPLICA_READ_OPS)

        parent = kv_validator.new_parent_span(OpName.GetAllReplicas)
        cb_env.collection.get_all_replicas(key, span=parent)
        kv_validator.validate_kv_op(OpName.GetAllReplicas, parent_span=parent, nested_ops=REPLICA_READ_OPS)

    @ANY_REPLICA_OPEN_SIBLING_RERUN
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_get_any_replica_with_parent(self, cb_env: SyncTestEnvironment,
                                         kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.GetAnyReplica)
        cb_env.collection.get_any_replica(key, GetAnyReplicaOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.GetAnyReplica,
                                    parent_span=parent,
                                    nested_ops=REPLICA_READ_OPS,
                                    server_duration_gt_zero=False)

        parent = kv_validator.new_parent_span(OpName.GetAnyReplica)
        cb_env.collection.get_any_replica(key, span=parent)
        kv_validator.validate_kv_op(OpName.GetAnyReplica,
                                    parent_span=parent,
                                    nested_ops=REPLICA_READ_OPS,
                                    server_duration_gt_zero=False)

    def test_get_and_lock_with_parent(self, cb_env: SyncTestEnvironment,
                                      kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.GetAndLock)
        cb_env.collection.get_and_lock(key, ONE_SECOND, GetAndLockOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.GetAndLock, parent_span=parent)

        # A fresh key, because the one above is still locked.
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.GetAndLock)
        cb_env.collection.get_and_lock(key, ONE_SECOND, span=parent)
        kv_validator.validate_kv_op(OpName.GetAndLock, parent_span=parent)

    def test_get_and_touch_with_parent(self, cb_env: SyncTestEnvironment,
                                       kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.GetAndTouch)
        cb_env.collection.get_and_touch(key, ONE_SECOND, GetAndTouchOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.GetAndTouch, parent_span=parent)

        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.GetAndTouch)
        cb_env.collection.get_and_touch(key, ONE_SECOND, span=parent)
        kv_validator.validate_kv_op(OpName.GetAndTouch, parent_span=parent)

    def test_touch_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.Touch)
        cb_env.collection.touch(key, ONE_SECOND, TouchOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Touch, parent_span=parent)

        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.Touch)
        cb_env.collection.touch(key, ONE_SECOND, span=parent)
        kv_validator.validate_kv_op(OpName.Touch, parent_span=parent)

    def test_lookup_in_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.LookupIn)
        cb_env.collection.lookup_in(key, LOOKUP_IN_SPECS, LookupInOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.LookupIn, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.LookupIn)
        cb_env.collection.lookup_in(key, LOOKUP_IN_SPECS, span=parent)
        kv_validator.validate_kv_op(OpName.LookupIn, parent_span=parent)

    @pytest.mark.usefixtures('check_subdoc_replica_read_supported')
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_lookup_in_all_replicas_with_parent(self, cb_env: SyncTestEnvironment,
                                                kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.LookupInAllReplicas)
        cb_env.collection.lookup_in_all_replicas(key,
                                                 LOOKUP_IN_SPECS,
                                                 LookupInAllReplicasOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.LookupInAllReplicas,
                                    parent_span=parent,
                                    nested_ops=SUBDOC_REPLICA_READ_OPS)

        parent = kv_validator.new_parent_span(OpName.LookupInAllReplicas)
        cb_env.collection.lookup_in_all_replicas(key, LOOKUP_IN_SPECS, span=parent)
        kv_validator.validate_kv_op(OpName.LookupInAllReplicas,
                                    parent_span=parent,
                                    nested_ops=SUBDOC_REPLICA_READ_OPS)

    @ANY_REPLICA_OPEN_SIBLING_RERUN
    @pytest.mark.usefixtures('check_subdoc_replica_read_supported')
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_lookup_in_any_replica_with_parent(self, cb_env: SyncTestEnvironment,
                                               kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.LookupInAnyReplica)
        cb_env.collection.lookup_in_any_replica(key,
                                                LOOKUP_IN_SPECS,
                                                LookupInAnyReplicaOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.LookupInAnyReplica,
                                    parent_span=parent,
                                    nested_ops=SUBDOC_REPLICA_READ_OPS,
                                    server_duration_gt_zero=False)

        parent = kv_validator.new_parent_span(OpName.LookupInAnyReplica)
        cb_env.collection.lookup_in_any_replica(key, LOOKUP_IN_SPECS, span=parent)
        kv_validator.validate_kv_op(OpName.LookupInAnyReplica,
                                    parent_span=parent,
                                    nested_ops=SUBDOC_REPLICA_READ_OPS,
                                    server_duration_gt_zero=False)

    def test_mutate_in_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.MutateIn)
        cb_env.collection.mutate_in(key, MUTATE_IN_SPECS, MutateInOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.MutateIn, parent_span=parent)

        parent = kv_validator.new_parent_span(OpName.MutateIn)
        cb_env.collection.mutate_in(key, MUTATE_IN_SPECS, span=parent)
        kv_validator.validate_kv_op(OpName.MutateIn, parent_span=parent)

    def test_remove_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.Remove)
        cb_env.collection.remove(key, RemoveOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Remove, parent_span=parent)

        key = cb_env.get_existing_doc(key_only=True)
        parent = kv_validator.new_parent_span(OpName.Remove)
        cb_env.collection.remove(key, span=parent)
        kv_validator.validate_kv_op(OpName.Remove, parent_span=parent)

    def test_unlock_with_parent(self, cb_env: SyncTestEnvironment, kv_validator: KeyValueSpanValidator) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.get_and_lock(key, FIVE_SECONDS)
        parent = kv_validator.new_parent_span(OpName.Unlock)
        cb_env.collection.unlock(key, result.cas, UnlockOptions(parent_span=parent))
        kv_validator.validate_kv_op(OpName.Unlock, parent_span=parent)

        result = cb_env.collection.get_and_lock(key, FIVE_SECONDS)
        parent = kv_validator.new_parent_span(OpName.Unlock)
        cb_env.collection.unlock(key, result.cas, span=parent)
        kv_validator.validate_kv_op(OpName.Unlock, parent_span=parent)


class KeyValueTracingTests(KeyValueTracingTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(KeyValueTracingTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[TracingType.Basic,
                                                          TracingType.Legacy,
                                                          TracingType.NoOp,
                                                          TracingType.ThresholdLogging])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env,
                                KeyValueTracingTestSuite,
                                tracer=TracingEnvironment.make_tracer(request.param))
        yield env
        teardown_environment(env, KeyValueTracingTestSuite)
