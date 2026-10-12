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

# Generated for all three api trees, on the same reasoning as binary_durability_t.
#
# The class names follow the filename: under `python_classes = ["*Tests"]`, two files
# declaring the same class name are collected under one name, so a failing node id cannot
# be attributed to a file without opening both.  `durability_multi_t` and
# `binary_durability_t` are the two this file would otherwise collide with.
#
# `check_sync_durability_supported` gates 10 of the 22 tests as a per-test fixture, because
# the 12 client-durability tests need no server support.  The topology guards are mutually
# exclusive, so any one cluster shape runs about half of these.
#
# Each test draws its own key, which is the reason this suite needs a pool at all.  Sharing
# one fixed key and one module-level content dict across the fourteen document-reusing
# tests couples them through that dict, and test_client_durable_mutate_in would then hold
# whether or not mutate_in did anything.

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest

import couchbase.subdocument as SD
from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  PersistToExtended,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import DocumentNotFoundException, DurabilityImpossibleException
from couchbase.options import (InsertOptions,
                               MutateInOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               UpsertOptions)
from tests.environments.managers import TestEnvironmentBucketManager
from tests.environments.test_environment import (CollectionStrategy,
                                                 SyncTestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.skips import skip_topology
from tests.suite_fixtures import TopologySuite

# CXXCBC-1056: the core gives the observe poll behind ClientDurability a fixed 5s, whatever the
# operation timeout, so a loaded cluster can report a write ambiguous that went on to persist
# and replicate.  Only the writes expected to succeed wait on it; drop this once the core
# bounds the poll by the operation timeout.
OBSERVE_DEADLINE_RERUN = pytest.mark.flaky(reruns=3, reruns_delay=1)


class DurabilityTestSuite(TopologySuite):

    # The bucket manager is what supplies num_replicas; without it the fixture reports a
    # degraded environment rather than skipping, since a suite asking for a number the
    # framework cannot produce is a wiring mistake and not a property of the cluster.
    MANAGERS = [TestEnvironmentBucketManager]
    NUM_DOCS = 20
    # A multi-node PERSIST_TO_MAJORITY write waits on disk on more than one node, and the
    # server gives up on it within the request's own timeout and reports it ambiguous.  The
    # default KV budget is short enough that a loaded cluster crosses it, which says nothing
    # about whether the write is durable.
    SERVER_DURABLE_TIMEOUT = timedelta(seconds=10)

    TEST_MANIFEST = [
        'test_client_durable_insert',
        'test_client_durable_insert_fail',
        'test_client_durable_mutate_in',
        'test_client_durable_mutate_in_fail',
        'test_client_durable_remove',
        'test_client_durable_remove_fail',
        'test_client_durable_replace',
        'test_client_durable_replace_fail',
        'test_client_durable_upsert',
        'test_client_durable_upsert_fail',
        'test_client_durable_upsert_single_node',
        'test_client_persist_to_extended',
        'test_server_durable_insert',
        'test_server_durable_insert_single_node',
        'test_server_durable_mutate_in',
        'test_server_durable_mutate_in_single_node',
        'test_server_durable_remove',
        'test_server_durable_remove_single_node',
        'test_server_durable_replace',
        'test_server_durable_replace_single_node',
        'test_server_durable_upsert',
        'test_server_durable_upsert_single_node',
    ]

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_insert(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        key, value = cb_env.get_new_doc()
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.insert(key, value, InsertOptions(durability=durability, timeout=timedelta(seconds=10)))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_insert_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        key, value = cb_env.get_new_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert(key, value, InsertOptions(durability=durability))

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_mutate_in(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        key, value = cb_env.get_existing_doc()
        value['make'] = 'New Make'
        value['model'] = 'New Model'
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.mutate_in(key,
                                    (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')),
                                    MutateInOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_mutate_in_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        key = cb_env.get_existing_doc(key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.mutate_in(key,
                                        (SD.upsert('make', 'New Make'),),
                                        MutateInOptions(durability=durability))

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_remove(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.remove(key, RemoveOptions(durability=durability))
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_remove_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        key = cb_env.get_existing_doc(key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.remove(key, RemoveOptions(durability=durability))

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_replace(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_replace_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_upsert(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.upsert(key, value,
                                 UpsertOptions(durability=durability), timeout=timedelta(seconds=3))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_upsert_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))

    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_client_durable_upsert_single_node(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    @pytest.mark.parametrize('persist_to', [PersistToExtended.NONE, PersistToExtended.ACTIVE, PersistToExtended.ONE])
    def test_client_persist_to_extended(self, cb_env: SyncTestEnvironment, persist_to: PersistToExtended) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ClientDurability(
            persist_to=persist_to, replicate_to=ReplicateTo.ONE)
        cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_insert(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        opts = InsertOptions(durability=durability, timeout=self.SERVER_DURABLE_TIMEOUT)
        cb_env.collection.insert(key, value, opts)
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_insert_single_node(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert(key, value, InsertOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_mutate_in(self, cb_env: SyncTestEnvironment) -> None:
        # No mock guard here.  A skip reading 'Mock will not return expiry in the xaddrs'
        # belongs to the subdoc suite, where the tests really do read expiry; nothing here
        # touches it.  If a real mock limitation surfaces, it gets a skip that names it.
        key, value = cb_env.get_existing_doc()
        value['make'] = 'New Make'
        value['model'] = 'New Model'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.mutate_in(key,
                                    (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model')),
                                    MutateInOptions(durability=durability, timeout=self.SERVER_DURABLE_TIMEOUT))
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_mutate_in_single_node(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.mutate_in(key,
                                        (SD.upsert('make', 'New Make'),),
                                        MutateInOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_remove(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        opts = RemoveOptions(durability=durability, timeout=self.SERVER_DURABLE_TIMEOUT)
        cb_env.collection.remove(key, opts)
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_remove_single_node(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.remove(key, RemoveOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_replace(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        opts = ReplaceOptions(durability=durability, timeout=self.SERVER_DURABLE_TIMEOUT)
        cb_env.collection.replace(key, value, opts)
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_replace_single_node(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.replace(key, value, ReplaceOptions(durability=durability))

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_upsert(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        opts = UpsertOptions(durability=durability, timeout=self.SERVER_DURABLE_TIMEOUT)
        cb_env.collection.upsert(key, value, opts)
        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_configured')
    def test_server_durable_upsert_single_node(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert(key, value, UpsertOptions(durability=durability))


class DurabilityTests(DurabilityTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(DurabilityTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, DurabilityTestSuite, request.param)
        yield env
        teardown_environment(env, DurabilityTestSuite, request.param)
