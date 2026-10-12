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

# Sync-only, and not registered for mirror generation: the multi-key surface is defined on
# `couchbase.Collection` alone.  `durability_t`, the single-key suite, does generate.
#
# The class names follow the filename, which matters more here than it looks: pytest reports
# a node as `ClassName::test_...` with no path, so two suites sharing a pair of class names
# cannot be told apart from a CI failure line.  This file and `durability_t` are exactly the
# pair that would collide.
#
# The node-count, replica, server-group and sync-durability fixtures come from
# `TopologySuite`, and inheriting it is what makes the environment gather the facts they read.

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest

from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  PersistToExtended,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import DurabilityImpossibleException, InvalidArgumentException
from couchbase.options import (InsertMultiOptions,
                               InsertOptions,
                               RemoveMultiOptions,
                               RemoveOptions,
                               ReplaceMultiOptions,
                               ReplaceOptions,
                               UpsertMultiOptions,
                               UpsertOptions)
from couchbase.result import MultiMutationResult, MutationResult
from tests.assert_helpers import assert_all_expired
from tests.environments.managers import TestEnvironmentBucketManager
from tests.environments.test_environment import (CollectionStrategy,
                                                 DocType,
                                                 SyncTestEnvironment,
                                                 build_environment,
                                                 teardown_environment,
                                                 verify_seeded_docs_readable)
from tests.manifest import validate_test_manifest
from tests.skips import skip_topology
from tests.suite_fixtures import TopologySuite

# CXXCBC-1056: the core gives the observe poll behind ClientDurability a fixed 5s, whatever the
# operation timeout, so a loaded cluster can report a write ambiguous that went on to persist
# and replicate.  Only the writes expected to succeed wait on it; drop this once the core
# bounds the poll by the operation timeout.
OBSERVE_DEADLINE_RERUN = pytest.mark.flaky(reruns=3, reruns_delay=1)


class DurabilityMultiTestSuite(TopologySuite):

    MANAGERS = [TestEnvironmentBucketManager]
    DATA_SET = [DocType.SIMPLE]
    NUM_DOCS = 100
    # A multi-node PERSIST_TO_MAJORITY write waits on disk on more than one node, and the
    # server gives up on it within the request's own timeout and reports it ambiguous.  The
    # default KV budget is short enough that a loaded cluster crosses it, which says nothing
    # about whether the write is durable.
    SERVER_DURABLE_TIMEOUT = timedelta(seconds=10)

    TEST_MANIFEST = [
        'test_client_durable_insert_multi',
        'test_client_durable_insert_multi_fail',
        'test_client_durable_remove_multi',
        'test_client_durable_remove_multi_fail',
        'test_client_durable_replace_multi',
        'test_client_durable_replace_multi_fail',
        'test_client_durable_upsert_multi',
        'test_client_durable_upsert_multi_fail',
        'test_multi_mixed_durability_fail',
        'test_server_durable_insert_multi',
        'test_server_durable_insert_multi_single_node',
        'test_server_durable_remove_multi',
        'test_server_durable_remove_multi_single_node',
        'test_server_durable_replace_multi',
        'test_server_durable_replace_multi_single_node',
        'test_server_durable_upsert_multi',
        'test_server_durable_upsert_multi_single_node',
    ]

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_insert_multi(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_insert_multi_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

        keys_and_docs = cb_env.get_new_docs(4)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert_multi(keys_and_docs, durability=durability, return_exceptions=False)

        keys_and_docs = cb_env.get_new_docs(4)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability,
                                                                             return_exceptions=False))

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_remove_multi(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ClientDurability(persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env, keys, ctx='after a client-durable remove_multi')

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_remove_multi_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_replace_multi(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_replace_multi_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @OBSERVE_DEADLINE_RERUN
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_upsert_multi(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_client_durable_upsert_multi_fail(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        if num_replicas > 2:
            skip_topology(replicas=num_replicas,
                          note='PersistToExtended.FOUR is satisfiable with this many replicas')
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert_multi(keys_and_docs, durability=durability, return_exceptions=False)

        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability,
                                                                             return_exceptions=False))

    @pytest.mark.parametrize(
        'op_info',
        [
            ('insert_multi', True, InsertMultiOptions, InsertOptions),
            ('upsert_multi', True, UpsertMultiOptions, UpsertOptions),
            ('replace_multi', True, ReplaceMultiOptions, ReplaceOptions),
            ('remove_multi', False, RemoveMultiOptions, RemoveOptions),
        ]
    )
    def test_multi_mixed_durability_fail(self,
                                         cb_env: SyncTestEnvironment,
                                         op_info: tuple[str, bool, type, type]) -> None:
        op_name, has_docs, multi_opts_type, key_opts_type = op_info
        keys_and_docs = cb_env.get_new_docs(2)
        keys = list(keys_and_docs.keys())
        key1 = keys[0]

        collection_op = getattr(cb_env.collection, op_name)

        # Scenario 1: global client durability, per-key server durability
        opts = multi_opts_type(
            durability=ServerDurability(level=DurabilityLevel.MAJORITY),
            per_key_options={key1: key_opts_type(durability=ClientDurability(replicate_to=ReplicateTo.ONE))}
        )

        err_msg = 'Durability must be either client or server for all operations.'
        with pytest.raises(InvalidArgumentException, match=err_msg):
            if has_docs:
                collection_op(keys_and_docs, opts)
            else:
                collection_op(keys, opts)

        # Scenario 2: global server durability, per-key client durability
        opts = multi_opts_type(
            durability=ClientDurability(persist_to=PersistTo.ONE),
            per_key_options={key1: key_opts_type(durability=ServerDurability(
                level=DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE))}
        )

        with pytest.raises(InvalidArgumentException, match=err_msg):
            if has_docs:
                collection_op(keys_and_docs, opts)
            else:
                collection_op(keys, opts)

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_insert_multi(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability,
                                                                               timeout=self.SERVER_DURABLE_TIMEOUT))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_insert_multi_single_node(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_new_docs(4)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.insert_multi(keys_and_docs, InsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_remove_multi(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability,
                                                                      timeout=self.SERVER_DURABLE_TIMEOUT))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True
        assert_all_expired(cb_env, keys, ctx='after a server-durable remove_multi')

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_remove_multi_single_node(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        keys = list(keys_and_docs.keys())
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.remove_multi(keys, RemoveMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_replace_multi(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability,
                                                                                 timeout=self.SERVER_DURABLE_TIMEOUT))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_replace_multi_single_node(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.replace_multi(keys_and_docs, ReplaceMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_upsert_multi(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability,
                                                                               timeout=self.SERVER_DURABLE_TIMEOUT))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    @pytest.mark.usefixtures('check_sync_durability_supported')
    @pytest.mark.usefixtures('check_single_node')
    @pytest.mark.usefixtures('check_replicas_online')
    def test_server_durable_upsert_multi_single_node(self, cb_env: SyncTestEnvironment) -> None:
        keys_and_docs = cb_env.get_docs(4)
        for _, v in keys_and_docs.items():
            v['what'] = 'An updated doc!'
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        res = cb_env.collection.upsert_multi(keys_and_docs, UpsertMultiOptions(durability=durability))
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is False
        assert res.results == {}
        assert isinstance(res.exceptions, dict)
        assert all(map(lambda e: issubclass(type(e), DurabilityImpossibleException), res.exceptions.values())) is True


class DurabilityMultiTests(DurabilityMultiTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(DurabilityMultiTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, DurabilityMultiTestSuite, request.param)
        # Kept per-suite rather than promoted into setup(); see verify_seeded_docs_readable.
        verify_seeded_docs_readable(env)
        yield env
        teardown_environment(env, DurabilityMultiTestSuite, request.param)
