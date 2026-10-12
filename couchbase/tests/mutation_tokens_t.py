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

from __future__ import annotations

from collections.abc import Generator

import pytest

import couchbase.subdocument as SD
from tests.environments.test_environment import (CollectionStrategy,
                                                 ConnectionStrategy,
                                                 SuiteRequirements,
                                                 SyncTestEnvironment,
                                                 TestEnvironment,
                                                 build_environment,
                                                 teardown_environment)
from tests.manifest import validate_test_manifest
from tests.skips import server_label, skip_mock_limitation


class MutationTokensEnabledTestSuite(SuiteRequirements):
    # Mutation tokens are on by default, so this suite needs no special connection.  The
    # verification lives in `cb_env.verify_mutation_tokens` rather than as a local helper;
    # see the note on the disabled suite below.

    TEST_MANIFEST = [
        'test_mutation_tokens_enabled_insert',
        'test_mutation_tokens_enabled_mutate_in',
        'test_mutation_tokens_enabled_remove',
        'test_mutation_tokens_enabled_replace',
        'test_mutation_tokens_enabled_upsert',
    ]

    def test_mutation_tokens_enabled_insert(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.insert, key, value)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_mutate_in(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5,
                                             3,
                                             cb_env.collection.mutate_in,
                                             key,
                                             (SD.upsert('make', 'New Make'),
                                              SD.replace('model', 'New Model'),))
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_remove(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.remove, key)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_replace(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.replace, key, value)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_upsert(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.upsert, key, value)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)


class MutationTokensDisabledTestSuite(SuiteRequirements):
    # NEW_CONNECTION is load-bearing: `enable_mutation_tokens` is a cluster option fixed at
    # connect time, so the only way to exercise the disabled path is a cluster of this
    # suite's own.  @FINDING(PYCBC-1885): both verifications go through the shared
    # `cb_env.verify_mutation_tokens*` helpers, and a check that asserts only `isinstance` on
    # the three token fields cannot tell an enabled token from an empty one.  The shared
    # helpers assert nothing about a nonzero partition_id, which is the flake half of 1885.

    CONNECTION_STRATEGY = ConnectionStrategy.NEW_CONNECTION

    TEST_MANIFEST = [
        'test_mutation_tokens_disabled_insert',
        'test_mutation_tokens_disabled_mutate_in',
        'test_mutation_tokens_disabled_remove',
        'test_mutation_tokens_disabled_replace',
        'test_mutation_tokens_disabled_upsert',
    ]

    def test_mutation_tokens_disabled_insert(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.insert, key, value)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_mutate_in(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5,
                                             3,
                                             cb_env.collection.mutate_in,
                                             key,
                                             (SD.upsert('make', 'New Make'),
                                              SD.replace('model', 'New Model'),))
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_remove(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.remove, key)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_replace(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.replace, key, value)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_upsert(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.upsert, key, value)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)


class MutationTokensEnabledTests(MutationTokensEnabledTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(MutationTokensEnabledTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env',
                    params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, MutationTokensEnabledTestSuite, request.param)
        yield env
        teardown_environment(env, MutationTokensEnabledTestSuite, request.param)


class MutationTokensDisabledTests(MutationTokensDisabledTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(MutationTokensDisabledTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env',
                    params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest
                                   ) -> Generator[SyncTestEnvironment, None, None]:
        # Checked before the connection is built, not inside the tests: on the mock every
        # one of these would open a cluster only to skip, and the whole point of
        # NEW_CONNECTION here is that the cluster is expensive and per-class.
        if cb_base_env.is_mock_server:
            skip_mock_limitation(mock=cb_base_env.mock_server_type,
                                 server=server_label(cb_base_env.is_mock_server),
                                 note='the mock does not support disabling mutation tokens')

        env = build_environment(cb_base_env,
                                MutationTokensDisabledTestSuite,
                                request.param,
                                enable_mutation_tokens=False)
        yield env
        teardown_environment(env, MutationTokensDisabledTestSuite, request.param)
