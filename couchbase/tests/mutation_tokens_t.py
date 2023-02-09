#  Copyright 2016-2022. Couchbase, Inc.
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

import pytest

import couchbase.subdocument as SD
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment


class MutationTokensDisabledTestSuite:

    TEST_MANIFEST = [
        'test_mutation_tokens_disabled_insert',
        'test_mutation_tokens_disabled_mutate_in',
        'test_mutation_tokens_disabled_remove',
        'test_mutation_tokens_disabled_replace',
        'test_mutation_tokens_disabled_upsert',
    ]

    def test_mutation_tokens_disabled_insert(self, cb_env):
        key, value = cb_env.get_new_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.insert, key, value)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_mutate_in(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5,
                                             3,
                                             cb_env.collection.mutate_in,
                                             key,
                                             (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model'),))
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_remove(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.remove, key)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_replace(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.replace, key, value)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)

    def test_mutation_tokens_disabled_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.upsert, key, value)
        cb_env.verify_mutation_tokens_disabled(cb_env.bucket.name, result)


class MutationTokensEnabledTestSuite:

    TEST_MANIFEST = [
        'test_mutation_tokens_enabled_insert',
        'test_mutation_tokens_enabled_mutate_in',
        'test_mutation_tokens_enabled_remove',
        'test_mutation_tokens_enabled_replace',
        'test_mutation_tokens_enabled_upsert',
    ]

    def test_mutation_tokens_enabled_insert(self, cb_env):
        key, value = cb_env.get_new_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.insert, key, value)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_mutate_in(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5,
                                             3,
                                             cb_env.collection.mutate_in,
                                             key,
                                             (SD.upsert('make', 'New Make'), SD.replace('model', 'New Model'),))
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_remove(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.remove, key)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_replace(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.replace, key, value)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)

    def test_mutation_tokens_enabled_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(5, 3, cb_env.collection.upsert, key, value)
        cb_env.verify_mutation_tokens(cb_env.bucket.name, result)


class ClassicMutationTokensDisabledTests(MutationTokensDisabledTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicMutationTokensDisabledTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicMutationTokensDisabledTests) if valid_test_method(meth)]
        compare = set(MutationTokensDisabledTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        if cb_base_env.is_mock_server:
            pytest.skip('Mock server does not support disabled mutation tokens.')

        cb_env = TestEnvironment.get_environment(couchbase_config=cb_base_env.config,
                                                 data_provider=cb_base_env.data_provider,
                                                 enable_mutation_tokens=False)
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)
        cb_env.cluster.close()


class ClassicMutationTokensEnabledTests(MutationTokensEnabledTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicMutationTokensEnabledTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicMutationTokensEnabledTests) if valid_test_method(meth)]
        compare = set(MutationTokensEnabledTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.setup(request.param)
        yield cb_base_env
        cb_base_env.teardown(request.param)
