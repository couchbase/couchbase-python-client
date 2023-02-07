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

import pytest

from couchbase.options import (DecrementMultiOptions,
                               DecrementOptions,
                               IncrementMultiOptions,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import (CounterResult,
                              MultiCounterResult,
                              MultiMutationResult,
                              MutationResult)
from tests.environments import CollectionType
from tests.environments.binary_environment import BinaryTestEnvironment


class BinaryCollectionMultiTestSuite:

    TEST_MANIFEST = [
        'test_append_multi_bytes',
        'test_append_multi_string',
        'test_counter_multi_decrement',
        'test_counter_multi_decrement_non_default',
        'test_counter_multi_decrement_non_default_per_key',
        'test_counter_multi_increment',
        'test_counter_multi_increment_non_default',
        'test_counter_multi_increment_non_default_per_key',
        'test_prepend_multi_bytes',
        'test_prepend_multi_string',
    ]

    def test_append_multi_bytes(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('bytes_empty', 4)
        values = [b'foo', b'bar', b'baz', b'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_append_multi_string(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('utf8_empty', 4)
        values = ['foo', 'bar', 'baz', 'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_counter_multi_decrement(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('counter_empty', 4)
        res = cb_env.collection.binary().decrement_multi(keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True

    def test_counter_multi_decrement_non_default(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('counter_empty', 4)
        res = cb_env.collection.binary().decrement_multi(keys, DecrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_decrement_non_default_per_key(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('counter_empty', 4)
        key1 = keys[0]
        opts = DecrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: DecrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().decrement_multi(keys, opts)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for k, v in res.results.items():
            if k == key1:
                assert v.content == 100
            else:
                assert v.content == 3

    def test_counter_multi_increment(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('counter_empty', 4)
        res = cb_env.collection.binary().increment_multi(keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True

    def test_counter_multi_increment_non_default(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('counter_empty', 4)
        res = cb_env.collection.binary().increment_multi(keys, IncrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_increment_non_default_per_key(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('counter_empty', 4)
        key1 = keys[0]
        opts = IncrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: IncrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().increment_multi(keys, opts)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for k, v in res.results.items():
            if k == key1:
                assert v.content == 100
            else:
                assert v.content == 3

    def test_prepend_multi_bytes(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('bytes_empty', 4)
        values = [b'foo', b'bar', b'baz', b'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_prepend_multi_string(self, cb_env):
        keys = cb_env.get_multiple_existing_docs_by_type('utf8_empty', 4)
        values = ['foo', 'bar', 'baz', 'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True


class ClassicBinaryCollectionMultiTests(BinaryCollectionMultiTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBinaryCollectionMultiTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBinaryCollectionMultiTests) if valid_test_method(meth)]
        compare = set(BinaryCollectionMultiTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = BinaryTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt()
        cb_env.setup(request.param, __name__)

        yield cb_env

        cb_env.teardown(request.param)
