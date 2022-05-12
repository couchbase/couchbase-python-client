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

from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import (DecrementMultiOptions,
                               DecrementOptions,
                               IncrementMultiOptions,
                               IncrementOptions,
                               SignedInt64)
from couchbase.result import (CounterResult,
                              MultiCounterResult,
                              MultiMutationResult,
                              MutationResult)
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder

from ._test_utils import CollectionType, TestEnvironment


class BinaryCollectionMultiTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False)

    @pytest.fixture(name='utf8_keys')
    def get_utf8_keys(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']

        tc = RawStringTranscoder()

        def setup():
            for k in keys:
                cb_env.try_n_times(10, 1, cb_env.collection.upsert, k, '', transcoder=tc)
                cb_env.try_n_times(10, 1, cb_env.collection.get, k, transcoder=tc)

        cb_env.try_n_times(3, 5, setup)
        yield keys
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove_multi,
                                          keys,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3,
                                          return_exceptions=False)

    @pytest.fixture(name='byte_keys')
    def get_byte_keys(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']

        tc = RawBinaryTranscoder()

        def setup():
            for k in keys:
                cb_env.try_n_times(10, 1, cb_env.collection.upsert, k, b'', transcoder=tc)
                cb_env.try_n_times(10, 1, cb_env.collection.get, k, transcoder=tc)

        cb_env.try_n_times(3, 5, setup)
        yield keys
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove_multi,
                                          keys,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3,
                                          return_exceptions=False)

    @pytest.fixture(name='counter_keys')
    def get_counter_keys(self, cb_env):
        keys = ['test-key1', 'test-key2', 'test-key3', 'test-key4']
        yield keys
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove_multi,
                                          keys,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3,
                                          return_exceptions=False)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_append_multi_string(self, cb_env, utf8_keys):
        keys = utf8_keys
        values = ['foo', 'bar', 'baz', 'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_prepend_multi_string(self, cb_env, utf8_keys):
        keys = utf8_keys
        values = ['foo', 'bar', 'baz', 'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_append_multi_bytes(self, cb_env, byte_keys):
        keys = byte_keys
        values = [b'foo', b'bar', b'baz', b'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().append_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_prepend_multi_bytes(self, cb_env, byte_keys):
        keys = byte_keys
        values = [b'foo', b'bar', b'baz', b'qux']
        keys_and_docs = dict(zip(keys, values))
        res = cb_env.collection.binary().prepend_multi(keys_and_docs)
        assert isinstance(res, MultiMutationResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, MutationResult), res.results.values())) is True

    def test_counter_multi_increment(self, cb_env, counter_keys):
        res = cb_env.collection.binary().increment_multi(counter_keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True

    def test_counter_multi_decrement(self, cb_env, counter_keys):
        res = cb_env.collection.binary().decrement_multi(counter_keys)
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True

    def test_counter_multi_increment_non_default(self, cb_env, counter_keys):
        res = cb_env.collection.binary().increment_multi(counter_keys, IncrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_decrement_non_default(self, cb_env, counter_keys):
        res = cb_env.collection.binary().decrement_multi(counter_keys, DecrementMultiOptions(initial=SignedInt64(3)))
        assert isinstance(res, MultiCounterResult)
        assert res.all_ok is True
        assert isinstance(res.results, dict)
        assert res.exceptions == {}
        assert all(map(lambda r: isinstance(r, CounterResult), res.results.values())) is True
        for r in res.results.values():
            assert r.content == 3

    def test_counter_multi_increment_non_default_per_key(self, cb_env, counter_keys):
        key1 = counter_keys[0]
        opts = IncrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: IncrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().increment_multi(counter_keys, opts)
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

    def test_counter_multi_decrement_non_default_per_key(self, cb_env, counter_keys):
        key1 = counter_keys[0]
        opts = DecrementMultiOptions(initial=SignedInt64(3), per_key_options={
                                     key1: DecrementOptions(initial=SignedInt64(100))})
        res = cb_env.collection.binary().decrement_multi(counter_keys, opts)
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
