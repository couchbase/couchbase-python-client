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

import types
from datetime import datetime, timedelta

import pytest

import couchbase.subdocument as SD
from couchbase.exceptions import (DocumentExistsException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException,
                                  InvalidValueException,
                                  PathExistsException,
                                  PathMismatchException,
                                  PathNotFoundException)
from couchbase.options import (GetOptions,
                               LookupInAllReplicasOptions,
                               LookupInAnyReplicaOptions,
                               LookupInOptions,
                               MutateInOptions)
from couchbase.result import (GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MutateInResult)
from tests.environments import CollectionType
from tests.environments.subdoc_environment import SubdocTestEnvironment
from tests.environments.test_environment import TestEnvironment
from tests.mock_server import MockServerType
from tests.test_features import EnvironmentFeatures


class SubDocumentTestSuite:

    TEST_MANIFEST = [
        'test_array_add_unique',
        'test_array_add_unique_create_parents',
        'test_array_add_unique_fail',
        'test_array_add_unique_mutate_in_macros',
        'test_array_append',
        'test_array_append_create_parents',
        'test_array_append_multi_insert',
        'test_array_add_append_mutate_in_macros',
        'test_array_as_document',
        'test_array_insert',
        'test_array_insert_multi_insert',
        'test_array_insert_mutate_in_macros',
        'test_array_prepend',
        'test_array_prepend_create_parents',
        'test_array_prepend_multi_insert',
        'test_array_add_prepend_mutate_in_macros',
        'test_count',
        'test_decrement',
        'test_decrement_create_parents',
        'test_increment',
        'test_increment_create_parents',
        'test_insert_create_parents',
        'test_lookup_in_all_replicas_bad_key',
        'test_lookup_in_all_replicas_exists',
        'test_lookup_in_all_replicas_exists_bad_path',
        'test_lookup_in_all_replicas_get',
        'test_lookup_in_all_replicas_get_bad_path',
        'test_lookup_in_all_replicas_get_full',
        'test_lookup_in_all_replicas_multiple_specs',
        'test_lookup_in_all_replicas_with_timeout',
        'test_lookup_in_any_replica_bad_key',
        'test_lookup_in_any_replica_exists',
        'test_lookup_in_any_replica_exists_bad_path',
        'test_lookup_in_any_replica_get',
        'test_lookup_in_any_replica_get_bad_path',
        'test_lookup_in_any_replica_get_full',
        'test_lookup_in_any_replica_multiple_specs',
        'test_lookup_in_any_replica_with_timeout',
        'test_lookup_in_macros',
        'test_lookup_in_multiple_specs',
        'test_lookup_in_one_path_not_found',
        'test_lookup_in_simple_exists',
        'test_lookup_in_simple_exists_bad_path',
        'test_lookup_in_simple_get',
        'test_lookup_in_simple_get_bad_path',
        'test_lookup_in_simple_get_spec_as_list',
        'test_lookup_in_simple_long_path',
        'test_lookup_in_simple_with_timeout',
        'test_lookup_in_valid_path_null_content',
        'test_mutate_in_expiry',
        'test_mutate_in_insert_semantics',
        'test_mutate_in_insert_semantics_fail',
        'test_mutate_in_insert_semantics_kwargs',
        'test_mutate_in_macros_insert',
        'test_mutate_in_macros_replace_upsert',
        'test_mutate_in_preserve_expiry',
        'test_mutate_in_preserve_expiry_fails',
        'test_mutate_in_preserve_expiry_not_used',
        'test_mutate_in_remove',
        'test_mutate_in_remove_blank_path',
        'test_mutate_in_replace_semantics',
        'test_mutate_in_replace_semantics_fail',
        'test_mutate_in_replace_semantics_kwargs',
        'test_mutate_in_replace_full_document',
        'test_mutate_in_simple',
        'test_mutate_in_simple_spec_as_list',
        'test_mutate_in_store_semantics_fail',
        'test_mutate_in_upsert_semantics',
        'test_mutate_in_upsert_semantics_kwargs',
        'test_upsert_create_parents',
    ]

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('preserve_expiry',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope="class")
    def check_xattr_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('xattr',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope="class")
    def skip_if_go_caves(self, cb_env):
        if cb_env.is_mock_server and cb_env.mock_server_type == MockServerType.GoCAVES:
            pytest.skip("GoCAVES does not like this operation.")

    @pytest.fixture(scope="class")
    def check_replica_read_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('subdoc_replica_read',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.mark.usefixtures('skip_if_go_caves')
    @pytest.mark.parametrize('value', [True,
                                       False,
                                       3.14159,
                                       13,
                                       'foo',
                                       None])
    def test_array_add_unique(self, cb_env, value):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(key, (SD.array_addunique('array', value),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[dict]
        assert isinstance(val['array'], list)
        assert value in val['array']

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_array_add_unique_create_parents(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('array')
        cb_env.collection.upsert(key, value)
        TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        result = cb_env.collection.mutate_in(key, (
            SD.array_addunique("new.set", "new", create_parents=True),
            SD.array_addunique("new.set", "unique"),
            SD.array_addunique("new.set", "set")))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        new_set = result.content_as[dict]["new"]["set"]
        assert isinstance(new_set, list)
        assert "new" in new_set
        assert "unique" in new_set
        assert "set" in new_set

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_array_add_unique_fail(self, cb_env):
        key = "simple-key"
        value = {
            "a": "aaa",
            "b": [0, 1, 2, 3],
            "c": [1.25, 1.5, {"nested": ["str", "array"]}],
        }
        cb_env.collection.upsert(key, value)
        TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)

        with pytest.raises(PathExistsException):
            cb_env.collection.mutate_in(key, (SD.array_addunique("b", 3),))

        with pytest.raises(InvalidValueException):
            cb_env.collection.mutate_in(key, (SD.array_addunique("b", [4, 5, 6]),))

        with pytest.raises(InvalidValueException):
            cb_env.collection.mutate_in(key, (SD.array_addunique("b", {"c": "d"}),))

        with pytest.raises(PathMismatchException):
            cb_env.collection.mutate_in(key, (SD.array_addunique("c", 2),))

    @pytest.mark.usefixtures('skip_if_go_caves')
    @pytest.mark.usefixtures('check_xattr_supported')
    def test_array_add_unique_mutate_in_macros(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        xattr_array = 'xattr_array'
        specs = [
            SD.upsert(xattr_array, [], xattr=True),
            SD.array_addunique(xattr_array, SD.MutationMacro.cas()),
            SD.array_addunique(xattr_array, SD.MutationMacro.seq_no()),
            SD.array_addunique(xattr_array, SD.MutationMacro.value_crc32c()),
        ]
        # TODO: PYCBC-1557: Server raises invalid argument when using mutate-in macro w/ arrayaddunique.
        # Update test if associated MB is addressed in the future.
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.mutate_in(key, specs)

    def test_array_append(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.array_append('array', 6),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[dict]
        assert isinstance(val['array'], list)
        assert len(val['array']) == 6
        assert val['array'][5] == 6

    def test_array_append_create_parents(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(key, (
            SD.array_append('new.array', 'Hello,', create_parents=True),
            SD.array_append('new.array', 'World!'),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['new']['array'] == [
            'Hello,', 'World!']

    def test_array_append_multi_insert(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.array_append('array', 8, 9, 10),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[dict]
        assert isinstance(val['array'], list)
        app_res = val['array'][5:]
        assert len(app_res) == 3
        assert app_res == [8, 9, 10]

    @pytest.mark.usefixtures('skip_if_go_caves')
    @pytest.mark.usefixtures('check_xattr_supported')
    def test_array_add_append_mutate_in_macros(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        xattr_array = 'xattr_array'
        specs = [
            SD.array_append(xattr_array, SD.MutationMacro.cas(), create_parents=True),
            SD.array_append(xattr_array, SD.MutationMacro.seq_no()),
            SD.array_append(xattr_array, SD.MutationMacro.value_crc32c()),
        ]
        result = cb_env.collection.mutate_in(key, specs)
        assert isinstance(result, MutateInResult)
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(xattr_array, xattr=True),))

        res_array = res.content_as[list](0)
        assert len(res_array) == 3
        assert all(map(lambda x: isinstance(x, str), res_array)) is True
        res_cas = res_array[0]
        res_seqno = res_array[1]
        res_value = res_array[2]

        assert res_cas.startswith('0x') is True
        assert res.cas == SD.convert_macro_cas_to_cas(res_cas)
        assert res_seqno.startswith('0x') is True
        assert res_value.startswith('0x') is True

    def test_array_as_document(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array_only', key_only=True)
        result = cb_env.collection.mutate_in(key, (SD.array_append(
            '', 2), SD.array_prepend('', 0), SD.array_insert('[1]', 1),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[list]
        assert isinstance(val, list)
        assert len(val) == 3
        assert val[0] == 0
        assert val[1] == 1
        assert val[2] == 2

    def test_array_insert(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.array_insert('array.[2]', 10),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[dict]
        assert isinstance(val['array'], list)
        assert len(val['array']) == 6
        assert val['array'][2] == 10

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_array_insert_multi_insert(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.array_insert('array.[3]', 6, 7, 8),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[dict]
        assert isinstance(val['array'], list)
        ins_res = val['array'][3:6]
        assert len(ins_res) == 3
        assert ins_res == [6, 7, 8]

    @pytest.mark.usefixtures('skip_if_go_caves')
    @pytest.mark.usefixtures('check_xattr_supported')
    def test_array_insert_mutate_in_macros(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        xattr_array = 'xattr_array'
        specs = [
            SD.upsert(xattr_array, [], xattr=True),
            SD.array_insert(f'{xattr_array}.[0]', SD.MutationMacro.cas()),
            SD.array_insert(f'{xattr_array}.[0]', SD.MutationMacro.seq_no()),
            SD.array_insert(f'{xattr_array}.[0]', SD.MutationMacro.value_crc32c()),
        ]
        result = cb_env.collection.mutate_in(key, specs)
        assert isinstance(result, MutateInResult)
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(xattr_array, xattr=True),))

        res_array = res.content_as[list](0)
        assert len(res_array) == 3
        assert all(map(lambda x: isinstance(x, str), res_array)) is True
        # prepending -- order is reversed
        res_cas = res_array[2]
        res_seqno = res_array[1]
        res_value = res_array[0]

        assert res_cas.startswith('0x') is True
        assert res.cas == SD.convert_macro_cas_to_cas(res_cas)
        assert res_seqno.startswith('0x') is True
        assert res_value.startswith('0x') is True

    def test_array_prepend(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.array_prepend('array', 0),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[dict]
        assert isinstance(val['array'], list)
        assert len(val['array']) == 6
        assert val['array'][0] == 0

    def test_array_prepend_create_parents(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(key, (
            SD.array_prepend('new.array', 'World!', create_parents=True),
            SD.array_prepend('new.array', 'Hello,'),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['new']['array'] == [
            'Hello,', 'World!']

    def test_array_prepend_multi_insert(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.array_prepend('array', -2, -1, 0),))
        assert isinstance(result, MutateInResult)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        val = result.content_as[dict]
        assert isinstance(val['array'], list)
        pre_res = val['array'][:3]
        assert len(pre_res) == 3
        assert pre_res == [-2, -1, 0]

    @pytest.mark.usefixtures('skip_if_go_caves')
    @pytest.mark.usefixtures('check_xattr_supported')
    def test_array_add_prepend_mutate_in_macros(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        xattr_array = 'xattr_array'
        specs = [
            SD.array_prepend(xattr_array, SD.MutationMacro.cas(), create_parents=True),
            SD.array_prepend(xattr_array, SD.MutationMacro.seq_no()),
            SD.array_prepend(xattr_array, SD.MutationMacro.value_crc32c()),
        ]
        result = cb_env.collection.mutate_in(key, specs)
        assert isinstance(result, MutateInResult)
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(xattr_array, xattr=True),))

        res_array = res.content_as[list](0)
        assert len(res_array) == 3
        assert all(map(lambda x: isinstance(x, str), res_array)) is True
        # prepending -- order is reversed
        res_cas = res_array[2]
        res_seqno = res_array[1]
        res_value = res_array[0]

        assert res_cas.startswith('0x') is True
        assert res.cas == SD.convert_macro_cas_to_cas(res_cas)
        assert res_seqno.startswith('0x') is True
        assert res_value.startswith('0x') is True

    def test_count(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.lookup_in(key, (SD.count('array'),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 5

    def test_decrement(self, cb_env):
        key = cb_env.get_existing_doc_by_type('count', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.decrement('count', 50),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['count'] == 50

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_decrement_create_parents(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.decrement('new.counter', 100, create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['new']['counter'] == -100

    def test_increment(self, cb_env):
        key = cb_env.get_existing_doc_by_type('count', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.increment('count', 50),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['count'] == 150

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_increment_create_parents(self, cb_env):
        key = cb_env.get_existing_doc_by_type('count', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.increment('new.counter', 100, create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['new']['counter'] == 100

    def test_insert_create_parents(self, cb_env):
        key = cb_env.get_existing_doc_by_type('array', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.insert('new.path', 'parents created', create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['new']['path'] == 'parents created'

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_bad_key(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.lookup_in_all_replicas('asdfgh', [SD.exists('batch')])

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_exists(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        results = cb_env.collection.lookup_in_all_replicas(key, [SD.exists('batch')])
        active_count = 0
        for result in results:
            assert isinstance(result, LookupInReplicaResult)
            assert result.exists(0)
            assert result.is_replica is not None
            active_count += not result.is_replica
        assert active_count == 1

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_exists_bad_path(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        results = cb_env.collection.lookup_in_all_replicas(key, [SD.exists('qzzxy')])
        active_count = 0
        for result in results:
            assert isinstance(result, LookupInReplicaResult)
            assert not result.exists(0)
            assert result.is_replica is not None
            active_count += not result.is_replica
        assert active_count == 1

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_get(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        results = cb_env.collection.lookup_in_all_replicas(key, [SD.get('batch')])
        active_count = 0
        for result in results:
            assert isinstance(result, LookupInReplicaResult)
            assert result.content_as[str](0) == value['batch']
            assert result.is_replica is not None
            active_count += not result.is_replica
        assert active_count == 1

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_get_bad_path(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        results = cb_env.collection.lookup_in_all_replicas(key, [SD.get('qzzxy')])
        active_count = 0
        for result in results:
            assert isinstance(result, LookupInReplicaResult)
            with pytest.raises(PathNotFoundException):
                result.content_as[str](0)
            assert result.is_replica is not None
            active_count += not result.is_replica
        assert active_count == 1

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_get_full(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        results = cb_env.collection.lookup_in_all_replicas(key, [SD.get_full()])
        active_count = 0
        for result in results:
            assert isinstance(result, LookupInReplicaResult)
            assert result.content_as[dict](0) == value
            assert result.is_replica is not None
            active_count += not result.is_replica
        assert active_count == 1

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_multiple_specs(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        results = cb_env.collection.lookup_in_all_replicas(key, [SD.get('batch'), SD.exists('manufacturer.city')])
        active_count = 0
        for result in results:
            assert isinstance(result, LookupInReplicaResult)
            assert result.content_as[str](0) == value['batch']
            assert result.exists(1)
            assert result.is_replica is not None
            active_count += not result.is_replica
        assert active_count == 1

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_all_replicas_with_timeout(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        opts = LookupInAllReplicasOptions(timeout=timedelta(milliseconds=5000))
        results = cb_env.collection.lookup_in_all_replicas(key, [SD.get('batch')], opts)
        active_count = 0
        for result in results:
            assert isinstance(result, LookupInReplicaResult)
            assert result.content_as[str](0) == value['batch']
            assert result.is_replica is not None
            active_count += not result.is_replica
        assert active_count == 1

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_bad_key(self, cb_env):
        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.lookup_in_any_replica('asdfgh', [SD.exists('batch')])

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_exists(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.lookup_in_any_replica(key, [SD.exists('batch')])
        assert isinstance(result, LookupInReplicaResult)
        assert result.exists(0)
        assert result.is_replica is not None

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_exists_bad_path(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.lookup_in_any_replica(key, [SD.exists('qzzxy')])
        assert isinstance(result, LookupInReplicaResult)
        assert not result.exists(0)
        assert result.is_replica is not None

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_get(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.lookup_in_any_replica(key, [SD.get('batch')])
        assert isinstance(result, LookupInReplicaResult)
        assert result.content_as[str](0) == value['batch']
        assert result.is_replica is not None

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_get_bad_path(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.lookup_in_any_replica(key, [SD.get('qzzxy')])
        assert isinstance(result, LookupInReplicaResult)
        with pytest.raises(PathNotFoundException):
            result.content_as[str](0)
        assert result.is_replica is not None

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_get_full(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.lookup_in_any_replica(key, [SD.get_full()])
        assert isinstance(result, LookupInReplicaResult)
        assert result.content_as[dict](0) == value
        assert result.is_replica is not None

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_multiple_specs(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.lookup_in_any_replica(key, [SD.get('batch'), SD.exists('manufacturer.city')])
        assert isinstance(result, LookupInReplicaResult)
        assert result.content_as[str](0) == value['batch']
        assert result.exists(1)
        assert result.is_replica is not None

    @pytest.mark.usefixtures('check_replica_read_supported')
    def test_lookup_in_any_replica_with_timeout(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        opts = LookupInAnyReplicaOptions(timeout=timedelta(milliseconds=5000))
        result = cb_env.collection.lookup_in_any_replica(key, [SD.get('batch')], opts)
        assert isinstance(result, LookupInReplicaResult)
        assert result.content_as[str](0) == value['batch']
        assert result.is_replica is not None

    @pytest.mark.usefixtures('check_xattr_supported')
    @pytest.mark.parametrize('macro',
                             [getattr(SD.LookupInMacro, m)() for m in SD.LookupInMacro.__dict__.keys() if isinstance(getattr(SD.LookupInMacro, m),  # noqa: E501
                                                                                                                     types.FunctionType)])  # noqa: E501
    def test_lookup_in_macros(self, cb_env, macro):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        macro_res = cb_env.collection.lookup_in(key, (SD.get(macro, xattr=True),))
        macro_res_value = macro_res.content_as[lambda x: x](0)
        document_res = cb_env.collection.lookup_in(key, (SD.get(SD.LookupInMacro.document(), xattr=True),))
        macro_key = macro.replace('$document.', '')
        document_value = document_res.content_as[dict](0)

        assert isinstance(macro_res, LookupInResult)

        if macro_key == '$document':
            assert document_value == macro_res_value
        else:
            assert document_value[macro_key] == macro_res_value
            # we expect these to be hex values
            if macro_key in ['CAS', 'seqno', 'vbucket_uuid', 'value_crc32c']:
                assert str(macro_res_value).startswith('0x')

    @pytest.mark.usefixtures("check_xattr_supported")
    def test_lookup_in_multiple_specs(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.lookup_in(key, (SD.get('$document.exptime', xattr=True),
                                                   SD.exists('manufacturer'),
                                                   SD.get('manufacturer'),
                                                   SD.get('manufacturer.geo.accuracy'),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 0
        assert result.exists(1)
        assert result.content_as[dict](2) == value['manufacturer']
        assert result.content_as[str](3) == value['manufacturer']['geo']['accuracy']

    def test_lookup_in_one_path_not_found(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.lookup_in(
            key, (SD.exists('batch'), SD.exists('qzzxy'),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        assert not result.exists(1)
        # PYCBC-1480, update exists to follow RFC
        assert result.content_as[bool](0) is True
        assert result.content_as[bool](1) is False

    def test_lookup_in_simple_exists(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.lookup_in(key, (SD.exists('batch'),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        # PYCBC-1480, update exists to follow RFC
        assert result.content_as[bool](0) is True

    def test_lookup_in_simple_exists_bad_path(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.lookup_in(key, (SD.exists('qzzxy'),))
        assert isinstance(result, LookupInResult)
        assert not result.exists(0)
        assert result.content_as[bool](0) is False

    def test_lookup_in_simple_get(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.lookup_in(key, (SD.get('batch'),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value['batch']

    def test_lookup_in_simple_get_bad_path(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.lookup_in(key, (SD.get('qzzxy'),))
        assert isinstance(result, LookupInResult)
        with pytest.raises(PathNotFoundException):
            result.content_as[str](0)

    def test_lookup_in_simple_get_spec_as_list(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.lookup_in(key, [SD.get('batch')])
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value['batch']

    def test_lookup_in_simple_long_path(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        result = cb_env.collection.lookup_in(
            key, (SD.get('manufacturer.geo.location.tz'),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value['manufacturer']['geo']['location']['tz']

    def test_lookup_in_simple_with_timeout(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.lookup_in(key,
                                             (SD.get('batch'),),
                                             LookupInOptions(timeout=timedelta(milliseconds=5000)))
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value['batch']

    def test_lookup_in_valid_path_null_content(self, cb_env):
        key, value = cb_env.get_new_doc_by_type('vehicle')
        value['empty_field'] = None
        cb_env.collection.upsert(key, value)
        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert 'empty_field' in res.content_as[dict]
        result = cb_env.collection.lookup_in(key, (SD.get('empty_field'), SD.get('batch')))
        assert isinstance(result, LookupInResult)
        assert result.content_as[lambda x: x](0) is None

    @pytest.mark.usefixtures("check_xattr_supported")
    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_expiry(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.mutate_in(key,
                                             (SD.upsert("make", "New Make"),
                                              SD.replace("model", "New Model")),
                                             MutateInOptions(expiry=timedelta(seconds=1000)))

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        TestEnvironment.try_n_times(10, 3, cas_matches, cb_env.collection, result.cas)

        result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        expires_in = (result.expiry_time - datetime.now()).total_seconds()
        assert expires_in > 0 and expires_in < 1021

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_insert_semantics(self, cb_env):
        key = cb_env.get_new_doc_by_type('vehicle', key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

        cb_env.collection.mutate_in(key,
                                    (SD.insert('new_path', 'im new'),),
                                    MutateInOptions(store_semantics=SD.StoreSemantics.INSERT))

        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_insert_semantics_kwargs(self, cb_env):
        key = cb_env.get_new_doc_by_type('vehicle', key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

        cb_env.collection.mutate_in(key,
                                    (SD.insert('new_path', 'im new'),),
                                    insert_doc=True)

        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_insert_semantics_fail(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        with pytest.raises(DocumentExistsException):
            cb_env.collection.mutate_in(key,
                                        (SD.insert('new_path', 'im new'),),
                                        insert_doc=True)

    @pytest.mark.usefixtures('skip_if_go_caves')
    @pytest.mark.usefixtures('check_xattr_supported')
    @pytest.mark.parametrize('macro', [SD.MutationMacro.cas(),
                                       SD.MutationMacro.seq_no(),
                                       SD.MutationMacro.value_crc32c()])
    def test_mutate_in_macros_insert(self, cb_env, macro):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        xattr_key = 'xattr_key'
        cb_env.collection.mutate_in(key, (SD.insert(xattr_key, macro),))
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(xattr_key, xattr=True),))
        macro_res = res.content_as[str](0)
        assert macro_res.startswith('0x') is True
        if 'CAS' in macro.value:
            assert res.cas == SD.convert_macro_cas_to_cas(macro_res)

    @pytest.mark.usefixtures('skip_if_go_caves')
    @pytest.mark.usefixtures('check_xattr_supported')
    @pytest.mark.parametrize('macro', [SD.MutationMacro.cas(),
                                       SD.MutationMacro.seq_no(),
                                       SD.MutationMacro.value_crc32c()])
    def test_mutate_in_macros_replace_upsert(self, cb_env, macro):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        xattr_key = 'xattr_key'
        cb_env.collection.mutate_in(key, (SD.upsert(xattr_key, macro),))
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(xattr_key, xattr=True),))
        upsert_macro_res = res.content_as[str](0)
        assert upsert_macro_res.startswith('0x') is True
        if 'CAS' in macro.value:
            assert res.cas == SD.convert_macro_cas_to_cas(upsert_macro_res)

        cb_env.collection.mutate_in(key, (SD.replace(xattr_key, macro),))
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(xattr_key, xattr=True),))
        replace_macro_res = res.content_as[str](0)
        assert replace_macro_res.startswith('0x') is True
        if 'CAS' in macro.value:
            assert res.cas == SD.convert_macro_cas_to_cas(replace_macro_res)
        if 'value_crc32c' in macro.value:
            assert replace_macro_res == upsert_macro_res
        else:
            assert replace_macro_res != upsert_macro_res

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_mutate_in_preserve_expiry(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        cb_env.collection.mutate_in(key,
                                    (SD.upsert('make', 'New Make'),
                                     SD.replace('model', 'New Model')),
                                    MutateInOptions(expiry=timedelta(seconds=2)))

        expiry_path = '$document.exptime'
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb_env.collection.mutate_in(key,
                                    (SD.upsert('make', 'Updated Make'),),
                                    MutateInOptions(preserve_expiry=True))
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        TestEnvironment.sleep(3)
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_mutate_in_preserve_expiry_fails(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.mutate_in(
                key,
                (SD.insert('c', 'ccc'),),
                MutateInOptions(preserve_expiry=True),
            )

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.mutate_in(
                key,
                (SD.replace('c', 'ccc'),),
                MutateInOptions(
                    expiry=timedelta(
                        seconds=5),
                    preserve_expiry=True),
            )

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_mutate_in_preserve_expiry_not_used(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        cb_env.collection.mutate_in(key,
                                    (SD.upsert('make', 'New Make'),
                                     SD.replace('model', 'New Model')),
                                    MutateInOptions(expiry=timedelta(seconds=5)))

        expiry_path = '$document.exptime'
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb_env.collection.mutate_in(key, (SD.upsert('make', 'Updated Make'),))
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        TestEnvironment.sleep(3)
        result = cb_env.collection.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict]['make'] == 'Updated Make'

    def test_mutate_in_remove(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        cb_env.collection.mutate_in(key, [SD.remove('manufacturer.geo')])
        result = cb_env.collection.get(key)
        assert 'geo' not in result.content_as[dict]['manufacturer']

    def test_mutate_in_remove_blank_path(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        cb_env.collection.mutate_in(key, [SD.remove('')])
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_replace_semantics(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        cb_env.collection.mutate_in(key,
                                    (SD.upsert('new_path', 'im new'),),
                                    MutateInOptions(store_semantics=SD.StoreSemantics.REPLACE))

        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert res.content_as[dict]['new_path'] == 'im new'

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_replace_semantics_fail(self, cb_env):
        key = cb_env.get_new_doc_by_type('vehicle', key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.mutate_in(key,
                                        (SD.upsert('new_path', 'im new'),),
                                        replace_doc=True)

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_replace_semantics_kwargs(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        cb_env.collection.mutate_in(key,
                                    (SD.upsert('new_path', 'im new'),),
                                    replace_doc=True)

        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert res.content_as[dict]['new_path'] == 'im new'

    def test_mutate_in_replace_full_document(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)

        cb_env.collection.mutate_in(key,
                                    (SD.replace('', {'make': 'New Make', 'model': 'New Model'}),))

        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert res.content_as[dict]['make'] == 'New Make'
        assert res.content_as[dict]['model'] == 'New Model'

    def test_mutate_in_simple(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.mutate_in(key,
                                             (SD.upsert('make', 'New Make'),
                                              SD.replace('model', 'New Model')))

        value['make'] = 'New Make'
        value['model'] = 'New Model'

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        TestEnvironment.try_n_times(10, 3, cas_matches, cb_env.collection, result.cas)

        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    def test_mutate_in_simple_spec_as_list(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('vehicle')
        result = cb_env.collection.mutate_in(key,
                                             [SD.upsert('make', 'New Make'),
                                              SD.replace('model', 'New Model')])

        value['make'] = 'New Make'
        value['model'] = 'New Model'

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        TestEnvironment.try_n_times(10, 3, cas_matches, cb_env.collection, result.cas)

        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    def test_mutate_in_store_semantics_fail(self, cb_env):
        key = cb_env.get_new_doc_by_type('vehicle', key_only=True)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.mutate_in(key,
                                        (SD.upsert('new_path', 'im new'),),
                                        insert_doc=True, upsert_doc=True)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.mutate_in(key,
                                        (SD.upsert('new_path', 'im new'),),
                                        insert_doc=True, replace_doc=True)

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.mutate_in(key,
                                        (SD.upsert('new_path', 'im new'),),
                                        upsert_doc=True, replace_doc=True)

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_upsert_semantics(self, cb_env):
        key = cb_env.get_new_doc_by_type('vehicle', key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

        cb_env.collection.mutate_in(key,
                                    (SD.upsert('new_path', 'im new'),),
                                    MutateInOptions(store_semantics=SD.StoreSemantics.UPSERT))

        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_mutate_in_upsert_semantics_kwargs(self, cb_env):
        key = cb_env.get_new_doc_by_type('vehicle', key_only=True)

        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

        cb_env.collection.mutate_in(key,
                                    (SD.upsert('new_path', 'im new'),),
                                    upsert_doc=True)

        res = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    def test_upsert_create_parents(self, cb_env):
        key = cb_env.get_existing_doc_by_type('vehicle', key_only=True)
        result = cb_env.collection.mutate_in(
            key, (SD.upsert('new.path', 'parents created', create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = cb_env.collection.get(key)
        assert result.content_as[dict]['new']['path'] == 'parents created'


# For whatever reason GoCAVES is rather flaky w/ subdocument tests
@pytest.mark.flaky(reruns=3, reruns_delay=1)
class ClassicSubDocumentTests(SubDocumentTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicSubDocumentTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicSubDocumentTests) if valid_test_method(meth)]
        compare = set(SubDocumentTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = SubdocTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt()
        cb_env.setup(request.param)

        yield cb_env

        cb_env.teardown(request.param)
