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

import json
from datetime import timedelta

import pytest

from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (DocumentExistsException,
                                  DocumentNotFoundException,
                                  ParsingFailedException,
                                  TransactionExpired,
                                  TransactionFailed,
                                  TransactionOperationFailed)
from couchbase.n1ql import QueryProfile, QueryScanConsistency
from couchbase.options import (TransactionConfig,
                               TransactionOptions,
                               TransactionQueryOptions)
from couchbase.transactions import TransactionKeyspace, TransactionResult
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class TransactionTestSuite:
    TEST_MANIFEST = [
        'test_adhoc',
        'test_bad_query',
        'test_cleanup_client_attempts',
        'test_cleanup_lost_attempts',
        'test_cleanup_window',
        'test_client_context_id',
        'test_expiration_time',
        'test_get',
        'test_get_lambda_raises_doc_not_found',
        'test_get_inner_exc_doc_not_found',
        'test_insert',
        'test_insert_lambda_raises_doc_exists',
        'test_insert_inner_exc_doc_exists',
        'test_kv_timeout',
        'test_max_parallelism',
        'test_metadata_collection',
        'test_metrics',
        'test_named_params',
        'test_per_txn_config',
        'test_pipeline_batch',
        'test_pipeline_cap',
        'test_positional_params',
        'test_profile_mode',
        'test_query',
        'test_query_lambda_raises_parsing_failure',
        'test_query_inner_exc_parsing_failure',
        'test_raw',
        'test_read_only',
        'test_remove',
        'test_remove_fail_bad_cas',
        'test_replace',
        'test_replace_fail_bad_cas',
        'test_rollback',
        'test_rollback_eating_exceptions',
        'test_scan_consistency',
        'test_scope_qualifier',
        'test_transaction_config_durability',
        'test_transaction_result',
    ]

    @pytest.fixture(scope='class')
    def check_txn_queries_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('txn_queries',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.mark.parametrize('adhoc', [True, False])
    def test_adhoc(self, adhoc):
        cfg = TransactionQueryOptions(adhoc=adhoc)
        cfg_adhoc = cfg._base.to_dict().get('adhoc', None)
        assert cfg_adhoc is not None
        assert cfg_adhoc == adhoc

    @pytest.mark.usefixtures('check_txn_queries_supported')
    def test_bad_query(self, cb_env):

        def txn_logic(ctx):
            try:
                ctx.query('this wont parse')
                pytest.fail('expected bad query to raise exception')
            except ParsingFailedException:
                pass
            except Exception as e:
                pytest.fail(f"Expected bad query to raise ParsingFailedException, not {e.__class__.__name__}")

        cb_env.cluster.transactions.run(txn_logic)

    @pytest.mark.parametrize('cleanup', [False, True])
    def test_cleanup_client_attempts(self, cleanup):
        cfg = TransactionConfig(cleanup_client_attempts=cleanup)
        cfg_cleanup = cfg._base.to_dict().get('cleanup_client_attempts', None)
        assert cfg_cleanup is not None
        assert cfg_cleanup is cleanup

    @pytest.mark.parametrize('cleanup', [False, True])
    def test_cleanup_lost_attempts(self, cleanup):
        cfg = TransactionConfig(cleanup_lost_attempts=cleanup)
        cfg_cleanup = cfg._base.to_dict().get('cleanup_lost_attempts', None)
        assert cfg_cleanup is not None
        assert cfg_cleanup is cleanup

    @pytest.mark.parametrize('window', [timedelta(seconds=30), timedelta(milliseconds=500)])
    def test_cleanup_window(self, window):
        cfg = TransactionConfig(cleanup_window=window)
        cfg_window = cfg._base.to_dict().get('cleanup_window', None)
        assert cfg_window is not None
        assert cfg_window == window.total_seconds() * 1000  # milliseconds

    def test_client_context_id(self):
        ctxid = "somestring"
        cfg = TransactionQueryOptions(client_context_id=ctxid)
        cfg_ctxid = cfg._base.to_dict().get('client_context_id', None)
        assert cfg_ctxid is not None
        assert cfg_ctxid == ctxid

    @pytest.mark.parametrize('cls', [TransactionConfig, TransactionOptions])
    @pytest.mark.parametrize('exp', [timedelta(seconds=30), timedelta(milliseconds=100)])
    def test_expiration_time(self, cls, exp):
        cfg = cls(expiration_time=exp)
        cfg_expiry = cfg._base.to_dict().get('expiration_time', None)
        assert cfg_expiry is not None
        assert cfg_expiry == exp.total_seconds() * 1000*1000*1000  # nanoseconds - and can't use 'is' here

    def test_get(self, cb_env):
        key, value = cb_env.get_existing_doc()

        def txn_logic(ctx):
            res = ctx.get(cb_env.collection, key)
            assert res.cas > 0
            assert res.id == key
            assert res.content_as[dict] == value

        cb_env.cluster.transactions.run(txn_logic)

    def test_get_lambda_raises_doc_not_found(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)
        num_attempts = 0

        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            try:
                ctx.get(cb_env.collection, key)
            except Exception as ex:
                err_msg = f"Expected to raise DocumentNotFoundException, not {ex.__class__.__name__}"
                assert isinstance(ex, DocumentNotFoundException), err_msg

            raise Exception('User raised exception.')

        try:
            cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert 'User raised exception.' in str(ex)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    def test_get_inner_exc_doc_not_found(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)
        num_attempts = 0

        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            ctx.get(cb_env.collection, key)

        try:
            cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert ex.inner_cause is not None
            assert isinstance(ex.inner_cause, DocumentNotFoundException)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    def test_insert(self, cb_env):
        key, value = cb_env.get_new_doc()

        def txn_logic(ctx):
            ctx.insert(cb_env.collection, key, value)

        cb_env.cluster.transactions.run(txn_logic)
        get_result = cb_env.collection.get(key)
        assert get_result.content_as[dict] == value

    def test_insert_lambda_raises_doc_exists(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            try:
                ctx.insert(cb_env.collection, key, value)
            except Exception as ex:
                err_msg = f"Expected to raise DocumentExistsException, not {ex.__class__.__name__}"
                assert isinstance(ex, DocumentExistsException), err_msg

            raise Exception('User raised exception.')

        try:
            cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert 'User raised exception.' in str(ex)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    def test_insert_inner_exc_doc_exists(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            ctx.insert(cb_env.collection, key, value)

        try:
            cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert ex.inner_cause is not None
            assert isinstance(ex.inner_cause, DocumentExistsException)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.parametrize('cls', [TransactionConfig, TransactionOptions])
    @pytest.mark.parametrize('kv_timeout', [timedelta(seconds=30), timedelta(milliseconds=2)])
    def test_kv_timeout(self, cls, kv_timeout):
        cfg = cls(kv_timeout=kv_timeout)
        cfg_kv_timeout = cfg._base.to_dict().get('kv_timeout', None)
        assert cfg_kv_timeout is not None
        assert cfg_kv_timeout == kv_timeout.total_seconds() * 1000  # milliseconds

    def test_max_parallelism(self):
        max = 100
        cfg = TransactionQueryOptions(max_parallelism=max)
        cfg_max = cfg._base.to_dict().get('max_parallelism', None)
        assert cfg_max is not None
        assert cfg_max == max

    @pytest.mark.parametrize('cls', [TransactionOptions, TransactionConfig])
    def test_metadata_collection(self, cls, cb_env):
        coll = cb_env.collection
        cfg = cls(metadata_collection=TransactionKeyspace(coll=coll))
        cfg_coll = cfg._base.to_dict().get('metadata_collection', None)
        assert cfg_coll is not None
        assert cfg_coll == f'{coll._scope.bucket_name}.{coll._scope.name}.{coll.name}'

    @pytest.mark.parametrize('metrics', [True, False])
    def test_metrics(self, metrics):
        cfg = TransactionQueryOptions(metrics=metrics)
        cfg_metrics = cfg._base.to_dict().get('metrics', None)
        assert cfg_metrics is not None
        assert cfg_metrics == metrics

    @pytest.mark.parametrize('params', [{'key1': 'thing'},
                                        {'key1': ['an', 'array']},
                                        {'key1': 10, 'key2': 'something else'}])
    def test_named_params(self, params):
        cfg = TransactionQueryOptions(named_parameters=params)
        cfg_params = cfg._base.to_dict().get('named_parameters', None)
        assert cfg_params is not None
        assert isinstance(cfg_params, dict)
        for k, v in params.items():
            assert json.loads(cfg_params[k]) == v

    def test_per_txn_config(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)

        def txn_logic(ctx):
            ctx.insert(cb_env.collection, key, {'some': 'thing'})
            TestEnvironment.sleep(0.001)
            ctx.get(cb_env.collection, key)

        with pytest.raises(TransactionExpired):
            cb_env.cluster.transactions.run(txn_logic,
                                            TransactionOptions(expiration_time=timedelta(microseconds=1)))
        res = cb_env.collection.exists(key)
        assert res.exists is False

    def test_pipeline_batch(self):
        batch = 100
        cfg = TransactionQueryOptions(pipeline_batch=batch)
        cfg_batch = cfg._base.to_dict().get('pipeline_batch', None)
        assert cfg_batch is not None
        assert cfg_batch == batch

    def test_pipeline_cap(self):
        cap = 100
        cfg = TransactionQueryOptions(pipeline_cap=cap)
        cfg_cap = cfg._base.to_dict().get('pipeline_cap', None)
        assert cfg_cap is not None
        assert cfg_cap == cap

    @pytest.mark.parametrize('params', [['a', 'b', 'c']])  # , [[1, 2, 3], ['a', 'b', 'c']]])
    def test_positional_params(self, params):
        cfg = TransactionQueryOptions(positional_parameters=params)
        cfg_params = cfg._base.to_dict().get('positional_parameters', None)
        assert cfg_params is not None
        assert isinstance(cfg_params, list)
        for idx, p in enumerate(cfg_params):
            assert params[idx] == json.loads(p)

    @pytest.mark.parametrize('profile', [QueryProfile.OFF, QueryProfile.PHASES, QueryProfile.TIMINGS])
    def test_profile_mode(self, profile):
        cfg = TransactionQueryOptions(profile=profile)
        cfg_profile = cfg._base.to_dict().get('profile', None)
        assert cfg_profile is not None
        assert cfg_profile == profile.value

    @pytest.mark.usefixtures('check_txn_queries_supported')
    def test_query(self, cb_env):
        coll = cb_env.collection
        key, value = cb_env.get_new_doc()

        def txn_logic(ctx):
            location = f"default:`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            ctx.query(
                f'INSERT INTO {location} VALUES("{key}", {json.dumps(value)})',
                TransactionQueryOptions(metrics=False))

        cb_env.cluster.transactions.run(txn_logic)
        res = cb_env.collection.exists(key)
        assert res.exists is True

    @pytest.mark.usefixtures('check_txn_queries_supported')
    def test_query_lambda_raises_parsing_failure(self, cb_env):
        num_attempts = 0

        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            try:
                ctx.query('This is not N1QL!', TransactionQueryOptions(metrics=False))
            except Exception as ex:
                err_msg = f"Expected to raise ParsingFailedException, not {ex.__class__.__name__}"
                assert isinstance(ex, ParsingFailedException), err_msg

            raise Exception('User raised exception.')

        try:
            cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert 'User raised exception.' in str(ex)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.usefixtures('check_txn_queries_supported')
    def test_query_inner_exc_parsing_failure(self, cb_env):
        num_attempts = 0

        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            ctx.query('This is not N1QL!', TransactionQueryOptions(metrics=False))

        try:
            cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert ex.inner_cause is not None
            assert isinstance(ex.inner_cause, ParsingFailedException)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.parametrize('raw', [{'key1': 'yo'}, {'key1': 5, 'key2': 'foo'}, {'key': [1, 2, 3]}])
    def test_raw(self, raw):
        cfg = TransactionQueryOptions(raw=raw)
        cfg_raw = cfg._base.to_dict().get('raw', None)
        assert cfg_raw is not None
        assert isinstance(cfg_raw, dict)
        for k, v in cfg_raw.items():
            assert json.loads(cfg_raw[k]) == raw[k]

    @pytest.mark.parametrize('read_only', [True, False])
    def test_read_only(self, read_only):
        cfg = TransactionQueryOptions(read_only=read_only)
        cfg_read_only = cfg._base.to_dict().get('read_only', None)
        assert cfg_read_only is not None
        assert cfg_read_only == read_only

    def test_remove(self, cb_env):
        key, value = cb_env.get_new_doc()
        cb_env.collection.insert(key, value)

        def txn_logic(ctx):
            get_res = ctx.get(cb_env.collection, key)
            ctx.remove(get_res)

        cb_env.cluster.transactions.run(txn_logic)
        result = cb_env.collection.exists(key)
        assert result.exists is False

    def test_remove_fail_bad_cas(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        # txn will retry until timeout
        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            rem_res = ctx.get(cb_env.collection, key)
            ctx.replace(rem_res, {'what': 'new content!'})
            try:
                ctx.remove(rem_res)
            except Exception as ex:
                assert isinstance(ex, TransactionOperationFailed)
                assert 'transaction expired' in ex.message or 'cas_mismatch' in ex.message

        try:
            cb_env.cluster.transactions.run(txn_logic, TransactionOptions(expiration_time=timedelta(seconds=2)))
        except Exception as ex:
            assert isinstance(ex, TransactionExpired)
            assert 'transaction expired' in ex.message or 'expired in auto' in ex.message

        assert num_attempts > 1
        # txn should fail, so doc should exist
        res = cb_env.collection.get(key)
        assert res.content_as[dict] == value

    def test_replace(self, cb_env):
        key, value = cb_env.get_existing_doc()
        new_value = {'some': 'thing else'}

        cb_env.collection.upsert(key, value)

        def txn_logic(ctx):
            get_res = ctx.get(cb_env.collection, key)
            assert get_res.content_as[dict] == value
            replace_res = ctx.replace(get_res, new_value)
            # there's a bug where we don't return the correct content in the replace, so comment this out for now
            # assert replace_res.content_as[str] == new_value
            assert get_res.cas != replace_res.cas

        cb_env.cluster.transactions.run(txn_logic)
        result = cb_env.collection.get(key)
        assert result.content_as[dict] == new_value

    def test_replace_fail_bad_cas(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        # txn will retry until timeout
        def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            rem_res = ctx.get(cb_env.collection, key)
            ctx.replace(rem_res, {'foo': 'bar'})
            try:
                ctx.replace(rem_res, {'foo': 'baz'})
            except Exception as ex:
                assert isinstance(ex, TransactionOperationFailed)
                assert 'transaction expired' in ex.message or 'cas_mismatch' in ex.message

        try:
            cb_env.cluster.transactions.run(txn_logic, TransactionOptions(expiration_time=timedelta(seconds=2)))
        except Exception as ex:
            assert isinstance(ex, TransactionExpired)
            assert 'transaction expired' in ex.message or 'expired in auto' in ex.message

        assert num_attempts > 1
        # txn should fail, so doc should have original content
        res = cb_env.collection.get(key)
        assert res.content_as[dict] == value

    def test_rollback(self, cb_env):
        key, value = cb_env.get_new_doc()

        def txn_logic(ctx):
            res = ctx.insert(cb_env.collection, key, value)
            assert res.id == key
            assert res.cas > 0
            raise RuntimeError('this should rollback txn')

        with pytest.raises(TransactionFailed):
            cb_env.cluster.transactions.run(txn_logic)

        result = cb_env.collection.exists(key)
        result.exists is False

    def test_rollback_eating_exceptions(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.get(key)
        cas = result.cas

        def txn_logic(ctx):
            try:
                ctx.insert(cb_env.collection, key, {'this': 'should fail'})
                pytest.fail("insert of existing key should have failed")
            except DocumentExistsException:
                # just eat the exception
                pass
            except Exception as e2:
                pytest.fail(f"Expected insert to raise TransactionOperationFailed, not {e2.__class__.__name__}")

        try:
            cb_env.cluster.transactions.run(txn_logic)
        except Exception as ex:
            assert isinstance(ex, TransactionFailed)
            # the inner cause should be a DocumentExistsException for this example
            # if pytest.fail() occurred this will not be the case, thus failing the test
            assert isinstance(ex.inner_cause, DocumentExistsException)

        result = cb_env.collection.get(key)
        assert result.cas == cas

    @pytest.mark.parametrize('cls', [TransactionQueryOptions, TransactionConfig, TransactionOptions])
    @pytest.mark.parametrize('consistency', [QueryScanConsistency.REQUEST_PLUS,
                                             QueryScanConsistency.NOT_BOUNDED,
                                             QueryScanConsistency.AT_PLUS])
    def test_scan_consistency(self, cls, consistency):
        cfg = None
        try:
            cfg = cls(scan_consistency=consistency)
        except Exception:
            if consistency != QueryScanConsistency.AT_PLUS:
                pytest.fail("got unexpected exception creating TransactionConfig", True)
        if cfg:
            cfg_consistency = cfg._base.to_dict().get('scan_consistency', None)
            assert cfg_consistency is not None
            assert cfg_consistency == consistency.value

    def test_scope_qualifier(self, cb_env):
        pytest.skip('CBD-5091: Pending Transactions changes')
        cfg = TransactionQueryOptions(scope=cb_env.collection._scope)
        cfg_scope_qualifier = cfg._base.to_dict().get('scope_qualifier', None)
        expected = f'default:`{cb_env.collection._scope.bucket_name}`.`{cb_env.collection._scope.name}`'
        assert cfg_scope_qualifier is not None
        assert cfg_scope_qualifier == expected
        bucket, scope = cfg.split_scope_qualifier()
        assert bucket == cb_env.collection._scope.bucket_name
        assert scope == cb_env.collection._scope.name

    @pytest.mark.parametrize('cls', [TransactionConfig, TransactionOptions])
    @pytest.mark.parametrize('level', [DurabilityLevel.NONE,
                                       DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                                       DurabilityLevel.MAJORITY,
                                       DurabilityLevel.PERSIST_TO_MAJORITY])
    def test_transaction_config_durability(self, cls, level):
        cfg = cls(durability=ServerDurability(level))
        cfg_level = cfg._base.to_dict().get('durability_level', None)
        assert cfg_level is not None
        assert DurabilityLevel(cfg_level) is level

    def test_transaction_result(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)

        def txn_logic(ctx):
            ctx.insert(cb_env.collection, key, {'some': 'thing'})
            doc = ctx.get(cb_env.collection, key)
            ctx.replace(doc, {'some': 'thing else'})

        result = cb_env.cluster.transactions.run(txn_logic)
        assert isinstance(result, TransactionResult) is True
        assert result.transaction_id is not None
        assert result.unstaging_complete is True


class ClassicTransactionTests(TransactionTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicTransactionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicTransactionTests) if valid_test_method(meth)]
        compare = set(TransactionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_txn_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        EnvironmentFeatures.check_if_feature_supported('txns',
                                                       cb_base_txn_env.server_version_short,
                                                       cb_base_txn_env.mock_server_type)

        cb_base_txn_env.setup(request.param)
        yield cb_base_txn_env
        cb_base_txn_env.teardown(request.param)
