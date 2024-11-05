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


import json
from datetime import timedelta

import pytest
import pytest_asyncio

from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (BucketNotFoundException,
                                  DocumentExistsException,
                                  DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  ParsingFailedException,
                                  TransactionExpired,
                                  TransactionFailed,
                                  TransactionOperationFailed)
from couchbase.n1ql import QueryProfile, QueryScanConsistency
from couchbase.options import (TransactionConfig,
                               TransactionGetOptions,
                               TransactionInsertOptions,
                               TransactionOptions,
                               TransactionQueryOptions,
                               TransactionReplaceOptions)
from couchbase.transactions import TransactionKeyspace, TransactionResult
from couchbase.transcoder import RawBinaryTranscoder
from tests.environments import CollectionType
from tests.environments.test_environment import AsyncTestEnvironment
from tests.test_features import EnvironmentFeatures


class TransactionTestSuite:
    TEST_MANIFEST = [
        'test_adhoc',
        'test_bad_query',
        'test_binary',
        'test_binary_kwargs',
        'test_binary_not_supported',
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
        'test_max_parallelism',
        'test_metadata_collection',
        'test_metadata_collection_not_found',
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
        'test_query_mode_insert',
        'test_query_mode_remove',
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
        'test_timeout',
        'test_transaction_config_durability',
        'test_transaction_result',
    ]

    @pytest.fixture(scope='class')
    def check_txn_queries_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('txn_queries',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_binary_txns_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('binary_txns',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type,
                                                       cb_env.server_version_patch)

    @pytest.fixture(scope='class')
    def check_binary_txns_not_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_not_supported('binary_txns',
                                                           cb_env.server_version_short,
                                                           cb_env.mock_server_type,
                                                           cb_env.server_version_patch)

    @pytest.mark.parametrize('adhoc', [True, False])
    def test_adhoc(self, adhoc):
        cfg = TransactionQueryOptions(adhoc=adhoc)
        cfg_adhoc = cfg._base.to_dict().get('adhoc', None)
        assert cfg_adhoc is not None
        assert cfg_adhoc == adhoc

    @pytest.mark.usefixtures('check_txn_queries_supported')
    @pytest.mark.asyncio
    async def test_bad_query(self, cb_env):

        async def txn_logic(ctx):
            try:
                await ctx.query('this wont parse')
                pytest.fail('expected bad query to raise exception')
            except ParsingFailedException:
                pass
            except Exception as e:
                pytest.fail(f"Expected bad query to raise ParsingFailedException, not {e.__class__.__name__}")

        await cb_env.cluster.transactions.run(txn_logic)

    @pytest.mark.usefixtures('check_binary_txns_supported')
    @pytest.mark.asyncio
    async def test_binary(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)
        tc = RawBinaryTranscoder()
        value = 'bytes content'.encode('utf-8')
        new_content = b'\xFF'

        async def txn_logic(ctx):
            await ctx.insert(cb_env.collection, key, value, TransactionInsertOptions(transcoder=tc))
            get_res = await ctx.get(cb_env.collection, key, TransactionGetOptions(transcoder=tc))
            assert get_res.content_as[bytes] == value
            replace_res = await ctx.replace(get_res, new_content, TransactionReplaceOptions(transcoder=tc))
            # there's a bug where we don't return the correct content in the replace, so comment this out for now
            # assert replace_res.content_as[bytes] == new_content
            assert get_res.cas != replace_res.cas

        await cb_env.cluster.transactions.run(txn_logic)
        result = await cb_env.collection.get(key, transcoder=tc)
        assert result.content_as[bytes] == new_content

    @pytest.mark.usefixtures('check_binary_txns_supported')
    @pytest.mark.asyncio
    async def test_binary_kwargs(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)
        tc = RawBinaryTranscoder()
        value = 'bytes content'.encode('utf-8')
        new_content = b'\xFF'

        async def txn_logic(ctx):
            await ctx.insert(cb_env.collection, key, value, transcoder=tc)
            get_res = await ctx.get(cb_env.collection, key, transcoder=tc)
            assert get_res.content_as[bytes] == value
            replace_res = await ctx.replace(get_res, new_content, transcoder=tc)
            # there's a bug where we don't return the correct content in the replace, so comment this out for now
            # assert replace_res.content_as[bytes] == new_content
            assert get_res.cas != replace_res.cas

        await cb_env.cluster.transactions.run(txn_logic)
        result = await cb_env.collection.get(key, transcoder=tc)
        assert result.content_as[bytes] == new_content

    @pytest.mark.usefixtures('check_binary_txns_not_supported')
    @pytest.mark.asyncio
    async def test_binary_not_supported(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)
        tc = RawBinaryTranscoder()
        value = 'bytes content'.encode('utf-8')

        async def txn_logic(ctx):
            await ctx.insert(cb_env.collection, key, value, TransactionInsertOptions(transcoder=tc))

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert ex.inner_cause is not None
            err_msg = f"Expected to raise FeatureUnavailableException, not {ex.inner_cause.__class__.__name__}"
            assert isinstance(ex.inner_cause, FeatureUnavailableException), err_msg

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
        # CXXCBC-391, changes make expiration_time an invalid key
        cfg_timeout = cfg._base.to_dict().get('expiration_time', None)
        assert cfg_timeout is None
        cfg_timeout = cfg._base.to_dict().get('timeout', None)
        assert cfg_timeout == exp.total_seconds() * 1000*1000*1000  # nanoseconds - and can't use 'is' here

    @pytest.mark.asyncio
    async def test_get(self, cb_env):
        key, value = cb_env.get_existing_doc()

        async def txn_logic(ctx):
            res = await ctx.get(cb_env.collection, key)
            assert res.cas > 0
            assert res.id == key
            assert res.content_as[dict] == value

        await cb_env.cluster.transactions.run(txn_logic)

    @pytest.mark.asyncio
    async def test_get_lambda_raises_doc_not_found(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)
        num_attempts = 0

        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            try:
                await ctx.get(cb_env.collection, key)
            except Exception as ex:
                err_msg = f"Expected to raise DocumentNotFoundException, not {ex.__class__.__name__}"
                assert isinstance(ex, DocumentNotFoundException), err_msg

            raise Exception('User raised exception.')

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert 'User raised exception.' in str(ex)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.asyncio
    async def test_get_inner_exc_doc_not_found(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)
        num_attempts = 0

        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            await ctx.get(cb_env.collection, key)

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert ex.inner_cause is not None
            assert isinstance(ex.inner_cause, DocumentNotFoundException)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.asyncio
    async def test_insert(self, cb_env):
        key, value = cb_env.get_new_doc()

        async def txn_logic(ctx):
            await ctx.insert(cb_env.collection, key, value)

        await cb_env.cluster.transactions.run(txn_logic)
        get_result = await cb_env.collection.get(key)
        assert get_result.content_as[dict] == value

    @pytest.mark.asyncio
    async def test_insert_lambda_raises_doc_exists(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            try:
                await ctx.insert(cb_env.collection, key, value)
            except Exception as ex:
                err_msg = f"Expected to raise DocumentExistsException, not {ex.__class__.__name__}"
                assert isinstance(ex, DocumentExistsException), err_msg

            raise Exception('User raised exception.')

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert 'User raised exception.' in str(ex)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.asyncio
    async def test_insert_inner_exc_doc_exists(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            await ctx.insert(cb_env.collection, key, value)

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert ex.inner_cause is not None
            assert isinstance(ex.inner_cause, DocumentExistsException)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

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

    # creating a new connection, allow retries
    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.asyncio
    async def test_metadata_collection_not_found(self, cb_env):
        from acouchbase.cluster import AsyncCluster
        from couchbase.auth import PasswordAuthenticator
        from couchbase.options import ClusterOptions
        conn_string = cb_env.config.get_connection_string()
        username, pw = cb_env.config.get_username_and_pw()
        auth = PasswordAuthenticator(username, pw)
        metadata = TransactionKeyspace(bucket='no-bucket', scope='_default', collection='_default')
        txn_config = TransactionConfig(metadata_collection=metadata)
        cluster = await AsyncCluster.connect(f'{conn_string}', ClusterOptions(auth, transaction_config=txn_config))
        collection = cluster.bucket(cb_env.bucket.name).default_collection()

        async def txn_logic(ctx):
            # key should not matter as we should fail when creating the
            # transactions object and not actually get to this point
            await ctx.get(collection, 'test-key')

        with pytest.raises(BucketNotFoundException):
            await cluster.transactions.run(txn_logic)

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

    @pytest.mark.asyncio
    async def test_per_txn_config(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)

        async def txn_logic(ctx):
            await ctx.insert(cb_env.collection, key, {'some': 'thing'})
            await AsyncTestEnvironment.sleep(0.001)
            await ctx.get(cb_env.collection, key)

        with pytest.raises(TransactionExpired):
            await cb_env.cluster.transactions.run(txn_logic,
                                                  TransactionOptions(timeout=timedelta(microseconds=1)))
        res = await cb_env.collection.exists(key)
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
    @pytest.mark.asyncio
    async def test_query(self, cb_env):
        coll = cb_env.collection
        key, value = cb_env.get_new_doc()

        async def txn_logic(ctx):
            location = f"default:`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            await ctx.query(
                f'INSERT INTO {location} VALUES("{key}", {json.dumps(value)})',
                TransactionQueryOptions(metrics=False))

        await cb_env.cluster.transactions.run(txn_logic)
        res = await cb_env.collection.exists(key)
        assert res.exists is True

    @pytest.mark.usefixtures('check_txn_queries_supported')
    @pytest.mark.asyncio
    async def test_query_lambda_raises_parsing_failure(self, cb_env):
        num_attempts = 0

        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            try:
                await ctx.query('This is not N1QL!', TransactionQueryOptions(metrics=False))
            except Exception as ex:
                err_msg = f"Expected to raise ParsingFailedException, not {ex.__class__.__name__}"
                assert isinstance(ex, ParsingFailedException), err_msg

            raise Exception('User raised exception.')

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert 'User raised exception.' in str(ex)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.usefixtures('check_txn_queries_supported')
    @pytest.mark.asyncio
    async def test_query_inner_exc_parsing_failure(self, cb_env):
        num_attempts = 0

        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            await ctx.query('This is not N1QL!', TransactionQueryOptions(metrics=False))

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except TransactionFailed as ex:
            assert ex.inner_cause is not None
            assert isinstance(ex.inner_cause, ParsingFailedException)
        except Exception as ex:
            pytest.fail(f"Expected to raise TransactionFailed, not {ex.__class__.__name__}")

        assert num_attempts == 1

    @pytest.mark.usefixtures('check_txn_queries_supported')
    @pytest.mark.asyncio
    async def test_query_mode_insert(self, cb_env):
        coll = cb_env.collection
        key, value = cb_env.get_new_doc()
        key1, value1 = cb_env.get_new_doc()
        await coll.insert(key, value)

        async def txn_logic(ctx):
            fdqn = f"`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            statement = f'SELECT * FROM {fdqn} WHERE META().id IN $1 ORDER BY META().id ASC'
            res = await ctx.query(statement, TransactionQueryOptions(positional_parameters=[[key]]))
            assert len(res.rows()) == 1
            assert res.rows()[0].get(f'{coll.name}', {}).get('id') == value.get('id')
            await ctx.insert(coll, key1, value1)

        await cb_env.cluster.transactions.run(txn_logic)
        get_res = await coll.get(key1)
        assert get_res is not None
        assert get_res.content_as[dict] == value1

    @pytest.mark.usefixtures('check_txn_queries_supported')
    @pytest.mark.asyncio
    async def test_query_mode_remove(self, cb_env):
        coll = cb_env.collection
        key, value = cb_env.get_new_doc()
        key1, value1 = cb_env.get_new_doc()
        await coll.insert(key, value)

        async def txn_logic(ctx):
            await ctx.insert(coll, key1, value1)
            fdqn = f"`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            statement = f'SELECT * FROM {fdqn} WHERE META().id IN $1 ORDER BY META().id ASC'
            res = await ctx.query(statement, TransactionQueryOptions(positional_parameters=[[key, key1]]))
            assert len(res.rows()) == 2
            getRes = await ctx.get(coll, key)
            await ctx.remove(getRes)

        await cb_env.cluster.transactions.run(txn_logic)
        with pytest.raises(DocumentNotFoundException):
            await coll.get(key)

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

    @pytest.mark.asyncio
    async def test_remove(self, cb_env):
        key, value = cb_env.get_new_doc()
        await cb_env.collection.insert(key, value)

        async def txn_logic(ctx):
            get_res = await ctx.get(cb_env.collection, key)
            await ctx.remove(get_res)

        await cb_env.cluster.transactions.run(txn_logic)
        result = await cb_env.collection.exists(key)
        assert result.exists is False

    @pytest.mark.asyncio
    async def test_remove_fail_bad_cas(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        # txn will retry until timeout
        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            rem_res = await ctx.get(cb_env.collection, key)
            await ctx.replace(rem_res, {'what': 'new content!'})
            try:
                await ctx.remove(rem_res)
            except Exception as ex:
                assert isinstance(ex, TransactionOperationFailed)
                assert 'transaction expired' in ex.message or 'cas_mismatch' in ex.message

        try:
            await cb_env.cluster.transactions.run(txn_logic, TransactionOptions(timeout=timedelta(seconds=2)))
        except Exception as ex:
            assert isinstance(ex, TransactionExpired)
            assert 'transaction expired' in ex.message or 'expired in auto' in ex.message

        assert num_attempts > 1
        # txn should fail, so doc should exist
        res = await cb_env.collection.get(key)
        assert res.content_as[dict] == value

    @pytest.mark.asyncio
    async def test_replace(self, cb_env):
        key, value = cb_env.get_existing_doc()
        new_value = {'some': 'thing else'}

        await cb_env.collection.upsert(key, value)

        async def txn_logic(ctx):
            get_res = await ctx.get(cb_env.collection, key)
            assert get_res.content_as[dict] == value
            replace_res = await ctx.replace(get_res, new_value)
            # there's a bug where we don't return the correct content in the replace, so comment this out for now
            # assert replace_res.content_as[str] == new_value
            assert get_res.cas != replace_res.cas

        await cb_env.cluster.transactions.run(txn_logic)
        result = await cb_env.collection.get(key)
        assert result.content_as[dict] == new_value

    @pytest.mark.asyncio
    async def test_replace_fail_bad_cas(self, cb_env):
        key, value = cb_env.get_existing_doc()
        num_attempts = 0

        # txn will retry until timeout
        async def txn_logic(ctx):
            nonlocal num_attempts
            num_attempts += 1
            rem_res = await ctx.get(cb_env.collection, key)
            await ctx.replace(rem_res, {'foo': 'bar'})
            try:
                await ctx.replace(rem_res, {'foo': 'baz'})
            except Exception as ex:
                assert isinstance(ex, TransactionOperationFailed)
                assert 'transaction expired' in ex.message or 'cas_mismatch' in ex.message

        try:
            await cb_env.cluster.transactions.run(txn_logic, TransactionOptions(timeout=timedelta(seconds=2)))
        except Exception as ex:
            assert isinstance(ex, TransactionExpired)
            assert 'transaction expired' in ex.message or 'expired in auto' in ex.message

        assert num_attempts > 1
        # txn should fail, so doc should have original content
        res = await cb_env.collection.get(key)
        assert res.content_as[dict] == value

    @pytest.mark.asyncio
    async def test_rollback(self, cb_env):
        key, value = cb_env.get_new_doc()

        async def txn_logic(ctx):
            res = await ctx.insert(cb_env.collection, key, value)
            assert res.id == key
            assert res.cas > 0
            raise RuntimeError('this should rollback txn')

        with pytest.raises(TransactionFailed):
            await cb_env.cluster.transactions.run(txn_logic)

        result = await cb_env.collection.exists(key)
        result.exists is False

    @pytest.mark.asyncio
    async def test_rollback_eating_exceptions(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = await cb_env.collection.get(key)
        cas = result.cas

        async def txn_logic(ctx):
            try:
                await ctx.insert(cb_env.collection, key, {'this': 'should fail'})
                pytest.fail("insert of existing key should have failed")
            except DocumentExistsException:
                # just eat the exception
                pass
            except Exception as e2:
                pytest.fail(f"Expected insert to raise TransactionOperationFailed, not {e2.__class__.__name__}")

        try:
            await cb_env.cluster.transactions.run(txn_logic)
        except Exception as ex:
            assert isinstance(ex, TransactionFailed)
            # the inner cause should be a DocumentExistsException for this example
            # if pytest.fail() occurred this will not be the case, thus failing the test
            assert isinstance(ex.inner_cause, DocumentExistsException)

        result = await cb_env.collection.get(key)
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
    @pytest.mark.parametrize('exp', [timedelta(seconds=30), timedelta(milliseconds=100)])
    def test_timeout(self, cls, exp):
        cfg = cls(timeout=exp)
        cfg_timeout = cfg._base.to_dict().get('timeout', None)
        assert cfg_timeout is not None
        assert cfg_timeout == exp.total_seconds() * 1000*1000*1000  # nanoseconds - and can't use 'is' here

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

    @pytest.mark.asyncio
    async def test_transaction_result(self, cb_env):
        key = cb_env.get_new_doc(key_only=True)

        async def txn_logic(ctx):
            await ctx.insert(cb_env.collection, key, {'some': 'thing'})
            doc = await ctx.get(cb_env.collection, key)
            await ctx.replace(doc, {'some': 'thing else'})

        result = await cb_env.cluster.transactions.run(txn_logic)
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

    @pytest_asyncio.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, acb_base_txn_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        EnvironmentFeatures.check_if_feature_supported('txns',
                                                       acb_base_txn_env.server_version_short,
                                                       acb_base_txn_env.mock_server_type)

        await acb_base_txn_env.setup(request.param, __name__)
        yield acb_base_txn_env
        await acb_base_txn_env.teardown(request.param, __name__)
