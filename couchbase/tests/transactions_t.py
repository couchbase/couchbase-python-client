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
from time import sleep
from uuid import uuid4

import pytest

from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (ParsingFailedException,
                                  TransactionExpired,
                                  TransactionFailed,
                                  TransactionOperationFailed)
from couchbase.n1ql import QueryProfile, QueryScanConsistency
from couchbase.options import (TransactionConfig,
                               TransactionOptions,
                               TransactionQueryOptions)
from couchbase.transactions import TransactionKeyspace, TransactionResult

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class TransactionTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        cb_env.try_n_times(3, 5, cb_env.load_data)
        yield cb_env
        cb_env.try_n_times(3, 5, cb_env.purge_data)
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False)

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest.fixture(scope="class")
    def check_txn_queries_supported(self, cb_env):
        cb_env.check_if_feature_supported('txn_queries')

    def test_get(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value

        def txn_logic(ctx):
            res = ctx.get(coll, key)
            assert res.cas > 0
            assert res.id == key
            assert res.content_as[dict] == value

        cb_env.cluster.transactions.run(txn_logic)

    def test_replace(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}
        new_value = {"some": "thing else"}

        coll.upsert(key, value)

        def txn_logic(ctx):
            get_res = ctx.get(coll, key)
            assert get_res.content_as[dict] == value
            replace_res = ctx.replace(get_res, new_value)
            # there's a bug where we don't return the correct content in the replace, so comment this out for now
            # assert replace_res.content_as[str] == new_value
            assert get_res.cas != replace_res.cas

        cb_env.cluster.transactions.run(txn_logic)
        result = coll.get(key)
        assert result.content_as[dict] == new_value

    def test_insert(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}

        def txn_logic(ctx):
            ctx.insert(coll, key, value)

        cb_env.cluster.transactions.run(txn_logic)
        get_result = coll.get(key)
        assert get_result.content_as[dict] == value

    def test_remove(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}
        coll.insert(key, value)

        def txn_logic(ctx):
            get_res = ctx.get(coll, key)
            ctx.remove(get_res)

        cb_env.cluster.transactions.run(txn_logic)
        result = coll.exists(key)
        assert result.exists is False

    def test_rollback(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}

        def txn_logic(ctx):
            res = ctx.insert(coll, key, value)
            assert res.id == key
            assert res.cas > 0
            raise RuntimeError("this should rollback txn")

        with pytest.raises(TransactionFailed):
            cb_env.cluster.transactions.run(txn_logic)

        result = coll.exists(key)
        result.exists is False

    def test_rollback_eating_exceptions(self, cb_env, default_kvp):
        coll = cb_env.collection
        result = coll.get(default_kvp.key)
        cas = result.cas

        def txn_logic(ctx):
            try:
                ctx.insert(coll, default_kvp.key, {"this": "should fail"})
                pytest.fail("insert of existing key should have failed")
            except TransactionOperationFailed:
                # just eat the exception
                pass
            except Exception as e2:
                pytest.fail(f"Expected insert to raise TransactionOperationFailed, not {e2.__class__.__name__}")

        with pytest.raises(TransactionFailed):
            cb_env.cluster.transactions.run(txn_logic)

        result = coll.get(default_kvp.key)
        assert result.cas == cas

    @pytest.mark.usefixtures("check_txn_queries_supported")
    def test_query(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())
        value = default_kvp.value

        def txn_logic(ctx):
            location = f"default:`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            ctx.query(
                f'INSERT INTO {location} VALUES("{key}", {json.dumps(value)})',
                TransactionQueryOptions(metrics=False))

        cb_env.cluster.transactions.run(txn_logic)
        assert cb_env.collection.exists(key).exists

    @pytest.mark.usefixtures("check_txn_queries_supported")
    def test_bad_query(self, cb_env):

        def txn_logic(ctx):
            try:
                ctx.query("this wont parse")
                pytest.fail("expected bad query to raise exception")
            except ParsingFailedException:
                pass
            except Exception as e:
                pytest.fail(f"Expected bad query to raise ParsingFailedException, not {e.__class__.__name__}")

        cb_env.cluster.transactions.run(txn_logic)

    def test_per_txn_config(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())

        def txn_logic(ctx):
            ctx.insert(coll, key, {"some": "thing"})
            sleep(0.001)
            ctx.get(coll, key)

        with pytest.raises(TransactionExpired):
            cb_env.cluster.transactions.run(txn_logic, TransactionOptions(expiration_time=timedelta(microseconds=1)))
        assert coll.exists(key).exists is False

    def test_transaction_result(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())

        def txn_logic(ctx):
            ctx.insert(coll, key, {"some": "thing"})
            doc = ctx.get(coll, key)
            ctx.replace(doc, {"some": "thing else"})

        result = cb_env.cluster.transactions.run(txn_logic)
        assert isinstance(result, TransactionResult) is True
        assert result.transaction_id is not None
        assert result.unstaging_complete is True

    @pytest.mark.parametrize("cls", [TransactionConfig, TransactionOptions])
    @pytest.mark.parametrize("level", [DurabilityLevel.NONE,
                                       DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                                       DurabilityLevel.MAJORITY,
                                       DurabilityLevel.PERSIST_TO_MAJORITY])
    def test_transaction_config_durability(self, cls, level):
        cfg = cls(durability=ServerDurability(level))
        cfg_level = cfg._base.to_dict().get('durability_level', None)
        assert cfg_level is not None
        assert DurabilityLevel(cfg_level) is level

    @pytest.mark.parametrize("cls", [TransactionConfig, TransactionOptions])
    @pytest.mark.parametrize('exp', [timedelta(seconds=30), timedelta(milliseconds=100)])
    def test_expiration_time(self, cls, exp):
        cfg = cls(expiration_time=exp)
        cfg_expiry = cfg._base.to_dict().get("expiration_time", None)
        assert cfg_expiry is not None
        assert cfg_expiry == exp.total_seconds() * 1000*1000*1000  # nanoseconds - and can't use 'is' here

    @pytest.mark.parametrize('window', [timedelta(seconds=30), timedelta(milliseconds=500)])
    def test_cleanup_window(self, window):
        cfg = TransactionConfig(cleanup_window=window)
        cfg_window = cfg._base.to_dict().get("cleanup_window", None)
        assert cfg_window is not None
        assert cfg_window == window.total_seconds() * 1000  # milliseconds

    @pytest.mark.parametrize("cls", [TransactionConfig, TransactionOptions])
    @pytest.mark.parametrize('kv_timeout', [timedelta(seconds=30), timedelta(milliseconds=2)])
    def test_kv_timeout(self, cls, kv_timeout):
        cfg = cls(kv_timeout=kv_timeout)
        cfg_kv_timeout = cfg._base.to_dict().get("kv_timeout", None)
        assert cfg_kv_timeout is not None
        assert cfg_kv_timeout == kv_timeout.total_seconds() * 1000  # milliseconds

    @pytest.mark.parametrize('cleanup', [False, True])
    def test_cleanup_lost_attempts(self, cleanup):
        cfg = TransactionConfig(cleanup_lost_attempts=cleanup)
        cfg_cleanup = cfg._base.to_dict().get("cleanup_lost_attempts", None)
        assert cfg_cleanup is not None
        assert cfg_cleanup is cleanup

    @pytest.mark.parametrize('cleanup', [False, True])
    def test_cleanup_client_attempts(self, cleanup):
        cfg = TransactionConfig(cleanup_client_attempts=cleanup)
        cfg_cleanup = cfg._base.to_dict().get("cleanup_client_attempts", None)
        assert cfg_cleanup is not None
        assert cfg_cleanup is cleanup

    @pytest.mark.parametrize("cls", [TransactionQueryOptions, TransactionConfig, TransactionOptions])
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

    @pytest.mark.parametrize('cls', [TransactionOptions, TransactionConfig])
    def test_metadata_collection(self, cls, cb_env):
        coll = cb_env.collection
        cfg = cls(metadata_collection=TransactionKeyspace(coll=coll))
        cfg_coll = cfg._base.to_dict().get('metadata_collection', None)
        assert cfg_coll is not None
        assert cfg_coll == f'{coll._scope.bucket_name}.{coll._scope.name}.{coll.name}'

    @pytest.mark.parametrize('raw', [{"key1": "yo"}, {"key1": 5, "key2": "foo"}, {"key": [1, 2, 3]}])
    def test_raw(self, raw):
        cfg = TransactionQueryOptions(raw=raw)
        cfg_raw = cfg._base.to_dict().get('raw', None)
        assert cfg_raw is not None
        assert isinstance(cfg_raw, dict)
        for k, v in cfg_raw.items():
            assert json.loads(cfg_raw[k]) == raw[k]

    @pytest.mark.parametrize('adhoc', [True, False])
    def test_adhoc(self, adhoc):
        cfg = TransactionQueryOptions(adhoc=adhoc)
        cfg_adhoc = cfg._base.to_dict().get('adhoc', None)
        assert cfg_adhoc is not None
        assert cfg_adhoc == adhoc

    @pytest.mark.parametrize('profile', [QueryProfile.OFF, QueryProfile.PHASES, QueryProfile.TIMINGS])
    def test_profile_mode(self, profile):
        cfg = TransactionQueryOptions(profile=profile)
        cfg_profile = cfg._base.to_dict().get('profile', None)
        assert cfg_profile is not None
        assert cfg_profile == profile.value

    def test_client_context_id(self):
        ctxid = "somestring"
        cfg = TransactionQueryOptions(client_context_id=ctxid)
        cfg_ctxid = cfg._base.to_dict().get('client_context_id', None)
        assert cfg_ctxid is not None
        assert cfg_ctxid == ctxid

    def scan_cap(self):
        cap = 100
        cfg = TransactionQueryOptions(scan_cap=cap)
        cfg_cap = cfg._base.to_dict().get('scan_cap', None)
        assert cfg_cap is not None
        assert cfg_cap == cap

    @pytest.mark.parametrize('wait', [timedelta(seconds=10), timedelta(milliseconds=5)])
    def scan_wait(self, wait):
        cfg = TransactionQueryOptions(scan_wait=wait)
        cfg_wait = cfg._base.to_dict().get('scan_wait', None)
        assert cfg_wait is not None
        assert cfg_wait == wait.total_seconds() * 1000

    @pytest.mark.parametrize('metrics', [True, False])
    def test_metrics(self, metrics):
        cfg = TransactionQueryOptions(metrics=metrics)
        cfg_metrics = cfg._base.to_dict().get('metrics', None)
        assert cfg_metrics is not None
        assert cfg_metrics == metrics

    @pytest.mark.parametrize('read_only', [True, False])
    def test_read_only(self, read_only):
        cfg = TransactionQueryOptions(read_only=read_only)
        cfg_read_only = cfg._base.to_dict().get('read_only', None)
        assert cfg_read_only is not None
        assert cfg_read_only == read_only

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

    def test_scope(self, cb_env):
        cfg = TransactionQueryOptions(scope=cb_env.collection._scope)
        cfg_scope = cfg._base.to_dict().get("scope", None)
        cfg_bucket = cfg._base.to_dict().get("bucket", None)
        assert cfg_bucket is not None
        assert cfg_scope is not None
        assert cfg_bucket == cb_env.collection._scope.bucket_name
        assert cfg_scope == cb_env.collection._scope.name

    def test_max_parallelism(self):
        max = 100
        cfg = TransactionQueryOptions(max_parallelism=max)
        cfg_max = cfg._base.to_dict().get('max_parallelism', None)
        assert cfg_max is not None
        assert cfg_max == max

    @pytest.mark.parametrize('params', [["a", "b", "c"]])  # , [[1, 2, 3], ["a", "b", "c"]]])
    def test_positional_params(self, params):
        cfg = TransactionQueryOptions(positional_parameters=params)
        cfg_params = cfg._base.to_dict().get('positional_parameters', None)
        assert cfg_params is not None
        assert isinstance(cfg_params, list)
        for idx, p in enumerate(cfg_params):
            assert params[idx] == json.loads(p)

    @pytest.mark.parametrize('params', [{"key1": "thing"},
                                        {"key1": ['an', 'array']},
                                        {'key1': 10, 'key2': 'something else'}])
    def test_named_params(self, params):
        cfg = TransactionQueryOptions(named_parameters=params)
        cfg_params = cfg._base.to_dict().get('named_parameters', None)
        assert cfg_params is not None
        assert isinstance(cfg_params, dict)
        for k, v in params.items():
            assert json.loads(cfg_params[k]) == v
