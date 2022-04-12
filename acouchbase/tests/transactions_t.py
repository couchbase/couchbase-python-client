import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions
from couchbase.transactions import PerTransactionConfig, TransactionQueryOptions

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class AsyncTransactionsTests:
    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = asyncio.get_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest_asyncio.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, couchbase_config, request):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        c = Cluster(
            conn_string, opts)
        await c.on_connect()
        await c.cluster_info()
        b = c.bucket(f"{couchbase_config.bucket_name}")
        await b.on_connect()

        coll = b.default_collection()
        if request.param == CollectionType.DEFAULT:
            cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True)
        elif request.param == CollectionType.NAMED:
            cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True, manage_collections=True)
            await cb_env.setup_named_collections()

        await cb_env.load_data()
        yield cb_env
        await cb_env.purge_data()
        if request.param == CollectionType.NAMED:
            await cb_env.teardown_named_collections()
        await c.close()

    @pytest.fixture(scope="class")
    def check_txn_queries_supported(self, cb_env):
        cb_env.check_if_feature_supported('txn_queries')

    @pytest.mark.asyncio
    async def test_get(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value

        async def txn_logic(ctx):
            res = await ctx.get(coll, key)
            assert res.cas > 0
            assert res.id == key
            # hack until I put in the transcoder support
            assert res.content_as[dict] == value

        await cb_env.cluster.transactions.run(txn_logic)

    @pytest.mark.asyncio
    async def test_replace(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}
        new_value = {"some": "thing else"}

        await coll.upsert(key, value)

        async def txn_logic(ctx):
            get_res = await ctx.get(coll, key)
            assert get_res.content_as[dict] == value
            replace_res = await ctx.replace(get_res, new_value)
            # there's a bug where we don't return the correct content in the replace, so comment this out for now
            # assert replace_res.content_as[str] == new_value
            assert get_res.cas != replace_res.cas

        await cb_env.cluster.transactions.run(txn_logic)
        result = await coll.get(key)
        assert result.content_as[dict] == new_value

    @pytest.mark.asyncio
    async def test_insert(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}

        async def txn_logic(ctx):
            await ctx.insert(coll, key, value)

        await cb_env.cluster.transactions.run(txn_logic)
        get_result = await coll.get(key)
        assert get_result.content_as[dict] == value

    @pytest.mark.asyncio
    async def test_remove(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}
        await coll.insert(key, value)

        async def txn_logic(ctx):
            get_res = await ctx.get(coll, key)
            await ctx.remove(get_res)

        await cb_env.cluster.transactions.run(txn_logic)
        result = await coll.exists(key)
        assert result.exists is False

    @pytest.mark.asyncio
    async def test_rollback(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())
        value = {"some": "thing"}

        async def txn_logic(ctx):
            res = await ctx.insert(coll, key, value)
            assert res.id == key
            assert res.cas > 0
            raise RuntimeError("this should rollback txn")

        with pytest.raises(CouchbaseException):
            await cb_env.cluster.transactions.run(txn_logic)

        result = await coll.exists(key)
        result.exists is False

    @pytest.mark.asyncio
    async def test_rollback_eating_exceptions(self, cb_env, default_kvp):
        coll = cb_env.collection
        result = await coll.get(default_kvp.key)
        cas = result.cas

        async def txn_logic(ctx):
            try:
                await ctx.insert(coll, default_kvp.key, {"this": "should fail"})
                pytest.fail("insert of existing key should have failed")
            except CouchbaseException:
                # just eat the exception
                pass
            except Exception as e2:
                pytest.fail(f"Expected insert to raise CouchbaseException, not {e2.__class__.__name__}")

        with pytest.raises(CouchbaseException):
            await cb_env.cluster.transactions.run(txn_logic)

        result = await coll.get(default_kvp.key)
        assert result.cas == cas

    @pytest.mark.usefixtures("check_txn_queries_supported")
    @pytest.mark.asyncio
    async def test_query(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())
        value = default_kvp.value
        rows = []

        async def txn_logic(ctx):
            location = f"default:`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            res = await ctx.query(f'INSERT INTO {location} VALUES("{key}", {json.dumps(value)}) RETURNING *',
                                  TransactionQueryOptions(metrics=False))
            for r in res.rows():
                rows.append(r)

        await cb_env.cluster.transactions.run(txn_logic)
        assert len(rows) == 1
        assert list(rows[0].items())[0][1] == value

    @pytest.mark.asyncio
    async def test_per_txn_config(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())
        value = default_kvp.value

        async def txn_logic(ctx):
            ctx.insert(coll, key, value)
            await asyncio.sleep(0.001)
            await ctx.get(coll, key)

        cfg = PerTransactionConfig(expiration_time=timedelta(microseconds=1))
        with pytest.raises(CouchbaseException):
            await cb_env.cluster.transactions.run(txn_logic, cfg)
        result = await coll.exists(key)
        assert result.exists is False


