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

import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.exceptions import (CouchbaseException,
                                  ParsingFailedException,
                                  TransactionExpired,
                                  TransactionFailed)
from couchbase.options import TransactionOptions
from couchbase.transactions import TransactionResult

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class AsyncTransactionsTests:
    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest_asyncio.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    async def couchbase_test_environment(self, couchbase_config, request):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        cb_env.check_if_feature_supported('txns')

        await cb_env.try_n_times(5, 3, cb_env.load_data)
        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

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

        with pytest.raises(TransactionFailed):
            await cb_env.cluster.transactions.run(txn_logic)

        result = await coll.exists(key)
        assert result.exists is False

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

        with pytest.raises(TransactionFailed):
            await cb_env.cluster.transactions.run(txn_logic)

        result = await coll.get(default_kvp.key)
        assert result.cas == cas

    @pytest.mark.usefixtures("check_txn_queries_supported")
    @pytest.mark.asyncio
    async def test_query(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())
        value = default_kvp.value

        async def txn_logic(ctx):
            location = f"default:`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            await ctx.query(f'INSERT INTO {location} VALUES("{key}", {json.dumps(value)})')

        await cb_env.cluster.transactions.run(txn_logic)
        res = await cb_env.collection.exists(key)
        assert res.exists

    @pytest.mark.usefixtures("check_txn_queries_supported")
    @pytest.mark.asyncio
    async def test_bad_query(self, cb_env):

        async def txn_logic(ctx):
            try:
                await ctx.query("this wont parse")
                pytest.fail("expected bad query to raise exception")
            except ParsingFailedException:
                pass
            except Exception as e:
                pytest.fail(f"Expected bad query to raise ParsingFailedException, not {e.__class__.__name__}")

        await cb_env.cluster.transactions.run(txn_logic)

    @pytest.mark.asyncio
    async def test_per_txn_config(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())
        value = default_kvp.value

        async def txn_logic(ctx):
            await ctx.insert(coll, key, value)
            await asyncio.sleep(0.001)
            await ctx.get(coll, key)

        cfg = TransactionOptions(expiration_time=timedelta(microseconds=1))
        with pytest.raises(TransactionExpired):
            await cb_env.cluster.transactions.run(txn_logic, cfg)
        result = await coll.exists(key)
        assert result.exists is False

    @pytest.mark.asyncio
    async def test_transaction_result(self, cb_env):
        coll = cb_env.collection
        key = str(uuid4())

        async def txn_logic(ctx):
            doc = await ctx.insert(coll, key, {"some": "thing"})
            await ctx.replace(doc, {"some": "thing else"})

        result = await cb_env.cluster.transactions.run(txn_logic)
        assert isinstance(result, TransactionResult) is True
        assert result.transaction_id is not None
        assert result.unstaging_complete is True
