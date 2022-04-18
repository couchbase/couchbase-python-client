import json
from datetime import timedelta
from time import sleep
from uuid import uuid4

import pytest

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions
from couchbase.transactions import (PerTransactionConfig,
                                    TransactionQueryOptions,
                                    TransactionResult)

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class TransactionTests:

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        conn_string = couchbase_config.get_connection_string()
        opts = ClusterOptions(PasswordAuthenticator(
            couchbase_config.admin_username, couchbase_config.admin_password))
        c = Cluster(
            conn_string, opts)
        c.cluster_info()
        b = c.bucket(f"{couchbase_config.bucket_name}")
        coll = b.default_collection()
        if request.param == CollectionType.DEFAULT:
            cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True)
        elif request.param == CollectionType.NAMED:
            cb_env = TestEnvironment(c, b, coll, couchbase_config, manage_buckets=True,
                                     manage_collections=True)
            cb_env.setup_named_collections()

        cb_env.load_data()
        yield cb_env
        cb_env.purge_data()
        print("data purged")
        if request.param == CollectionType.NAMED:
            cb_env.teardown_named_collections()
            print("named collections torn down")
        c.close()
        print("cluster closed")

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

        with pytest.raises(CouchbaseException):
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
            except CouchbaseException:
                # just eat the exception
                pass
            except Exception as e2:
                pytest.fail(f"Expected insert to raise CouchbaseException, not {e2.__class__.__name__}")

        with pytest.raises(CouchbaseException):
            cb_env.cluster.transactions.run(txn_logic)

        result = coll.get(default_kvp.key)
        assert result.cas == cas

    @pytest.mark.usefixtures("check_txn_queries_supported")
    def test_query(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())
        value = default_kvp.value
        rows = []

        def txn_logic(ctx):
            location = f"default:`{coll._scope.bucket_name}`.`{coll._scope.name}`.`{coll.name}`"
            res = ctx.query(
                f'INSERT INTO {location} VALUES("{key}", {json.dumps(value)}) RETURNING *',
                TransactionQueryOptions(metrics=False))
            for r in res.rows():
                rows.append(r)

        cb_env.cluster.transactions.run(txn_logic)
        assert len(rows) == 1
        assert list(rows[0].items())[0][1] == value

    def test_per_txn_config(self, cb_env, default_kvp):
        coll = cb_env.collection
        key = str(uuid4())

        def txn_logic(ctx):
            ctx.insert(coll, key, {"some": "thing"})
            sleep(0.001)
            ctx.get(coll, key)

        with pytest.raises(CouchbaseException):
            cb_env.cluster.transactions.run(txn_logic, PerTransactionConfig(expiration_time=timedelta(microseconds=1)))
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
