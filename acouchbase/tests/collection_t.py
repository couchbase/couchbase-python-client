import asyncio
from datetime import datetime, timedelta
from time import time

import pytest
import pytest_asyncio

import couchbase.subdocument as SD
from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.diagnostics import ServiceType
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DurabilityImpossibleException,
                                  InvalidArgumentException,
                                  PathNotFoundException,
                                  TemporaryFailException)
from couchbase.options import (ClusterOptions,
                               GetOptions,
                               InsertOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetResult,
                              MutationResult)

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class CollectionTests:

    NO_KEY = "not-a-key"
    FIFTY_YEARS = 50 * 365 * 24 * 60 * 60
    THIRTY_DAYS = 30 * 24 * 60 * 60

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

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
    def check_preserve_expiry_supported(self, cb_env):
        cb_env.check_if_feature_supported('preserve_expiry')

    @pytest.fixture(scope="class")
    def check_sync_durability_supported(self, cb_env):
        cb_env.check_if_feature_supported('sync_durability')

    @pytest.fixture(scope="class")
    def check_xattr_supported(self, cb_env):
        cb_env.check_if_feature_supported('xattr')

    @pytest_asyncio.fixture(name="new_kvp")
    async def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = await cb_env.get_new_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb_env.collection.remove,
                                                key,
                                                expected_exceptions=(DocumentNotFoundException,))

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest_asyncio.fixture(name="default_kvp_and_reset")
    async def default_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)
        await cb_env.collection.upsert(key, value)

    @pytest_asyncio.fixture(scope="class")
    async def check_replicas(self, cb_env):
        bucket_settings = await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        ping_res = await cb_env.bucket.ping()
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)
        if kv_endpoints is None or len(kv_endpoints) < (num_replicas + 1):
            pytest.skip("Not all replicas are online")

    @pytest_asyncio.fixture(scope="class")
    async def num_replicas(self, cb_env):
        bucket_settings = await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        return num_replicas

    @pytest.mark.asyncio
    async def test_exists(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        result = await cb.exists(key)
        assert isinstance(result, ExistsResult)
        assert result.exists is True

    @pytest.mark.asyncio
    async def test_does_not_exists(self, cb_env):
        cb = cb_env.collection
        result = await cb.exists(self.NO_KEY)
        assert isinstance(result, ExistsResult)
        assert result.exists is False

    @pytest.mark.asyncio
    async def test_get(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = await cb.get(key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    @pytest.mark.asyncio
    async def test_get_options(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = await cb.get(key, GetOptions(timeout=timedelta(seconds=2), with_expiry=False))
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    @pytest.mark.asyncio
    async def test_get_fails(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            await cb.get(self.NO_KEY)

    @pytest.mark.usefixtures("check_xattr_supported")
    @pytest.mark.asyncio
    async def test_get_with_expiry(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=1000)))

        expiry_path = "$document.exptime"
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry = res.content_as[int](0)
        assert expiry is not None
        assert expiry > 0
        expires_in = (datetime.fromtimestamp(expiry) - datetime.now()).total_seconds()
        # when running local, this can be be up to 1050, so just make sure > 0
        assert expires_in > 0

    @pytest.mark.asyncio
    async def test_expiry_really_expires(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = await cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        assert result.cas != 0
        await asyncio.sleep(3)
        with pytest.raises(DocumentNotFoundException):
            await cb.get(key)

    @pytest.mark.asyncio
    async def test_project(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = await cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))

        async def cas_matches(cb, new_cas):
            r = await cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        await cb_env.try_n_times(10, 3, cas_matches, cb, result.cas)
        result = await cb.get(key, GetOptions(project=["faa"]))
        assert {"faa": "ORD"} == result.content_as[dict]
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None

    @pytest.mark.asyncio
    async def test_project_bad_path(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        with pytest.raises(PathNotFoundException):
            await cb.get(key, GetOptions(project=["some", "qzx"]))

    @pytest.mark.asyncio
    async def test_project_project_not_list(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        # @TODO(jc):  better exception
        with pytest.raises(InvalidArgumentException):
            await cb.get(key, GetOptions(project="thiswontwork"))

    @pytest.mark.asyncio
    async def test_project_too_many_projections(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        project = []
        for _ in range(17):
            project.append("something")

        with pytest.raises(InvalidArgumentException):
            await cb.get(key, GetOptions(project=project))

    @pytest.mark.asyncio
    async def test_upsert(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = await cb.upsert(key, value, UpsertOptions(timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = await cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @pytest.mark.asyncio
    async def test_upsert_preserve_expiry_not_used(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value
        await cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        await cb.upsert(key, value1)
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        await asyncio.sleep(3)
        result = await cb.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @pytest.mark.asyncio
    async def test_upsert_preserve_expiry(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value

        await cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        await cb.upsert(key, value1, UpsertOptions(preserve_expiry=True))
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        await asyncio.sleep(3)
        with pytest.raises(DocumentNotFoundException):
            await cb.get(key)

    @pytest.mark.asyncio
    async def test_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = await cb.insert(key, value, InsertOptions(timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = await cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @pytest.mark.asyncio
    async def test_insert_document_exists(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        with pytest.raises(DocumentExistsException):
            await cb.insert(key, value)

    @pytest.mark.asyncio
    async def test_replace(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = await cb.replace(key, value, ReplaceOptions(timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = await cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @pytest.mark.asyncio
    async def test_replace_with_cas(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value1 = new_kvp.value
        result = await cb.get(key)
        old_cas = result.cas
        result = await cb.replace(key, value1, ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        with pytest.raises(CasMismatchException):
            await cb.replace(key, value1, ReplaceOptions(cas=old_cas))

    @pytest.mark.asyncio
    async def test_replace_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            await cb.replace(self.NO_KEY, {"some": "content"})

    @pytest.mark.asyncio
    async def test_remove(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        result = await cb.remove(key)
        assert isinstance(result, MutationResult)

        with pytest.raises(DocumentNotFoundException):
            await cb.get(key)

    @pytest.mark.asyncio
    async def test_remove_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            await cb.remove(self.NO_KEY)

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @pytest.mark.asyncio
    async def test_replace_preserve_expiry_not_used(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value

        await cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        await cb.replace(key, value1)
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        await asyncio.sleep(3)
        result = await cb.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @pytest.mark.asyncio
    async def test_replace_preserve_expiry(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value

        await cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        await cb.replace(key, value1, ReplaceOptions(preserve_expiry=True))
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        await asyncio.sleep(3)
        with pytest.raises(DocumentNotFoundException):
            await cb.get(key)

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    @pytest.mark.asyncio
    async def test_replace_preserve_expiry_fail(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value

        opts = ReplaceOptions(
            expiry=timedelta(
                seconds=5),
            preserve_expiry=True)
        with pytest.raises(InvalidArgumentException):
            await cb.replace(key, value, opts)

    @pytest.mark.asyncio
    async def test_touch(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value)
        await cb_env.try_n_times(10, 1, cb.get, key)
        result = await cb.touch(key, timedelta(seconds=2))
        assert isinstance(result, MutationResult)
        await asyncio.sleep(3)
        with pytest.raises(DocumentNotFoundException):
            await cb.get(key)

    @pytest.mark.asyncio
    async def test_touch_no_expire(self, cb_env, new_kvp):
        # TODO: handle MOCK
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value)
        await cb_env.try_n_times(10, 1, cb.get, key)
        await cb.touch(key, timedelta(seconds=15))
        g_result = await cb.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is not None
        await cb.touch(key, timedelta(seconds=0))
        g_result = await cb.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    @pytest.mark.asyncio
    async def test_get_and_touch(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value)
        await cb_env.try_n_times(10, 1, cb.get, key)
        result = await cb.get_and_touch(key, timedelta(seconds=2))
        assert isinstance(result, GetResult)
        await asyncio.sleep(3)
        with pytest.raises(DocumentNotFoundException):
            await cb.get(key)

    @pytest.mark.asyncio
    async def test_get_and_touch_no_expire(self, cb_env, new_kvp):
        # TODO: handle MOCK
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        await cb.upsert(key, value)
        await cb_env.try_n_times(10, 1, cb.get, key)
        await cb.get_and_touch(key, timedelta(seconds=15))
        g_result = await cb.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is not None
        await cb.get_and_touch(key, timedelta(seconds=0))
        g_result = await cb.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    @pytest.mark.asyncio
    async def test_get_and_lock(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = await cb.get_and_lock(key, timedelta(seconds=3))
        assert isinstance(result, GetResult)
        with pytest.raises(DocumentLockedException):
            await cb.upsert(key, value)

        await cb_env.try_n_times(10, 1, cb.upsert, key, value)

    @pytest.mark.asyncio
    async def test_get_after_lock(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        orig = await cb.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(orig, GetResult)
        result = await cb.get(key)
        assert orig.content_as[dict] == result.content_as[dict]
        assert orig.cas != result.cas

        # @TODO(jc):  cxx client raises ambiguous timeout w/ retry reason: kv_temporary_failure
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb.unlock,
                                                key,
                                                orig.cas,
                                                expected_exceptions=(TemporaryFailException,))

    @pytest.mark.asyncio
    async def test_get_and_lock_replace_with_cas(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        result = await cb.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        cas = result.cas
        with pytest.raises(DocumentLockedException):
            await cb.upsert(key, value)

        await cb.replace(key, value, ReplaceOptions(cas=cas))
        # @TODO(jc):  cxx client raises ambiguous timeout w/ retry reason: kv_temporary_failure
        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb.unlock,
                                                key,
                                                cas,
                                                expected_exceptions=(TemporaryFailException,))

    @pytest.mark.asyncio
    async def test_unlock(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        result = await cb.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        await cb.unlock(key, result.cas)
        await cb.upsert(key, value)

    @pytest.mark.asyncio
    async def test_unlock_wrong_cas(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        result = await cb.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas
        # @TODO(jc): MOCK - TemporaryFailException
        with pytest.raises((DocumentLockedException)):
            await cb.unlock(key, 100)

        await cb_env.try_n_times_till_exception(10,
                                                1,
                                                cb.unlock,
                                                key,
                                                cas,
                                                expected_exceptions=(TemporaryFailException,))

    @pytest.mark.usefixtures("check_replicas")
    @pytest.mark.asyncio
    async def test_get_any_replica(self, cb_env):
        pytest.skip("C++ client has not implemented replica operations.")
        # self._check_replicas(False)
        # self.coll.upsert('imakey100', self.CONTENT)
        # result = self.try_n_times(
        #     10, 3, self.coll.get_any_replica, 'imakey100')
        # self.assertDictEqual(self.CONTENT, result.content_as[dict])

    @pytest.mark.usefixtures("check_replicas")
    @pytest.mark.asyncio
    async def test_get_all_replicas(self, cb_env):
        pytest.skip("C++ client has not implemented replica operations.")
        # self._check_replicas()
        # self.coll.upsert(self.KEY, self.CONTENT)
        # # wait till it it there...
        # result = self.try_n_times(10, 3, self.coll.get_all_replicas, self.KEY)
        # if not hasattr(result, '__iter__'):
        #     result = [result]
        # for r in result:
        #     self.assertDictEqual(self.CONTENT, r.content_as[dict])

    @pytest.mark.usefixtures("check_replicas")
    @pytest.mark.asyncio
    async def test_get_all_replicas_returns_master(self, cb_env):
        pytest.skip("C++ client has not implemented replica operations.")
        # self._check_replicas()
        # self.coll.upsert('imakey100', self.CONTENT)
        # result = self.try_n_times(
        #     10, 3, self.coll.get_all_replicas, 'imakey100')
        # if not hasattr(result, '__iter__'):
        #     result = [result]
        # active_cnt = 0
        # replica_cnt = 0
        # for r in result:
        #     self.assertDictEqual(self.CONTENT, r.content_as[dict])
        #     if r.is_active:
        #         active_cnt += 1
        #     else:
        #         replica_cnt += 1

        # self.assertEqual(active_cnt, 1)
        # self.assertGreaterEqual(replica_cnt, active_cnt)

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.asyncio
    async def test_server_durable_upsert(self, cb_env, default_kvp_and_reset, num_replicas):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        if num_replicas > 1:
            await cb.upsert(key, value,
                            UpsertOptions(durability=durability))
            result = await cb.get(key)
            assert value == result.content_as[dict]
        else:
            with pytest.raises(DurabilityImpossibleException):
                await cb.upsert(key, value,
                                UpsertOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.asyncio
    async def test_server_durable_insert(self, cb_env, new_kvp, num_replicas):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        if num_replicas > 1:
            await cb.insert(key, value,
                            InsertOptions(durability=durability))
            result = await cb.get(key)
            assert value == result.content_as[dict]
        else:
            with pytest.raises(DurabilityImpossibleException):
                await cb.insert(key, value,
                                InsertOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.asyncio
    async def test_server_durable_replace(self, cb_env, default_kvp_and_reset, num_replicas):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        if num_replicas > 1:
            await cb.replace(key, value,
                             ReplaceOptions(durability=durability))
            result = await cb.get(key)
            assert value == result.content_as[dict]
        else:
            with pytest.raises(DurabilityImpossibleException):
                await cb.replace(key, value,
                                 ReplaceOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.asyncio
    async def test_server_durable_remove(self, cb_env, default_kvp_and_reset, num_replicas):
        cb = cb_env.collection
        key = default_kvp_and_reset.key

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        if num_replicas > 1:
            await cb.remove(key, RemoveOptions(durability=durability))
            with pytest.raises(DocumentNotFoundException):
                await cb.get(key)
        else:
            with pytest.raises(DurabilityImpossibleException):
                await cb.remove(key, RemoveOptions(durability=durability))

    @pytest.mark.asyncio
    async def test_client_durable_upsert(self, cb_env, num_replicas):
        pytest.skip("C++ client has not implemented replicate/persist durability.")
    #     durability = ClientDurability(
    #         persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
    #     self.cb.upsert(self.NOKEY, self.CONTENT,
    #                    UpsertOptions(durability=durability))
    #     result = self.cb.get(self.NOKEY)
    #     self.assertEqual(self.CONTENT, result.content_as[dict])

    @pytest.mark.asyncio
    async def test_client_durable_insert(self, cb_env):
        pytest.skip("C++ client has not implemented replicate/persist durability.")
    #     num_replicas = self.bucket._bucket.configured_replica_count
    #     durability = ClientDurability(
    #         persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
    #     self.cb.insert(self.NOKEY, self.CONTENT,
    #                    InsertOptions(durability=durability))
    #     result = self.cb.get(self.NOKEY)
    #     self.assertEqual(self.CONTENT, result.content_as[dict])

    @pytest.mark.asyncio
    async def test_client_durable_replace(self, cb_env):
        pytest.skip("C++ client has not implemented replicate/persist durability.")
    #     num_replicas = self.bucket._bucket.configured_replica_count
    #     content = {"new": "content"}
    #     durability = ClientDurability(
    #         persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
    #     self.cb.replace(self.KEY, content,
    #                     ReplaceOptions(durability=durability))
    #     result = self.cb.get(self.KEY)
    #     self.assertEqual(content, result.content_as[dict])

    @pytest.mark.asyncio
    async def test_client_durable_remove(self):
        pytest.skip("C++ client has not implemented replicate/persist durability.")
    #     num_replicas = self.bucket._bucket.configured_replica_count
    #     durability = ClientDurability(
    #         persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
    #     self.cb.remove(self.KEY, RemoveOptions(durability=durability))
    #     self.assertRaises(DocumentNotFoundException, self.cb.get, self.KEY)

    # @TODO(jc): - should an expiry of -1 raise an InvalidArgumentException?
    @pytest.mark.usefixtures("check_xattr_supported")
    @pytest.mark.asyncio
    @pytest.mark.parametrize("expiry", [FIFTY_YEARS + 1,
                                        FIFTY_YEARS,
                                        THIRTY_DAYS - 1,
                                        THIRTY_DAYS,
                                        int(time() - 1.0), 60, -1])
    async def test_document_expiry_values(self, cb_env, new_kvp, expiry):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        before = int(time() - 1.0)
        try:
            result = await cb.upsert(key, value, expiry=timedelta(seconds=expiry))
            assert result.cas is not None
        except InvalidArgumentException:
            if expiry != -1:
                raise

        expiry_path = "$document.exptime"
        res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        res_expiry = res.content_as[int](0)

        after = int(time() + 1.0)
        before + expiry <= res_expiry <= after + expiry
