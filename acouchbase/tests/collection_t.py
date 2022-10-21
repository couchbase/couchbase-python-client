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
from datetime import datetime, timedelta
from time import time

import pytest
import pytest_asyncio

import couchbase.subdocument as SD
from acouchbase.cluster import get_event_loop
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import (CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException,
                                  PathNotFoundException,
                                  TemporaryFailException)
from couchbase.options import (GetOptions,
                               InsertOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
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
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       request.param,
                                                       manage_buckets=True)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        await cb_env.try_n_times(3, 5, cb_env.load_data)
        yield cb_env
        await cb_env.try_n_times_till_exception(3, 5,
                                                cb_env.purge_data,
                                                raise_if_no_exception=False)
        if request.param == CollectionType.NAMED:
            await cb_env.try_n_times_till_exception(5, 3,
                                                    cb_env.teardown_named_collections,
                                                    raise_if_no_exception=False)

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
                                                expected_exceptions=(DocumentNotFoundException,),
                                                reset_on_timeout=True,
                                                reset_num_times=3)

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest_asyncio.fixture(name="default_kvp_and_reset")
    async def default_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)
        await cb_env.try_n_times(5, 3, cb_env.collection.upsert, key, value)

    @pytest_asyncio.fixture(scope="class")
    async def num_replicas(self, cb_env):
        bucket_settings = await cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        return num_replicas

    @pytest_asyncio.fixture(scope="class")
    async def check_replicas(self, cb_env, num_replicas):
        ping_res = await cb_env.bucket.ping()
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)
        if kv_endpoints is None or len(kv_endpoints) < (num_replicas + 1):
            pytest.skip("Not all replicas are online")

    @pytest.fixture(scope="class")
    def num_nodes(self, cb_env):
        return len(cb_env.cluster._cluster_info.nodes)

    @pytest.fixture(scope="class")
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip("Test only for clusters with more than a single node.")

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
    async def test_get_any_replica(self, cb_env, default_kvp):
        result = await cb_env.try_n_times(10, 3, cb_env.collection.get_any_replica, default_kvp.key)
        assert isinstance(result, GetReplicaResult)
        assert isinstance(result.is_replica, bool)
        assert default_kvp.value == result.content_as[dict]

    @pytest.mark.usefixtures("check_replicas")
    @pytest.mark.asyncio
    async def test_get_any_replica_fail(self, cb_env):
        with pytest.raises(DocumentUnretrievableException):
            await cb_env.collection.get_any_replica('not-a-key')

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    @pytest.mark.asyncio
    async def test_get_all_replicas(self, cb_env, default_kvp):
        result = await cb_env.try_n_times(10, 3, cb_env.collection.get_all_replicas, default_kvp.key)
        # make sure we can iterate over results
        while True:
            try:
                res = next(result)
                assert isinstance(res, GetReplicaResult)
                assert isinstance(res.is_replica, bool)
                assert default_kvp.value == res.content_as[dict]
            except StopIteration:
                break

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    @pytest.mark.asyncio
    async def test_get_all_replicas_fail(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            await cb_env.collection.get_all_replicas('not-a-key')

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    @pytest.mark.asyncio
    async def test_get_all_replicas_results(self, cb_env, default_kvp):
        result = await cb_env.try_n_times(10, 3, cb_env.collection.get_all_replicas, default_kvp.key)
        active_cnt = 0
        replica_cnt = 0
        for res in result:
            assert isinstance(res, GetReplicaResult)
            assert isinstance(res.is_replica, bool)
            assert default_kvp.value == res.content_as[dict]
            if res.is_replica:
                replica_cnt += 1
            else:
                active_cnt += 1

        assert active_cnt == 1
        assert replica_cnt >= active_cnt

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
        if expiry == -1:
            with pytest.raises(InvalidArgumentException):
                await cb.upsert(key, value, expiry=timedelta(seconds=expiry))
        else:
            before = int(time() - 1.0)
            result = await cb.upsert(key, value, expiry=timedelta(seconds=expiry))
            assert result.cas is not None

            expiry_path = "$document.exptime"
            res = await cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
            res_expiry = res.content_as[int](0)

            after = int(time() + 1.0)
            before + expiry <= res_expiry <= after + expiry
