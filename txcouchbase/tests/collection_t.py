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


from datetime import datetime, timedelta
from time import time

import pytest

import couchbase.subdocument as SD
from couchbase.diagnostics import ServiceType
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  DurabilityImpossibleException,
                                  InvalidArgumentException,
                                  PathNotFoundException,
                                  TemporaryFailException)
from couchbase.options import (GetOptions,
                               InsertOptions,
                               RemoveOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              MutationResult)

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment,
                          run_in_reactor_thread)


class CollectionTests:
    NO_KEY = "not-a-key"
    FIFTY_YEARS = 50 * 365 * 24 * 60 * 60
    THIRTY_DAYS = 30 * 24 * 60 * 60

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param, manage_buckets=True)

        if request.param == CollectionType.NAMED:
            cb_env.try_n_times(5, 3, cb_env.setup_named_collections, is_deferred=False)

        cb_env.try_n_times(3, 5, cb_env.load_data, is_deferred=False)
        yield cb_env
        cb_env.try_n_times(3, 5, cb_env.purge_data, is_deferred=False)
        if request.param == CollectionType.NAMED:
            cb_env.try_n_times_till_exception(5, 3,
                                              cb_env.teardown_named_collections,
                                              raise_if_no_exception=False,
                                              is_deferred=False)

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        cb_env.check_if_feature_supported('preserve_expiry')

    @pytest.fixture(scope="class")
    def check_sync_durability_supported(self, cb_env):
        cb_env.check_if_feature_supported('sync_durability')

    @pytest.fixture(scope="class")
    def check_xattr_supported(self, cb_env):
        cb_env.check_if_feature_supported('xattr')

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
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

    @pytest.fixture(name="default_kvp_and_reset")
    def default_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times(5, 3, cb_env.collection.upsert, key, value)

    @pytest.fixture(scope="class")
    def check_replicas(self, cb_env):
        bucket_settings = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        ping_res = run_in_reactor_thread(cb_env.bucket.ping)
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)
        if kv_endpoints is None or len(kv_endpoints) < (num_replicas + 1):
            pytest.skip("Not all replicas are online")

    @pytest.fixture(scope="class")
    def num_replicas(self, cb_env):
        bucket_settings = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        return num_replicas

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_exists(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        result = run_in_reactor_thread(cb.exists, key)
        assert isinstance(result, ExistsResult)
        assert result.exists is True

    def test_does_not_exists(self, cb_env):
        cb = cb_env.collection
        result = run_in_reactor_thread(cb.exists, self.NO_KEY)
        assert isinstance(result, ExistsResult)
        assert result.exists is False

    def test_get(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = run_in_reactor_thread(cb.get, key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_options(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = run_in_reactor_thread(cb.get,
                                       key,
                                       GetOptions(timeout=timedelta(seconds=2),
                                                  with_expiry=False))
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_fails(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, self.NO_KEY)

    @pytest.mark.usefixtures("check_xattr_supported")
    def test_get_with_expiry(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value, UpsertOptions(expiry=timedelta(seconds=1000)))

        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry = res.content_as[int](0)
        assert expiry is not None
        assert expiry > 0
        expires_in = (datetime.fromtimestamp(expiry) - datetime.now()).total_seconds()
        # when running local, this can be be up to 1050, so just make sure > 0
        assert expires_in > 0

    def test_expiry_really_expires(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = run_in_reactor_thread(cb.upsert,
                                       key,
                                       value,
                                       UpsertOptions(expiry=timedelta(seconds=2)))
        assert result.cas != 0

        cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

    def test_project(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = run_in_reactor_thread(cb.upsert,
                                       key,
                                       value,
                                       UpsertOptions(expiry=timedelta(seconds=2)))

        def cas_matches(cb, new_cas):
            r = run_in_reactor_thread(cb.get, key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        print('waiting for cas matches')
        cb_env.try_n_times(10, 3, cas_matches, cb, result.cas, is_deferred=False)
        result = run_in_reactor_thread(cb.get, key, GetOptions(project=["faa"]))
        assert {"faa": "ORD"} == result.content_as[dict]
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None

    def test_project_bad_path(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        with pytest.raises(PathNotFoundException):
            run_in_reactor_thread(cb.get,
                                  key,
                                  GetOptions(project=["some", "qzx"]))

    def test_project_project_not_list(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.get,
                                  key,
                                  GetOptions(project="thiswontwork"))

    def test_project_too_many_projections(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        project = []
        for _ in range(17):
            project.append("something")

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.get,
                                  key,
                                  GetOptions(project=project))

    def test_upsert(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = run_in_reactor_thread(cb.upsert,
                                       key,
                                       value,
                                       UpsertOptions(timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    def test_upsert_preserve_expiry_not_used(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        run_in_reactor_thread(cb.upsert, key, value1)
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        cb_env.sleep(3.0)
        result = run_in_reactor_thread(cb.get, key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    def test_upsert_preserve_expiry(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value

        run_in_reactor_thread(cb.upsert,
                              key,
                              value,
                              UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        run_in_reactor_thread(cb.upsert,
                              key,
                              value1,
                              UpsertOptions(preserve_expiry=True))
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

    def test_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = run_in_reactor_thread(cb.insert,
                                       key,
                                       value,
                                       InsertOptions(timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert_document_exists(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        with pytest.raises(DocumentExistsException):
            run_in_reactor_thread(cb.insert, key, value)

    def test_replace(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = run_in_reactor_thread(cb.replace,
                                       key,
                                       value,
                                       ReplaceOptions(timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_replace_with_cas(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value1 = new_kvp.value
        result = run_in_reactor_thread(cb.get, key)
        old_cas = result.cas
        result = run_in_reactor_thread(cb.replace,
                                       key,
                                       value1,
                                       ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        with pytest.raises(CasMismatchException):
            run_in_reactor_thread(cb.replace,
                                  key,
                                  value1,
                                  ReplaceOptions(cas=old_cas))

    def test_replace_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.replace, self.NO_KEY, {"some": "content"})

    def test_remove(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        result = run_in_reactor_thread(cb.remove, key)
        assert isinstance(result, MutationResult)

        with pytest.raises(DocumentNotFoundException):
            cb_env.try_n_times_till_exception(3,
                                              1,
                                              cb.get,
                                              key,
                                              expected_exceptions=(DocumentNotFoundException,),
                                              raise_exception=True)

    def test_remove_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.remove, self.NO_KEY)

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    def test_replace_preserve_expiry_not_used(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value

        run_in_reactor_thread(cb.upsert,
                              key,
                              value,
                              UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        run_in_reactor_thread(cb.replace, key, value1)
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        cb_env.sleep(3.0)
        result = run_in_reactor_thread(cb.get, key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    def test_replace_preserve_expiry(self, cb_env, default_kvp_and_reset, new_kvp):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        value1 = new_kvp.value

        run_in_reactor_thread(cb.upsert,
                              key,
                              value,
                              UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        run_in_reactor_thread(cb.replace,
                              key,
                              value1,
                              ReplaceOptions(preserve_expiry=True))
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

    @pytest.mark.usefixtures("check_preserve_expiry_supported")
    def test_replace_preserve_expiry_fail(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value

        opts = ReplaceOptions(
            expiry=timedelta(
                seconds=5),
            preserve_expiry=True)
        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.replace, key, value, opts)

    def test_touch(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 1, cb.get, key)
        result = run_in_reactor_thread(cb.touch, key, timedelta(seconds=2))
        assert isinstance(result, MutationResult)
        cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

    def test_touch_no_expire(self, cb_env, new_kvp):
        # TODO: handle MOCK
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 1, cb.get, key)
        run_in_reactor_thread(cb.touch, key, timedelta(seconds=15))
        g_result = run_in_reactor_thread(cb.get, key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is not None
        run_in_reactor_thread(cb.touch, key, timedelta(seconds=0))
        g_result = run_in_reactor_thread(cb.get, key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    def test_get_and_touch(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 1, cb.get, key)
        result = run_in_reactor_thread(cb.get_and_touch, key, timedelta(seconds=2))
        assert isinstance(result, GetResult)
        cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

    def test_get_and_touch_no_expire(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 1, cb.get, key)
        run_in_reactor_thread(cb.get_and_touch, key, timedelta(seconds=15))
        g_result = run_in_reactor_thread(cb.get, key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is not None
        run_in_reactor_thread(cb.get_and_touch, key, timedelta(seconds=0))
        g_result = run_in_reactor_thread(cb.get, key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    def test_get_and_lock(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = run_in_reactor_thread(cb.get_and_lock, key, timedelta(seconds=3))
        assert isinstance(result, GetResult)
        with pytest.raises(DocumentLockedException):
            run_in_reactor_thread(cb.upsert, key, value)

        cb_env.try_n_times(10, 1, cb.upsert, key, value)

    def test_get_after_lock(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        orig = run_in_reactor_thread(cb.get_and_lock, key, timedelta(seconds=5))
        assert isinstance(orig, GetResult)
        result = run_in_reactor_thread(cb.get, key)
        assert orig.content_as[dict] == result.content_as[dict]
        assert orig.cas != result.cas

        # @TODO(jc):  cxx client raises ambiguous timeout w/ retry reason: kv_temporary_failure
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb.unlock,
                                          key,
                                          orig.cas,
                                          expected_exceptions=(TemporaryFailException,))

    def test_get_and_lock_replace_with_cas(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        result = run_in_reactor_thread(cb.get_and_lock, key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        cas = result.cas
        # TODO: handle retry reasons, looks to be where we can get the locked
        # exception
        with pytest.raises((AmbiguousTimeoutException, DocumentLockedException)):
            run_in_reactor_thread(cb.upsert, key, value)

        run_in_reactor_thread(cb.replace, key, value, ReplaceOptions(cas=cas))
        # @TODO(jc):  cxx client raises ambiguous timeout w/ retry reason: kv_temporary_failure
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb.unlock,
                                          key,
                                          cas,
                                          expected_exceptions=(TemporaryFailException,))

    def test_unlock(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        result = run_in_reactor_thread(cb.get_and_lock, key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        run_in_reactor_thread(cb.unlock, key, result.cas)
        run_in_reactor_thread(cb.upsert, key, value)

    def test_unlock_wrong_cas(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        result = run_in_reactor_thread(cb.get_and_lock, key, timedelta(seconds=5))
        cas = result.cas
        # @TODO(jc): MOCK - TemporaryFailException
        with pytest.raises((DocumentLockedException)):
            run_in_reactor_thread(cb.unlock, key, 100)

        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb.unlock,
                                          key,
                                          cas,
                                          expected_exceptions=(TemporaryFailException,))

    @pytest.mark.usefixtures("check_replicas")
    def test_get_any_replica(self, cb_env, default_kvp):
        result = cb_env.try_n_times(10, 3, cb_env.collection.get_any_replica, default_kvp.key)
        assert isinstance(result, GetReplicaResult)
        assert isinstance(result.is_replica, bool)
        assert default_kvp.value == result.content_as[dict]

    @pytest.mark.usefixtures("check_replicas")
    def test_get_any_replica_fail(self, cb_env):
        with pytest.raises(DocumentUnretrievableException):
            run_in_reactor_thread(cb_env.collection.get_any_replica, 'not-a-key')

    @pytest.mark.usefixtures("check_replicas")
    def test_get_all_replicas(self, cb_env, default_kvp):
        result = cb_env.try_n_times(10, 3, cb_env.collection.get_all_replicas, default_kvp.key)
        # make sure we can iterate over results
        while True:
            try:
                res = next(result)
                assert isinstance(res, GetReplicaResult)
                assert isinstance(res.is_replica, bool)
                assert default_kvp.value == res.content_as[dict]
            except StopIteration:
                break

    @pytest.mark.usefixtures("check_replicas")
    def test_get_all_replicas_fail(self, cb_env):
        pytest.skip("Pending merge of CXXCBC-52")
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb_env.collection.get_all_replicas, 'not-a-key')

    @pytest.mark.usefixtures("check_replicas")
    def test_get_all_replicas_results(self, cb_env, default_kvp):
        result = cb_env.try_n_times(10, 3, cb_env.collection.get_all_replicas, default_kvp.key)
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

    @pytest.mark.usefixtures("check_sync_durability_supported")
    def test_server_durable_upsert(self, cb_env, default_kvp_and_reset, num_replicas):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        if num_replicas > 1:
            run_in_reactor_thread(cb.upsert,
                                  key,
                                  value,
                                  UpsertOptions(durability=durability))
            result = run_in_reactor_thread(cb.get, key)
            assert value == result.content_as[dict]
        else:
            try:
                run_in_reactor_thread(cb.upsert,
                                      key,
                                      value,
                                      UpsertOptions(durability=durability))
            except DurabilityImpossibleException:
                pass  # this is okay -- server not setup correctly

    @pytest.mark.usefixtures("check_sync_durability_supported")
    def test_server_durable_insert(self, cb_env, new_kvp, num_replicas):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        if num_replicas > 1:
            run_in_reactor_thread(cb.insert,
                                  key,
                                  value,
                                  InsertOptions(durability=durability))
            result = run_in_reactor_thread(cb.get, key)
            assert value == result.content_as[dict]
        else:
            try:
                run_in_reactor_thread(cb.insert,
                                      key,
                                      value,
                                      InsertOptions(durability=durability))
            except DurabilityImpossibleException:
                pass  # this is okay -- server not setup correctly

    @pytest.mark.usefixtures("check_sync_durability_supported")
    def test_server_durable_replace(self, cb_env, default_kvp_and_reset, num_replicas):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value = default_kvp_and_reset.value
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)

        if num_replicas > 1:
            run_in_reactor_thread(cb.replace,
                                  key,
                                  value,
                                  ReplaceOptions(durability=durability))
            result = run_in_reactor_thread(cb.get, key)
            assert value == result.content_as[dict]
        else:
            try:
                run_in_reactor_thread(cb.replace,
                                      key,
                                      value,
                                      ReplaceOptions(durability=durability))
            except DurabilityImpossibleException:
                pass  # this is okay -- server not setup correctly

    @pytest.mark.usefixtures("check_sync_durability_supported")
    def test_server_durable_remove(self, cb_env, default_kvp_and_reset, num_replicas):
        cb = cb_env.collection
        key = default_kvp_and_reset.key

        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        if num_replicas > 1:
            run_in_reactor_thread(cb.remove, key, RemoveOptions(durability=durability))
            with pytest.raises(DocumentNotFoundException):
                run_in_reactor_thread(cb.get, key)
        else:
            try:
                run_in_reactor_thread(cb.remove, key, RemoveOptions(durability=durability))
            except DurabilityImpossibleException:
                pass  # this is okay -- server not setup correctly

    def test_client_durable_upsert(self):
        pytest.skip("C++ client has not implemented replicate/persist durability.")

    def test_client_durable_insert(self):
        pytest.skip("C++ client has not implemented replicate/persist durability.")

    def test_client_durable_replace(self):
        pytest.skip("C++ client has not implemented replicate/persist durability.")

    def test_client_durable_remove(self):
        pytest.skip("C++ client has not implemented replicate/persist durability.")

    # @TODO(jc): - should an expiry of -1 raise an InvalidArgumentException?

    @pytest.mark.usefixtures("check_xattr_supported")
    @pytest.mark.parametrize("expiry", [FIFTY_YEARS + 1,
                                        FIFTY_YEARS,
                                        THIRTY_DAYS - 1,
                                        THIRTY_DAYS,
                                        int(time() - 1.0), 60, -1])
    def test_document_expiry_values(self, cb_env, new_kvp, expiry):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        before = int(time() - 1.0)
        try:
            result = run_in_reactor_thread(cb.upsert, key, value, expiry=timedelta(seconds=expiry))
            assert result.cas is not None
        except InvalidArgumentException:
            if expiry != -1:
                raise

        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        res_expiry = res.content_as[int](0)

        after = int(time() + 1.0)
        before + expiry <= res_expiry <= after + expiry
