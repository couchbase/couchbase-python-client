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

import pytest

import couchbase.subdocument as SD
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (DocumentExistsException,
                                  DocumentNotFoundException,
                                  DurabilityImpossibleException,
                                  InvalidArgumentException,
                                  InvalidValueException,
                                  PathExistsException,
                                  PathMismatchException,
                                  PathNotFoundException)
from couchbase.options import GetOptions, MutateInOptions
from couchbase.result import (GetResult,
                              LookupInResult,
                              MutateInResult)

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment,
                          run_in_reactor_thread)


class SubDocumentTests:
    NO_KEY = "not-a-key"

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param)

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
    def skip_if_less_than_cheshire_cat(self, cb_env):
        if cb_env.cluster.server_version_short < 7.0:
            pytest.skip("Feature only available on CBS >= 7.0")

    @pytest.fixture(scope="class")
    def skip_if_less_than_alice(self, cb_env):
        if cb_env.cluster.server_version_short < 6.5:
            pytest.skip("Feature only available on CBS >= 6.5")

    @pytest.fixture(scope="class")
    def check_xattr_supported(self, cb_env):
        cb_env.check_if_feature_supported('xattr')

    @pytest.fixture(scope="class")
    def num_replicas(self, cb_env):
        pytest.skip('num_replicas not supported, pending txcouchbase mgmt updates.')
        bucket_settings = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        return num_replicas

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
    def skip_mock_mutate_in(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES + couchbase++ not playing nice...")

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_lookup_in_simple_get(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.lookup_in, key, (SD.get("geo"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[dict](0) == value["geo"]

    def test_lookup_in_simple_get_spec_as_list(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = run_in_reactor_thread(cb.lookup_in, key, [SD.get("geo")])
        assert isinstance(result, LookupInResult)
        assert result.content_as[dict](0) == value["geo"]

    def test_lookup_in_simple_exists(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.lookup_in, key, (SD.exists("geo"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        # no value content w/ EXISTS operation
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_simple_exists_bad_path(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.lookup_in, key, (SD.exists("qzzxy"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0) is False
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_one_path_not_found(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.lookup_in,
                                       key,
                                       (SD.exists("geo"), SD.exists("qzzxy"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        assert result.exists(1) is False
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](1)

    def test_lookup_in_simple_long_path(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        # add longer path to doc
        value["long_path"] = {"a": {"b": {"c": "yo!"}}}
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.lookup_in,
                                       key,
                                       (SD.get("long_path.a.b.c"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value["long_path"]["a"]["b"]["c"]
        # reset to norm
        run_in_reactor_thread(cb.remove, key)

    @pytest.mark.usefixtures("check_xattr_supported")
    def test_lookup_in_multiple_specs(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.lookup_in,
                                       key,
                                       (SD.get("$document.exptime", xattr=True),
                                        SD.exists("geo"),
                                        SD.get("geo"), SD.get("geo.alt"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 0
        assert result.exists(1) is True
        assert result.content_as[dict](2) == value["geo"]
        assert result.content_as[int](3) == value["geo"]["alt"]

    def test_count(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["count"] = [1, 2, 3, 4, 5]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.lookup_in, key, (SD.count("count"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 5

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_simple(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)

        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.upsert("city", "New City"),
                                        SD.replace("faa", "CTY")),
                                       MutateInOptions(expiry=timedelta(seconds=1000)))

        value["city"] = "New City"
        value["faa"] = "CTY"

        def cas_matches(cb, new_cas):
            r = run_in_reactor_thread(cb.get, key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        cb_env.try_n_times(10, 3, cas_matches, cb, result.cas, is_deferred=False)

        result = run_in_reactor_thread(cb.get, key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_simple_spec_as_list(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)

        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       [SD.upsert("city", "New City"),
                                        SD.replace("faa", "CTY")],
                                       MutateInOptions(expiry=timedelta(seconds=1000)))

        value["city"] = "New City"
        value["faa"] = "CTY"

        def cas_matches(cb, new_cas):
            r = run_in_reactor_thread(cb.get, key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        cb_env.try_n_times(10, 3, cas_matches, cb, result.cas, is_deferred=False)

        result = run_in_reactor_thread(cb.get, key)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures("check_xattr_supported")
    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_expiry(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)

        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.upsert("city", "New City"),
                                        SD.replace("faa", "CTY")),
                                       MutateInOptions(expiry=timedelta(seconds=1000)))

        def cas_matches(cb, new_cas):
            r = run_in_reactor_thread(cb.get, key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        cb_env.try_n_times(10, 3, cas_matches, cb, result.cas, is_deferred=False)

        result = run_in_reactor_thread(cb.get, key, GetOptions(with_expiry=True))
        expires_in = (result.expiry_time - datetime.now()).total_seconds()
        assert expires_in > 0 and expires_in < 1021

        # reset to norm
        run_in_reactor_thread(cb.remove, key)

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_remove(self, cb_env, new_kvp):

        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)

        run_in_reactor_thread(cb.mutate_in, key, [SD.remove('geo.alt')])
        result = run_in_reactor_thread(cb.get, key)
        assert 'alt' not in result.content_as[dict]['geo']

    @pytest.mark.usefixtures("skip_if_less_than_cheshire_cat")
    def test_mutate_in_preserve_expiry_not_used(self, cb_env, default_kvp_and_reset):
        if cb_env.is_mock_server:
            pytest.skip("Mock will not return expiry in the xaddrs.")

        cb = cb_env.collection
        key = default_kvp_and_reset.key

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.upsert("city", "New City"),
                               SD.replace("faa", "CTY")),
                              MutateInOptions(expiry=timedelta(seconds=5)))

        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        run_in_reactor_thread(cb.mutate_in, key, (SD.upsert("city", "Updated City"),))
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        cb_env.sleep(3)
        result = run_in_reactor_thread(cb.get, key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict]['city'] == 'Updated City'

    @pytest.mark.usefixtures("skip_if_less_than_cheshire_cat")
    def test_mutate_in_preserve_expiry(self, cb_env, default_kvp_and_reset):
        if cb_env.is_mock_server:
            pytest.skip("Mock will not return expiry in the xaddrs.")

        cb = cb_env.collection
        key = default_kvp_and_reset.key

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.upsert("city", "New City"),
                               SD.replace("faa", "CTY")),
                              MutateInOptions(expiry=timedelta(seconds=2)))

        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.upsert("city", "Updated City"),),
                              MutateInOptions(preserve_expiry=True))
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        cb_env.sleep(3)
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

    @pytest.mark.usefixtures("skip_if_less_than_cheshire_cat")
    def test_mutate_in_preserve_expiry_fails(self, cb_env, default_kvp_and_reset):
        if cb_env.is_mock_server:
            pytest.skip("Mock will not return expiry in the xaddrs.")

        cb = cb_env.collection
        key = default_kvp_and_reset.key
        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.insert("c", "ccc"),),
                                  MutateInOptions(preserve_expiry=True),
                                  )

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.replace("c", "ccc"),),
                                  MutateInOptions(
                                      expiry=timedelta(
                                          seconds=5),
                                      preserve_expiry=True),
                                  )

    @pytest.mark.usefixtures("skip_if_less_than_alice")
    def test_mutate_in_server_durability(self, cb_env, default_kvp_and_reset, num_replicas):
        if cb_env.is_mock_server:
            pytest.skip("Mock will not return expiry in the xaddrs.")

        cb = cb_env.collection
        key = default_kvp_and_reset.key
        if num_replicas > 1:
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.upsert("city", "New City"),
                                   SD.replace("faa", "CTY")),
                                  MutateInOptions(durability=ServerDurability(
                                      level=DurabilityLevel.PERSIST_TO_MAJORITY)))
        else:
            try:
                run_in_reactor_thread(cb.mutate_in,
                                      key,
                                      (SD.upsert("city", "New City"),
                                       SD.replace("faa", "CTY")),
                                      MutateInOptions(durability=ServerDurability(
                                          level=DurabilityLevel.PERSIST_TO_MAJORITY)))
            except DurabilityImpossibleException:
                pass  # this is okay -- server not setup correctly

    @pytest.mark.usefixtures("skip_if_less_than_alice")
    def test_mutate_in_client_durability(self, cb_env, default_kvp_and_reset, num_replicas):
        pytest.skip("C++ client has not implemented replicate/persist durability.")

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_upsert_semantics(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.upsert('new_path', 'im new'),),
                              MutateInOptions(store_semantics=SD.StoreSemantics.UPSERT))

        res = cb_env.try_n_times(10, 3, cb.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_upsert_semantics_kwargs(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.upsert('new_path', 'im new'),),
                              upsert_doc=True)

        res = cb_env.try_n_times(10, 3, cb.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_insert_semantics(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.insert('new_path', 'im new'),),
                              MutateInOptions(store_semantics=SD.StoreSemantics.INSERT))

        res = cb_env.try_n_times(10, 3, cb.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_mutate_in_insert_semantics_kwargs(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.insert('new_path', 'im new'),),
                              insert_doc=True)

        res = cb_env.try_n_times(10, 3, cb.get, key)
        assert res.content_as[dict] == {'new_path': 'im new'}

    def test_mutate_in_insert_semantics_fail(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key

        with pytest.raises(DocumentExistsException):
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.insert('new_path', 'im new'),),
                                  insert_doc=True)

    def test_mutate_in_replace_semantics(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.upsert('new_path', 'im new'),),
                              MutateInOptions(store_semantics=SD.StoreSemantics.REPLACE))

        res = cb_env.try_n_times(10, 3, cb.get, key)
        assert res.content_as[dict]['new_path'] == 'im new'

    def test_mutate_in_replace_semantics_kwargs(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key

        run_in_reactor_thread(cb.mutate_in,
                              key,
                              (SD.upsert('new_path', 'im new'),),
                              replace_doc=True)

        res = cb_env.try_n_times(10, 3, cb.get, key)
        assert res.content_as[dict]['new_path'] == 'im new'

    def test_mutate_in_replace_semantics_fail(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.upsert('new_path', 'im new'),),
                                  replace_doc=True)

    def test_mutate_in_store_semantics_fail(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.upsert('new_path', 'im new'),),
                                  insert_doc=True, upsert_doc=True)

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.upsert('new_path', 'im new'),),
                                  insert_doc=True, replace_doc=True)

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb.mutate_in,
                                  key,
                                  (SD.upsert('new_path', 'im new'),),
                                  upsert_doc=True, replace_doc=True)

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_array_append(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["array"] = [1, 2, 3, 4]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_append("array", 5),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert val["array"][4] == 5

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_array_prepend(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["array"] = [1, 2, 3, 4]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_prepend("array", 0),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert val["array"][0] == 0

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_array_insert(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["array"] = [1, 2, 4, 5]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_insert("array.[2]", 3),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert val["array"][2] == 3

    def test_array_add_unique(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["array"] = [0, 1, 2, 3]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_addunique("array", 4),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert 4 in val["array"]

    def test_array_as_document(self, cb_env):
        cb = cb_env.collection
        key = "simple-key"
        run_in_reactor_thread(cb.upsert, key, [])
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_append("", 2),
                                        SD.array_prepend("", 0),
                                        SD.array_insert("[1]", 1),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[list]
        assert isinstance(val, list)
        assert len(val) == 3
        assert val[0] == 0
        assert val[1] == 1
        assert val[2] == 2

        # clean-up
        run_in_reactor_thread(cb.remove, key)

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_array_append_multi_insert(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["array"] = [1, 2, 3, 4, 5, 6, 7]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_append("array", 8, 9, 10),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        app_res = val["array"][7:]
        assert len(app_res) == 3
        assert app_res == [8, 9, 10]

    def test_array_prepend_multi_insert(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["array"] = [4, 5, 6, 7, 8, 9, 10]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_prepend("array", 1, 2, 3),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        pre_res = val["array"][:3]
        assert len(pre_res) == 3
        assert pre_res == [1, 2, 3]

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_array_insert_multi_insert(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["array"] = [1, 2, 3, 4, 8, 9, 10]
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_insert("array.[4]", 5, 6, 7),))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        ins_res = val["array"][4:7]
        assert len(ins_res) == 3
        assert ins_res == [5, 6, 7]

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_array_add_unique_fail(self, cb_env):
        cb = cb_env.collection
        key = "simple-key"
        value = {
            "a": "aaa",
            "b": [0, 1, 2, 3],
            "c": [1.25, 1.5, {"nested": ["str", "array"]}],
        }
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)

        with pytest.raises(PathExistsException):
            run_in_reactor_thread(cb.mutate_in, key, (SD.array_addunique("b", 3),))

        with pytest.raises(InvalidValueException):
            run_in_reactor_thread(cb.mutate_in, key, (SD.array_addunique("b", [4, 5, 6]),))

        with pytest.raises(InvalidValueException):
            run_in_reactor_thread(cb.mutate_in, key, (SD.array_addunique("b", {"c": "d"}),))

        with pytest.raises(PathMismatchException):
            run_in_reactor_thread(cb.mutate_in, key, (SD.array_addunique("c", 2),))

    def test_increment(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["count"] = 100
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.increment("count", 50),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["count"] == 150

    def test_decrement(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["count"] = 100
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.decrement("count", 50),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["count"] == 50

    def test_insert_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.insert("new.path",
                                                  "parents created",
                                                  create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["new"]["path"] == "parents created"

    def test_upsert_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.upsert("new.path",
                                                  "parents created",
                                                  create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["new"]["path"] == "parents created"

    def test_array_append_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_append("new.array", "Hello,", create_parents=True),
                                        SD.array_append("new.array", "World!"),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["new"]["array"] == [
            "Hello,", "World!"]

    def test_array_prepend_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_prepend("new.array", "World!", create_parents=True),
                                        SD.array_prepend("new.array", "Hello,"),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["new"]["array"] == [
            "Hello,", "World!"]

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_array_add_unique_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.array_addunique("new.set", "new", create_parents=True),
                                        SD.array_addunique("new.set", "unique"),
                                        SD.array_addunique("new.set", "set")))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        new_set = result.content_as[dict]["new"]["set"]
        assert isinstance(new_set, list)
        assert "new" in new_set
        assert "unique" in new_set
        assert "set" in new_set

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_increment_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.increment("new.counter", 100, create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["new"]["counter"] == 100

    @pytest.mark.usefixtures('skip_mock_mutate_in')
    def test_decrement_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        run_in_reactor_thread(cb.upsert, key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = run_in_reactor_thread(cb.mutate_in,
                                       key,
                                       (SD.decrement("new.counter", 100, create_parents=True),))
        assert isinstance(result, MutateInResult)
        result = run_in_reactor_thread(cb.get, key)
        assert result.content_as[dict]["new"]["counter"] == -100
