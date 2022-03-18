from datetime import datetime, timedelta

import pytest

import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (DocumentNotFoundException,
                                  InvalidValueException,
                                  PathExistsException,
                                  PathMismatchException,
                                  PathNotFoundException)
from couchbase.options import (ClusterOptions,
                               GetOptions,
                               MutateInOptions)
from couchbase.result import LookupInResult, MutateInResult
from txcouchbase.cluster import Cluster

from ._test_utils import TestEnvironment, wait_for_deferred


class SubDocumentTests:
    NO_KEY = "not-a-key"

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        opts = ClusterOptions(PasswordAuthenticator(
            couchbase_config.admin_username, couchbase_config.admin_password))
        c = Cluster(
            conn_string, opts)
        b = c.bucket(f"{couchbase_config.bucket_name}")
        wait_for_deferred(b.on_connect())
        coll = b.default_collection()
        cb_env = TestEnvironment(c, b, coll, couchbase_config)
        wait_for_deferred(cb_env.load_data())
        yield cb_env
        wait_for_deferred(cb_env.purge_data())
        wait_for_deferred(c.close())

    def test_lookup_in_simple_get(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.lookup_in(key, (SD.get("geo"),)))
        assert isinstance(result, LookupInResult)
        assert result.content_as[dict](0) == value["geo"]

    def test_lookup_in_simple_exists(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.lookup_in(key, (SD.exists("geo"),)))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        # no value content w/ EXISTS operation
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_simple_exists_bad_path(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.lookup_in(key, (SD.exists("qzzxy"),)))
        assert isinstance(result, LookupInResult), "result should be LookupInResult"
        assert result.exists(0) is False, "Path shouldn't exist, but does"
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_one_path_not_found(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.lookup_in(
            key, (SD.exists("geo"), SD.exists("qzzxy"),)))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        assert result.exists(1) is False
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](1)

    def test_lookup_in_simple_long_path(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        # add longer path to doc
        value["long_path"] = {"a": {"b": {"c": "yo!"}}}
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.lookup_in(
            key, (SD.get("long_path.a.b.c"),)))
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value["long_path"]["a"]["b"]["c"]
        # reset to norm
        wait_for_deferred(cb.remove(key))

    def test_lookup_in_multiple_specs(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("Mock will not return expiry in the xaddrs.")

        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.lookup_in(key, (SD.get(
            "$document.exptime", xattr=True), SD.exists("geo"), SD.get("geo"), SD.get("geo.alt"),)))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 0
        assert result.exists(1) is True
        assert result.content_as[dict](2) == value["geo"]
        assert result.content_as[int](3) == value["geo"]["alt"]

    def test_count(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["count"] = [1, 2, 3, 4, 5]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.lookup_in(key, (SD.count("count"),)))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 5

    def test_mutate_in_simple(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)

        result = wait_for_deferred(cb.mutate_in(key,
                                                (SD.upsert("city", "New City"),
                                                 SD.replace("faa", "CTY")),
                                                MutateInOptions(expiry=timedelta(seconds=1000))))

        value["city"] = "New City"
        value["faa"] = "CTY"

        def cas_matches(cb, new_cas):
            r = wait_for_deferred(cb.get(key))
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        cb_env.try_n_times(10, 3, cas_matches, cb,
                           result.cas, is_deferred=False)

        result = wait_for_deferred(cb.get(key))
        assert value == result.content_as[dict]

    def test_mutate_in_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("Mock will not return expiry in the xaddrs.")

        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)

        result = wait_for_deferred(cb.mutate_in(key,
                                                (SD.upsert("city", "New City"),
                                                 SD.replace("faa", "CTY")),
                                                MutateInOptions(expiry=timedelta(seconds=1000))))

        def cas_matches(cb, new_cas):
            r = wait_for_deferred(cb.get(key))
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        cb_env.try_n_times(10, 3, cas_matches, cb,
                           result.cas, is_deferred=False)

        result = wait_for_deferred(cb.get(key, GetOptions(with_expiry=True)))
        expires_in = (result.expiry_time - datetime.now()).total_seconds()
        assert expires_in > 0 and expires_in < 1021

        # reset to norm
        wait_for_deferred(cb.remove(key))

    # # # TODO: preserve expiry

    def test_array_append(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["array"] = [1, 2, 3, 4]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.array_append("array", 5),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert val["array"][4] == 5

    def test_array_prepend(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["array"] = [1, 2, 3, 4]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.array_prepend("array", 0),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert val["array"][0] == 0

    def test_array_insert(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["array"] = [1, 2, 4, 5]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.array_insert("array.[2]", 3),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert val["array"][2] == 3

    def test_array_add_unique(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["array"] = [0, 1, 2, 3]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.array_addunique("array", 4),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        assert len(val["array"]) == 5
        assert 4 in val["array"]

    def test_array_as_document(self, cb_env):
        cb = cb_env.collection
        key = "simple-key"
        wait_for_deferred(cb.upsert(key, []))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(key, (SD.array_append(
            "", 2), SD.array_prepend("", 0), SD.array_insert("[1]", 1),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[list]
        assert isinstance(val, list)
        assert len(val) == 3
        assert val[0] == 0
        assert val[1] == 1
        assert val[2] == 2

        # clean-up
        wait_for_deferred(cb.remove(key))

    def test_array_append_multi_insert(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["array"] = [1, 2, 3, 4, 5, 6, 7]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.array_append("array", 8, 9, 10),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        app_res = val["array"][7:]
        assert len(app_res) == 3
        assert app_res == [8, 9, 10]

    def test_array_prepend_multi_insert(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["array"] = [4, 5, 6, 7, 8, 9, 10]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.array_prepend("array", 1, 2, 3),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        pre_res = val["array"][:3]
        assert len(pre_res) == 3
        assert pre_res == [1, 2, 3]

    def test_array_insert_multi_insert(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["array"] = [1, 2, 3, 4, 8, 9, 10]
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.array_insert("array.[4]", 5, 6, 7),)))
        assert isinstance(result, MutateInResult)
        result = cb_env.try_n_times(10, 3, cb.get, key)
        val = result.content_as[dict]
        assert isinstance(val["array"], list)
        ins_res = val["array"][4:7]
        assert len(ins_res) == 3
        assert ins_res == [5, 6, 7]

    def test_array_add_unique_fail(self, cb_env):
        cb = cb_env.collection
        key = "simple-key"
        value = {
            "a": "aaa",
            "b": [0, 1, 2, 3],
            "c": [1.25, 1.5, {"nested": ["str", "array"]}],
        }
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)

        with pytest.raises(PathExistsException):
            wait_for_deferred(cb.mutate_in(key, (SD.array_addunique("b", 3),)))

        with pytest.raises(InvalidValueException):
            wait_for_deferred(cb.mutate_in(key, (SD.array_addunique("b", [4, 5, 6]),)))

        with pytest.raises(InvalidValueException):
            wait_for_deferred(cb.mutate_in(key, (SD.array_addunique("b", {"c": "d"}),)))

        with pytest.raises(PathMismatchException):
            wait_for_deferred(cb.mutate_in(key, (SD.array_addunique("c", 2),)))

    def test_increment(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["count"] = 100
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.increment("count", 50),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["count"] == 150

    def test_decrement(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        value["count"] = 100
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.decrement("count", 50),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["count"] == 50

    def test_insert_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.insert("new.path", "parents created", create_parents=True),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["new"]["path"] == "parents created"

    def test_upsert_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.upsert("new.path", "parents created", create_parents=True),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["new"]["path"] == "parents created"

    def test_array_append_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(key, (
            SD.array_append("new.array", "Hello,", create_parents=True),
            SD.array_append("new.array", "World!"),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["new"]["array"] == [
            "Hello,", "World!"]

    def test_array_prepend_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(key, (
            SD.array_prepend("new.array", "World!", create_parents=True),
            SD.array_prepend("new.array", "Hello,"),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["new"]["array"] == [
            "Hello,", "World!"]

    def test_array_add_unique_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(key, (
            SD.array_addunique("new.set", "new", create_parents=True),
            SD.array_addunique("new.set", "unique"),
            SD.array_addunique("new.set", "set"))))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        new_set = result.content_as[dict]["new"]["set"]
        assert isinstance(new_set, list)
        assert "new" in new_set
        assert "unique" in new_set
        assert "set" in new_set

    def test_increment_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.increment("new.counter", 100, create_parents=True),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["new"]["counter"] == 100

    def test_decrement_create_parents(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 3, cb.get, key)
        result = wait_for_deferred(cb.mutate_in(
            key, (SD.decrement("new.counter", 100, create_parents=True),)))
        assert isinstance(result, MutateInResult)
        result = wait_for_deferred(cb.get(key))
        assert result.content_as[dict]["new"]["counter"] == -100
