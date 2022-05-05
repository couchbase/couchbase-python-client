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

import time
from datetime import datetime, timedelta

import pytest

import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException,
                                  PathNotFoundException)
from couchbase.options import (ClusterOptions,
                               GetOptions,
                               InsertOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetResult,
                              MutationResult)
from txcouchbase.cluster import Cluster

from ._test_utils import (TestEnvironment,
                          run_in_reactor_thread,
                          wait_for_deferred)

# from .conftest import sleep_in_reactor_thread


class CollectionTests:
    NO_KEY = "not-a-key"

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        opts = ClusterOptions(PasswordAuthenticator(
            couchbase_config.admin_username, couchbase_config.admin_password))
        c = Cluster(
            conn_string, opts)
        b = c.bucket(f"{couchbase_config.bucket_name}")
        # wait_for_deferred(b.on_connect())
        run_in_reactor_thread(b.on_connect)
        coll = b.default_collection()
        cb_env = TestEnvironment(c, b, coll, couchbase_config)
        # wait_for_deferred(cb_env.load_data())
        cb_env.load_data()
        return cb_env
        # cb_env.purge_data()
        # wait_for_deferred(cb_env.purge_data())
        # wait_for_deferred(c.close())
        # wait_for_deferred1(c.close)
        # time.sleep(.5)

    def test_exists(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.exists, key)
        assert isinstance(result, ExistsResult)
        assert result.exists is True

    def test_does_not_exists(self, cb_env):
        cb = cb_env.collection
        result = run_in_reactor_thread(cb.exists, self.NO_KEY)
        assert isinstance(result, ExistsResult)
        assert result.exists is False

    def test_get(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.get, key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_options(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.get, key, GetOptions(
            timeout=timedelta(seconds=2), with_expiry=False))
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_fails(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, self.NO_KEY)

    def test_get_with_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("Mock will not return expiry in the xaddrs.")

        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        run_in_reactor_thread(
            cb.upsert, key, value, UpsertOptions(expiry=timedelta(seconds=1000)))

        expiry_path = "$document.exptime"
        res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry = res.content_as[int](0)
        assert expiry is not None
        assert expiry > 0
        expires_in = (datetime.fromtimestamp(expiry) - datetime.now()).total_seconds()
        # when running local, this can be be up to 1050, so just make sure > 0
        assert expires_in > 0

    def test_expiry_really_expires(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        result = run_in_reactor_thread(
            cb.upsert, key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        assert result.cas != 0

        # cb_env.sleep(3.0)
        # run_in_reactor_thread(cb_env.deferred_sleep(3.0))
        # sleep_in_reactor_thread(3.0)
        time.sleep(3.0)
        # with pytest.raises(DocumentNotFoundException):
        run_in_reactor_thread(cb.get, key)

    def test_project(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = run_in_reactor_thread(
            cb.upsert, key, value, UpsertOptions(expiry=timedelta(seconds=2)))

        def cas_matches(cb, new_cas):
            r = run_in_reactor_thread(cb.get, key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        cb_env.try_n_times(
            10, 3, cas_matches, cb, result.cas, is_deferred=False)
        result = run_in_reactor_thread(cb.get, key, GetOptions(project=["faa"]))
        assert {"faa": "ORD"} == result.content_as[dict]
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None

    def test_project_bad_path(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        with pytest.raises(PathNotFoundException):
            wait_for_deferred(cb.get(key, GetOptions(project=["some", "qzx"])))

    def test_project_project_not_list(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        # TODO:  better exception
        with pytest.raises(Exception, match=r"Unable to perform kv operation\."):
            wait_for_deferred(cb.get(key, GetOptions(project="thiswontwork")))

    def test_project_too_many_projections(self, cb_env):
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        project = []
        for _ in range(17):
            project.append("something")

        with pytest.raises(InvalidArgumentException):
            wait_for_deferred(cb.get(key, GetOptions(project=project)))

    def test_upsert(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = wait_for_deferred(
            cb.upsert(key, value, UpsertOptions(timeout=timedelta(seconds=3))))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        result = wait_for_deferred(cb.insert(key, value, InsertOptions(timeout=timedelta(seconds=3))))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert_document_exists(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        with pytest.raises(DocumentExistsException):
            wait_for_deferred(cb.insert(key, value))

    def test_replace(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.replace(key, value, ReplaceOptions(timeout=timedelta(seconds=3))))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_replace_with_cas(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        _, value1 = cb_env.get_new_key_value()
        result = run_in_reactor_thread(cb.get, key)
        old_cas = result.cas
        result = run_in_reactor_thread(cb.replace, key, value1, ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        with pytest.raises(CasMismatchException):
            run_in_reactor_thread(cb.replace, key, value1, ReplaceOptions(cas=old_cas))

        # reset back to normal
        # wait_for_deferred1(cb.replace, key, value)

    def test_replace_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            wait_for_deferred(cb.replace(self.NO_KEY, {"some": "content"}))

    def test_remove(self, cb_env):

        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = run_in_reactor_thread(cb.remove, key)
        assert isinstance(result, MutationResult)

        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(cb.get, key)

        # reset back to normal
        run_in_reactor_thread(cb.upsert, key, value)

    def test_remove_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            wait_for_deferred(cb.remove(self.NO_KEY))

    def test_touch(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 1, cb.get, key)
        result = wait_for_deferred(cb.touch(key, timedelta(seconds=2)))
        assert isinstance(result, MutationResult)
        cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            wait_for_deferred(cb.get(key))

    def test_touch_no_expire(self, cb_env):
        # TODO: handle MOCK
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 1, cb.get, key)
        wait_for_deferred(cb.touch(key, timedelta(seconds=15)))
        g_result = wait_for_deferred(cb.get(key, GetOptions(with_expiry=True)))
        assert g_result.expiry_time is not None
        wait_for_deferred(cb.touch(key, timedelta(seconds=0)))
        g_result = wait_for_deferred(cb.get(key, GetOptions(with_expiry=True)))
        assert g_result.expiry_time is None

    def test_get_and_touch(self, cb_env):
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 1, cb.get, key)
        result = wait_for_deferred(cb.get_and_touch(key, timedelta(seconds=2)))
        assert isinstance(result, GetResult)
        cb_env.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            wait_for_deferred(cb.get(key))

    def test_get_and_touch_no_expire(self, cb_env):
        # TODO: handle MOCK
        cb = cb_env.collection
        key, value = wait_for_deferred(cb_env.get_new_key_value())
        wait_for_deferred(cb.upsert(key, value))
        cb_env.try_n_times(10, 1, cb.get, key)
        wait_for_deferred(cb.get_and_touch(key, timedelta(seconds=15)))
        g_result = wait_for_deferred(cb.get(key, GetOptions(with_expiry=True)))
        assert g_result.expiry_time is not None
        wait_for_deferred(cb.get_and_touch(key, timedelta(seconds=0)))
        g_result = wait_for_deferred(cb.get(key, GetOptions(with_expiry=True)))
        assert g_result.expiry_time is None

    def test_get_and_lock(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.get_and_lock(key, timedelta(seconds=3)))
        assert isinstance(result, GetResult)
        # TODO: handle retry reasons, looks to be where we can get the locked
        # exception
        with pytest.raises((AmbiguousTimeoutException, DocumentLockedException)):
            wait_for_deferred(cb.upsert(key, value))

        cb_env.try_n_times(10, 1, cb.upsert, key, value)

    def test_get_after_lock(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        orig = wait_for_deferred(cb.get_and_lock(key, timedelta(seconds=5)))
        assert isinstance(orig, GetResult)
        result = wait_for_deferred(cb.get(key))
        assert orig.content_as[dict] == result.content_as[dict]
        assert orig.cas != result.cas

        # reset to normal
        wait_for_deferred(cb.unlock(key, orig.cas))

    def test_get_and_lock_replace_with_cas(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.get_and_lock(key, timedelta(seconds=5)))
        assert isinstance(result, GetResult)
        cas = result.cas
        # TODO: handle retry reasons, looks to be where we can get the locked
        # exception
        with pytest.raises((AmbiguousTimeoutException, DocumentLockedException)):
            wait_for_deferred(cb.upsert(key, value))

        wait_for_deferred(cb.replace(key, value, ReplaceOptions(cas=cas)))
        # reset to normal
        # TODO:  why does this fail?
        # wait_for_deferred(cb.unlock(key, cas))
        # cb_env.try_n_times(10, 1, cb.unlock, key, cas)

    def test_unlock(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = wait_for_deferred(cb.get_and_lock(key, timedelta(seconds=5)))
        assert isinstance(result, GetResult)
        wait_for_deferred(cb.unlock(key, result.cas))
        wait_for_deferred(cb.upsert(key, value))
