#  Copyright 2016-2023. Couchbase, Inc.
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
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException,
                                  TemporaryFailException)
from couchbase.options import (GetOptions,
                               InsertOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              MutationResult)
from tests.environments import CollectionType
from tests.environments.test_environment import TestEnvironment
from tests.mock_server import MockServerType
from tests.test_features import EnvironmentFeatures


class CollectionTestSuite:
    FIFTY_YEARS = 50 * 365 * 24 * 60 * 60
    THIRTY_DAYS = 30 * 24 * 60 * 60

    TEST_MANIFEST = [
        'test_document_expiry_values',
        'test_does_not_exists',
        'test_exists',
        'test_expiry_really_expires',
        'test_get',
        'test_get_after_lock',
        'test_get_all_replicas',
        'test_get_all_replicas_fail',
        'test_get_all_replicas_results',
        'test_get_and_lock',
        'test_get_and_lock_replace_with_cas',
        'test_get_and_touch',
        'test_get_and_touch_no_expire',
        'test_get_any_replica',
        'test_get_any_replica_fail',
        'test_get_fails',
        'test_get_options',
        'test_get_with_expiry',
        'test_insert',
        'test_insert_document_exists',
        'test_project',
        'test_project_bad_path',
        'test_project_project_not_list',
        'test_project_too_many_projections',
        'test_remove',
        'test_remove_fail',
        'test_replace',
        'test_replace_fail',
        'test_replace_preserve_expiry',
        'test_replace_preserve_expiry_fail',
        'test_replace_preserve_expiry_not_used',
        'test_replace_with_cas',
        'test_touch',
        'test_touch_no_expire',
        'test_unlock',
        'test_unlock_wrong_cas',
        'test_upsert',
        'test_upsert_preserve_expiry',
        'test_upsert_preserve_expiry_not_used',
    ]

    @pytest.fixture(scope='class')
    def check_preserve_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('preserve_expiry',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def check_xattr_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('xattr',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def num_replicas(self, cb_env):
        bucket_settings = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get("num_replicas")
        return num_replicas

    @pytest.fixture(scope='class')
    def check_replicas(self, cb_env, num_replicas):
        if cb_env.is_mock_server and cb_env.mock_server_type == MockServerType.GoCAVES:
            pytest.skip('GoCaves inconstent w/ replicas')
        ping_res = cb_env.bucket.ping()
        kv_endpoints = ping_res.endpoints.get(ServiceType.KeyValue, None)
        if kv_endpoints is None or len(kv_endpoints) < (num_replicas + 1):
            pytest.skip("Not all replicas are online")

    @pytest.fixture(scope='class')
    def num_nodes(self, cb_env):
        return len(cb_env.cluster._cluster_info.nodes)

    @pytest.fixture(scope='class')
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip("Test only for clusters with more than a single node.")

    @pytest.mark.usefixtures('check_xattr_supported')
    @pytest.mark.parametrize("expiry", [FIFTY_YEARS + 1,
                                        FIFTY_YEARS,
                                        THIRTY_DAYS - 1,
                                        THIRTY_DAYS,
                                        int(time() - 1.0),
                                        60,
                                        -1])
    def test_document_expiry_values(self, cb_env, expiry):
        key, value = cb_env.get_new_doc()
        # expiry should be >= 0 (0 being don't expire)
        if expiry == -1:
            with pytest.raises(InvalidArgumentException):
                cb_env.collection.upsert(key, value, expiry=timedelta(seconds=expiry))
        else:
            before = int(time() - 1.0)
            result = cb_env.collection.upsert(key, value, expiry=timedelta(seconds=expiry))
            assert result.cas is not None

            expiry_path = '$document.exptime'
            res = TestEnvironment.try_n_times(10,
                                              3,
                                              cb_env.collection.lookup_in,
                                              key,
                                              (SD.get(expiry_path, xattr=True),))
            res_expiry = res.content_as[int](0)

            after = int(time() + 1.0)
            before + expiry <= res_expiry <= after + expiry

    def test_does_not_exists(self, cb_env):
        result = cb_env.collection.exists(TestEnvironment.NOT_A_KEY)
        assert isinstance(result, ExistsResult)
        assert result.exists is False

    def test_exists(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.exists(key)
        assert isinstance(result, ExistsResult)
        assert result.exists is True

    def test_expiry_really_expires(self, cb_env):
        key, value = cb_env.get_new_doc()
        result = cb_env.collection.upsert(key, value, UpsertOptions(
            expiry=timedelta(seconds=2)))
        assert result.cas != 0

        TestEnvironment.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    def test_get(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get(key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_after_lock(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        orig = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(orig, GetResult)
        result = cb_env.collection.get(key)
        assert orig.content_as[dict] == result.content_as[dict]
        assert orig.cas != result.cas

        # @TODO(jc):  cxx client raises ambiguous timeout w/ retry reason: kv_temporary_failure
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.unlock,
                                                   key,
                                                   orig.cas,
                                                   expected_exceptions=(TemporaryFailException,))

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    def test_get_all_replicas(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get_all_replicas, key)
        # make sure we can iterate over results
        while True:
            try:
                res = next(result)
                assert isinstance(res, GetReplicaResult)
                assert isinstance(res.is_replica, bool)
                assert value == res.content_as[dict]
            except StopIteration:
                break

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    def test_get_all_replicas_fail(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_all_replicas('not-a-key')

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas")
    def test_get_all_replicas_results(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get_all_replicas, key)
        active_cnt = 0
        replica_cnt = 0
        for res in result:
            assert isinstance(res, GetReplicaResult)
            assert isinstance(res.is_replica, bool)
            assert value == res.content_as[dict]
            if res.is_replica:
                replica_cnt += 1
            else:
                active_cnt += 1

        assert active_cnt == 1
        if num_replicas > 0:
            assert replica_cnt >= active_cnt

    def test_get_and_lock(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=3))
        assert isinstance(result, GetResult)
        with pytest.raises(DocumentLockedException):
            cb_env.collection.upsert(key, value)

        TestEnvironment.try_n_times(10, 1, cb_env.collection.upsert, key, value)

    def test_get_and_lock_replace_with_cas(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        cas = result.cas
        # TODO: handle retry reasons, looks to be where we can get the locked
        # exception
        with pytest.raises((AmbiguousTimeoutException, DocumentLockedException)):
            cb_env.collection.upsert(key, value)

        cb_env.collection.replace(key, value, ReplaceOptions(cas=cas))
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.unlock,
                                                   key,
                                                   cas,
                                                   expected_exceptions=(TemporaryFailException,))

    def test_get_and_touch(self, cb_env):
        key, value = cb_env.get_new_doc()
        cb_env.collection.upsert(key, value)
        TestEnvironment.try_n_times(10, 1, cb_env.collection.get, key)
        result = cb_env.collection.get_and_touch(key, timedelta(seconds=2))
        assert isinstance(result, GetResult)
        TestEnvironment.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    def test_get_and_touch_no_expire(self, cb_env):
        key, value = cb_env.get_existing_doc()
        # @TODO(jc):  GoCAVES does not seem to like nested doc structures in this scenario, fails
        #               on the last get consistently
        if cb_env.is_mock_server and 'manufacturer' in value:
            value.pop('manufacturer')
        cb_env.collection.upsert(key, value)
        TestEnvironment.try_n_times(10, 1, cb_env.collection.get, key)
        cb_env.collection.get_and_touch(key, timedelta(seconds=15))
        g_result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is not None
        cb_env.collection.get_and_touch(key, timedelta(seconds=0))
        # this get seems to be a problem
        g_result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    @pytest.mark.usefixtures("check_replicas")
    def test_get_any_replica(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get_any_replica, key)
        assert isinstance(result, GetReplicaResult)
        assert isinstance(result.is_replica, bool)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures("check_replicas")
    def test_get_any_replica_fail(self, cb_env):
        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.get_any_replica('not-a-key')

    def test_get_options(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get(key, GetOptions(
            timeout=timedelta(seconds=2), with_expiry=False))
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_fails(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(TestEnvironment.NOT_A_KEY)

    @pytest.mark.usefixtures('check_xattr_supported')
    def test_get_with_expiry(self, cb_env):
        key, value = cb_env.get_new_doc()
        cb_env.collection.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=1000)))
        expiry_path = '$document.exptime'
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry = res.content_as[int](0)
        assert expiry is not None
        assert expiry > 0
        expires_in = (datetime.fromtimestamp(expiry) - datetime.now()).total_seconds()
        # when running local, this can be be up to 1050, so just make sure > 0
        assert expires_in > 0

    def test_insert(self, cb_env):
        key, value = cb_env.get_new_doc()
        result = cb_env.collection.insert(key, value, InsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert_document_exists(self, cb_env):
        key, value = cb_env.get_existing_doc()
        with pytest.raises(DocumentExistsException):
            cb_env.collection.insert(key, value)

    def test_project(self, cb_env):
        # @TODO(jc): Why does caves not like the dealership type???
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.upsert(key, value, UpsertOptions(
            expiry=timedelta(seconds=2)))

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        TestEnvironment.try_n_times(10, 3, cas_matches, cb_env.collection, result.cas)
        result = cb_env.collection.get(key, GetOptions(project=['batch']))
        assert 'batch' in result.content_as[dict]
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None

    def test_project_bad_path(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        # CXXCBC-295 - b8bb98c31d377100934dd4b33998f0a118df41e8, bad path no longer raises PathNotFoundException
        result = cb_env.collection.get(key, GetOptions(project=['qzx']))
        assert result.cas is not None
        res_dict = result.content_as[dict]
        assert res_dict == {}
        assert 'qzx' not in res_dict

    def test_project_project_not_list(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get(key, GetOptions(project='thiswontwork'))

    def test_project_too_many_projections(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        project = []
        for _ in range(17):
            project.append('something')

        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get(key, GetOptions(project=project))

    def test_remove(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.remove(key)
        assert isinstance(result, MutationResult)

        with pytest.raises(DocumentNotFoundException):
            TestEnvironment.try_n_times_till_exception(3,
                                                       1,
                                                       cb_env.collection.get,
                                                       key,
                                                       expected_exceptions=(DocumentNotFoundException,),
                                                       raise_exception=True)

    def test_remove_fail(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.remove(TestEnvironment.NOT_A_KEY)

    def test_replace(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.replace(key, value, ReplaceOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_replace_with_cas(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        _, value1 = cb_env.get_new_doc()
        result = cb_env.collection.get(key)
        old_cas = result.cas
        result = cb_env.collection.replace(key, value1, ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        with pytest.raises(CasMismatchException):
            cb_env.collection.replace(key, value1, ReplaceOptions(cas=old_cas))

    def test_replace_fail(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace(TestEnvironment.NOT_A_KEY, {"some": "content"})

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_replace_preserve_expiry(self, cb_env):
        key, value = cb_env.get_existing_doc()
        _, value1 = cb_env.get_new_doc()

        cb_env.collection.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = '$document.exptime'
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb_env.collection.replace(key, value1, ReplaceOptions(preserve_expiry=True))
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        TestEnvironment.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_replace_preserve_expiry_fail(self, cb_env):
        key, value = cb_env.get_existing_doc()

        opts = ReplaceOptions(expiry=timedelta(seconds=5), preserve_expiry=True)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.replace(key, value, opts)

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_replace_preserve_expiry_not_used(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_existing_doc()
        _, value1 = cb_env.get_new_doc()

        cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = '$document.exptime'
        res = TestEnvironment.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb.replace(key, value1)
        res = TestEnvironment.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        TestEnvironment.sleep(3.0)
        result = cb.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    def test_touch(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.touch(key, timedelta(seconds=2))
        assert isinstance(result, MutationResult)
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.get,
                                                   key,
                                                   expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    def test_touch_no_expire(self, cb_env):
        key, value = cb_env.get_existing_doc()
        # @TODO(jc):  GoCAVES does not seem to like nested doc structures in this scenario, fails
        #               on the last get consistently
        if cb_env.is_mock_server and 'manufacturer' in value:
            value.pop('manufacturer')
        cb_env.collection.touch(key, timedelta(seconds=0))
        # this get seems to be a problem
        g_result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    def test_unlock(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        cb_env.collection.unlock(key, result.cas)
        cb_env.collection.upsert(key, value)

    def test_unlock_wrong_cas(self, cb_env):
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas
        # @TODO(jc): MOCK - TemporaryFailException
        with pytest.raises((DocumentLockedException)):
            cb_env.collection.unlock(key, 100)

        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.unlock,
                                                   key,
                                                   cas,
                                                   expected_exceptions=(TemporaryFailException,))

    def test_upsert(self, cb_env):
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.upsert(key, value, UpsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_upsert_preserve_expiry(self, cb_env):
        key, value = cb_env.get_existing_doc()
        _, value1 = cb_env.get_new_doc()

        cb_env.collection.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = '$document.exptime'
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb_env.collection.upsert(key, value1, UpsertOptions(preserve_expiry=True))
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 == expiry2
        # if expiry was set, should be expired by now
        TestEnvironment.sleep(3.0)
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(key)

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_upsert_preserve_expiry_not_used(self, cb_env):
        key, value = cb_env.get_existing_doc()
        _, value1 = cb_env.get_new_doc()
        cb_env.collection.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=2)))
        expiry_path = '$document.exptime'
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry1 = res.content_as[int](0)

        cb_env.collection.upsert(key, value1)
        res = TestEnvironment.try_n_times(10,
                                          3,
                                          cb_env.collection.lookup_in,
                                          key,
                                          (SD.get(expiry_path, xattr=True),))
        expiry2 = res.content_as[int](0)

        assert expiry1 is not None
        assert expiry2 is not None
        assert expiry1 != expiry2
        # if expiry was set, should be expired by now
        TestEnvironment.sleep(3.0)
        result = cb_env.collection.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1


class ClassicCollectionTests(CollectionTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicCollectionTests) if valid_test_method(meth)]
        compare = set(CollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_base_env.enable_bucket_mgmt()
        cb_base_env.setup(request.param, num_docs=100)
        yield cb_base_env
        cb_base_env.teardown(request.param)
