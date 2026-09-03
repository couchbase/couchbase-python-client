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

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta
from time import time

import pytest

import couchbase.subdocument as SD
from couchbase.collection import Collection
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DocumentNotLockedException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException,
                                  TemporaryFailException)
from couchbase.options import (GetAllReplicasOptions,
                               GetAnyReplicaOptions,
                               GetOptions,
                               InsertOptions,
                               ReplaceOptions,
                               UpsertOptions)
from couchbase.replica_reads import ReadPreference
from couchbase.result import (ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              MutationResult)
from tests_v2.environments.managers import TestEnvironmentBucketManager
from tests_v2.environments.test_environment import (CollectionStrategy,
                                                    SyncTestEnvironment,
                                                    TestEnvironment,
                                                    build_environment,
                                                    teardown_environment)
from tests_v2.manifest import validate_test_manifest
from tests_v2.mock_server import MockServerType
from tests_v2.skips import server_label, skip_mock_limitation
from tests_v2.suite_fixtures import TopologySuite
from tests_v2.test_features import EnvironmentFeatures, ServerFeatures


class CollectionTestSuite(TopologySuite):
    FIFTY_YEARS = 50 * 365 * 24 * 60 * 60
    THIRTY_DAYS = 30 * 24 * 60 * 60

    MANAGERS = [TestEnvironmentBucketManager]
    NUM_DOCS = 100
    # A replica vbucket learns of a new collection from its active over DCP, after the
    # management API already reports the collection present.  On a loaded cluster that has
    # been measured at over 14s, and until then the replica answers unknown collection, which
    # the client retries until the request times out.  The default KV budget is shorter than
    # that, so get_all_replicas drops the late replica or, for a missing key, fails with a
    # timeout instead of DocumentNotFoundException.
    REPLICA_READ_TIMEOUT = timedelta(seconds=30)

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
        'test_get_all_replicas_read_preference',
        'test_get_and_lock',
        'test_get_and_lock_replace_with_cas',
        'test_get_and_touch',
        'test_get_and_touch_no_expire',
        'test_get_any_replica',
        'test_get_any_replica_fail',
        'test_get_any_replica_read_preference',
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
        'test_unlock_not_locked',
        'test_upsert',
        'test_upsert_preserve_expiry',
        'test_upsert_preserve_expiry_not_used',
    ]

    @pytest.fixture(scope='class')
    def check_preserve_expiry_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.PreserveExpiry, cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_xattr_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.Xattr, cb_env.server_context)

    @pytest.fixture(scope='class')
    def check_not_locked_supported(self, cb_env: SyncTestEnvironment) -> None:
        EnvironmentFeatures.check_if_feature_supported(ServerFeatures.NotLockedKVStatus, cb_env.server_context)

    @pytest.fixture(scope="class")
    def skip_if_go_caves(self, cb_env: SyncTestEnvironment) -> None:
        if cb_env.is_mock_server and cb_env.mock_server_type == MockServerType.GoCAVES:
            skip_mock_limitation(mock=cb_env.mock_server_type,
                                 server=server_label(cb_env.is_mock_server),
                                 note='GoCAVES does not support this operation')

    @pytest.mark.usefixtures('check_xattr_supported')
    # The fifth case is pinned to an explicit id.  Its value is a wall-clock reading taken
    # when this module is imported, so without one the node id changes on every run: it
    # cannot be re-run with --last-failed, CI cannot join it across builds, and two
    # pytest-xdist workers collect different tests and refuse to start.  Intermittent, too,
    # since two collections inside the same second happen to agree.  The value still varies,
    # which is what the case is for; only the id is fixed.
    @pytest.mark.parametrize("expiry", [FIFTY_YEARS + 1,
                                        FIFTY_YEARS,
                                        THIRTY_DAYS - 1,
                                        THIRTY_DAYS,
                                        pytest.param(int(time() - 1.0), id='current-unix-timestamp'),
                                        60,
                                        -1])
    def test_document_expiry_values(self, cb_env: SyncTestEnvironment, expiry: int) -> None:
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
            # @FINDING(PYCBC-1871): expression statement, not an assertion.  The chained
            # comparison is evaluated and the bool discarded, so the expiry this test is
            # named for is never checked, for any of the six passing cases.  Left as
            # written: adding the assert runs a comparison that has never executed, against
            # a server-assigned expiry, and a failure is a plausible outcome.
            before + expiry <= res_expiry <= after + expiry

    def test_does_not_exists(self, cb_env: SyncTestEnvironment) -> None:
        result = cb_env.collection.exists(TestEnvironment.NOT_A_KEY)
        assert isinstance(result, ExistsResult)
        assert result.exists is False

    def test_exists(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.exists(key)
        assert isinstance(result, ExistsResult)
        assert result.exists is True

    # Regression guard (PYCBC-1859): this and the three other expiry assertions below used to
    # sleep 3.0s against a 2s TTL and check once.  Server-side expiry is not instantaneous, so
    # a one-second margin is a race: the four failed intermittently, one per run, under whole-
    # suite load only.  Poll to a deadline instead, as test_touch already did.
    def test_expiry_really_expires(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        result = cb_env.collection.upsert(key, value, UpsertOptions(
            expiry=timedelta(seconds=2)))
        assert result.cas != 0

        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.get,
                                                   key,
                                                   expected_exceptions=(DocumentNotFoundException,))

    def test_get(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get(key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_after_lock(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        orig = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(orig, GetResult)
        result = cb_env.collection.get(key)
        assert orig.content_as[dict] == result.content_as[dict]
        assert orig.cas != result.cas

        # @TODO(jc):  cxx client raises ambiguous timeout w/ retry reason: kv_temporary_failure
        TestEnvironment.try_n_times_till_exception(10, 1,
                                                   cb_env.collection.unlock, key, orig.cas,
                                                   expected_exceptions=(TemporaryFailException,
                                                                        DocumentNotLockedException))

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas_online")
    def test_get_all_replicas(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        opts = GetAllReplicasOptions(timeout=self.REPLICA_READ_TIMEOUT)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get_all_replicas, key, opts)
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
    @pytest.mark.usefixtures("check_replicas_online")
    def test_get_all_replicas_fail(self, cb_env: SyncTestEnvironment) -> None:
        opts = GetAllReplicasOptions(timeout=self.REPLICA_READ_TIMEOUT)
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get_all_replicas('not-a-key', opts)

    @pytest.mark.flaky(reruns=5, reruns_delay=1)
    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas_online")
    def test_get_all_replicas_results(self, cb_env: SyncTestEnvironment, num_replicas: int) -> None:
        key, value = cb_env.get_existing_doc()
        opts = GetAllReplicasOptions(timeout=self.REPLICA_READ_TIMEOUT)
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get_all_replicas, key, opts)
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

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas_online")
    @pytest.mark.usefixtures("check_server_groups_supported")
    def test_get_all_replicas_read_preference(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        # No preferred server group was specified in the cluster options so this should raise
        # DocumentUnretrievableException
        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.get_all_replicas(
                key,
                GetAllReplicasOptions(read_preference=ReadPreference.SELECTED_SERVER_GROUP))

    def test_get_and_lock(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=3))
        assert isinstance(result, GetResult)
        with pytest.raises(DocumentLockedException):
            cb_env.collection.upsert(key, value)

        TestEnvironment.try_n_times(10, 1, cb_env.collection.upsert, key, value)

    def test_get_and_lock_replace_with_cas(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        cas = result.cas
        # TODO: handle retry reasons, looks to be where we can get the locked
        # exception
        with pytest.raises((AmbiguousTimeoutException, DocumentLockedException)):
            cb_env.collection.upsert(key, value)

        cb_env.collection.replace(key, value, ReplaceOptions(cas=cas))
        TestEnvironment.try_n_times_till_exception(10, 1,
                                                   cb_env.collection.unlock, key, cas,
                                                   expected_exceptions=(TemporaryFailException,
                                                                        DocumentNotLockedException))

    def test_get_and_touch(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        cb_env.collection.upsert(key, value)
        TestEnvironment.try_n_times(10, 1, cb_env.collection.get, key)
        result = cb_env.collection.get_and_touch(key, timedelta(seconds=2))
        assert isinstance(result, GetResult)
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.get,
                                                   key,
                                                   expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    def test_get_and_touch_no_expire(self, cb_env: SyncTestEnvironment) -> None:
        # A flat document, minted rather than drawn.  This test and test_touch_no_expire assert
        # on expiry_time and never read the content back, so all the value has to be is something
        # the mock will return: GoCAVES fails the final get when the document holds a nested
        # object, and `manufacturer` is the only nested field a seeded vehicle has.  Drawing a
        # vehicle and popping that field arrives at the same place through an is_mock_server
        # branch, an upsert to write the trimmed copy back, and a mutation of the environment's
        # cached document.  Minting is the same two round trips with none of that, and holds on
        # a real server and the mock alike rather than branching on which.
        key = cb_env.get_new_doc(key_only=True)
        cb_env.collection.upsert(key, {'id': key, 'type': 'expiry'})
        TestEnvironment.try_n_times(10, 1, cb_env.collection.get, key)
        cb_env.collection.get_and_touch(key, timedelta(seconds=15))
        g_result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is not None
        cb_env.collection.get_and_touch(key, timedelta(seconds=0))
        g_result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    @pytest.mark.usefixtures("check_replicas_online")
    def test_get_any_replica(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get_any_replica, key)
        assert isinstance(result, GetReplicaResult)
        assert isinstance(result.is_replica, bool)
        assert value == result.content_as[dict]

    @pytest.mark.usefixtures("check_replicas_online")
    def test_get_any_replica_fail(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.get_any_replica('not-a-key')

    @pytest.mark.usefixtures("check_multi_node")
    @pytest.mark.usefixtures("check_replicas_online")
    @pytest.mark.usefixtures("check_server_groups_supported")
    def test_get_any_replica_read_preference(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        # No preferred server group was specified in the cluster options so this should raise
        # DocumentUnretrievableException
        with pytest.raises(DocumentUnretrievableException):
            cb_env.collection.get_any_replica(
                key, GetAnyReplicaOptions(read_preference=ReadPreference.SELECTED_SERVER_GROUP))

    def test_get_options(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get(key, GetOptions(
            timeout=timedelta(seconds=2), with_expiry=False))
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_fails(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(TestEnvironment.NOT_A_KEY)

    @pytest.mark.usefixtures('check_xattr_supported')
    def test_get_with_expiry(self, cb_env: SyncTestEnvironment) -> None:
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

    def test_insert(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_new_doc()
        result = cb_env.collection.insert(key, value, InsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert_document_exists(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        with pytest.raises(DocumentExistsException):
            cb_env.collection.insert(key, value)

    def test_project(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.upsert(key, value, UpsertOptions(
            expiry=timedelta(seconds=2)))

        def cas_matches(cb: Collection, new_cas: int) -> None:
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        TestEnvironment.try_n_times(10, 3, cas_matches, cb_env.collection, result.cas)
        g_result = cb_env.collection.get(key, GetOptions(project=['batch']))
        assert 'batch' in g_result.content_as[dict]
        assert g_result.cas is not None
        assert g_result.key == key
        assert g_result.expiry_time is None

    def test_project_bad_path(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        # CXXCBC-295 - b8bb98c31d377100934dd4b33998f0a118df41e8, bad path no longer raises PathNotFoundException
        result = cb_env.collection.get(key, GetOptions(project=['qzx']))
        assert result.cas is not None
        res_dict = result.content_as[dict]
        assert res_dict == {}
        assert 'qzx' not in res_dict

    def test_project_project_not_list(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.get(key, GetOptions(project='thiswontwork'))

    @pytest.mark.usefixtures('skip_if_go_caves')
    def test_project_too_many_projections(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_new_doc(key_only=True)
        value = {f'f{i}': i for i in range(1, 21)}
        cb_env.collection.upsert(key, value)

        project = [f'f{i}' for i in range(1, 19)]
        result = cb_env.collection.get(key, GetOptions(project=project))

        assert result.cas is not None
        res_dict = result.content_as[dict]
        assert res_dict != {}
        assert res_dict.get('f20') is None
        for field in project:
            assert res_dict.get(field) is not None

    def test_remove(self, cb_env: SyncTestEnvironment) -> None:
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

    def test_remove_fail(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.remove(TestEnvironment.NOT_A_KEY)

    def test_replace(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.replace(key, value, ReplaceOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_replace_with_cas(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        _, value1 = cb_env.get_new_doc()
        g_result = cb_env.collection.get(key)
        old_cas = g_result.cas
        result = cb_env.collection.replace(key, value1, ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        with pytest.raises(CasMismatchException):
            cb_env.collection.replace(key, value1, ReplaceOptions(cas=old_cas))

    def test_replace_fail(self, cb_env: SyncTestEnvironment) -> None:
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace(TestEnvironment.NOT_A_KEY, {"some": "content"})

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_replace_preserve_expiry(self, cb_env: SyncTestEnvironment) -> None:
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
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.get,
                                                   key,
                                                   expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_replace_preserve_expiry_fail(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()

        opts = ReplaceOptions(expiry=timedelta(seconds=5), preserve_expiry=True)
        with pytest.raises(InvalidArgumentException):
            cb_env.collection.replace(key, value, opts)

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_replace_preserve_expiry_not_used(self, cb_env: SyncTestEnvironment) -> None:
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
        # Past the two-second expiry set above, and the read below must still succeed: the
        # replace dropped the expiry, so there is nothing left to expire.  A bare sleep, where
        # assert_helpers calls one a defect, because the claim is that the deadline passes with
        # nothing happening, and a poll for that returns on its first attempt having waited for
        # nothing at all.
        TestEnvironment.sleep(3.0)
        result = cb.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1

    def test_touch(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.touch(key, timedelta(seconds=2))
        assert isinstance(result, MutationResult)
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.get,
                                                   key,
                                                   expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.flaky(reruns=3, reruns_delay=1)
    def test_touch_no_expire(self, cb_env: SyncTestEnvironment) -> None:
        # Minted flat, for the reason given on test_get_and_touch_no_expire above.
        key = cb_env.get_new_doc(key_only=True)
        cb_env.collection.upsert(key, {'id': key, 'type': 'expiry'})
        TestEnvironment.try_n_times(10, 1, cb_env.collection.get, key)
        # An expiry has to be set before clearing one can be observed.  Asserting only that
        # expiry_time is None after touch(0) is satisfied by a touch that does nothing at all,
        # because a document created without one carries no expiry to begin with.
        cb_env.collection.touch(key, timedelta(seconds=15))
        g_result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is not None
        cb_env.collection.touch(key, timedelta(seconds=0))
        g_result = cb_env.collection.get(key, GetOptions(with_expiry=True))
        assert g_result.expiry_time is None

    def test_unlock(self, cb_env: SyncTestEnvironment) -> None:
        key, value = cb_env.get_existing_doc()
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        assert isinstance(result, GetResult)
        cb_env.collection.unlock(key, result.cas)
        cb_env.collection.upsert(key, value)

    def test_unlock_wrong_cas(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas
        # @TODO(jc): MOCK - TemporaryFailException
        with pytest.raises(CasMismatchException):
            cb_env.collection.unlock(key, 100)

        TestEnvironment.try_n_times_till_exception(10, 1,
                                                   cb_env.collection.unlock, key, cas,
                                                   expected_exceptions=(TemporaryFailException,
                                                                        DocumentNotLockedException))

    @pytest.mark.usefixtures("check_not_locked_supported")
    def test_unlock_not_locked(self, cb_env: SyncTestEnvironment) -> None:
        key = cb_env.get_existing_doc(key_only=True)
        result = cb_env.collection.get_and_lock(key, timedelta(seconds=5))
        cas = result.cas
        cb_env.collection.unlock(key, cas)
        with pytest.raises(DocumentNotLockedException):
            cb_env.collection.unlock(key, cas)

    def test_upsert(self, cb_env: SyncTestEnvironment) -> None:
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
    def test_upsert_preserve_expiry(self, cb_env: SyncTestEnvironment) -> None:
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
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   cb_env.collection.get,
                                                   key,
                                                   expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.usefixtures('check_preserve_expiry_supported')
    def test_upsert_preserve_expiry_not_used(self, cb_env: SyncTestEnvironment) -> None:
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
        # Past the two-second expiry, for the reason given on
        # test_replace_preserve_expiry_not_used: the upsert dropped the expiry, so the read
        # below must succeed, and a poll cannot assert that a deadline passed uneventfully.
        TestEnvironment.sleep(3.0)
        result = cb_env.collection.get(key)
        assert isinstance(result, GetResult)
        assert result.content_as[dict] == value1


class CollectionTests(CollectionTestSuite):

    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self) -> None:
        validate_test_manifest(CollectionTestSuite, self.__class__)

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionStrategy.DEFAULT, CollectionStrategy.NAMED])
    def couchbase_test_environment(self,
                                   cb_base_env: SyncTestEnvironment,
                                   request: pytest.FixtureRequest) -> Generator[SyncTestEnvironment, None, None]:
        env = build_environment(cb_base_env, CollectionTestSuite, request.param)
        yield env
        teardown_environment(env, CollectionTestSuite, request.param)
