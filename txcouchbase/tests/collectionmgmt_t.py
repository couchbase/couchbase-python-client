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

from datetime import timedelta
from uuid import uuid4

import pytest

from couchbase.exceptions import (BucketDoesNotExistException,
                                  CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.buckets import StorageBackend
from couchbase.management.collections import (CollectionSpec,
                                              CreateCollectionSettings,
                                              UpdateCollectionSettings)
from tests.environments.test_environment import EnvironmentFeatures

from ._test_utils import TestEnvironment, run_in_reactor_thread


class CollectionManagementTests:

    TEST_BUCKET = "test-bucket"
    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_buckets=True,
                                                 manage_collections=True)
        # will create a new bucket w/ name test-bucket
        cb_env.try_n_times(3, 5, cb_env.setup_collection_mgmt, self.TEST_BUCKET, is_deferred=False)
        yield cb_env
        if cb_env.is_feature_supported('bucket_mgmt'):
            cb_env.purge_buckets([self.TEST_BUCKET])

    @pytest.fixture(scope="class")
    def check_non_deduped_history_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('non_deduped_history',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope="class")
    def check_update_collection_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('update_collection',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope="class")
    def check_update_collection_max_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('update_collection_max_expiry',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope="class")
    def check_negative_collection_max_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('negative_collection_max_expiry',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture()
    def cleanup_scope(self, cb_env):
        cb_env.try_n_times_till_exception(5, 1,
                                          cb_env.test_bucket_cm.drop_scope,
                                          self.TEST_SCOPE,
                                          expected_exceptions=(ScopeNotFoundException,))
        yield
        cb_env.try_n_times_till_exception(5, 1,
                                          cb_env.test_bucket_cm.drop_scope,
                                          self.TEST_SCOPE,
                                          expected_exceptions=(ScopeNotFoundException,))

    @pytest.fixture()
    def cleanup_collection(self, cb_env):
        cb_env.try_n_times_till_exception(5, 1,
                                          cb_env.test_bucket_cm.drop_collection,
                                          CollectionSpec(self.TEST_COLLECTION),
                                          expected_exceptions=(CollectionNotFoundException,))
        yield
        cb_env.try_n_times_till_exception(5, 1,
                                          cb_env.test_bucket_cm.drop_collection,
                                          CollectionSpec(self.TEST_COLLECTION),
                                          expected_exceptions=(CollectionNotFoundException,))

    # temporary until we consoldate w/ new test env setup
    def _get_collection(self, cm, scope_name, coll_name):
        scopes = run_in_reactor_thread(cm.get_all_scopes)
        scope = next((s for s in scopes if s.name == scope_name), None)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    @pytest.mark.usefixtures("cleanup_scope")
    def test_create_scope(self, cb_env):
        run_in_reactor_thread(cb_env.test_bucket_cm.create_scope, self.TEST_SCOPE)
        assert cb_env.get_scope(self.TEST_SCOPE) is not None

    @pytest.mark.usefixtures("cleanup_scope")
    def test_create_scope_already_exists(self, cb_env):
        run_in_reactor_thread(cb_env.test_bucket_cm.create_scope, self.TEST_SCOPE)
        assert cb_env.get_scope(self.TEST_SCOPE) is not None
        with pytest.raises(ScopeAlreadyExistsException):
            run_in_reactor_thread(cb_env.test_bucket_cm.create_scope, self.TEST_SCOPE)

    def test_get_all_scopes(self, cb_env):
        scope_names = [str(uuid4())[:8] for _ in range(4)]
        for name in scope_names:
            run_in_reactor_thread(cb_env.test_bucket_cm.create_scope, name)
            for _ in range(2):
                run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, name, str(uuid4())[:8])
        scopes = run_in_reactor_thread(cb_env.test_bucket_cm.get_all_scopes)

        assert (sum(s.name[0] != '_' for s in scopes) == len(scope_names))
        # should have a _default scope
        assert any(map(lambda s: s.name == '_default', scopes))
        for scope_name in scope_names:
            assert any(map(lambda s: s.name == scope_name, scopes))
        for s in scopes:
            for c in s.collections:
                assert isinstance(c, CollectionSpec)
                assert isinstance(c.name, str)
                assert isinstance(c.scope_name, str)
                assert isinstance(c.max_expiry, timedelta)
                assert c.history is None or isinstance(c.history, bool)

    def test_drop_scope(self, cb_env):
        run_in_reactor_thread(cb_env.test_bucket_cm.create_scope, self.TEST_SCOPE)
        assert cb_env.get_scope(self.TEST_SCOPE) is not None
        run_in_reactor_thread(cb_env.test_bucket_cm.drop_scope, self.TEST_SCOPE)
        with pytest.raises(ScopeNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_scope, self.TEST_SCOPE)

    def test_drop_scope_not_found(self, cb_env):
        with pytest.raises(ScopeNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_scope, "some-random-scope")

    @pytest.mark.usefixtures("cleanup_collection")
    def test_create_collection(self, cb_env):
        # create a collection under default_ scope
        collection_name = self.TEST_COLLECTION
        scope_name = '_default'
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        assert cb_env.get_collection(scope_name, collection_name) is not None

    @pytest.mark.usefixtures("cleanup_scope")
    def test_create_scope_and_collection(self, cb_env):
        run_in_reactor_thread(cb_env.test_bucket_cm.create_scope, self.TEST_SCOPE)
        assert cb_env.get_scope(self.TEST_SCOPE) is not None
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, self.TEST_SCOPE, self.TEST_COLLECTION)
        assert cb_env.get_collection(self.TEST_SCOPE, self.TEST_COLLECTION) is not None

    @pytest.mark.usefixtures("cleanup_collection")
    def test_create_collection_max_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection TTL.")

        scope_name = '_default'
        collection_name = self.TEST_COLLECTION
        settings = CreateCollectionSettings(max_expiry=timedelta(seconds=2))

        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name, settings)
        assert cb_env.get_collection(scope_name, collection_name) is not None
        # pop a doc in with no ttl, verify it goes away...
        coll = cb_env.test_bucket.collection(collection_name)
        key = "test-coll-key0"
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        cb_env.try_n_times(10, 1, coll.upsert, key, {"some": "thing"})
        cb_env.try_n_times(10, 1, coll.get, key)
        cb_env.try_n_times_till_exception(
            4, 1, coll.get, key, expected_exceptions=(
                DocumentNotFoundException,))

    @pytest.mark.usefixtures("cleanup_collection")
    def test_create_collection_max_expiry_default(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection expiry.")

        collection_name = self.TEST_COLLECTION
        scope_name = '_default'

        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        # TODO: consistency
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=0)

    @pytest.mark.usefixtures("cleanup_collection")
    def test_create_collection_max_expiry_invalid(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection expiry.")
        collection_name = self.TEST_COLLECTION
        scope_name = '_default'
        settings = CreateCollectionSettings(max_expiry=timedelta(seconds=-20))

        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name, settings)

    @pytest.mark.usefixtures('check_update_collection_max_expiry_supported')
    @pytest.mark.usefixtures("cleanup_collection")
    def test_create_collection_max_expiry_no_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection expiry.")

        collection_name = self.TEST_COLLECTION
        scope_name = '_default'
        settings = CreateCollectionSettings(max_expiry=timedelta(seconds=-1))
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name, settings)
        # TODO: consistency
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=-1)

    def test_create_collection_bad_scope(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION, "im-a-fake-scope")
        with pytest.raises(ScopeNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)

    @pytest.mark.usefixtures("cleanup_collection")
    def test_create_collection_already_exists(self, cb_env):
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        # verify the collection exists w/in other-bucket
        assert cb_env.get_collection(scope_name, collection_name) is not None
        # now, it will fail if we try to create it again...
        with pytest.raises(CollectionAlreadyExistsException):
            run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)

    @pytest.mark.usefixtures("cleanup_collection")
    def test_collection_goes_in_correct_bucket(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)
        # make sure it actually is in the other-bucket
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # also be sure this isn't in the default bucket
        assert cb_env.get_collection(collection.scope_name,
                                     collection.name,
                                     bucket_name=cb_env.bucket.name) is None

    @pytest.mark.usefixtures("cleanup_collection")
    def test_deprecated_create_collection(self, cb_env):
        # create a collection under default_ scope
        collection = CollectionSpec(self.TEST_COLLECTION)
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None

    @pytest.mark.usefixtures("cleanup_scope")
    def test_deprecated_create_scope_and_collection(self, cb_env):
        run_in_reactor_thread(cb_env.test_bucket_cm.create_scope, self.TEST_SCOPE)
        assert cb_env.get_scope(self.TEST_SCOPE) is not None
        collection = CollectionSpec(self.TEST_COLLECTION, self.TEST_SCOPE)
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None

    @pytest.mark.usefixtures("cleanup_collection")
    def test_deprecated_create_collection_max_ttl(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection TTL.")

        collection = CollectionSpec(
            self.TEST_COLLECTION,
            max_ttl=timedelta(
                seconds=2))

        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # pop a doc in with no ttl, verify it goes away...
        coll = cb_env.test_bucket.collection(collection.name)
        key = "test-coll-key0"
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        cb_env.try_n_times(10, 1, coll.upsert, key, {"some": "thing"})
        cb_env.try_n_times(10, 1, coll.get, key)
        cb_env.try_n_times_till_exception(
            4, 1, coll.get, key, expected_exceptions=(
                DocumentNotFoundException,))

    @pytest.mark.usefixtures("cleanup_collection")
    def test_deprecated_create_collection_already_exists(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)
        # verify the collection exists w/in other-bucket
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # now, it will fail if we try to create it again...
        with pytest.raises(CollectionAlreadyExistsException):
            run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)

    @pytest.mark.usefixtures("cleanup_collection")
    def test_deprecated_drop_collection(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, collection)
        # verify the collection exists w/in other-bucket
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # attempt to drop it again will raise CollectionNotFoundException
        run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, collection)
        with pytest.raises(CollectionNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, collection)

    def test_deprecated_drop_collection_not_found(self, cb_env):
        collection = CollectionSpec("fake-collection")
        with pytest.raises(CollectionNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, collection)

    def test_deprecated_drop_collection_scope_not_found(self, cb_env):
        collection = CollectionSpec("fake-collection", "fake-scope")
        with pytest.raises(ScopeNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, collection)

    @pytest.mark.usefixtures("cleanup_collection")
    def test_drop_collection(self, cb_env):
        collection_name = self.TEST_COLLECTION
        scope_name = '_default'
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        # verify the collection exists w/in other-bucket
        assert cb_env.get_collection(scope_name, collection_name) is not None
        # attempt to drop it again will raise CollectionNotFoundException
        run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, scope_name, collection_name)
        with pytest.raises(CollectionNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, scope_name, collection_name)

    def test_drop_collection_not_found(self, cb_env):
        collection_name = 'fake-collection'
        scope_name = '_default'
        with pytest.raises(CollectionNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, scope_name, collection_name)

    def test_drop_collection_scope_not_found(self, cb_env):
        collection_name = 'fake-collection'
        scope_name = 'fake-scope'
        with pytest.raises(ScopeNotFoundException):
            run_in_reactor_thread(cb_env.test_bucket_cm.drop_collection, scope_name, collection_name)

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    def test_create_collection_history_retention(self, cb_env):
        bucket_name = 'test-magma-bucket'
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        cb_env.create_bucket(bucket_name, storage_backend=StorageBackend.MAGMA)
        bucket = cb_env.cluster.bucket(bucket_name)
        cb_env.try_n_times(10, 1, bucket.on_connect)
        cm = bucket.collections()

        run_in_reactor_thread(cm.create_collection,
                              scope_name,
                              collection_name,
                              CreateCollectionSettings(history=True))
        collection_spec = None
        retry = 0
        while retry < 5 and collection_spec is None:
            collection_spec = self._get_collection(cm, scope_name, collection_name)
            cb_env.sleep(1)
            retry += 1
        assert collection_spec is not None
        assert collection_spec.history

        cb_env.try_n_times_till_exception(10,
                                          3,
                                          cb_env.bm.drop_bucket,
                                          bucket_name,
                                          expected_exceptions=(BucketDoesNotExistException,))

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    def test_create_collection_history_retention_unsupported(self, cb_env):
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        # Couchstore does not support history retention
        with pytest.raises(FeatureUnavailableException):
            run_in_reactor_thread(cb_env.test_bucket_cm.create_collection,
                                  scope_name,
                                  collection_name,
                                  CreateCollectionSettings(history=True))

        with pytest.raises(FeatureUnavailableException):
            run_in_reactor_thread(cb_env.test_bucket_cm.create_collection,
                                  scope_name,
                                  collection_name,
                                  CreateCollectionSettings(history=False))

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.usefixtures("cleanup_collection")
    def test_update_collection_history_retention(self, cb_env):
        bucket_name = 'test-magma-bucket'
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        cb_env.create_bucket(bucket_name, storage_backend=StorageBackend.MAGMA)
        bucket = cb_env.cluster.bucket(bucket_name)
        cb_env.try_n_times(10, 1, bucket.on_connect)
        cm = bucket.collections()

        run_in_reactor_thread(cm.create_collection, scope_name, collection_name,
                              CreateCollectionSettings(history=False))
        collection_spec = None
        retry = 0
        while retry < 5 and collection_spec is None:
            collection_spec = self._get_collection(cm, scope_name, collection_name)
            cb_env.sleep(1)
            retry += 1
        assert collection_spec is not None
        assert not collection_spec.history

        run_in_reactor_thread(cm.update_collection, scope_name, collection_name, UpdateCollectionSettings(history=True))
        collection_spec = None
        retry = 0
        while retry < 5 and collection_spec is None:
            collection_spec = self._get_collection(cm, scope_name, collection_name)
            cb_env.sleep(1)
            retry += 1
        assert collection_spec is not None
        assert collection_spec.history

        cb_env.try_n_times_till_exception(10,
                                          3,
                                          cb_env.bm.drop_bucket,
                                          bucket_name,
                                          expected_exceptions=(BucketDoesNotExistException,))

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_update_collection_supported')
    def test_update_collection_history_retention_unsupported(self, cb_env):
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        collection_spec = cb_env.get_collection(scope_name, collection_name)
        assert collection_spec is not None
        assert collection_spec.history is False

        # Couchstore does not support history retention
        with pytest.raises(FeatureUnavailableException):
            run_in_reactor_thread(cb_env.test_bucket_cm.update_collection,
                                  scope_name,
                                  collection_name,
                                  UpdateCollectionSettings(history=True))

        # Collection history retention setting remains unchanged
        collection_spec = cb_env.get_collection(scope_name, collection_name)
        assert collection_spec is not None
        assert collection_spec.history is False

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.usefixtures('check_update_collection_max_expiry_supported')
    def test_update_collection_max_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection expiry.")

        collection_name = self.TEST_COLLECTION
        scope_name = '_default'
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(0)

        settings = UpdateCollectionSettings(max_expiry=timedelta(seconds=2))
        run_in_reactor_thread(cb_env.test_bucket_cm.update_collection, scope_name, collection_name, settings)

        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=2)

        # pop a doc in with no ttl, verify it goes away...
        coll = cb_env.test_bucket.collection(collection_name)
        key = 'test-coll-key0'
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        cb_env.try_n_times(10, 1, coll.upsert, key, {'some': 'thing'})
        cb_env.try_n_times(10, 1, coll.get, key)
        cb_env.try_n_times_till_exception(4,
                                          1,
                                          coll.get,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.usefixtures('check_negative_collection_max_expiry_supported')
    def test_update_collection_max_expiry_bucket_default(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection expiry.")

        collection_name = self.TEST_COLLECTION
        scope_name = '_default'
        settings = CreateCollectionSettings(max_expiry=timedelta(seconds=5))

        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name, settings)
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=5)

        settings = UpdateCollectionSettings(max_expiry=timedelta(seconds=0))
        run_in_reactor_thread(cb_env.test_bucket_cm.update_collection, scope_name, collection_name, settings)
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=0)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.usefixtures('check_update_collection_max_expiry_supported')
    @pytest.mark.usefixtures('check_negative_collection_max_expiry_supported')
    def test_update_collection_max_expiry_invalid(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection expiry.")
        collection_name = self.TEST_COLLECTION
        scope_name = '_default'

        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=0)

        settings = UpdateCollectionSettings(max_expiry=timedelta(seconds=-20))
        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(cb_env.test_bucket_cm.update_collection, scope_name, collection_name, settings)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.usefixtures('check_update_collection_max_expiry_supported')
    @pytest.mark.usefixtures('check_negative_collection_max_expiry_supported')
    def test_update_collection_max_expiry_no_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection expiry.")

        collection_name = self.TEST_COLLECTION
        scope_name = '_default'
        run_in_reactor_thread(cb_env.test_bucket_cm.create_collection, scope_name, collection_name)
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=0)

        settings = UpdateCollectionSettings(max_expiry=timedelta(seconds=-1))
        run_in_reactor_thread(cb_env.test_bucket_cm.update_collection, scope_name, collection_name, settings)
        coll_spec = cb_env.get_collection(scope_name, collection_name)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=-1)
