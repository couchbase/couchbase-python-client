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

from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from acouchbase.cluster import get_event_loop
from couchbase.exceptions import (BucketDoesNotExistException,
                                  CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.buckets import StorageBackend
from couchbase.management.collections import (CollectionSpec,
                                              CreateCollectionSettings,
                                              UpdateCollectionSettings)

from ._test_utils import TestEnvironment


class CollectionManagementTests:

    TEST_BUCKET = "test-bucket"
    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"

    @pytest_asyncio.fixture(scope="class")
    def event_loop(self):
        loop = get_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(scope="class")
    def check_non_deduped_history_supported(self, cb_env):
        cb_env.check_if_feature_supported('non_deduped_history')

    @pytest.fixture(scope="class")
    def check_update_collection_supported(self, cb_env):
        cb_env.check_if_feature_supported('update_collection')

    @pytest.fixture(scope="class")
    def check_update_collection_max_expiry_supported(self, cb_env):
        cb_env.check_if_feature_supported('update_collection_max_expiry')

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        cb_env = await TestEnvironment.get_environment(__name__,
                                                       couchbase_config,
                                                       manage_buckets=True,
                                                       manage_collections=True)
        # will create a new bucket w/ name test-bucket
        await cb_env.try_n_times(3, 5, cb_env.setup_collection_mgmt, self.TEST_BUCKET)
        yield cb_env
        if cb_env.is_feature_supported('bucket_mgmt'):
            await cb_env.purge_buckets([self.TEST_BUCKET])

    @pytest_asyncio.fixture()
    async def cleanup_scope(self, cb_env):
        await cb_env.try_n_times_till_exception(5, 1,
                                                cb_env.test_bucket_cm.drop_scope,
                                                self.TEST_SCOPE,
                                                expected_exceptions=(ScopeNotFoundException,))
        yield
        await cb_env.try_n_times_till_exception(5, 1,
                                                cb_env.test_bucket_cm.drop_scope,
                                                self.TEST_SCOPE,
                                                expected_exceptions=(ScopeNotFoundException,))

    @pytest_asyncio.fixture()
    async def cleanup_collection(self, cb_env):
        await cb_env.try_n_times_till_exception(5, 1,
                                                cb_env.test_bucket_cm.drop_collection,
                                                CollectionSpec(self.TEST_COLLECTION),
                                                expected_exceptions=(CollectionNotFoundException,))
        yield
        await cb_env.try_n_times_till_exception(5, 1,
                                                cb_env.test_bucket_cm.drop_collection,
                                                CollectionSpec(self.TEST_COLLECTION),
                                                expected_exceptions=(CollectionNotFoundException,))

    @pytest.mark.usefixtures("cleanup_scope")
    @pytest.mark.asyncio
    async def test_create_scope(self, cb_env):
        await cb_env.test_bucket_cm.create_scope(self.TEST_SCOPE)
        assert await cb_env.get_scope(self.TEST_SCOPE) is not None

    @pytest.mark.usefixtures("cleanup_scope")
    @pytest.mark.asyncio
    async def test_create_scope_already_exists(self, cb_env):
        await cb_env.test_bucket_cm.create_scope(self.TEST_SCOPE)
        assert await cb_env.get_scope(self.TEST_SCOPE) is not None
        with pytest.raises(ScopeAlreadyExistsException):
            await cb_env.test_bucket_cm.create_scope(self.TEST_SCOPE)

    @pytest.mark.asyncio
    async def test_get_all_scopes(self, cb_env):
        scope_names = [str(uuid4())[:8] for _ in range(4)]
        for name in scope_names:
            await cb_env.test_bucket_cm.create_scope(name)
            for _ in range(2):
                await cb_env.test_bucket_cm.create_collection(name, str(uuid4())[:8])

        scopes = await cb_env.test_bucket_cm.get_all_scopes()
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

        for name in scope_names:
            await cb_env.test_bucket_cm.drop_scope(name)

    # deprecated
    # @pytest.mark.asyncio
    # async def test_get_scope(self, cb_env):
    #     await cb_env.test_bucket_cm.get_scope('_default')

    @pytest.mark.asyncio
    async def test_drop_scope(self, cb_env):
        await cb_env.test_bucket_cm.create_scope(self.TEST_SCOPE)
        assert await cb_env.get_scope(self.TEST_SCOPE) is not None
        await cb_env.test_bucket_cm.drop_scope(self.TEST_SCOPE)
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.drop_scope(self.TEST_SCOPE)

    @pytest.mark.asyncio
    async def test_drop_scope_not_found(self, cb_env):
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.drop_scope("some-random-scope")

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_create_collection(self, cb_env):
        # create a collection under default_ scope
        await cb_env.test_bucket_cm.create_collection("_default", self.TEST_COLLECTION)
        assert await cb_env.get_collection("_default", self.TEST_COLLECTION) is not None

    @pytest.mark.usefixtures("cleanup_scope")
    @pytest.mark.asyncio
    async def test_create_scope_and_collection(self, cb_env):
        await cb_env.test_bucket_cm.create_scope(self.TEST_SCOPE)
        assert await cb_env.get_scope(self.TEST_SCOPE) is not None
        await cb_env.test_bucket_cm.create_collection(self.TEST_SCOPE, self.TEST_COLLECTION)
        assert await cb_env.get_collection(self.TEST_SCOPE, self.TEST_COLLECTION) is not None

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_create_collection_max_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection TTL.")

        settings = CreateCollectionSettings(max_expiry=timedelta(seconds=2))

        await cb_env.test_bucket_cm.create_collection("_default", self.TEST_COLLECTION, settings)
        coll_spec = await cb_env.get_collection("_default", self.TEST_COLLECTION)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=2)

        # pop a doc in with no ttl, verify it goes away...
        coll = cb_env.test_bucket.collection(self.TEST_COLLECTION)
        key = "test-coll-key0"
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        await cb_env.try_n_times(10, 1, coll.upsert, key, {"some": "thing"})
        await cb_env.try_n_times(10, 1, coll.get, key)
        await cb_env.try_n_times_till_exception(4, 1, coll.get, key, expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.usefixtures('check_update_collection_max_expiry_supported')
    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_update_collection_max_expiry(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection TTL.")

        await cb_env.test_bucket_cm.create_collection("_default", self.TEST_COLLECTION)

        coll_spec = await cb_env.get_collection("_default", self.TEST_COLLECTION)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(0)

        settings = UpdateCollectionSettings(max_expiry=timedelta(seconds=2))
        await cb_env.test_bucket_cm.update_collection("_default", self.TEST_COLLECTION, settings)

        coll_spec = await cb_env.get_collection("_default", self.TEST_COLLECTION)
        assert coll_spec is not None
        assert coll_spec.max_expiry == timedelta(seconds=2)

        # pop a doc in with no ttl, verify it goes away...
        coll = cb_env.test_bucket.collection(self.TEST_COLLECTION)
        key = "test-coll-key0"
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        await cb_env.try_n_times(10, 1, coll.upsert, key, {"some": "thing"})
        await cb_env.try_n_times(10, 1, coll.get, key)
        await cb_env.try_n_times_till_exception(4, 1, coll.get, key, expected_exceptions=(DocumentNotFoundException,))

    @pytest.mark.asyncio
    async def test_create_collection_bad_scope(self, cb_env):
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.create_collection("im-a-fake-scope", self.TEST_COLLECTION)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_create_collection_already_exists(self, cb_env):
        await cb_env.test_bucket_cm.create_collection("_default", self.TEST_COLLECTION)
        # verify the collection exists w/in other-bucket
        assert await cb_env.get_collection("_default", self.TEST_COLLECTION) is not None
        # now, it will fail if we try to create it again...
        with pytest.raises(CollectionAlreadyExistsException):
            await cb_env.test_bucket_cm.create_collection("_default", self.TEST_COLLECTION)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_collection_goes_in_correct_bucket(self, cb_env):
        await cb_env.test_bucket_cm.create_collection("_default", self.TEST_COLLECTION)
        # make sure it actually is in the other-bucket
        assert await cb_env.get_collection("_default", self.TEST_COLLECTION) is not None
        # also be sure this isn't in the default bucket
        assert await cb_env.get_collection("_default",
                                           self.TEST_COLLECTION,
                                           bucket_name=cb_env.bucket.name) is None

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_drop_collection(self, cb_env):
        await cb_env.test_bucket_cm.create_collection("_default", self.TEST_COLLECTION)
        # verify the collection exists w/in other-bucket
        assert await cb_env.get_collection("_default", self.TEST_COLLECTION) is not None
        # attempt to drop it again will raise CollectionNotFoundException
        await cb_env.test_bucket_cm.drop_collection("_default", self.TEST_COLLECTION)
        with pytest.raises(CollectionNotFoundException):
            await cb_env.test_bucket_cm.drop_collection("_default", self.TEST_COLLECTION)

    @pytest.mark.asyncio
    async def test_drop_collection_not_found(self, cb_env):
        with pytest.raises(CollectionNotFoundException):
            await cb_env.test_bucket_cm.drop_collection("_default", "fake-collection")

    @pytest.mark.asyncio
    async def test_drop_collection_scope_not_found(self, cb_env):
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.drop_collection("fake-scope", "fake-collection")

    @pytest.mark.asyncio
    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    async def test_create_collection_history_retention(self, cb_env):
        bucket_name = 'test-magma-bucket'
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        await cb_env.create_bucket(bucket_name, storage_backend=StorageBackend.MAGMA)
        bucket = cb_env.cluster.bucket(bucket_name)
        await cb_env.try_n_times(10, 1, bucket.on_connect)
        cm = bucket.collections()

        await cm.create_collection(scope_name, collection_name, CreateCollectionSettings(history=True))
        collection_spec = await cb_env.get_collection(scope_name, collection_name, bucket_name=bucket_name)
        assert collection_spec is not None
        assert collection_spec.history

        await cb_env.try_n_times_till_exception(10,
                                                3,
                                                cb_env.bm.drop_bucket,
                                                bucket_name,
                                                expected_exceptions=(BucketDoesNotExistException,))

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.asyncio
    async def test_create_collection_history_retention_unsupported(self, cb_env):
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        # Couchstore does not support history retention
        with pytest.raises(FeatureUnavailableException):
            await cb_env.test_bucket_cm.create_collection(
                scope_name, collection_name, CreateCollectionSettings(history=True))

        with pytest.raises(FeatureUnavailableException):
            await cb_env.test_bucket_cm.create_collection(
                scope_name, collection_name, CreateCollectionSettings(history=False))

    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.asyncio
    async def test_update_collection_history_retention(self, cb_env):
        bucket_name = 'test-magma-bucket'
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        await cb_env.create_bucket(bucket_name, storage_backend=StorageBackend.MAGMA)
        bucket = cb_env.cluster.bucket(bucket_name)
        await cb_env.try_n_times(10, 1, bucket.on_connect)
        cm = bucket.collections()

        await cm.create_collection(scope_name, collection_name, CreateCollectionSettings(history=False))
        collection_spec = await cb_env.get_collection(scope_name, collection_name, bucket_name=bucket_name)
        assert collection_spec is not None
        assert not collection_spec.history

        await cm.update_collection(scope_name, collection_name, UpdateCollectionSettings(history=True))
        collection_spec = await cb_env.get_collection(scope_name, collection_name, bucket_name=bucket_name)
        assert collection_spec is not None
        assert collection_spec.history

        await cb_env.try_n_times_till_exception(10,
                                                3,
                                                cb_env.bm.drop_bucket,
                                                bucket_name,
                                                expected_exceptions=(BucketDoesNotExistException,))

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.usefixtures('check_non_deduped_history_supported')
    @pytest.mark.usefixtures('check_update_collection_supported')
    @pytest.mark.asyncio
    async def test_update_collection_history_retention_unsupported(self, cb_env):
        scope_name = '_default'
        collection_name = self.TEST_COLLECTION

        await cb_env.test_bucket_cm.create_collection(scope_name, collection_name)
        collection_spec = await cb_env.get_collection(scope_name, collection_name)
        assert collection_spec is not None
        assert collection_spec.history is False

        # Couchstore does not support history retention
        with pytest.raises(FeatureUnavailableException):
            await cb_env.test_bucket_cm.update_collection(
                scope_name, collection_name, UpdateCollectionSettings(history=True))

        # Collection history retention setting remains unchanged
        collection_spec = await cb_env.get_collection(scope_name, collection_name)
        assert collection_spec is not None
        assert collection_spec.history is False

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_deprecated_create_collection(self, cb_env):
        # create a collection under default_ scope
        collection = CollectionSpec(self.TEST_COLLECTION)
        await cb_env.test_bucket_cm.create_collection(collection)
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None

    @pytest.mark.usefixtures("cleanup_scope")
    @pytest.mark.asyncio
    async def test_deprecated_create_scope_and_collection(self, cb_env):
        await cb_env.test_bucket_cm.create_scope(self.TEST_SCOPE)
        assert await cb_env.get_scope(self.TEST_SCOPE) is not None
        collection = CollectionSpec(self.TEST_COLLECTION, self.TEST_SCOPE)
        await cb_env.test_bucket_cm.create_collection(collection)
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_deprecated_create_collection_max_ttl(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection TTL.")
        collection = CollectionSpec(
            self.TEST_COLLECTION,
            max_ttl=timedelta(
                seconds=2))

        await cb_env.test_bucket_cm.create_collection(collection)
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None
        # pop a doc in with no ttl, verify it goes away...
        coll = cb_env.test_bucket.collection(collection.name)
        key = "test-coll-key0"
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        await cb_env.try_n_times(10, 1, coll.upsert, key, {"some": "thing"})
        await cb_env.try_n_times(10, 1, coll.get, key)
        await cb_env.try_n_times_till_exception(
            4, 1, coll.get, key, expected_exceptions=(
                DocumentNotFoundException,))

    @pytest.mark.asyncio
    async def test_deprecated_create_collection_bad_scope(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION, "im-a-fake-scope")
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.create_collection(collection)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_deprecated_create_collection_already_exists(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        await cb_env.test_bucket_cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None
        # now, it will fail if we try to create it again...
        with pytest.raises(CollectionAlreadyExistsException):
            await cb_env.test_bucket_cm.create_collection(collection)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_deprecated_collection_goes_in_correct_bucket(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        await cb_env.test_bucket_cm.create_collection(collection)
        # make sure it actually is in the other-bucket
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None
        # also be sure this isn't in the default bucket
        assert await cb_env.get_collection(collection.scope_name,
                                           collection.name,
                                           bucket_name=cb_env.bucket.name) is None

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_deprecated_drop_collection(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        await cb_env.test_bucket_cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None
        # attempt to drop it again will raise CollectionNotFoundException
        await cb_env.test_bucket_cm.drop_collection(collection)
        with pytest.raises(CollectionNotFoundException):
            await cb_env.test_bucket_cm.drop_collection(collection)

    @pytest.mark.asyncio
    async def test_deprecated_drop_collection_not_found(self, cb_env):
        collection = CollectionSpec("fake-collection")
        with pytest.raises(CollectionNotFoundException):
            await cb_env.test_bucket_cm.drop_collection(collection)

    @pytest.mark.asyncio
    async def test_deprecated_drop_collection_scope_not_found(self, cb_env):
        collection = CollectionSpec("fake-collection", "fake-scope")
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.drop_collection(collection)
