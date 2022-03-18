from datetime import timedelta

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  DocumentNotFoundException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.collections import CollectionSpec
from couchbase.options import ClusterOptions

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

    @pytest_asyncio.fixture(scope="class", name="cb_env")
    async def couchbase_test_environment(self, couchbase_config):
        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        cluster = Cluster(
            conn_string, opts)
        await cluster.on_connect()
        await cluster.cluster_info()
        bucket = cluster.bucket(f"{couchbase_config.bucket_name}")
        await bucket.on_connect()
        coll = bucket.default_collection()
        cb_env = TestEnvironment(
            cluster, bucket, coll, couchbase_config, manage_buckets=True, manage_collections=True)
        # will create a new bucket w/ name test-bucket
        await cb_env.setup_collection_mgmt(self.TEST_BUCKET)
        yield cb_env
        if cb_env.is_feature_supported('bucket_mgmt'):
            await cb_env.purge_buckets([self.TEST_BUCKET])
        await cluster.close()

    @pytest_asyncio.fixture()
    async def cleanup_scope(self, cb_env):
        yield
        await cb_env.try_n_times_till_exception(
            5, 1, cb_env.test_bucket_cm.drop_scope, self.TEST_SCOPE, expected_exceptions=(
                ScopeNotFoundException,))

    @pytest_asyncio.fixture()
    async def cleanup_collection(self, cb_env):
        yield
        await cb_env.try_n_times_till_exception(
            5, 1, cb_env.test_bucket_cm.drop_collection, CollectionSpec(self.TEST_COLLECTION), expected_exceptions=(
                CollectionNotFoundException,))

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
        scopes = await cb_env.test_bucket_cm.get_all_scopes()
        # this is a brand-new bucket, so it should only have _default scope and
        # a _default collection
        assert len(scopes) == 1
        scope = scopes[0]
        assert scope.name == "_default"
        assert len(scope.collections) == 1
        collection = scope.collections[0]
        assert collection.name == "_default"
        assert collection.scope_name == "_default"

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
        collection = CollectionSpec(self.TEST_COLLECTION)
        await cb_env.test_bucket_cm.create_collection(collection)
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None

    @pytest.mark.usefixtures("cleanup_scope")
    @pytest.mark.asyncio
    async def test_create_scope_and_collection(self, cb_env):
        await cb_env.test_bucket_cm.create_scope(self.TEST_SCOPE)
        assert await cb_env.get_scope(self.TEST_SCOPE) is not None
        collection = CollectionSpec(self.TEST_COLLECTION, self.TEST_SCOPE)
        await cb_env.test_bucket_cm.create_collection(collection)
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_create_collection_max_ttl(self, cb_env):
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
    async def test_create_collection_bad_scope(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION, "im-a-fake-scope")
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.create_collection(collection)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_create_collection_already_exists(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        await cb_env.test_bucket_cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None
        # now, it will fail if we try to create it again...
        with pytest.raises(CollectionAlreadyExistsException):
            await cb_env.test_bucket_cm.create_collection(collection)

    @pytest.mark.usefixtures("cleanup_collection")
    @pytest.mark.asyncio
    async def test_collection_goes_in_correct_bucket(self, cb_env):
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
    async def test_drop_collection(self, cb_env):
        collection = CollectionSpec(self.TEST_COLLECTION)
        await cb_env.test_bucket_cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        assert await cb_env.get_collection(collection.scope_name, collection.name) is not None
        # attempt to drop it again will raise CollectionNotFoundException
        await cb_env.test_bucket_cm.drop_collection(collection)
        with pytest.raises(CollectionNotFoundException):
            await cb_env.test_bucket_cm.drop_collection(collection)

    @pytest.mark.asyncio
    async def test_drop_collection_not_found(self, cb_env):
        collection = CollectionSpec("fake-collection")
        with pytest.raises(CollectionNotFoundException):
            await cb_env.test_bucket_cm.drop_collection(collection)

    @pytest.mark.asyncio
    async def test_drop_collection_scope_not_found(self, cb_env):
        collection = CollectionSpec("fake-collection", "fake-scope")
        with pytest.raises(ScopeNotFoundException):
            await cb_env.test_bucket_cm.drop_collection(collection)
