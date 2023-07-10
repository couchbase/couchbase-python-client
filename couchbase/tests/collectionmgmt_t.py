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

import pytest

from couchbase.exceptions import (CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  DocumentNotFoundException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.collections import CollectionSpec
from tests.environments import CollectionType
from tests.environments.collection_mgmt_environment import CollectionManagementTestEnvironment
from tests.environments.test_environment import TestEnvironment


class CollectionManagementTestSuite:
    TEST_MANIFEST = [
        'test_collection_goes_in_correct_bucket',
        'test_create_collection',
        'test_create_collection_already_exists',
        'test_create_collection_bad_scope',
        'test_create_collection_max_ttl',
        'test_create_scope',
        'test_create_scope_already_exists',
        'test_create_scope_and_collection',
        'test_drop_collection',
        'test_drop_collection_not_found',
        'test_drop_collection_scope_not_found',
        'test_drop_scope',
        'test_drop_scope_not_found',
        'test_get_all_scopes',
    ]

    def test_collection_goes_in_correct_bucket(self, cb_env):
        collection_name = cb_env.get_collection_name()
        collection = CollectionSpec(collection_name)
        cb_env.test_bucket_cm.create_collection(collection)
        # make sure it actually is in the other-bucket
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # also be sure this isn't in the default bucket
        assert cb_env.get_collection(collection.scope_name,
                                     collection.name,
                                     bucket_name=cb_env.bucket.name) is None

    def test_create_collection(self, cb_env):
        # create a collection under default_ scope
        collection_name = cb_env.get_collection_name()
        collection = CollectionSpec(collection_name)
        cb_env.cm.create_collection(collection)
        coll = cb_env.get_collection(collection.scope_name, collection.name, bucket_name=cb_env.bucket.name)
        assert collection.scope_name == '_default'
        assert coll is not None

    def test_create_collection_already_exists(self, cb_env):
        collection_name = cb_env.get_collection_name()
        collection = CollectionSpec(collection_name)
        cb_env.test_bucket_cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # now, it will fail if we try to create it again...
        with pytest.raises(CollectionAlreadyExistsException):
            cb_env.test_bucket_cm.create_collection(collection)

    def test_create_collection_bad_scope(self, cb_env):
        collection_name = cb_env.get_collection_name()
        collection = CollectionSpec(collection_name, 'im-a-fake-scope')
        with pytest.raises(ScopeNotFoundException):
            cb_env.test_bucket_cm.create_collection(collection)

    def test_create_collection_max_ttl(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("CAVES doesn't support collection TTL.")

        collection_name = cb_env.get_collection_name()
        collection = CollectionSpec(collection_name, max_ttl=timedelta(seconds=2))

        cb_env.test_bucket_cm.create_collection(collection)
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # pop a doc in with no ttl, verify it goes away...
        coll = cb_env.test_bucket.collection(collection.name)
        key = 'test-coll-key0'
        # we _can_ get a temp fail here, as we just created the collection.  So we
        # retry the upsert.
        TestEnvironment.try_n_times(10, 1, coll.upsert, key, {'some': 'thing'})
        TestEnvironment.try_n_times(10, 1, coll.get, key)
        TestEnvironment.try_n_times_till_exception(4,
                                                   1,
                                                   coll.get,
                                                   key,
                                                   expected_exceptions=(DocumentNotFoundException,))

    def test_create_scope(self, cb_env):
        scope_name = cb_env.get_scope_name()
        cb_env.test_bucket_cm.create_scope(scope_name)
        assert cb_env.get_scope(scope_name) is not None

    def test_create_scope_already_exists(self, cb_env):
        scope_name = cb_env.get_scope_name()
        cb_env.test_bucket_cm.create_scope(scope_name)
        assert cb_env.get_scope(scope_name) is not None
        with pytest.raises(ScopeAlreadyExistsException):
            cb_env.test_bucket_cm.create_scope(scope_name)

    def test_create_scope_and_collection(self, cb_env):
        scope_name = cb_env.get_scope_name()
        collection_name = cb_env.get_collection_name()
        cb_env.test_bucket_cm.create_scope(scope_name)
        assert cb_env.get_scope(scope_name) is not None
        collection = CollectionSpec(collection_name, scope_name)
        cb_env.test_bucket_cm.create_collection(collection)
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None

    def test_drop_collection(self, cb_env):
        collection_name = cb_env.get_collection_name()
        collection = CollectionSpec(collection_name)
        cb_env.test_bucket_cm.create_collection(collection)
        # verify the collection exists w/in other-bucket
        assert cb_env.get_collection(collection.scope_name, collection.name) is not None
        # attempt to drop it again will raise CollectionNotFoundException
        cb_env.test_bucket_cm.drop_collection(collection)
        with pytest.raises(CollectionNotFoundException):
            cb_env.test_bucket_cm.drop_collection(collection)

    def test_drop_collection_not_found(self, cb_env):
        collection = CollectionSpec('fake-collection')
        with pytest.raises(CollectionNotFoundException):
            cb_env.test_bucket_cm.drop_collection(collection)

    def test_drop_collection_scope_not_found(self, cb_env):
        collection = CollectionSpec('fake-collection', 'fake-scope')
        with pytest.raises(ScopeNotFoundException):
            cb_env.test_bucket_cm.drop_collection(collection)

    def test_drop_scope(self, cb_env):
        scope_name = cb_env.get_scope_name()
        cb_env.test_bucket_cm.create_scope(scope_name)
        assert cb_env.get_scope(scope_name) is not None
        cb_env.test_bucket_cm.drop_scope(scope_name)
        cb_env.add_dropped_scope(scope_name)
        with pytest.raises(ScopeNotFoundException):
            cb_env.test_bucket_cm.drop_scope(scope_name)

    def test_drop_scope_not_found(self, cb_env):
        with pytest.raises(ScopeNotFoundException):
            cb_env.test_bucket_cm.drop_scope('some-random-scope')

    def test_get_all_scopes(self, cb_env):
        scope_names = cb_env.get_scope_names()
        scopes = cb_env.test_bucket_cm.get_all_scopes()
        assert len(scopes) == len(scope_names) + 1
        # should have a _default scope
        assert any(map(lambda s: s.name == '_default', scopes))
        for scope_name in scope_names:
            assert any(map(lambda s: s.name == scope_name, scopes))
        for s in scopes:
            for c in s.collections:
                assert isinstance(c, CollectionSpec)
                assert isinstance(c.name, str)
                assert isinstance(c.scope_name, str)
                assert c.max_expiry is not None


class ClassicCollectionManagementTests(CollectionManagementTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicCollectionManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicCollectionManagementTests) if valid_test_method(meth)]
        compare = set(CollectionManagementTestSuite.TEST_MANIFEST).difference(method_list)
        return compare

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, test_manifest_validated, request):
        if test_manifest_validated:
            pytest.fail(f'Test manifest not validated.  Missing tests: {test_manifest_validated}.')

        cb_env = CollectionManagementTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt().enable_collection_mgmt()
        cb_env.setup()
        yield cb_env
        cb_env.teardown()
