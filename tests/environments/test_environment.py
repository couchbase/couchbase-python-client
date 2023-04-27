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

import asyncio
import random
import time
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    Optional,
                    Tuple,
                    Type,
                    Union)

import pytest
import pytest_asyncio

from acouchbase.cluster import Cluster as AsyncCluster
from acouchbase.cluster import get_event_loop
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  BucketAlreadyExistsException,
                                  CollectionAlreadyExistsException,
                                  CouchbaseException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException,
                                  UnAmbiguousTimeoutException)
from couchbase.management.buckets import BucketType, CreateBucketSettings
from couchbase.management.collections import CollectionSpec
from couchbase.options import ClusterOptions, TransactionConfig
from tests.data_provider import DataProvider
from tests.environments import CollectionType, CouchbaseTestEnvironmentException
from tests.test_features import EnvironmentFeatures

if TYPE_CHECKING:
    from tests.mock_server import MockServerType


class TestEnvironment:
    NOT_A_KEY = 'not-a-key'
    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        self._bucket = kwargs.pop('bucket', None)
        self._cluster = kwargs.pop('cluster', None)
        self._config = kwargs.pop('couchbase_config', None)
        self._data_provider = kwargs.pop('data_provider', None)
        self._default_collection = kwargs.pop('default_collection', None)
        self._default_scope = self._default_collection._scope if self._default_collection else None
        self._extra_docs = {}
        self._loaded_docs = {}
        self._named_collection = None
        self._named_scope = None
        self._test_bucket = None
        self._test_bucket_cm = None
        self._use_named_collections = False
        self._used_docs = set()
        self._used_extras = set()
        self._doc_types = ['dealership', 'vehicle']

    @property
    def aixm(self) -> Optional[Any]:
        """Returns the default AnalyticsIndexManager"""
        return self._aixm if hasattr(self, '_aixm') else None

    @property
    def bm(self) -> Optional[Any]:
        """Returns the default bucket's BucketManager"""
        return self._bm if hasattr(self, '_bm') else None

    @property
    def bucket(self):
        return self._bucket

    @property
    def config(self):
        return self._config

    @property
    def cluster(self):
        return self._cluster

    @property
    def cm(self) -> Optional[Any]:
        """Returns the default CollectionManager"""
        return self._cm if hasattr(self, '_cm') else None

    @property
    def collection(self):
        if self._use_named_collections:
            return self._named_collection
        return self._default_collection

    @property
    def data_provider(self):
        return self._data_provider

    @property
    def default_collection(self):
        return self._default_collection

    @property
    def default_scope(self):
        return self._default_scope

    @property
    def efm(self) -> Optional[Any]:
        """Returns the default EventingFunctionManager"""
        return self._efm if hasattr(self, '_efm') else None

    @property
    def is_developer_preview(self) -> Optional[bool]:
        return self._cluster.is_developer_preview

    @property
    def is_mock_server(self) -> bool:
        return self._config.is_mock_server

    @property
    def is_real_server(self):
        return not self._config.is_mock_server

    @property
    def mock_server_type(self) -> MockServerType:
        if self.is_mock_server:
            return self._config.mock_server.mock_type
        return None

    @property
    def named_collection(self):
        return self._named_collection

    @property
    def named_scope(self):
        return self._named_scope

    @property
    def qixm(self) -> Optional[Any]:
        """Returns the default QueryIndexManager"""
        return self._qixm if hasattr(self, '_qixm') else None

    @property
    def scope(self):
        if self._use_named_collections:
            return self._named_scope
        return self._default_scope

    @property
    def server_version(self) -> Optional[str]:
        return self._cluster.server_version

    @property
    def server_version_full(self) -> Optional[str]:
        return self._cluster.server_version_full

    @property
    def server_version_short(self) -> Optional[float]:
        return self._cluster.server_version_short

    @property
    def sixm(self) -> Optional[Any]:
        """Returns the default SearchIndexManager"""
        return self._sixm if hasattr(self, '_sixm') else None

    @property
    def test_bucket(self) -> Optional[Any]:
        """Returns the test bucket object"""
        return self._test_bucket if hasattr(self, '_test_bucket') else None

    @property
    def test_bucket_cm(self) -> Optional[Any]:
        """Returns the test bucket's CollectionManager"""
        return self._test_bucket_cm if hasattr(self, '_test_bucket_cm') else None

    @property
    def um(self) -> Optional[Any]:
        """Returns the default UserManager"""
        return self._um if hasattr(self, '_um') else None

    @property
    def vixm(self) -> Optional[Any]:
        """Returns the default ViewIndexManager"""
        return self._vixm if hasattr(self, '_vixm') else None

    def create_bucket(self, bucket_name):
        try:
            self.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    bucket_type=BucketType.COUCHBASE,
                    ram_quota_mb=100))
        except BucketAlreadyExistsException:
            pass
        TestEnvironment.try_n_times(10, 1, self.bm.get_bucket, bucket_name)

    def disable_analytics_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('analytics',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if hasattr(self, '_aixm'):
            del self._aixm

        return self

    def disable_bucket_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('basic_bucket_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self, '_bm'):
            del self._bm

        return self

    def disable_collection_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('collections',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self, '_cm'):
            del self._cm

        return self

    def disable_eventing_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('eventing_function_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if hasattr(self, '_efm'):
            del self._efm

        return self

    def disable_named_collections(self) -> None:
        self._use_named_collections = False

    def disable_query_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('query_index_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self, '_qixm'):
            del self._qixm

        return self

    def disable_search_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('search_index_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self, '_sixm'):
            del self._sixm

        return self

    def disable_views_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('view_index_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self, '_vixm'):
            del self._vixm

        return self

    def disable_user_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('user_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self, '_um'):
            del self._um

        return self

    def enable_analytics_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('analytics',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self.cluster, 'analytics_indexes'):
            pytest.skip('Analytics index management not available on cluster.')

        self._aixm = self.cluster.analytics_indexes()
        return self

    def enable_bucket_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('basic_bucket_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self.cluster, 'buckets'):
            pytest.skip('Bucket management not available on cluster.')

        self._bm = self.cluster.buckets()
        return self

    def enable_named_collections(self) -> None:
        self._use_named_collections = True

    def enable_collection_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('collections',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self.bucket, 'collections'):
            pytest.skip('Collection management not available on bucket.')

        self._cm = self.bucket.collections()
        return self

    def enable_eventing_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('eventing_function_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self.cluster, 'eventing_functions'):
            pytest.skip('Eventing functions management not available on cluster.')

        self._efm = self.cluster.eventing_functions()
        return self

    def enable_query_mgmt(self, from_collection=False) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('query_index_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not from_collection and not hasattr(self.cluster, 'query_indexes'):
            pytest.skip('Query index management not available on cluster.')

        if not from_collection and not hasattr(self.collection, 'query_indexes'):
            pytest.skip('Query index management not available on collection.')

        if from_collection:
            self._qixm = self.collection.query_indexes()
        else:
            self._qixm = self.cluster.query_indexes()
        return self

    def enable_search_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('search_index_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self.cluster, 'search_indexes'):
            pytest.skip('Search index management not available on cluster.')

        self._sixm = self.cluster.search_indexes()
        return self

    def enable_views_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('view_index_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self.bucket, 'view_indexes'):
            pytest.skip('View index not available on bucket.')

        self._vixm = self.bucket.view_indexes()
        return self

    def enable_user_mgmt(self) -> TestEnvironment:
        EnvironmentFeatures.check_if_feature_supported('user_mgmt',
                                                       self.server_version_short,
                                                       self.mock_server_type)
        if not hasattr(self.cluster, 'users'):
            pytest.skip('User management not available on cluster.')

        self._um = self.cluster.users()
        return self

    def get_collection(self, scope_name, coll_name, bucket_name=None):
        scope = self.get_scope(scope_name, bucket_name=bucket_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    def get_existing_doc(self, key_only=False):
        if not self._loaded_docs:
            self.load_data()

        all_keys = set(self._loaded_docs.keys())
        available_keys = all_keys.difference(self._used_docs)
        key = random.choice(list(available_keys))
        self._used_docs.add(key)
        if key_only is True:
            return key
        return key, self._loaded_docs[key]

    def get_new_doc(self, key_only=False):
        doc = self.data_provider.get_new_vehicle()
        self._used_extras.add(doc['id'])
        if key_only is True:
            return doc['id']
        return doc['id'], doc

    def get_scope(self, scope_name, bucket_name=None):
        if bucket_name is None and self._test_bucket is None:
            raise CouchbaseTestEnvironmentException("Must provide a bucket name or have a valid test_bucket available")

        bucket_names = ['default']
        if self._test_bucket is not None:
            bucket_names.append(self._test_bucket.name)

        bucket = bucket_name or self._test_bucket.name
        if bucket not in bucket_names:
            raise CouchbaseTestEnvironmentException(
                f"{bucket} is an invalid bucket name.")

        scopes = []
        if bucket == self.bucket.name:
            scopes = self.cm.get_all_scopes()
        else:
            scopes = self.test_bucket_cm.get_all_scopes()
        return next((s for s in scopes if s.name == scope_name), None)

    def load_data(self, num_docs=50, test_suite=None):
        if test_suite and test_suite == 'datastructures_t':
            for k in self.data_provider.generate_keys(num_docs):
                self._loaded_docs[k] = {}
            return

        # for d in self.data_provider.get_dealerships():
        #     for _ in range(3):
        #         try:
        #             key = f'{d["id"]}'
        #             _ = self.collection.upsert(key, d)
        #             self._loaded_docs[key] = d
        #             break
        #         except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
        #             time.sleep(3)
        #             continue
        #         except Exception as ex:
        #             print(ex)
        #             raise

        for v in self.data_provider.get_vehicles()[:num_docs]:
            for _ in range(3):
                try:
                    key = f'{v["id"]}'
                    _ = self.collection.upsert(key, v)
                    self._loaded_docs[key] = v
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    time.sleep(3)
                    continue
                except Exception as ex:
                    print(ex)
                    raise

        self._doc_types = ['vehicle']

    def purge_data(self):
        for k in self._loaded_docs.keys():
            try:
                self.collection.remove(k)
            except CouchbaseException:
                pass
            except Exception:
                raise

        for k in self._used_extras:
            try:
                self.collection.remove(k)
            except CouchbaseException:
                pass
            except Exception:
                raise

        self._loaded_docs.clear()
        self._used_docs.clear()
        self._used_extras.clear()

    def setup(self,
              collection_type=None,  # type: Optional[CollectionType]
              test_suite=None,  # type: Optional[str]
              num_docs=50,  # type: Optional[int]
              ):
        if collection_type is None:
            collection_type = CollectionType.DEFAULT

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            TestEnvironment.try_n_times(5, 3, self.setup_named_collections)

        if test_suite:
            TestEnvironment.try_n_times(5,
                                        3,
                                        self.load_data,
                                        num_docs=num_docs,
                                        test_suite=test_suite.split('.')[-1])
        else:
            TestEnvironment.try_n_times(5, 3, self.load_data, num_docs=num_docs)

    def setup_collection_mgmt(self, bucket_name):
        self.create_bucket(bucket_name)
        self._test_bucket = TestEnvironment.try_n_times(3, 5, self.cluster.bucket, bucket_name)
        self._test_bucket_cm = self._test_bucket.collections()

    def setup_named_collections(self):
        try:
            self.cm.create_scope(self.TEST_SCOPE)
        except ScopeAlreadyExistsException:
            self.cm.drop_scope(self.TEST_SCOPE)
            self.cm.create_scope(self.TEST_SCOPE)

        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   self.cm.create_scope,
                                                   self.TEST_SCOPE,
                                                   expected_exceptions=(ScopeAlreadyExistsException,))

        self._collection_spec = CollectionSpec(self.TEST_COLLECTION, self.TEST_SCOPE)
        try:
            self.cm.create_collection(self._collection_spec)
        except CollectionAlreadyExistsException:
            self.cm.drop_collection(self._collection_spec)
            self.cm.create_collection(self._collection_spec)

        TestEnvironment.try_n_times_till_exception(5,
                                                   1,
                                                   self.cm.create_collection,
                                                   self._collection_spec,
                                                   expected_exceptions=(CollectionAlreadyExistsException,))

        c = self.get_collection(self.TEST_SCOPE, self.TEST_COLLECTION, bucket_name=self.bucket.name)
        if c is None:
            raise CouchbaseTestEnvironmentException("Unabled to create collection for name collection testing")

        self._named_scope = self.bucket.scope(self.TEST_SCOPE)
        self._named_collection = self._named_scope.collection(self.TEST_COLLECTION)

    def teardown(self,
                 collection_type=None,  # type: Optional[CollectionType]
                 ):

        if collection_type is None:
            collection_type = CollectionType.DEFAULT

        TestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            TestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
            self.disable_named_collections()

    def teardown_named_collections(self):
        self.cm.drop_scope(self.TEST_SCOPE)
        TestEnvironment.try_n_times_till_exception(10,
                                                   1,
                                                   self.cm.drop_scope,
                                                   self.TEST_SCOPE,
                                                   expected_exceptions=(ScopeNotFoundException,)
                                                   )
        self._collection_spec = None
        self._scope = None
        self._named_collection = None

    def verify_mutation_tokens(self, bucket_name, result):
        mutation_token = result.mutation_token()
        assert mutation_token is not None
        partition_id, partition_uuid, sequence_number, mt_bucket_name = mutation_token.as_tuple()
        assert isinstance(partition_id, int)
        assert partition_id != 0
        assert isinstance(partition_uuid, int)
        assert partition_uuid != 0
        assert isinstance(sequence_number, int)
        assert sequence_number != 0
        assert bucket_name == mt_bucket_name

    def verify_mutation_tokens_disabled(self, bucket_name, result):
        mutation_token = result.mutation_token()
        assert mutation_token is not None
        partition_id, partition_uuid, sequence_number, mt_bucket_name = mutation_token.as_tuple()
        assert partition_id != 0
        assert partition_uuid == 0
        assert sequence_number == 0
        assert bucket_name == mt_bucket_name

    @classmethod  # noqa: C901
    def get_environment(cls, **kwargs  # type: Dict[str, Any]  # noqa: C901
                        ) -> TestEnvironment:  # noqa: C901

        config = kwargs.get('couchbase_config', None)
        if config is None:
            raise CouchbaseTestEnvironmentException('No test config provided.')

        conn_string = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))

        enable_mutation_tokens = kwargs.pop('enable_mutation_tokens', None)
        if enable_mutation_tokens is not None:
            opts['enable_mutation_tokens'] = enable_mutation_tokens

        meter = kwargs.pop('meter', None)
        if meter:
            opts['meter'] = meter

        tracer = kwargs.pop('tracer', None)
        if tracer:
            opts['tracer'] = tracer

        transaction_config = kwargs.pop('transaction_config', None)
        if transaction_config:
            opts['transaction_config'] = transaction_config

        env_args = {}
        for _ in range(3):
            try:
                cluster = Cluster.connect(conn_string, opts)
                env_args['cluster'] = cluster
                print(f'Cluster: {id(cluster)}')
                bucket = cluster.bucket(f'{config.bucket_name}')
                env_args['bucket'] = bucket
                cluster.cluster_info()
                env_args['default_collection'] = bucket.default_collection()
                break
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException):
                continue
        env_args.update(**kwargs)
        cb_env = cls(**env_args)
        return cb_env

    @staticmethod
    def sleep(num_seconds  # type: float
              ) -> None:
        time.sleep(num_seconds)

    @staticmethod
    def try_n_times(num_times,  # type: int
                    seconds_between,  # type: Union[int, float]
                    func,  # type: Callable
                    *args,  # type: Any
                    **kwargs  # type: Dict[str, Any]
                    ) -> Any:
        for _ in range(num_times):
            try:
                return func(*args, **kwargs)
            except Exception:
                print(f'trying {func} failed, sleeping for {seconds_between} seconds...')
                time.sleep(seconds_between)

    @staticmethod
    def try_n_times_till_exception(num_times,  # type: int
                                   seconds_between,  # type: Union[int, float]
                                   func,  # type: Callable
                                   *args,  # type: Any
                                   expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                   raise_exception=False,  # type: Optional[bool]
                                   **kwargs  # type: Dict[str, Any]
                                   ) -> None:
        for _ in range(num_times):
            try:
                func(*args, **kwargs)
                time.sleep(seconds_between)
            except expected_exceptions:
                if raise_exception:
                    raise
                # helpful to have this print statement when tests fail
                return
            except Exception:
                raise


class AsyncTestEnvironment(TestEnvironment):

    async def create_bucket(self, bucket_name):
        try:
            await self.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    bucket_type=BucketType.COUCHBASE,
                    ram_quota_mb=100))
        except BucketAlreadyExistsException:
            pass
        await AsyncTestEnvironment.try_n_times(10, 1, self.bm.get_bucket, bucket_name)

    async def get_collection(self, scope_name, coll_name, bucket_name=None):
        scope = await self.get_scope(scope_name, bucket_name=bucket_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    async def get_scope(self, scope_name, bucket_name=None):
        if bucket_name is None and self._test_bucket is None:
            raise CouchbaseTestEnvironmentException("Must provide a bucket name or have a valid test_bucket available")

        bucket_names = ['default']
        if self._test_bucket is not None:
            bucket_names.append(self._test_bucket.name)

        bucket = bucket_name or self._test_bucket.name
        if bucket not in bucket_names:
            raise CouchbaseTestEnvironmentException(
                f"{bucket} is an invalid bucket name.")

        scopes = []
        if bucket == self.bucket.name:
            scopes = await self.cm.get_all_scopes()
        else:
            scopes = await self.test_bucket_cm.get_all_scopes()
        return next((s for s in scopes if s.name == scope_name), None)

    async def load_data(self, num_docs=50, test_suite=None):
        if test_suite and test_suite == 'datastructures_t':
            for k in self.data_provider.generate_keys(num_docs):
                self._loaded_docs[k] = {}
            return

        for v in self.data_provider.get_vehicles()[:num_docs]:
            for _ in range(3):
                try:
                    key = f'{v["id"]}'
                    _ = await self.collection.upsert(key, v)
                    self._loaded_docs[key] = v
                    break
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    await asyncio.sleep(3)
                    continue
                except Exception as ex:
                    print(ex)
                    raise

        self._doc_types = ['vehicle']

    async def purge_data(self):
        for k in self._loaded_docs.keys():
            try:
                await self.collection.remove(k)
            except CouchbaseException:
                pass
            except Exception:
                raise

        for k in self._used_extras:
            try:
                await self.collection.remove(k)
            except CouchbaseException:
                pass
            except Exception:
                raise

        self._loaded_docs.clear()
        self._used_docs.clear()
        self._used_extras.clear()

    async def setup(self,
                    collection_type=None,  # type: Optional[CollectionType]
                    test_suite=None,  # type: Optional[str]
                    num_docs=50,  # type: Optional[int]
                    ):
        if collection_type is None:
            collection_type = CollectionType.DEFAULT

        if collection_type == CollectionType.NAMED:
            self.enable_collection_mgmt().enable_named_collections()
            await AsyncTestEnvironment.try_n_times(5, 3, self.setup_named_collections)

        if test_suite:
            await AsyncTestEnvironment.try_n_times(5,
                                                   3,
                                                   self.load_data,
                                                   num_docs=num_docs,
                                                   test_suite=test_suite.split('.')[-1])
        else:
            await AsyncTestEnvironment.try_n_times(5, 3, self.load_data, num_docs=num_docs)

    async def setup_collection_mgmt(self, bucket_name):
        await self.create_bucket(bucket_name)
        self._test_bucket = await AsyncTestEnvironment.try_n_times(3, 5, self.cluster.bucket, bucket_name)
        self._test_bucket_cm = self._test_bucket.collections()

    async def setup_named_collections(self):
        try:
            await self.cm.create_scope(self.TEST_SCOPE)
        except ScopeAlreadyExistsException:
            await self.cm.drop_scope(self.TEST_SCOPE)
            await self.cm.create_scope(self.TEST_SCOPE)

        await AsyncTestEnvironment.try_n_times_till_exception(5,
                                                              1,
                                                              self.cm.create_scope,
                                                              self.TEST_SCOPE,
                                                              expected_exceptions=(ScopeAlreadyExistsException,))

        self._collection_spec = CollectionSpec(self.TEST_COLLECTION, self.TEST_SCOPE)
        try:
            await self.cm.create_collection(self._collection_spec)
        except CollectionAlreadyExistsException:
            await self.cm.drop_collection(self._collection_spec)
            await self.cm.create_collection(self._collection_spec)

        await AsyncTestEnvironment.try_n_times_till_exception(5,
                                                              1,
                                                              self.cm.create_collection,
                                                              self._collection_spec,
                                                              expected_exceptions=(CollectionAlreadyExistsException,))

        c = await self.get_collection(self.TEST_SCOPE, self.TEST_COLLECTION, bucket_name=self.bucket.name)
        if c is None:
            raise CouchbaseTestEnvironmentException("Unabled to create collection for name collection testing")

        self._named_scope = self.bucket.scope(self.TEST_SCOPE)
        self._named_collection = self._named_scope.collection(self.TEST_COLLECTION)

    async def teardown(self,
                       collection_type=None,  # type: Optional[CollectionType]
                       ):

        if collection_type is None:
            collection_type = CollectionType.DEFAULT

        await AsyncTestEnvironment.try_n_times(5, 3, self.purge_data)

        if collection_type == CollectionType.NAMED:
            await AsyncTestEnvironment.try_n_times(5, 3, self.teardown_named_collections)
            self.disable_named_collections()

    async def teardown_named_collections(self):
        await self.cm.drop_scope(self.TEST_SCOPE)
        await AsyncTestEnvironment.try_n_times_till_exception(10,
                                                              1,
                                                              self.cm.drop_scope,
                                                              self.TEST_SCOPE,
                                                              expected_exceptions=(ScopeNotFoundException,)
                                                              )
        self._collection_spec = None
        self._scope = None
        self._named_collection = None

    @classmethod  # noqa: C901
    async def get_environment(cls, **kwargs  # type: Dict[str, Any]  # noqa: C901
                              ) -> TestEnvironment:  # noqa: C901

        config = kwargs.get('couchbase_config', None)
        if config is None:
            raise CouchbaseTestEnvironmentException('No test config provided.')

        conn_string = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))

        enable_mutation_tokens = kwargs.pop('enable_mutation_tokens', None)
        if enable_mutation_tokens is not None:
            opts['enable_mutation_tokens'] = enable_mutation_tokens

        meter = kwargs.pop('meter', None)
        if meter:
            opts['meter'] = meter

        tracer = kwargs.pop('tracer', None)
        if tracer:
            opts['tracer'] = tracer

        transaction_config = kwargs.pop('transaction_config', None)
        if transaction_config:
            opts['transaction_config'] = transaction_config

        env_args = {}
        for _ in range(3):
            try:
                cluster = await AsyncCluster.connect(conn_string, opts)
                env_args['cluster'] = cluster
                print(f'Cluster: {id(cluster)}')
                bucket = cluster.bucket(f'{config.bucket_name}')
                await bucket.on_connect()
                env_args['bucket'] = bucket
                await cluster.cluster_info()
                env_args['default_collection'] = bucket.default_collection()
                break
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException):
                continue
        env_args.update(**kwargs)
        cb_env = cls(**env_args)
        return cb_env

    @staticmethod
    async def sleep(num_seconds  # type: float
                    ) -> None:
        await asyncio.sleep(num_seconds)

    @staticmethod
    async def try_n_times(num_times,  # type: int
                          seconds_between,  # type: Union[int, float]
                          func,  # type: Callable
                          *args,  # type: Any
                          **kwargs  # type: Dict[str, Any]
                          ) -> Any:
        for _ in range(num_times):
            try:
                return await func(*args, **kwargs)
            except Exception:
                print(f'trying {func} failed, sleeping for {seconds_between} seconds...')
                await asyncio.sleep(seconds_between)

    @staticmethod
    async def try_n_times_till_exception(num_times,  # type: int
                                         seconds_between,  # type: Union[int, float]
                                         func,  # type: Callable
                                         *args,  # type: Any
                                         expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                         raise_exception=False,  # type: Optional[bool]
                                         **kwargs  # type: Dict[str, Any]
                                         ) -> None:
        for _ in range(num_times):
            try:
                await func(*args, **kwargs)
                await asyncio.sleep(seconds_between)
            except expected_exceptions:
                if raise_exception:
                    raise
                # helpful to have this print statement when tests fail
                return
            except Exception:
                raise


@pytest.fixture(scope='session', name='test_env')
def base_test_environment(couchbase_config):
    data_provider = DataProvider(100)
    data_provider.build_docs()
    return couchbase_config, data_provider


@pytest.fixture(scope='session', name='cb_base_env')
def couchbase_base_environment(test_env):
    couchbase_config, data_provider = test_env
    return TestEnvironment.get_environment(couchbase_config=couchbase_config, data_provider=data_provider)


@pytest.fixture(scope='session', name='cb_base_txn_env')
def couchbase_base_txn_environment(test_env):
    couchbase_config, data_provider = test_env
    transaction_config = TransactionConfig(durability=ServerDurability(DurabilityLevel.NONE))
    return TestEnvironment.get_environment(couchbase_config=couchbase_config,
                                           data_provider=data_provider,
                                           transaction_config=transaction_config)


@pytest_asyncio.fixture(scope='session')
def event_loop():
    loop = get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope='session', name='acb_base_env')
async def acouchbase_base_environment(test_env):
    couchbase_config, data_provider = test_env
    return await AsyncTestEnvironment.get_environment(couchbase_config=couchbase_config,
                                                      data_provider=data_provider)


@pytest_asyncio.fixture(scope='session', name='acb_base_txn_env')
async def acouchbase_base_txn_environment(test_env):
    couchbase_config, data_provider = test_env
    transaction_config = TransactionConfig(durability=ServerDurability(DurabilityLevel.NONE))
    return await AsyncTestEnvironment.get_environment(couchbase_config=couchbase_config,
                                                      data_provider=data_provider,
                                                      transaction_config=transaction_config)
