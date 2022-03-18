import asyncio
from typing import (Any,
                    Callable,
                    Optional,
                    Tuple,
                    Type,
                    Union)

from acouchbase.bucket import Bucket
from acouchbase.collection import Collection
from acouchbase.management.analytics import AnalyticsIndexManager
from acouchbase.management.buckets import BucketManager
from acouchbase.management.collections import CollectionManager
from acouchbase.management.eventing import EventingFunctionManager
from acouchbase.management.queries import QueryIndexManager
from acouchbase.management.search import SearchIndexManager
from acouchbase.management.users import UserManager
from acouchbase.management.views import ViewIndexManager
from acouchbase.scope import Scope
from acouchbase.transactions import Transactions
from couchbase.exceptions import (BucketDoesNotExistException,
                                  CollectionAlreadyExistsException,
                                  CouchbaseException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.buckets import BucketType, CreateBucketSettings
from couchbase.management.collections import CollectionSpec
from couchbase.transactions import TransactionConfig
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.helpers import CollectionType  # noqa: F401
from tests.helpers import KVPair  # noqa: F401
from tests.helpers import CouchbaseTestEnvironment, CouchbaseTestEnvironmentException


class TestEnvironment(CouchbaseTestEnvironment):

    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"

    def __init__(self, cluster, bucket, collection, cluster_config, **kwargs):
        super().__init__(cluster, bucket, collection, cluster_config)
        if kwargs.get("manage_buckets", False) is True:
            self.check_if_feature_supported('basic_bucket_mgmt')
            self._bm = self.cluster.buckets()

        if kwargs.get("manage_collections", False) is True:
            self.check_if_feature_supported('collections')
            self._cm = self.bucket.collections()

        if kwargs.get("manage_query_indexes", False) is True:
            self.check_if_feature_supported('query_index_mgmt')
            self._ixm = self.cluster.query_indexes()

        if kwargs.get("manage_search_indexes", False) is True:
            self.check_if_feature_supported('search_index_mgmt')
            self._sixm = self.cluster.search_indexes()

        if kwargs.get("manage_view_indexes", False) is True:
            self.check_if_feature_supported('view_index_mgmt')
            self._vixm = self.bucket.view_indexes()

        if kwargs.get("manage_users", False) is True:
            self.check_if_feature_supported('user_mgmt')
            self._um = self.cluster.users()

        if kwargs.get("manage_analytics", False) is True:
            self.check_if_feature_supported('analytics')
            self._am = self.cluster.analytics_indexes()

        if kwargs.get('manage_eventing_functions', False) is True:
            self.check_if_feature_supported('eventing_function_mgmt')
            self._efm = self.cluster.eventing_functions()

        if kwargs.get("transactions", False) is True:
            # for now, turn the lost attempts thread off, so we don't have issues shutting down.
            self._txns = Transactions(self.cluster, TransactionConfig(cleanup_lost_attempts=False))

        self._test_bucket = None
        self._test_bucket_cm = None
        self._collection_spec = None
        self._scope = None
        self._named_collection = None

    @property
    def collection(self) -> Collection:
        if self._named_collection is None:
            return super().collection
        return self._named_collection

    @property
    def scope(self) -> Optional[Scope]:
        return self._scope

    @property
    def fqdn(self) -> Optional[str]:
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    @property
    def bm(self) -> Optional[BucketManager]:
        """Returns the default bucket's BucketManager"""
        return self._bm if hasattr(self, '_bm') else None

    @property
    def cm(self) -> Optional[CollectionManager]:
        """Returns the default CollectionManager"""
        return self._cm if hasattr(self, '_cm') else None

    @property
    def ixm(self) -> Optional[QueryIndexManager]:
        """Returns the default QueryIndexManager"""
        return self._ixm if hasattr(self, '_ixm') else None

    @property
    def sixm(self) -> Optional[SearchIndexManager]:
        """Returns the default SearchIndexManager"""
        return self._sixm if hasattr(self, '_sixm') else None

    @property
    def vixm(self) -> Optional[ViewIndexManager]:
        """Returns the default ViewIndexManager"""
        return self._vixm if hasattr(self, '_vixm') else None

    @property
    def um(self) -> Optional[UserManager]:
        """Returns the default UserManager"""
        return self._um if hasattr(self, '_um') else None

    @property
    def am(self) -> Optional[AnalyticsIndexManager]:
        """Returns the default AnalyticsIndexManager"""
        return self._am if hasattr(self, '_am') else None

    @property
    def efm(self) -> Optional[EventingFunctionManager]:
        """Returns the default EventingFunctionManager"""
        return self._efm if hasattr(self, '_efm') else None

    @property
    def test_bucket(self) -> Optional[Bucket]:
        """Returns the test bucket object"""
        return self._test_bucket if hasattr(self, '_test_bucket') else None

    @property
    def test_bucket_cm(self) -> Optional[CollectionManager]:
        """Returns the test bucket's CollectionManager"""
        return self._test_bucket_cm if hasattr(self, '_test_bucket_cm') else None

    @property
    def transactions(self) -> Optional[Transactions]:
        """Returns a transactions object connected to the cluster"""
        return self._txns

    async def get_new_key_value(self, reset=True):
        if reset is True:
            try:
                await self.collection.remove(self.NEW_KEY)
            except BaseException:
                pass
        return self.NEW_KEY, self.NEW_CONTENT

    # standard data load/purge

    async def load_data(self):
        data_types, sample_json = self.load_data_from_file()
        for dt in data_types:
            data = sample_json.get(dt, None)
            if data and "results" in data:
                await asyncio.gather(*[self.collection.upsert(f"{r['type']}_{r['id']}", r) for r in data["results"]])
                # for r in data["results"]:
                #     key = f"{r['type']}_{r['id']}"
                #     await self.collection.upsert(key, r)
                self._loaded_keys.extend(
                    [f"{r['type']}_{r['id']}" for r in data["results"]])

    async def purge_data(self):
        await asyncio.gather(*[self.collection.remove(key) for key in self._loaded_keys])

    # binary data load/purge

    async def load_utf8_binary_data(self, start_value=None):
        utf8_key = self.get_binary_key('UTF8')
        value = start_value or ''
        tc = RawStringTranscoder()
        await self.collection.upsert(utf8_key, value, transcoder=tc)
        await self.try_n_times(10, 1, self.collection.get, utf8_key, transcoder=tc)
        return utf8_key, value

    async def load_bytes_binary_data(self, start_value=None):
        bytes_key = self.get_binary_key('BYTES')
        value = start_value or b''
        tc = RawBinaryTranscoder()
        await self.collection.upsert(bytes_key, value, transcoder=tc)
        await self.try_n_times(10, 1, self.collection.get, bytes_key, transcoder=tc)
        return bytes_key, value

    async def load_counter_binary_data(self, start_value=None):
        counter_key = self.get_binary_key('COUNTER')
        if start_value:
            await self.collection.upsert(counter_key, start_value)
            await self.try_n_times(10, 1, self.collection.get, counter_key)

        return counter_key, start_value

    async def purge_binary_data(self):
        for k in self.get_binary_keys():
            try:
                await self.collection.remove(k)
            except CouchbaseException:
                pass

    # Collection MGMT

    async def setup_collection_mgmt(self, bucket_name):
        await self.bm.create_bucket(
            CreateBucketSettings(
                name=bucket_name,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100))
        await self.try_n_times(10, 1, self.bm.get_bucket, bucket_name)
        self._test_bucket = self.cluster.bucket(bucket_name)
        await self.try_n_times(10, 1, self._test_bucket.on_connect)
        self._test_bucket_cm = self._test_bucket.collections()

    async def setup_named_collections(self):
        try:
            await self.cm.create_scope(self.TEST_SCOPE)
        except ScopeAlreadyExistsException:
            await self.cm.drop_scope(self.TEST_SCOPE)
            await self.cm.create_scope(self.TEST_SCOPE)

        self._collection_spec = CollectionSpec(self.TEST_COLLECTION, self.TEST_SCOPE)
        try:
            await self.cm.create_collection(self._collection_spec)
        except CollectionAlreadyExistsException:
            await self.cm.drop_collection(self._collection_spec)
            await self.cm.create_collection(self._collection_spec)

        c = await self.get_collection(self.TEST_SCOPE, self.TEST_COLLECTION, bucket_name=self.bucket.name)
        if c is None:
            raise CouchbaseTestEnvironmentException("Unabled to create collection for name collection testing")

        self._scope = self.bucket.scope(self.TEST_SCOPE)
        self._named_collection = self._scope.collection(self.TEST_COLLECTION)

    async def teardown_named_collections(self):
        await self.cm.drop_scope(self.TEST_SCOPE)
        await self.try_n_times_till_exception(10,
                                              1,
                                              self.cm.drop_scope,
                                              self.TEST_SCOPE,
                                              expected_exceptions=(ScopeNotFoundException,)
                                              )
        self._collection_spec = None
        self._scope = None
        self._named_collection = None

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

    async def get_collection(self, scope_name, coll_name, bucket_name=None):
        scope = await self.get_scope(scope_name, bucket_name=bucket_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    # Bucket MGMT

    async def purge_buckets(self, buckets):
        for bucket in buckets:
            try:
                await self.bm.drop_bucket(bucket)
            except BucketDoesNotExistException:
                continue
            except Exception:
                raise

            # now be sure it is really gone
            await self.try_n_times_till_exception(10,
                                                  3,
                                                  self.bm.get_bucket,
                                                  bucket,
                                                  expected_exceptions=(BucketDoesNotExistException))

    # helper methods

    async def try_n_times(self,
                          num_times,  # type: int
                          seconds_between,  # type: Union[int, float]
                          func,  # type: Callable
                          *args,  # type: Any
                          #   expected_exceptions=(
                          #       Exception,),  # type: Tuple[Type[Exception]]
                          **kwargs  # type: Any
                          ) -> Any:

        for _ in range(num_times):
            try:
                return await func(*args, **kwargs)
            except Exception:
                await asyncio.sleep(float(seconds_between))

        # TODO: option to restart mock server?

        raise CouchbaseTestEnvironmentException(
            f"Unsuccessful execution of {func} after {num_times} times, "
            "waiting {seconds_between} seconds between calls.")

    async def try_n_times_till_exception(self,
                                         num_times,  # type: int
                                         seconds_between,  # type: Union[int, float]
                                         func,  # type: Callable
                                         *args,  # type: Any
                                         expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                         **kwargs  # type: Any
                                         ):
        # type: (...) -> Any
        for _ in range(num_times):
            try:
                await func(*args, **kwargs)
                await asyncio.sleep(float(seconds_between))
            except expected_exceptions:
                # helpful to have this print statement when tests fail
                return
            except Exception:
                raise

        # TODO: option to restart mock server?

        raise CouchbaseTestEnvironmentException(
            f"successful {func} after {num_times} times waiting {seconds_between} seconds between calls.")