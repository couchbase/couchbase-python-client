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

from typing import (Any,
                    Callable,
                    Dict,
                    Optional,
                    Tuple,
                    Type,
                    Union)

import pytest
from twisted.internet import defer, reactor

from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  CollectionAlreadyExistsException,
                                  CouchbaseException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException,
                                  UnAmbiguousTimeoutException)
from couchbase.management.buckets import BucketType, CreateBucketSettings
from couchbase.management.collections import CollectionSpec
from couchbase.options import ClusterOptions
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.helpers import CollectionType  # noqa: F401
from tests.helpers import FakeTestObj  # noqa: F401
from tests.helpers import KVPair  # noqa: F401
from tests.helpers import CouchbaseTestEnvironment, CouchbaseTestEnvironmentException
from txcouchbase.bucket import Bucket
from txcouchbase.cluster import Cluster
from txcouchbase.management.buckets import BucketManager
from txcouchbase.management.collections import CollectionManager
from txcouchbase.scope import Scope

from .conftest import run_in_reactor_thread


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

        self._test_bucket = None
        self._test_bucket_cm = None
        self._collection_spec = None
        self._scope = None
        self._named_collection = None

    @property
    def collection(self):
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
    def test_bucket(self) -> Optional[Bucket]:
        """Returns the test bucket object"""
        return self._test_bucket if hasattr(self, '_test_bucket') else None

    @property
    def test_bucket_cm(self) -> Optional[CollectionManager]:
        """Returns the test bucket's CollectionManager"""
        return self._test_bucket_cm if hasattr(self, '_test_bucket_cm') else None

    @classmethod  # noqa: C901
    def get_environment(cls, test_suite, couchbase_config, coll_type=CollectionType.DEFAULT, **kwargs):  # noqa: C901

        # this will only return False _if_ using the mock server
        tmp_name = test_suite.split('.')[-1]
        if 'collection_new' in tmp_name:
            tmp_name = 'collection_t'
        mock_supports = CouchbaseTestEnvironment.mock_supports_feature(tmp_name,
                                                                       couchbase_config.is_mock_server)
        if not mock_supports:
            pytest.skip(f'Mock server does not support feature(s) required for test suite: {test_suite}')

        conn_string = couchbase_config.get_connection_string()
        username, pw = couchbase_config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        transcoder = kwargs.pop('transcoder', None)
        if transcoder:
            opts['transcoder'] = transcoder
        okay = False
        for _ in range(3):
            try:
                cluster = Cluster(conn_string, opts)
                run_in_reactor_thread(cluster.on_connect)
                bucket = cluster.bucket(f"{couchbase_config.bucket_name}")
                run_in_reactor_thread(bucket.on_connect)
                run_in_reactor_thread(cluster.cluster_info)
                okay = True
                break
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException):
                continue

        if not okay and couchbase_config.is_mock_server:
            pytest.skip(('CAVES does not seem to be happy. Skipping tests as failure is not'
                        ' an accurate representation of the state of the test, but rather'
                         ' there is an environment issue.'))

        coll = bucket.default_collection()
        if coll_type == CollectionType.DEFAULT:
            cb_env = cls(cluster, bucket, coll, couchbase_config, **kwargs)
        elif coll_type == CollectionType.NAMED:
            if 'manage_collections' not in kwargs:
                kwargs['manage_collections'] = True
            cb_env = cls(cluster,
                         bucket,
                         coll,
                         couchbase_config,
                         **kwargs)

        return cb_env

    def get_new_key_value(self, reset=True):
        if reset is True:
            try:
                run_in_reactor_thread(self.collection.remove, self.NEW_KEY)
            except BaseException:
                pass
        return self.NEW_KEY, self.NEW_CONTENT

    # standard data load/purge

    def load_data(self):
        data_types, sample_json = self.load_data_from_file()
        for dt in data_types:
            data = sample_json.get(dt, None)

            if data and "results" in data:
                stable = False
                for _ in range(3):
                    try:
                        for idx, r in enumerate(data["results"]):
                            key = f"{r['type']}_{r['id']}"
                            run_in_reactor_thread(self.collection.upsert, key, r)
                            self._loaded_keys.append(key)

                        stable = True
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        run_in_reactor_thread(TestEnvironment.deferred_sleep, 3)
                        continue
                    except Exception:
                        raise

                self.skip_if_mock_unstable(stable)

    def purge_data(self):
        for key in self._loaded_keys:
            try:
                run_in_reactor_thread(self.collection.remove, key)
            except CouchbaseException:
                pass

    # binary data load/purge

    def load_utf8_binary_data(self, start_value=None):
        utf8_key = self.get_binary_key('UTF8')
        value = start_value or ''
        tc = RawStringTranscoder()
        run_in_reactor_thread(self.collection.upsert, utf8_key, value, transcoder=tc)
        self.try_n_times(10, 1, self.collection.get, utf8_key, transcoder=tc)
        return utf8_key, value

    def load_bytes_binary_data(self, start_value=None):
        bytes_key = self.get_binary_key('BYTES')
        value = start_value or b''
        tc = RawBinaryTranscoder()
        run_in_reactor_thread(self.collection.upsert, bytes_key, value, transcoder=tc)
        self.try_n_times(10, 1, self.collection.get, bytes_key, transcoder=tc)
        return bytes_key, value

    def load_counter_binary_data(self, start_value=None):
        counter_key = self.get_binary_key('COUNTER')
        if start_value:
            run_in_reactor_thread(self.collection.upsert, counter_key, start_value)
            self.try_n_times(10, 1, self.collection.get, counter_key)

        return counter_key, start_value

    def purge_binary_data(self):
        for k in self.get_binary_keys():
            try:
                run_in_reactor_thread(self.collection.remove, k)
            except CouchbaseException:
                pass

    # Bucket MGMT

    def create_bucket(self, bucket_name):
        try:
            run_in_reactor_thread(self.bm.create_bucket,
                                  CreateBucketSettings(
                                      name=bucket_name,
                                      bucket_type=BucketType.COUCHBASE,
                                      ram_quota_mb=100))
        except BucketAlreadyExistsException:
            pass
        self.try_n_times(10, 1, self.bm.get_bucket, bucket_name)

    def purge_buckets(self, buckets):
        for bucket in buckets:
            try:
                run_in_reactor_thread(self.bm.drop_bucket, bucket)
            except BucketDoesNotExistException:
                pass
            except Exception:
                raise

            # now be sure it is really gone
            self.try_n_times_till_exception(10,
                                            3,
                                            self.bm.get_bucket,
                                            bucket,
                                            expected_exceptions=(BucketDoesNotExistException))

    # Collection MGMT

    def setup_collection_mgmt(self, bucket_name):
        self.create_bucket(bucket_name)
        self._test_bucket = self.try_n_times(3, 5, self.cluster.bucket, bucket_name, is_deferred=False)
        self._test_bucket_cm = self._test_bucket.collections()

    def setup_named_collections(self):
        try:
            run_in_reactor_thread(self.cm.create_scope, self.TEST_SCOPE)
        except ScopeAlreadyExistsException:
            run_in_reactor_thread(self.cm.drop_scope, self.TEST_SCOPE)
            run_in_reactor_thread(self.cm.create_scope, self.TEST_SCOPE)

        self._collection_spec = CollectionSpec(self.TEST_COLLECTION, self.TEST_SCOPE)
        try:
            run_in_reactor_thread(self.cm.create_collection, self._collection_spec)
        except CollectionAlreadyExistsException:
            run_in_reactor_thread(self.cm.drop_collection, self._collection_spec)
            run_in_reactor_thread(self.cm.create_collection, self._collection_spec)

        c = self.get_collection(self.TEST_SCOPE, self.TEST_COLLECTION, bucket_name=self.bucket.name)
        if c is None:
            raise CouchbaseTestEnvironmentException("Unabled to create collection for name collection testing")

        self._scope = self.bucket.scope(self.TEST_SCOPE)
        self._named_collection = self._scope.collection(self.TEST_COLLECTION)

    def teardown_named_collections(self):
        run_in_reactor_thread(self.cm.drop_scope, self.TEST_SCOPE)
        self.try_n_times_till_exception(10,
                                        1,
                                        self.cm.drop_scope,
                                        self.TEST_SCOPE,
                                        expected_exceptions=(ScopeNotFoundException,)
                                        )
        self._collection_spec = None
        self._scope = None
        self._named_collection = None

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
            scopes = run_in_reactor_thread(self.cm.get_all_scopes)
        else:
            scopes = run_in_reactor_thread(self.test_bucket_cm.get_all_scopes)
        return next((s for s in scopes if s.name == scope_name), None)

    def get_collection(self, scope_name, coll_name, bucket_name=None):
        scope = self.get_scope(scope_name, bucket_name=bucket_name)
        if scope:
            return next(
                (c for c in scope.collections if c.name == coll_name), None)

        return None

    # helper methods

    def sleep(self, sleep_seconds  # type: float
              ) -> None:
        run_in_reactor_thread(TestEnvironment.deferred_sleep, sleep_seconds)

    @staticmethod
    def deferred_sleep(sleep_seconds  # type: float
                       ) -> None:
        d = defer.Deferred()
        reactor.callLater(sleep_seconds, d.callback, "")
        return d

    def _try_n_times(self,
                     num_times,  # type: int
                     seconds_between,  # type: Union[int, float]
                     func,  # type: Callable
                     *args,  # type: Any
                     is_deferred=True,  # type: Optional[bool]
                     **kwargs  # type: Dict[str, Any]
                     ) -> Any:
        for _ in range(num_times):
            try:
                if is_deferred:
                    res = run_in_reactor_thread(func, *args, **kwargs)
                else:
                    res = func(*args, **kwargs)
                return res
            except Exception:
                print(f'trying {func} failed, sleeping for {seconds_between} seconds...')
                run_in_reactor_thread(TestEnvironment.deferred_sleep, seconds_between)

    def try_n_times(self,
                    num_times,  # type: int
                    seconds_between,  # type: Union[int, float]
                    func,  # type: Callable
                    *args,  # type: Any
                    is_deferred=True,  # type: Optional[bool]
                    reset_on_timeout=False,  # type: Optional[bool]
                    reset_num_times=None,  # type: Optional[int]
                    **kwargs  # type: Dict[str, Any]
                    ) -> Any:
        if reset_on_timeout:
            reset_times = reset_num_times or num_times
            for _ in range(reset_times):
                try:
                    return self._try_n_times(num_times,
                                             seconds_between,
                                             func,
                                             *args,
                                             is_deferred=is_deferred,
                                             **kwargs)
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    continue
                except Exception:
                    raise
        else:
            return self._try_n_times(num_times,
                                     seconds_between,
                                     func,
                                     *args,
                                     is_deferred=is_deferred,
                                     **kwargs)

        raise CouchbaseTestEnvironmentException(
            f"Unsuccessful execution of {func.__name__} after {num_times} times, "
            f"waiting {seconds_between} seconds between calls.")

    def _try_n_times_until_exception(self,
                                     num_times,  # type: int
                                     seconds_between,  # type: Union[int, float]
                                     func,  # type: Callable
                                     *args,  # type: Any
                                     is_deferred=True,  # type: Optional[bool]
                                     expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                     raise_exception=False,  # type: Optional[bool]
                                     **kwargs  # type: Dict[str, Any]
                                     ) -> None:
        for _ in range(num_times):
            try:
                if is_deferred:
                    run_in_reactor_thread(func, *args, **kwargs)
                else:
                    func(*args, **kwargs)
                run_in_reactor_thread(TestEnvironment.deferred_sleep, seconds_between)
            except expected_exceptions:
                if raise_exception:
                    raise
                # helpful to have this print statement when tests fail
                return
            except Exception:
                raise

    def try_n_times_till_exception(self,
                                   num_times,  # type: int
                                   seconds_between,  # type: Union[int, float]
                                   func,  # type: Callable
                                   *args,  # type: Any
                                   is_deferred=True,  # type: Optional[bool]
                                   expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                   raise_exception=False,  # type: Optional[bool]
                                   raise_if_no_exception=True,  # type: Optional[bool]
                                   reset_on_timeout=False,  # type: Optional[bool]
                                   reset_num_times=None,  # type: Optional[int]
                                   **kwargs  # type: Dict[str, Any]
                                   ) -> None:
        if reset_on_timeout:
            reset_times = reset_num_times or num_times
            for _ in range(reset_times):
                try:
                    self._try_n_times_until_exception(num_times,
                                                      seconds_between,
                                                      func,
                                                      *args,
                                                      is_deferred=is_deferred,
                                                      expected_exceptions=expected_exceptions,
                                                      raise_exception=raise_exception,
                                                      **kwargs)
                    return
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    continue
                except Exception:
                    raise
        else:
            self._try_n_times_until_exception(num_times,
                                              seconds_between,
                                              func,
                                              *args,
                                              is_deferred=is_deferred,
                                              expected_exceptions=expected_exceptions,
                                              raise_exception=raise_exception,
                                              **kwargs)
            return  # success -- return now

        # TODO: option to restart mock server?
        if raise_if_no_exception is False:
            return

        raise CouchbaseTestEnvironmentException((f"Exception not raised calling {func.__name__} {num_times} times "
                                                 f"waiting {seconds_between} seconds between calls."))
