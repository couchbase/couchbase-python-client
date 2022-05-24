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
                                  CouchbaseException,
                                  UnAmbiguousTimeoutException)
from couchbase.management.buckets import (BucketType,
                                          CreateBucketSettings)
from couchbase.options import ClusterOptions
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.helpers import CollectionType  # noqa: F401
from tests.helpers import KVPair  # noqa: F401
from tests.helpers import CouchbaseTestEnvironment, CouchbaseTestEnvironmentException

# from txcouchbase.bucket import Bucket
from txcouchbase.cluster import Cluster
from txcouchbase.management.buckets import BucketManager

from .conftest import run_in_reactor_thread


class TestEnvironment(CouchbaseTestEnvironment):
    def __init__(self, cluster, bucket, collection, cluster_config, **kwargs):
        super().__init__(cluster, bucket, collection, cluster_config)

        if kwargs.get("manage_buckets", False) is True:
            self.check_if_feature_supported('basic_bucket_mgmt')
            self._bm = self.cluster.buckets()

    @property
    def bm(self) -> Optional[BucketManager]:
        """Returns the default bucket's BucketManager"""
        return self._bm if hasattr(self, '_bm') else None

    @classmethod
    def get_environment(cls, test_suite, couchbase_config, coll_type=CollectionType.DEFAULT, **kwargs):

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
                        run_in_reactor_thread(self.deferred_sleep, 3)
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

    # helper methods

    def sleep(self, sleep_seconds  # type: float
              ) -> None:
        run_in_reactor_thread(self.deferred_sleep, sleep_seconds)

    def deferred_sleep(self, sleep_seconds  # type: float
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
                run_in_reactor_thread(self.deferred_sleep, seconds_between)

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
                run_in_reactor_thread(self.deferred_sleep, seconds_between)
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
