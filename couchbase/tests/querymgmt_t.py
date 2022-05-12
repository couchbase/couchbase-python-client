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

from couchbase.exceptions import (ParsingFailedException,
                                  QueryIndexAlreadyExistsException,
                                  QueryIndexNotFoundException,
                                  WatchQueryIndexTimeoutException)
from couchbase.management.options import (CreatePrimaryQueryIndexOptions,
                                          CreateQueryIndexOptions,
                                          DropPrimaryQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions,
                                          WatchQueryIndexOptions)

from ._test_utils import TestEnvironment


class QueryIndexManagementTests:

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_query_indexes=True)

        yield cb_env
        cb_env.try_n_times(5, 3, self._clear_all_indexes, cb_env, ignore_fail=True)

    def _clear_all_indexes(self, cb_env, ignore_fail=False):
        # Drop all indexes!
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        indexes = ixm.get_all_indexes(bucket_name)
        for index in indexes:
            # @TODO:  will need to update once named primary allowed
            if index.is_primary:
                ixm.drop_primary_index(bucket_name)
            else:
                ixm.drop_index(bucket_name, index.name)
        for _ in range(10):
            indexes = ixm.get_all_indexes(bucket_name)
            if 0 == len(indexes):
                return
            cb_env.sleep(2)

        if ignore_fail is True:
            return

        pytest.xfail(
            "Indexes were not dropped after {} waits of {} seconds each".format(10, 3))

    @pytest.fixture(scope="class")
    def check_query_index_mgmt_supported(self, cb_env):
        cb_env.check_if_feature_supported('query_index_mgmt')

    @pytest.fixture()
    def clear_all_indexes(self, cb_env):
        self._clear_all_indexes(cb_env)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_primary(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.create_primary_index(
            bucket_name, timeout=timedelta(seconds=60))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM `{bucket_name}` LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        ixm.drop_primary_index(bucket_name)
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    # @TODO: couchbase++ does not handle named primary
    # @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    # @pytest.mark.usefixtures("clear_all_indexes")
    # def test_create_named_primary(self, cb_env):
    #     bucket_name = cb_env.bucket.name
    #     ixname = 'namedPrimary'
    #     n1ql = f'SELECT * FROM {bucket_name} LIMIT 1'
    #     ixm = cb_env.ixm
    #     # Try to create a _named_ primary index
    #     ixm.create_index(bucket_name, ixname, [], primary=True)
    #     cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_primary_ignore_if_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.create_primary_index(bucket_name)
        ixm.create_primary_index(
            bucket_name, CreatePrimaryQueryIndexOptions(ignore_if_exists=True))

        with pytest.raises(QueryIndexAlreadyExistsException):
            ixm.create_primary_index(bucket_name)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_primary_ignore_if_exists_kwargs(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.create_primary_index(bucket_name)
        ixm.create_primary_index(bucket_name, ignore_if_exists=True)

        with pytest.raises(QueryIndexAlreadyExistsException):
            ixm.create_primary_index(bucket_name)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_primary(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm

        # create an index so we can drop
        ixm.create_primary_index(
            bucket_name, timeout=timedelta(seconds=60))

        ixm.drop_primary_index(
            bucket_name, timeout=timedelta(seconds=60))
        # this should fail now
        with pytest.raises(QueryIndexNotFoundException):
            ixm.drop_primary_index(bucket_name)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_primary_ignore_if_not_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.drop_primary_index(bucket_name, ignore_if_not_exists=True)
        ixm.drop_primary_index(bucket_name, DropPrimaryQueryIndexOptions(ignore_if_not_exists=True))
        with pytest.raises(QueryIndexNotFoundException):
            ixm.drop_primary_index(bucket_name)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_secondary_indexes(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        ixm.create_index(bucket_name, ixname,
                         fields=fields, timeout=timedelta(seconds=120))
        n1ql = "SELECT {1}, {2} FROM `{0}` WHERE {1}=1 AND {2}=2 LIMIT 1".format(
            bucket_name, *fields)
        cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_secondary_indexes_condition(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        fields = ('fld1', 'fld2')

        cb_env.try_n_times_till_exception(10, 5, ixm.drop_index, bucket_name, ixname,
                                          expected_exceptions=(QueryIndexNotFoundException,))
        condition = '((`fld1` = 1) and (`fld2` = 2))'
        ixm.create_index(bucket_name, ixname, fields,
                         CreateQueryIndexOptions(timeout=timedelta(days=1), condition=condition))

        def check_index():
            indexes = ixm.get_all_indexes(bucket_name)
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        result = cb_env.try_n_times(10, 5, check_index)
        assert result.condition == condition

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_secondary_indexes(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        ixm.create_index(bucket_name, ixname,
                         fields=fields, timeout=timedelta(seconds=120))

        n1ql = "SELECT {1}, {2} FROM `{0}` WHERE {1}=1 AND {2}=2 LIMIT 1".format(
            bucket_name, *fields)

        # Drop the index
        ixm.drop_index(bucket_name, ixname)
        # Issue the query again
        with pytest.raises((QueryIndexNotFoundException, ParsingFailedException)):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_index_no_fields(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # raises a TypeError b/c not providing fields means
        #   create_index() is missing a required positional param
        with pytest.raises(TypeError):
            ixm.create_index(bucket_name, 'noFields')

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_secondary_indexes_ignore_if_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        ixm.create_index(bucket_name, ixname, fields=['hello'])
        ixm.create_index(bucket_name, ixname, fields=[
            'hello'], ignore_if_exists=True)
        ixm.create_index(bucket_name, ixname, ['hello'], CreateQueryIndexOptions(ignore_if_exists=True))
        with pytest.raises(QueryIndexAlreadyExistsException):
            ixm.create_index(bucket_name, ixname, fields=['hello'])

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_secondary_indexes_ignore_if_not_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # Create it
        ixname = 'ix2'
        ixm.create_index(bucket_name, ixname, ['hello'])
        # Drop it
        ixm.drop_index(bucket_name, ixname)
        ixm.drop_index(bucket_name, ixname, ignore_if_not_exists=True)
        ixm.drop_index(bucket_name, ixname, DropQueryIndexOptions(ignore_if_not_exists=True))
        with pytest.raises(QueryIndexNotFoundException):
            ixm.drop_index(bucket_name, ixname)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_list_indexes(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # start with no indexes
        ixs = ixm.get_all_indexes(bucket_name)
        assert len(ixs) == 0

        # Create the primary index
        ixm.create_primary_index(bucket_name)
        ixs = ixm.get_all_indexes(bucket_name)
        assert len(ixs) == 1
        assert ixs[0].is_primary is True
        assert ixs[0].name == '#primary'
        assert ixs[0].bucket_name == bucket_name

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_index_partition_info(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # use query to create index w/ partition, cannot do that via manager
        # ATM
        n1ql = 'CREATE INDEX idx_fld1 ON `{0}`(fld1) PARTITION BY HASH(fld1)'.format(
            bucket_name)
        cb_env.cluster.query(n1ql).execute()
        ixs = ixm.get_all_indexes(bucket_name)
        idx = next((ix for ix in ixs if ix.name == "idx_fld1"), None)
        assert idx is not None
        assert idx.partition is not None
        assert idx.partition == 'HASH(`fld1`)'

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_watch(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # Create primary index
        ixm.create_primary_index(bucket_name, deferred=True)
        ixs = ixm.get_all_indexes(bucket_name)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            ixm.create_index(bucket_name,
                             'ix{0}'.format(n), fields=['fld{0}'.format(n)], deferred=defer)

        ixs = ixm.get_all_indexes(bucket_name)
        assert len(ixs) == 6
        # by not building deffered indexes, should timeout
        with pytest.raises(WatchQueryIndexTimeoutException):
            ixm.watch_indexes(bucket_name, [i.name for i in ixs], WatchQueryIndexOptions(timeout=timedelta(seconds=5)))

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_deferred(self, cb_env):
        if cb_env.server_version_short < 6.5:
            pytest.skip(
                f'Skipped on server versions < 6.5 (using {cb_env.server_version_short}). Pending CXX updates...')
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # Create primary index
        ixm.create_primary_index(bucket_name, deferred=True)
        ixs = ixm.get_all_indexes(bucket_name)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            ixm.create_index(bucket_name,
                             'ix{0}'.format(n), ['fld{0}'.format(n)], CreateQueryIndexOptions(deferred=True))

        ixs = ixm.get_all_indexes(bucket_name)
        assert len(ixs) == 6

        ix_names = list(map(lambda i: i.name, ixs))

        ixm.build_deferred_indexes(bucket_name)
        ixm.watch_indexes(bucket_name,
                          ix_names,
                          WatchQueryIndexOptions(timeout=timedelta(seconds=30)))  # Should be OK
        ixm.watch_indexes(bucket_name,
                          ix_names,
                          WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                 watch_primary=True))  # Should be OK again
        with pytest.raises(QueryIndexNotFoundException):
            ixm.watch_indexes(bucket_name, ['idontexist'], WatchQueryIndexOptions(timeout=timedelta(seconds=10)))


class QueryIndexCollectionManagementTests:
    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):
        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_collections=True,
                                                 manage_query_indexes=True)

        cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        yield cb_env
        cb_env.try_n_times(5, 3, self._clear_all_indexes, cb_env, ignore_fail=True)
        cb_env.try_n_times_till_exception(5, 3,
                                          cb_env.teardown_named_collections,
                                          raise_if_no_exception=False)

    def _clear_all_indexes(self, cb_env, ignore_fail=False):
        # Drop all indexes!
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        indexes = ixm.get_all_indexes(bucket_name,
                                      GetAllQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                              collection_name=self.TEST_COLLECTION))
        for index in indexes:
            # @TODO:  will need to update once named primary allowed
            if index.is_primary:
                ixm.drop_primary_index(bucket_name,
                                       DropPrimaryQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                                    collection_name=self.TEST_COLLECTION))
            else:
                ixm.drop_index(bucket_name,
                               index.name,
                               DropQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                     collection_name=self.TEST_COLLECTION))
        for _ in range(10):
            indexes = ixm.get_all_indexes(bucket_name,
                                          GetAllQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                                  collection_name=self.TEST_COLLECTION))
            if 0 == len(indexes):
                return
            cb_env.sleep(2)

        if ignore_fail is True:
            return

        pytest.xfail(
            "Indexes were not dropped after {} waits of {} seconds each".format(10, 3))

    @pytest.fixture(scope="class")
    def check_query_index_mgmt_supported(self, cb_env):
        cb_env.check_if_feature_supported('query_index_mgmt')

    @pytest.fixture(scope="class", name="fqdn")
    def get_fqdn(self, cb_env):
        return f'`{cb_env.bucket.name}`.`{self.TEST_SCOPE}`.`{self.TEST_COLLECTION}`'

    @pytest.fixture()
    def clear_all_indexes(self, cb_env):
        self._clear_all_indexes(cb_env)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_primary(self, cb_env, fqdn):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.create_primary_index(bucket_name,
                                 CreatePrimaryQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                                collection_name=self.TEST_COLLECTION,
                                                                timeout=timedelta(seconds=60)))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM {fqdn} LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        ixm.drop_primary_index(bucket_name,
                               DropPrimaryQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                            collection_name=self.TEST_COLLECTION))
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_primary_ignore_if_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.create_primary_index(bucket_name,
                                 CreatePrimaryQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                                collection_name=self.TEST_COLLECTION))
        ixm.create_primary_index(bucket_name,
                                 CreatePrimaryQueryIndexOptions(ignore_if_exists=True,
                                                                scope_name=self.TEST_SCOPE,
                                                                collection_name=self.TEST_COLLECTION))

        with pytest.raises(QueryIndexAlreadyExistsException):
            ixm.create_primary_index(bucket_name,
                                     CreatePrimaryQueryIndexOptions(scope_name=self.TEST_SCOPE,
                                                                    collection_name=self.TEST_COLLECTION))

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_primary_ignore_if_exists_kwargs(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.create_primary_index(bucket_name,
                                 scope_name=self.TEST_SCOPE,
                                 collection_name=self.TEST_COLLECTION)
        ixm.create_primary_index(bucket_name,
                                 ignore_if_exists=True,
                                 scope_name=self.TEST_SCOPE,
                                 collection_name=self.TEST_COLLECTION)

        with pytest.raises(QueryIndexAlreadyExistsException):
            ixm.create_primary_index(bucket_name,
                                     scope_name=self.TEST_SCOPE,
                                     collection_name=self.TEST_COLLECTION)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_primary(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm

        # create an index so we can drop
        ixm.create_primary_index(bucket_name,
                                 timeout=timedelta(seconds=60),
                                 scope_name=self.TEST_SCOPE,
                                 collection_name=self.TEST_COLLECTION)

        ixm.drop_primary_index(bucket_name,
                               timeout=timedelta(seconds=60),
                               scope_name=self.TEST_SCOPE,
                               collection_name=self.TEST_COLLECTION)
        # this should fail now
        with pytest.raises(QueryIndexNotFoundException):
            ixm.drop_primary_index(bucket_name,
                                   scope_name=self.TEST_SCOPE,
                                   collection_name=self.TEST_COLLECTION)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_primary_ignore_if_not_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixm.drop_primary_index(bucket_name,
                               ignore_if_not_exists=True,
                               scope_name=self.TEST_SCOPE,
                               collection_name=self.TEST_COLLECTION)
        ixm.drop_primary_index(bucket_name,
                               DropPrimaryQueryIndexOptions(ignore_if_not_exists=True,
                                                            scope_name=self.TEST_SCOPE,
                                                            collection_name=self.TEST_COLLECTION))
        with pytest.raises(QueryIndexNotFoundException):
            ixm.drop_primary_index(bucket_name,
                                   scope_name=self.TEST_SCOPE,
                                   collection_name=self.TEST_COLLECTION)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_secondary_indexes(self, cb_env, fqdn):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        ixm.create_index(bucket_name, ixname,
                         fields=fields,
                         timeout=timedelta(seconds=120),
                         scope_name=self.TEST_SCOPE,
                         collection_name=self.TEST_COLLECTION)
        n1ql = "SELECT {1}, {2} FROM {0} WHERE {1}=1 AND {2}=2 LIMIT 1".format(
            fqdn, *fields)
        cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_secondary_index_default_coll(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        ixm.create_index(bucket_name, ixname,
                         fields=fields,
                         timeout=timedelta(seconds=120),
                         ignore_if_exists=True)

        def check_index():
            indexes = ixm.get_all_indexes(bucket_name,
                                          scope_name='_default')
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        cb_env.try_n_times(10, 5, check_index)
        ixm.drop_index(bucket_name,
                       ixname,
                       timeout=timedelta(seconds=120))

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_secondary_indexes_condition(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        fields = ('fld1', 'fld2')

        cb_env.try_n_times_till_exception(10, 5, ixm.drop_index, bucket_name, ixname,
                                          expected_exceptions=(QueryIndexNotFoundException,),
                                          scope_name=self.TEST_SCOPE,
                                          collection_name=self.TEST_COLLECTION)
        condition = '((`fld1` = 1) and (`fld2` = 2))'
        ixm.create_index(bucket_name, ixname, fields,
                         CreateQueryIndexOptions(timeout=timedelta(days=1),
                                                 condition=condition,
                                                 scope_name=self.TEST_SCOPE,
                                                 collection_name=self.TEST_COLLECTION))

        def check_index():
            indexes = ixm.get_all_indexes(bucket_name,
                                          scope_name=self.TEST_SCOPE,
                                          collection_name=self.TEST_COLLECTION)
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        result = cb_env.try_n_times(10, 5, check_index)
        assert result.condition == condition

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_secondary_indexes(self, cb_env, fqdn):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        ixm.create_index(bucket_name,
                         ixname,
                         fields=fields,
                         timeout=timedelta(seconds=120),
                         scope_name=self.TEST_SCOPE,
                         collection_name=self.TEST_COLLECTION)

        n1ql = "SELECT {1}, {2} FROM `{0}` WHERE {1}=1 AND {2}=2 LIMIT 1".format(
            fqdn, *fields)

        # Drop the index
        ixm.drop_index(bucket_name,
                       ixname,
                       scope_name=self.TEST_SCOPE,
                       collection_name=self.TEST_COLLECTION)
        # Issue the query again
        with pytest.raises((QueryIndexNotFoundException, ParsingFailedException)):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_index_no_fields(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # raises a TypeError b/c not providing fields means
        #   create_index() is missing a required positional param
        with pytest.raises(TypeError):
            ixm.create_index(bucket_name,
                             'noFields',
                             scope_name=self.TEST_SCOPE,
                             collection_name=self.TEST_COLLECTION)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_create_secondary_indexes_ignore_if_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        ixname = 'ix2'
        ixm.create_index(bucket_name,
                         ixname,
                         fields=['hello'],
                         scope_name=self.TEST_SCOPE,
                         collection_name=self.TEST_COLLECTION)
        ixm.create_index(bucket_name,
                         ixname, fields=['hello'],
                         ignore_if_exists=True,
                         scope_name=self.TEST_SCOPE,
                         collection_name=self.TEST_COLLECTION)
        ixm.create_index(bucket_name,
                         ixname,
                         ['hello'],
                         CreateQueryIndexOptions(ignore_if_exists=True,
                                                 scope_name=self.TEST_SCOPE,
                                                 collection_name=self.TEST_COLLECTION))
        with pytest.raises(QueryIndexAlreadyExistsException):
            ixm.create_index(bucket_name,
                             ixname,
                             fields=['hello'],
                             scope_name=self.TEST_SCOPE,
                             collection_name=self.TEST_COLLECTION)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_drop_secondary_indexes_ignore_if_not_exists(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # Create it
        ixname = 'ix2'
        ixm.create_index(bucket_name,
                         ixname,
                         ['hello'],
                         scope_name=self.TEST_SCOPE,
                         collection_name=self.TEST_COLLECTION)
        # Drop it
        ixm.drop_index(bucket_name,
                       ixname,
                       scope_name=self.TEST_SCOPE,
                       collection_name=self.TEST_COLLECTION)
        ixm.drop_index(bucket_name,
                       ixname,
                       ignore_if_not_exists=True,
                       scope_name=self.TEST_SCOPE,
                       collection_name=self.TEST_COLLECTION)
        ixm.drop_index(bucket_name,
                       ixname,
                       DropQueryIndexOptions(ignore_if_not_exists=True,
                                             scope_name=self.TEST_SCOPE,
                                             collection_name=self.TEST_COLLECTION))
        with pytest.raises(QueryIndexNotFoundException):
            ixm.drop_index(bucket_name,
                           ixname,
                           scope_name=self.TEST_SCOPE,
                           collection_name=self.TEST_COLLECTION)

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_list_indexes(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # start with no indexes
        ixs = ixm.get_all_indexes(bucket_name,
                                  scope_name=self.TEST_SCOPE,
                                  collection_name=self.TEST_COLLECTION)
        assert len(ixs) == 0

        # Create the primary index
        ixm.create_primary_index(bucket_name,
                                 scope_name=self.TEST_SCOPE,
                                 collection_name=self.TEST_COLLECTION)
        ixs = ixm.get_all_indexes(bucket_name,
                                  scope_name=self.TEST_SCOPE,
                                  collection_name=self.TEST_COLLECTION)
        assert len(ixs) == 1
        assert ixs[0].is_primary is True
        assert ixs[0].name == '#primary'
        assert ixs[0].bucket_name == bucket_name

    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_index_partition_info(self, cb_env, fqdn):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # use query to create index w/ partition, cannot do that via manager
        # ATM
        n1ql = f'CREATE INDEX idx_fld1 ON {fqdn}(fld1) PARTITION BY HASH(fld1)'
        cb_env.cluster.query(n1ql).execute()
        ixs = ixm.get_all_indexes(bucket_name,
                                  scope_name=self.TEST_SCOPE,
                                  collection_name=self.TEST_COLLECTION)
        idx = next((ix for ix in ixs if ix.name == "idx_fld1"), None)
        assert idx is not None
        assert idx.partition is not None
        assert idx.partition == 'HASH(`fld1`)'

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_watch(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # Create primary index
        ixm.create_primary_index(bucket_name,
                                 deferred=True,
                                 scope_name=self.TEST_SCOPE,
                                 collection_name=self.TEST_COLLECTION)
        ixs = ixm.get_all_indexes(bucket_name,
                                  scope_name=self.TEST_SCOPE,
                                  collection_name=self.TEST_COLLECTION)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            ixm.create_index(bucket_name,
                             'ix{0}'.format(n),
                             fields=['fld{0}'.format(n)],
                             deferred=defer,
                             scope_name=self.TEST_SCOPE,
                             collection_name=self.TEST_COLLECTION)

        ixs = ixm.get_all_indexes(bucket_name,
                                  scope_name=self.TEST_SCOPE,
                                  collection_name=self.TEST_COLLECTION)
        assert len(ixs) == 6
        # by not building deffered indexes, should timeout
        with pytest.raises(WatchQueryIndexTimeoutException):
            ixm.watch_indexes(bucket_name,
                              [i.name for i in ixs],
                              WatchQueryIndexOptions(timeout=timedelta(seconds=5),
                                                     scope_name=self.TEST_SCOPE,
                                                     collection_name=self.TEST_COLLECTION))

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures("check_query_index_mgmt_supported")
    @pytest.mark.usefixtures("clear_all_indexes")
    def test_deferred(self, cb_env):
        bucket_name = cb_env.bucket.name
        ixm = cb_env.ixm
        # Create primary index
        ixm.create_primary_index(bucket_name,
                                 deferred=True,
                                 scope_name=self.TEST_SCOPE,
                                 collection_name=self.TEST_COLLECTION)
        ixs = ixm.get_all_indexes(bucket_name,
                                  scope_name=self.TEST_SCOPE,
                                  collection_name=self.TEST_COLLECTION)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            ixm.create_index(bucket_name,
                             'ix{0}'.format(n),
                             ['fld{0}'.format(n)],
                             CreateQueryIndexOptions(deferred=True,
                                                     scope_name=self.TEST_SCOPE,
                                                     collection_name=self.TEST_COLLECTION))

        ixs = ixm.get_all_indexes(bucket_name,
                                  scope_name=self.TEST_SCOPE,
                                  collection_name=self.TEST_COLLECTION)
        assert len(ixs) == 6

        ix_names = list(map(lambda i: i.name, ixs))

        ixm.build_deferred_indexes(bucket_name,
                                   scope_name=self.TEST_SCOPE,
                                   collection_name=self.TEST_COLLECTION)
        ixm.watch_indexes(bucket_name,
                          ix_names,
                          WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                 scope_name=self.TEST_SCOPE,
                                                 collection_name=self.TEST_COLLECTION))  # Should be OK
        ixm.watch_indexes(bucket_name,
                          ix_names,
                          WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                 watch_primary=True,
                                                 scope_name=self.TEST_SCOPE,
                                                 collection_name=self.TEST_COLLECTION))  # Should be OK again
        with pytest.raises(QueryIndexNotFoundException):
            ixm.watch_indexes(bucket_name,
                              ['idontexist'],
                              WatchQueryIndexOptions(timeout=timedelta(seconds=10),
                                                     scope_name=self.TEST_SCOPE,
                                                     collection_name=self.TEST_COLLECTION))
