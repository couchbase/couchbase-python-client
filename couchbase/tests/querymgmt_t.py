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
                                          WatchQueryIndexOptions)
from tests.environments import CollectionType
from tests.environments.query_index_mgmt_environment import QueryIndexManagementTestEnvironment
from tests.environments.test_environment import TestEnvironment


class CollectionQueryCIndexManagementTestSuite:
    TEST_MANIFEST = [
        'test_create_index_no_fields',
        'test_create_named_primary',
        'test_create_primary',
        'test_create_primary_ignore_if_exists',
        'test_create_primary_ignore_if_exists_kwargs',
        'test_create_secondary_index_default_coll',
        'test_create_secondary_indexes',
        'test_create_secondary_indexes_condition',
        'test_create_secondary_indexes_ignore_if_exists',
        'test_deferred',
        'test_drop_primary',
        'test_drop_primary_ignore_if_not_exists',
        'test_drop_secondary_indexes',
        'test_drop_secondary_indexes_ignore_if_not_exists',
        'test_index_partition_info',
        'test_list_indexes',
        'test_watch',
    ]

    @pytest.fixture()
    def clear_all_indexes(self, cb_env):
        cb_env.clear_all_indexes()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_index_no_fields(self, cb_env):
        # raises a TypeError b/c not providing fields means
        #   create_index() is missing a required positional param
        with pytest.raises(TypeError):
            cb_env.qixm.create_index('noFields')

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_named_primary(self, cb_env):
        ixname = 'namedPrimary'
        cb_env.qixm.create_primary_index(CreatePrimaryQueryIndexOptions(index_name=ixname,
                                                                        timeout=timedelta(seconds=60)))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM {cb_env.get_fqdn()} LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        cb_env.qixm.drop_primary_index(DropPrimaryQueryIndexOptions(index_name=ixname))
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary(self, cb_env):
        cb_env.qixm.create_primary_index(CreatePrimaryQueryIndexOptions(timeout=timedelta(seconds=60)))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM {cb_env.get_fqdn()} LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        cb_env.qixm.drop_primary_index()
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary_ignore_if_exists(self, cb_env):
        cb_env.qixm.create_primary_index()
        cb_env.qixm.create_primary_index(CreatePrimaryQueryIndexOptions(ignore_if_exists=True))

        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_primary_index()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary_ignore_if_exists_kwargs(self, cb_env):
        cb_env.qixm.create_primary_index()
        cb_env.qixm.create_primary_index(ignore_if_exists=True)

        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_primary_index()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_index_default_coll(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(ixname,
                                 fields=fields,
                                 timeout=timedelta(seconds=120),
                                 ignore_if_exists=True)

        def check_index():
            indexes = cb_env.qixm.get_all_indexes()
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        TestEnvironment.try_n_times(10, 5, check_index)
        cb_env.qixm.drop_index(ixname, timeout=timedelta(seconds=120))

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(ixname,
                                 fields=fields,
                                 timeout=timedelta(seconds=120))
        n1ql = "SELECT {1}, {2} FROM {0} WHERE {1}=1 AND {2}=2 LIMIT 1".format(cb_env.get_fqdn(), *fields)
        cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes_condition(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')

        TestEnvironment.try_n_times_till_exception(10,
                                                   5,
                                                   cb_env.qixm.drop_index,
                                                   ixname,
                                                   expected_exceptions=(QueryIndexNotFoundException,))
        condition = '((`fld1` = 1) and (`fld2` = 2))'
        cb_env.qixm.create_index(ixname,
                                 fields,
                                 CreateQueryIndexOptions(timeout=timedelta(days=1),
                                                         condition=condition))

        def check_index():
            indexes = cb_env.qixm.get_all_indexes()
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        result = TestEnvironment.try_n_times(10, 5, check_index)
        assert result.condition == condition

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes_ignore_if_exists(self, cb_env):
        ixname = 'ix2'
        cb_env.qixm.create_index(ixname, fields=['hello'])
        cb_env.qixm.create_index(ixname, fields=['hello'], ignore_if_exists=True)
        cb_env.qixm.create_index(ixname, ['hello'], CreateQueryIndexOptions(ignore_if_exists=True))
        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_index(ixname, fields=['hello'])

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures('clear_all_indexes')
    def test_deferred(self, cb_env):
        # Create primary index
        cb_env.qixm.create_primary_index(deferred=True)
        ixs = cb_env.qixm.get_all_indexes()
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            cb_env.qixm.create_index(f'ix{n}', [f'fld{n}'], CreateQueryIndexOptions(deferred=True))

        ixs = cb_env.qixm.get_all_indexes()
        assert len(ixs) == 6

        ix_names = list(map(lambda i: i.name, ixs))

        cb_env.qixm.build_deferred_indexes()
        cb_env.qixm.watch_indexes(ix_names, WatchQueryIndexOptions(timeout=timedelta(seconds=30)))  # Should be OK
        cb_env.qixm.watch_indexes(ix_names,
                                  WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                         watch_primary=True))  # Should be OK again
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.watch_indexes(['idontexist'], WatchQueryIndexOptions(timeout=timedelta(seconds=10)))

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_primary(self, cb_env):
        # create an index so we can drop
        cb_env.qixm.create_primary_index(timeout=timedelta(seconds=60))

        cb_env.qixm.drop_primary_index(timeout=timedelta(seconds=60))
        # this should fail now
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_primary_index()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_primary_ignore_if_not_exists(self, cb_env):
        cb_env.qixm.drop_primary_index(ignore_if_not_exists=True)
        cb_env.qixm.drop_primary_index(DropPrimaryQueryIndexOptions(ignore_if_not_exists=True))
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_primary_index()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_secondary_indexes(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(ixname, fields=fields, timeout=timedelta(seconds=120))

        n1ql = "SELECT {1}, {2} FROM `{0}` WHERE {1}=1 AND {2}=2 LIMIT 1".format(cb_env.get_fqdn(), *fields)

        # Drop the index
        cb_env.qixm.drop_index(ixname)
        # Issue the query again
        with pytest.raises((QueryIndexNotFoundException, ParsingFailedException)):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_secondary_indexes_ignore_if_not_exists(self, cb_env):
        # Create it
        ixname = 'ix2'
        cb_env.qixm.create_index(ixname, ['hello'])
        # Drop it
        cb_env.qixm.drop_index(ixname)
        cb_env.qixm.drop_index(ixname, ignore_if_not_exists=True)
        cb_env.qixm.drop_index(ixname, DropQueryIndexOptions(ignore_if_not_exists=True))
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_index(ixname)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_index_partition_info(self, cb_env):
        # use query to create index w/ partition, cannot do that via manager ATM
        n1ql = f'CREATE INDEX idx_fld1 ON {cb_env.get_fqdn()}(fld1) PARTITION BY HASH(fld1)'
        cb_env.cluster.query(n1ql).execute()
        ixs = cb_env.qixm.get_all_indexes()
        idx = next((ix for ix in ixs if ix.name == "idx_fld1"), None)
        assert idx is not None
        assert idx.partition is not None
        assert idx.partition == 'HASH(`fld1`)'

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_list_indexes(self, cb_env):
        # start with no indexes
        ixs = cb_env.qixm.get_all_indexes()
        assert len(ixs) == 0

        # Create the primary index
        cb_env.qixm.create_primary_index()
        ixs = cb_env.qixm.get_all_indexes()
        assert len(ixs) == 1
        assert ixs[0].is_primary is True
        assert ixs[0].name == '#primary'
        assert ixs[0].bucket_name == cb_env.bucket.name

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures('clear_all_indexes')
    def test_watch(self, cb_env):
        # Create primary index
        cb_env.qixm.create_primary_index(deferred=True)
        ixs = cb_env.qixm.get_all_indexes()
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            cb_env.qixm.create_index(f'ix{n}', fields=[f'fld{n}'], deferred=defer)

        ixs = cb_env.qixm.get_all_indexes()
        assert len(ixs) == 6
        # by not building deffered indexes, should timeout
        with pytest.raises(WatchQueryIndexTimeoutException):
            cb_env.qixm.watch_indexes([i.name for i in ixs],
                                      WatchQueryIndexOptions(timeout=timedelta(seconds=5)))


class QueryIndexManagementCollectionTestSuite:
    TEST_MANIFEST = [
        'test_create_index_no_fields',
        'test_create_named_primary',
        'test_create_primary',
        'test_create_primary_ignore_if_exists',
        'test_create_primary_ignore_if_exists_kwargs',
        'test_create_secondary_index_default_coll',
        'test_create_secondary_indexes',
        'test_create_secondary_indexes_condition',
        'test_create_secondary_indexes_ignore_if_exists',
        'test_deferred',
        'test_drop_primary',
        'test_drop_primary_ignore_if_not_exists',
        'test_drop_secondary_indexes',
        'test_drop_secondary_indexes_ignore_if_not_exists',
        'test_get_all_correct_collection',
        'test_index_partition_info',
        'test_list_indexes',
        'test_watch',
    ]

    @pytest.fixture()
    def clear_all_indexes(self, cb_env):
        cb_env.clear_all_indexes(CollectionType.NAMED)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_index_no_fields(self, cb_env):
        # raises a TypeError b/c not providing fields means
        #   create_index() is missing a required positional param
        with pytest.raises(TypeError):
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     'noFields',
                                     scope_name=cb_env.TEST_SCOPE,
                                     collection_name=cb_env.TEST_COLLECTION)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_named_primary(self, cb_env):
        ixname = 'namedPrimary'
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         CreatePrimaryQueryIndexOptions(index_name=ixname,
                                                                        scope_name=cb_env.TEST_SCOPE,
                                                                        collection_name=cb_env.TEST_COLLECTION,
                                                                        timeout=timedelta(seconds=60)))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM {cb_env.get_fqdn()} LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        cb_env.qixm.drop_primary_index(cb_env.bucket.name,
                                       DropPrimaryQueryIndexOptions(index_name=ixname,
                                                                    scope_name=cb_env.TEST_SCOPE,
                                                                    collection_name=cb_env.TEST_COLLECTION))
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary(self, cb_env):
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         CreatePrimaryQueryIndexOptions(scope_name=cb_env.TEST_SCOPE,
                                                                        collection_name=cb_env.TEST_COLLECTION,
                                                                        timeout=timedelta(seconds=60)))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM {cb_env.get_fqdn()} LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        cb_env.qixm.drop_primary_index(cb_env.bucket.name,
                                       DropPrimaryQueryIndexOptions(scope_name=cb_env.TEST_SCOPE,
                                                                    collection_name=cb_env.TEST_COLLECTION))
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary_ignore_if_exists(self, cb_env):
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         CreatePrimaryQueryIndexOptions(scope_name=cb_env.TEST_SCOPE,
                                                                        collection_name=cb_env.TEST_COLLECTION))
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         CreatePrimaryQueryIndexOptions(ignore_if_exists=True,
                                                                        scope_name=cb_env.TEST_SCOPE,
                                                                        collection_name=cb_env.TEST_COLLECTION))

        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                             CreatePrimaryQueryIndexOptions(scope_name=cb_env.TEST_SCOPE,
                                                                            collection_name=cb_env.TEST_COLLECTION))

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary_ignore_if_exists_kwargs(self, cb_env):
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         scope_name=cb_env.TEST_SCOPE,
                                         collection_name=cb_env.TEST_COLLECTION)
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         ignore_if_exists=True,
                                         scope_name=cb_env.TEST_SCOPE,
                                         collection_name=cb_env.TEST_COLLECTION)

        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                             scope_name=cb_env.TEST_SCOPE,
                                             collection_name=cb_env.TEST_COLLECTION)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_index_default_coll(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 fields=fields,
                                 timeout=timedelta(seconds=120),
                                 ignore_if_exists=True)

        def check_index():
            indexes = cb_env.qixm.get_all_indexes(cb_env.bucket.name, scope_name='_default')
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        TestEnvironment.try_n_times(10, 5, check_index)
        cb_env.qixm.drop_index(cb_env.bucket.name, ixname, timeout=timedelta(seconds=120))

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 fields=fields,
                                 timeout=timedelta(seconds=120),
                                 scope_name=cb_env.TEST_SCOPE,
                                 collection_name=cb_env.TEST_COLLECTION)
        n1ql = "SELECT {1}, {2} FROM {0} WHERE {1}=1 AND {2}=2 LIMIT 1".format(cb_env.get_fqdn(), *fields)
        cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes_condition(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')

        TestEnvironment.try_n_times_till_exception(10,
                                                   5,
                                                   cb_env.qixm.drop_index,
                                                   cb_env.bucket.name,
                                                   ixname,
                                                   expected_exceptions=(QueryIndexNotFoundException,),
                                                   scope_name=cb_env.TEST_SCOPE,
                                                   collection_name=cb_env.TEST_COLLECTION)
        condition = '((`fld1` = 1) and (`fld2` = 2))'
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 fields,
                                 CreateQueryIndexOptions(timeout=timedelta(days=1),
                                                         condition=condition,
                                                         scope_name=cb_env.TEST_SCOPE,
                                                         collection_name=cb_env.TEST_COLLECTION))

        def check_index():
            indexes = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                                  scope_name=cb_env.TEST_SCOPE,
                                                  collection_name=cb_env.TEST_COLLECTION)
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        result = TestEnvironment.try_n_times(10, 5, check_index)
        assert result.condition == condition

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes_ignore_if_exists(self, cb_env):
        ixname = 'ix2'
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 fields=['hello'],
                                 scope_name=cb_env.TEST_SCOPE,
                                 collection_name=cb_env.TEST_COLLECTION)
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 fields=['hello'],
                                 ignore_if_exists=True,
                                 scope_name=cb_env.TEST_SCOPE,
                                 collection_name=cb_env.TEST_COLLECTION)
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 ['hello'],
                                 CreateQueryIndexOptions(ignore_if_exists=True,
                                                         scope_name=cb_env.TEST_SCOPE,
                                                         collection_name=cb_env.TEST_COLLECTION))
        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     ixname,
                                     fields=['hello'],
                                     scope_name=cb_env.TEST_SCOPE,
                                     collection_name=cb_env.TEST_COLLECTION)

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures('clear_all_indexes')
    def test_deferred(self, cb_env):
        # Create primary index
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         deferred=True,
                                         scope_name=cb_env.TEST_SCOPE,
                                         collection_name=cb_env.TEST_COLLECTION)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                          scope_name=cb_env.TEST_SCOPE,
                                          collection_name=cb_env.TEST_COLLECTION)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     'ix{0}'.format(n),
                                     ['fld{0}'.format(n)],
                                     CreateQueryIndexOptions(deferred=True,
                                                             scope_name=cb_env.TEST_SCOPE,
                                                             collection_name=cb_env.TEST_COLLECTION))

        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                          scope_name=cb_env.TEST_SCOPE,
                                          collection_name=cb_env.TEST_COLLECTION)
        assert len(ixs) == 6

        ix_names = list(map(lambda i: i.name, ixs))

        cb_env.qixm.build_deferred_indexes(cb_env.bucket.name,
                                           scope_name=cb_env.TEST_SCOPE,
                                           collection_name=cb_env.TEST_COLLECTION)
        cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                  ix_names,
                                  WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                         scope_name=cb_env.TEST_SCOPE,
                                                         collection_name=cb_env.TEST_COLLECTION))  # Should be OK
        cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                  ix_names,
                                  WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                         watch_primary=True,
                                                         scope_name=cb_env.TEST_SCOPE,
                                                         collection_name=cb_env.TEST_COLLECTION))  # Should be OK again
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                      ['idontexist'],
                                      WatchQueryIndexOptions(timeout=timedelta(seconds=10),
                                                             scope_name=cb_env.TEST_SCOPE,
                                                             collection_name=cb_env.TEST_COLLECTION))

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_primary(self, cb_env):
        # create an index so we can drop
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         timeout=timedelta(seconds=60),
                                         scope_name=cb_env.TEST_SCOPE,
                                         collection_name=cb_env.TEST_COLLECTION)

        cb_env.qixm.drop_primary_index(cb_env.bucket.name,
                                       timeout=timedelta(seconds=60),
                                       scope_name=cb_env.TEST_SCOPE,
                                       collection_name=cb_env.TEST_COLLECTION)
        # this should fail now
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_primary_index(cb_env.bucket.name,
                                           scope_name=cb_env.TEST_SCOPE,
                                           collection_name=cb_env.TEST_COLLECTION)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_primary_ignore_if_not_exists(self, cb_env):
        cb_env.qixm.drop_primary_index(cb_env.bucket.name,
                                       ignore_if_not_exists=True,
                                       scope_name=cb_env.TEST_SCOPE,
                                       collection_name=cb_env.TEST_COLLECTION)
        cb_env.qixm.drop_primary_index(cb_env.bucket.name,
                                       DropPrimaryQueryIndexOptions(ignore_if_not_exists=True,
                                                                    scope_name=cb_env.TEST_SCOPE,
                                                                    collection_name=cb_env.TEST_COLLECTION))
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_primary_index(cb_env.bucket.name,
                                           scope_name=cb_env.TEST_SCOPE,
                                           collection_name=cb_env.TEST_COLLECTION)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_secondary_indexes(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 fields=fields,
                                 timeout=timedelta(seconds=120),
                                 scope_name=cb_env.TEST_SCOPE,
                                 collection_name=cb_env.TEST_COLLECTION)

        n1ql = "SELECT {1}, {2} FROM `{0}` WHERE {1}=1 AND {2}=2 LIMIT 1".format(cb_env.get_fqdn(), *fields)

        # Drop the index
        cb_env.qixm.drop_index(cb_env.bucket.name,
                               ixname,
                               scope_name=cb_env.TEST_SCOPE,
                               collection_name=cb_env.TEST_COLLECTION)
        # Issue the query again
        with pytest.raises((QueryIndexNotFoundException, ParsingFailedException)):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_secondary_indexes_ignore_if_not_exists(self, cb_env):
        # Create it
        ixname = 'ix2'
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 ['hello'],
                                 scope_name=cb_env.TEST_SCOPE,
                                 collection_name=cb_env.TEST_COLLECTION)
        # Drop it
        cb_env.qixm.drop_index(cb_env.bucket.name,
                               ixname,
                               scope_name=cb_env.TEST_SCOPE,
                               collection_name=cb_env.TEST_COLLECTION)
        cb_env.qixm.drop_index(cb_env.bucket.name,
                               ixname,
                               ignore_if_not_exists=True,
                               scope_name=cb_env.TEST_SCOPE,
                               collection_name=cb_env.TEST_COLLECTION)
        cb_env.qixm.drop_index(cb_env.bucket.name,
                               ixname,
                               DropQueryIndexOptions(ignore_if_not_exists=True,
                                                     scope_name=cb_env.TEST_SCOPE,
                                                     collection_name=cb_env.TEST_COLLECTION))
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_index(cb_env.bucket.name,
                                   ixname,
                                   scope_name=cb_env.TEST_SCOPE,
                                   collection_name=cb_env.TEST_COLLECTION)

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures('clear_all_indexes')
    def test_get_all_correct_collection(self, cb_env):
        # create some indexes in the _default scope & collection
        for i in range(2):
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     f'ix{i}',
                                     [f'fld{i}'],
                                     CreateQueryIndexOptions(deferred=True))

        # create some indexes in the test scope & collection
        for i in range(2):
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     f'ix{i}',
                                     [f'fld{i}'],
                                     CreateQueryIndexOptions(deferred=True,
                                                             scope_name=cb_env.TEST_SCOPE,
                                                             collection_name=cb_env.TEST_COLLECTION))

        # all indexes in bucket (i.e. in test and _default scopes/collections)
        all_ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)

        # _should_ be only indexes in test scope & collection
        collection_ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                                     scope_name=cb_env.TEST_SCOPE,
                                                     collection_name=cb_env.TEST_COLLECTION)

        assert len(all_ixs) != len(collection_ixs)
        assert len(all_ixs) == 4
        assert len(collection_ixs) == 2

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_index_partition_info(self, cb_env):
        # use query to create index w/ partition, cannot do that via manager ATM
        n1ql = f'CREATE INDEX idx_fld1 ON {cb_env.get_fqdn()}(fld1) PARTITION BY HASH(fld1)'
        cb_env.cluster.query(n1ql).execute()
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                          scope_name=cb_env.TEST_SCOPE,
                                          collection_name=cb_env.TEST_COLLECTION)
        idx = next((ix for ix in ixs if ix.name == "idx_fld1"), None)
        assert idx is not None
        assert idx.partition is not None
        assert idx.partition == 'HASH(`fld1`)'

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_list_indexes(self, cb_env):
        # start with no indexes
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                          scope_name=cb_env.TEST_SCOPE,
                                          collection_name=cb_env.TEST_COLLECTION)
        assert len(ixs) == 0

        # Create the primary index
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         scope_name=cb_env.TEST_SCOPE,
                                         collection_name=cb_env.TEST_COLLECTION)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                          scope_name=cb_env.TEST_SCOPE,
                                          collection_name=cb_env.TEST_COLLECTION)
        assert len(ixs) == 1
        assert ixs[0].is_primary is True
        assert ixs[0].name == '#primary'
        assert ixs[0].bucket_name == cb_env.bucket.name

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures('clear_all_indexes')
    def test_watch(self, cb_env):
        # Create primary index
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         deferred=True,
                                         scope_name=cb_env.TEST_SCOPE,
                                         collection_name=cb_env.TEST_COLLECTION)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                          scope_name=cb_env.TEST_SCOPE,
                                          collection_name=cb_env.TEST_COLLECTION)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     'ix{0}'.format(n),
                                     fields=['fld{0}'.format(n)],
                                     deferred=defer,
                                     scope_name=cb_env.TEST_SCOPE,
                                     collection_name=cb_env.TEST_COLLECTION)

        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name,
                                          scope_name=cb_env.TEST_SCOPE,
                                          collection_name=cb_env.TEST_COLLECTION)
        assert len(ixs) == 6
        # by not building deffered indexes, should timeout
        with pytest.raises(WatchQueryIndexTimeoutException):
            cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                      [i.name for i in ixs],
                                      WatchQueryIndexOptions(timeout=timedelta(seconds=5),
                                                             scope_name=cb_env.TEST_SCOPE,
                                                             collection_name=cb_env.TEST_COLLECTION))


class QueryIndexManagementTestSuite:
    TEST_MANIFEST = [
        'test_create_index_no_fields',
        'test_create_named_primary',
        'test_create_primary',
        'test_create_primary_ignore_if_exists',
        'test_create_primary_ignore_if_exists_kwargs',
        'test_create_secondary_indexes',
        'test_create_secondary_indexes_condition',
        'test_create_secondary_indexes_ignore_if_exists',
        'test_deferred',
        'test_drop_primary',
        'test_drop_primary_ignore_if_not_exists',
        'test_drop_secondary_indexes',
        'test_drop_secondary_indexes_ignore_if_not_exists',
        'test_index_partition_info',
        'test_list_indexes',
        'test_watch',
    ]

    @pytest.fixture()
    def clear_all_indexes(self, cb_env):
        cb_env.clear_all_indexes()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_index_no_fields(self, cb_env):
        # raises a TypeError b/c not providing fields means
        #   create_index() is missing a required positional param
        with pytest.raises(TypeError):
            cb_env.qixm.create_index(cb_env.bucket.name, 'noFields')

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_named_primary(self, cb_env):
        ixname = 'namedPrimary'
        # Try to create a _named_ primary index
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         CreatePrimaryQueryIndexOptions(index_name=ixname,
                                                                        timeout=timedelta(seconds=60)))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM `{cb_env.bucket.name}` LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        cb_env.qixm.drop_primary_index(cb_env.bucket.name, DropPrimaryQueryIndexOptions(index_name=ixname))
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary(self, cb_env):
        cb_env.qixm.create_primary_index(cb_env.bucket.name, timeout=timedelta(seconds=60))

        # Ensure we can issue a query
        n1ql = f'SELECT * FROM `{cb_env.bucket.name}` LIMIT 1'

        cb_env.cluster.query(n1ql).execute()
        # Drop the primary index
        cb_env.qixm.drop_primary_index(cb_env.bucket.name)
        # Ensure we get an error when executing the query
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary_ignore_if_exists(self, cb_env):
        cb_env.qixm.create_primary_index(cb_env.bucket.name)
        cb_env.qixm.create_primary_index(cb_env.bucket.name,
                                         CreatePrimaryQueryIndexOptions(ignore_if_exists=True))

        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_primary_index(cb_env.bucket.name)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_primary_ignore_if_exists_kwargs(self, cb_env):
        cb_env.qixm.create_primary_index(cb_env.bucket.name)
        cb_env.qixm.create_primary_index(cb_env.bucket.name, ignore_if_exists=True)

        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_primary_index(cb_env.bucket.name)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(cb_env.bucket.name, ixname, fields=fields, timeout=timedelta(seconds=120))
        n1ql = "SELECT {1}, {2} FROM `{0}` WHERE {1}=1 AND {2}=2 LIMIT 1".format(cb_env.bucket.name, *fields)
        cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes_condition(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')

        TestEnvironment.try_n_times_till_exception(10,
                                                   5,
                                                   cb_env.qixm.drop_index,
                                                   cb_env.bucket.name,
                                                   ixname,
                                                   expected_exceptions=(QueryIndexNotFoundException,))
        condition = '((`fld1` = 1) and (`fld2` = 2))'
        cb_env.qixm.create_index(cb_env.bucket.name,
                                 ixname,
                                 fields,
                                 CreateQueryIndexOptions(timeout=timedelta(days=1), condition=condition))

        def check_index():
            indexes = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
            result = next((idx for idx in indexes if idx.name == ixname), None)
            assert result is not None
            return result
        result = TestEnvironment.try_n_times(10, 5, check_index)
        assert result.condition == condition

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_create_secondary_indexes_ignore_if_exists(self, cb_env):
        ixname = 'ix2'
        cb_env.qixm.create_index(cb_env.bucket.name, ixname, fields=['hello'])
        cb_env.qixm.create_index(cb_env.bucket.name, ixname, fields=['hello'], ignore_if_exists=True)
        cb_env.qixm.create_index(cb_env.bucket.name, ixname, ['hello'], CreateQueryIndexOptions(ignore_if_exists=True))
        with pytest.raises(QueryIndexAlreadyExistsException):
            cb_env.qixm.create_index(cb_env.bucket.name, ixname, fields=['hello'])

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures('clear_all_indexes')
    def test_deferred(self, cb_env):
        # Create primary index
        cb_env.qixm.create_primary_index(cb_env.bucket.name, deferred=True)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     'ix{0}'.format(n),
                                     ['fld{0}'.format(n)],
                                     CreateQueryIndexOptions(deferred=True))

        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        assert len(ixs) == 6

        ix_names = list(map(lambda i: i.name, ixs))

        cb_env.qixm.build_deferred_indexes(cb_env.bucket.name)
        cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                  ix_names,
                                  WatchQueryIndexOptions(timeout=timedelta(seconds=30)))  # Should be OK
        cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                  ix_names,
                                  WatchQueryIndexOptions(timeout=timedelta(seconds=30),
                                                         watch_primary=True))  # Should be OK again
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                      ['idontexist'],
                                      WatchQueryIndexOptions(timeout=timedelta(seconds=10)))

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_primary(self, cb_env):
        # create an index so we can drop
        cb_env.qixm.create_primary_index(cb_env.bucket.name, timeout=timedelta(seconds=60))

        cb_env.qixm.drop_primary_index(cb_env.bucket.name, timeout=timedelta(seconds=60))
        # this should fail now
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_primary_index(cb_env.bucket.name)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_primary_ignore_if_not_exists(self, cb_env):
        cb_env.qixm.drop_primary_index(cb_env.bucket.name, ignore_if_not_exists=True)
        cb_env.qixm.drop_primary_index(cb_env.bucket.name, DropPrimaryQueryIndexOptions(ignore_if_not_exists=True))
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_primary_index(cb_env.bucket.name)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_secondary_indexes(self, cb_env):
        ixname = 'ix2'
        fields = ('fld1', 'fld2')
        cb_env.qixm.create_index(cb_env.bucket.name, ixname, fields=fields, timeout=timedelta(seconds=120))

        n1ql = "SELECT {1}, {2} FROM `{0}` WHERE {1}=1 AND {2}=2 LIMIT 1".format(cb_env.bucket.name, *fields)

        # Drop the index
        cb_env.qixm.drop_index(cb_env.bucket.name, ixname)
        # Issue the query again
        with pytest.raises((QueryIndexNotFoundException, ParsingFailedException)):
            cb_env.cluster.query(n1ql).execute()

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_drop_secondary_indexes_ignore_if_not_exists(self, cb_env):
        # Create it
        ixname = 'ix2'
        cb_env.qixm.create_index(cb_env.bucket.name, ixname, ['hello'])
        # Drop it
        cb_env.qixm.drop_index(cb_env.bucket.name, ixname)
        cb_env.qixm.drop_index(cb_env.bucket.name, ixname, ignore_if_not_exists=True)
        cb_env.qixm.drop_index(cb_env.bucket.name, ixname, DropQueryIndexOptions(ignore_if_not_exists=True))
        with pytest.raises(QueryIndexNotFoundException):
            cb_env.qixm.drop_index(cb_env.bucket.name, ixname)

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_index_partition_info(self, cb_env):
        # use query to create index w/ partition, cannot do that via manager ATM
        n1ql = 'CREATE INDEX idx_fld1 ON `{0}`(fld1) PARTITION BY HASH(fld1)'.format(cb_env.bucket.name)
        cb_env.cluster.query(n1ql).execute()
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        idx = next((ix for ix in ixs if ix.name == "idx_fld1"), None)
        assert idx is not None
        assert idx.partition is not None
        assert idx.partition == 'HASH(`fld1`)'

    @pytest.mark.usefixtures('clear_all_indexes')
    def test_list_indexes(self, cb_env):
        # start with no indexes
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        assert len(ixs) == 0

        # Create the primary index
        cb_env.qixm.create_primary_index(cb_env.bucket.name)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        assert len(ixs) == 1
        assert ixs[0].is_primary is True
        assert ixs[0].name == '#primary'
        assert ixs[0].bucket_name == cb_env.bucket.name

    @pytest.mark.flaky(reruns=5, reruns_delay=2)
    @pytest.mark.usefixtures('clear_all_indexes')
    def test_watch(self, cb_env):
        # Create primary index
        cb_env.qixm.create_primary_index(cb_env.bucket.name, deferred=True)
        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        assert len(ixs) == 1
        assert ixs[0].state == 'deferred'

        # Create a bunch of other indexes
        for n in range(5):
            defer = False
            if n % 2 == 0:
                defer = True
            cb_env.qixm.create_index(cb_env.bucket.name,
                                     'ix{0}'.format(n),
                                     fields=['fld{0}'.format(n)],
                                     deferred=defer)

        ixs = cb_env.qixm.get_all_indexes(cb_env.bucket.name)
        assert len(ixs) == 6
        # by not building deffered indexes, should timeout
        with pytest.raises(WatchQueryIndexTimeoutException):
            cb_env.qixm.watch_indexes(cb_env.bucket.name,
                                      [i.name for i in ixs],
                                      WatchQueryIndexOptions(timeout=timedelta(seconds=5)))


class ClassicQueryIndexManagementCollectionTests(QueryIndexManagementCollectionTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicQueryIndexManagementCollectionTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicQueryIndexManagementCollectionTests) if valid_test_method(meth)]
        test_list = set(QueryIndexManagementCollectionTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra test(s): {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, request):
        cb_env = QueryIndexManagementTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)


class ClassicQueryIndexManagementTests(QueryIndexManagementTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicQueryIndexManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicQueryIndexManagementTests) if valid_test_method(meth)]
        test_list = set(QueryIndexManagementTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra test(s): {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, request):
        cb_env = QueryIndexManagementTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param)
        yield cb_env
        cb_env.teardown(request.param)


class ClassicCollectionQueryIndexManagementTests(CollectionQueryCIndexManagementTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicCollectionQueryIndexManagementTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicCollectionQueryIndexManagementTests) if valid_test_method(meth)]
        test_list = set(CollectionQueryCIndexManagementTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra test(s): {test_list}.')

    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT, CollectionType.NAMED])
    def couchbase_test_environment(self, cb_base_env, request):
        cb_env = QueryIndexManagementTestEnvironment.from_environment(cb_base_env)
        cb_env.setup(request.param, test_suite=self.__class__.__name__)
        yield cb_env
        cb_env.teardown(request.param)
