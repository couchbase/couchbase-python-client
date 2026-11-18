#  Copyright 2016-2026. Couchbase, Inc.
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

"""The couchbase2 query index manager: its requests, its reading of the listing, and both impls end to end."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any, cast

import grpc
import pytest

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (CouchbaseException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException,
                                  QueryIndexAlreadyExistsException,
                                  QueryIndexNotFoundException,
                                  WatchQueryIndexTimeoutException)
from couchbase.management.options import (CreatePrimaryQueryIndexOptions,
                                          CreateQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          GetAllQueryIndexOptions,
                                          WatchQueryIndexOptions)
from couchbase.management.queries import QueryIndex
from couchbase.options import ClusterOptions
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._generated.admin.query.v1 import query_pb2
from couchbase.protocol.couchbase2._management._query_index_mgmt_impl import QueryIndexMgmtImpl
from couchbase.protocol.couchbase2._management._query_index_mgmt_req_builder import QueryIndexMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._query_index_mgmt_types import query_indexes
from tests.couchbase2_gateway import (Certificates,
                                      Fail,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

BUCKET = 'travel'

Index = query_pb2.GetAllIndexesResponse.Index


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


@pytest.fixture
def builder(core: ClusterCore) -> QueryIndexMgmtRequestBuilder:
    return QueryIndexMgmtRequestBuilder(core)


class RequestUnitTests:

    def test_no_keyspace_is_the_whole_bucket(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        message = builder.build_get_all_indexes_request(BUCKET, None).message
        assert message.bucket_name == BUCKET
        assert not message.HasField('scope_name')
        assert not message.HasField('collection_name')

    def test_an_empty_scope_is_left_out(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        message = builder.build_get_all_indexes_request(BUCKET, None, scope_name='', collection_name='').message
        assert not message.HasField('scope_name')
        assert not message.HasField('collection_name')

    def test_a_named_keyspace(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        options = GetAllQueryIndexOptions(scope_name='inventory', collection_name='hotels')
        message = builder.build_get_all_indexes_request(BUCKET, None, options).message
        assert (message.scope_name, message.collection_name) == ('inventory', 'hotels')

    def test_a_collections_manager_addresses_its_collection(self, core: ClusterCore) -> None:
        builder = QueryIndexMgmtRequestBuilder(core, ('hotels', 'inventory'))
        message = builder.build_drop_index_request(BUCKET, 'ix', None).message
        assert (message.bucket_name, message.scope_name, message.collection_name) == (BUCKET, 'inventory', 'hotels')

    def test_a_collections_manager_refuses_another_keyspace(self, core: ClusterCore) -> None:
        builder = QueryIndexMgmtRequestBuilder(core, ('hotels', 'inventory'))
        with pytest.raises(InvalidArgumentException, match='scope_name cannot be set'):
            builder.build_get_all_indexes_request(BUCKET, None, scope_name='other')

    def test_a_create_carries_its_options(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        options = CreateQueryIndexOptions(deferred=True, num_replicas=1, ignore_if_exists=True)
        message = builder.build_create_index_request(BUCKET, 'ix', ['a'], None, options).message
        assert (message.name, message.deferred, message.num_replicas, message.ignore_if_exists) == ('ix', True, 1, True)

    def test_a_create_without_options_sets_none(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        message = builder.build_create_index_request(BUCKET, 'ix', ['a'], None).message
        assert [f.name for f, _ in message.ListFields()] == ['bucket_name', 'name', 'fields']

    @pytest.mark.parametrize('key, sent', [
        ('fld1', '`fld1`'),
        ('two words', '`two words`'),
        ('`fld1`', '`fld1`'),
        ('a`b', '`a\\`b`'),
        ('a\\b', '`a\\\\b`'),
        ('`a``b`', '`a``b`'),
        ('`foo`.`bar`', '`foo`.`bar`'),
        ('`a`,`b`', '`a`,`b`'),
        ('`', '`\\``'),
    ])
    def test_each_key_is_quoted_unless_already_wrapped(self,
                                                       builder: QueryIndexMgmtRequestBuilder,
                                                       key: str,
                                                       sent: str) -> None:
        assert list(builder.build_create_index_request(BUCKET, 'ix', [key], None).message.fields) == [sent]

    def test_a_condition_is_unavailable(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            builder.build_create_index_request(BUCKET, 'ix', ['a'], None, CreateQueryIndexOptions(condition='a > 1'))

    def test_an_empty_condition_is_none(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        builder.build_create_index_request(BUCKET, 'ix', ['a'], None, CreateQueryIndexOptions(condition=''))

    @pytest.mark.parametrize('build, message', [
        (lambda b: b.build_create_index_request(BUCKET, 'ix', [], None), 'Keys required'),
        (lambda b: b.build_create_index_request(BUCKET, 'ix', 'a', None), 'Index keys must be provided'),
        (lambda b: b.build_create_index_request(BUCKET, None, ['a'], None), 'index_name must be provided'),
        (lambda b: b.build_get_all_indexes_request(None, None), 'bucket_name must be provided'),
        (lambda b: b.build_create_primary_index_request(BUCKET, None, CreatePrimaryQueryIndexOptions(condition='a')),
         'cannot specify condition for primary index'),
        (lambda b: b.build_watch_indexes_request(BUCKET, ['ix'], None), 'Must specify a timeout'),
        (lambda b: b.build_watch_indexes_request(BUCKET, 'ix', None, timeout=timedelta(seconds=1)),
         'An iterable of index names'),
    ])
    def test_the_arguments_refused(self, builder: QueryIndexMgmtRequestBuilder,
                                   build: Callable[[QueryIndexMgmtRequestBuilder], object], message: str) -> None:
        with pytest.raises(InvalidArgumentException, match=message):
            build(builder)

    def test_an_unnamed_primary_is_the_servers(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        create = builder.build_create_primary_index_request(BUCKET, None, index_name='').message
        drop = builder.build_drop_primary_index_request(BUCKET, None).message
        assert not create.HasField('name') and not drop.HasField('name')

    def test_a_named_primary(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        create = builder.build_create_primary_index_request(BUCKET, None, index_name='p', deferred=False).message
        drop = builder.build_drop_primary_index_request(BUCKET, None, index_name='p').message
        assert (create.name, create.HasField('deferred'), drop.name) == ('p', True, 'p')

    def test_ignore_if_not_exists_is_ignore_if_missing(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        message = builder.build_drop_index_request(BUCKET, 'ix', None, DropQueryIndexOptions(ignore_if_not_exists=True))
        assert message.message.ignore_if_missing

    def test_a_watch(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        req = builder.build_watch_indexes_request(BUCKET,
                                                  (n for n in ['ix']),
                                                  None,
                                                  WatchQueryIndexOptions(timeout=timedelta(seconds=2),
                                                                         watch_primary=True))
        assert [(wait.bucket_name, wait.name) for wait in req.waits] == [(BUCKET, 'ix'), (BUCKET, '#primary')]
        assert req.timeout == 2.0

    def test_the_timeout_defaults_to_seventy_five_seconds(self, builder: QueryIndexMgmtRequestBuilder) -> None:
        assert builder.build_build_deferred_indexes_request(BUCKET, None).timeout == 75.0


class QueryIndexesUnitTests:

    def _read(self, index: Index) -> QueryIndex:
        [read] = query_indexes(query_pb2.GetAllIndexesResponse(indexes=[index]))
        return read

    def test_an_index_on_a_collection(self) -> None:
        index = self._read(Index(bucket_name=BUCKET, scope_name='inventory', collection_name='hotels', name='ix',
                                 type=query_pb2.INDEX_TYPE_GSI, state=query_pb2.INDEX_STATE_BUILDING,
                                 fields=['`a`'], condition='', partition='HASH(`a`)'))
        assert (index.name, index.is_primary, index.type, index.state, index.index_key) == (
            'ix', False, 'gsi', 'building', ['`a`'])
        assert (index.bucket_name, index.scope_name, index.collection_name, index.keyspace) == (
            BUCKET, 'inventory', 'hotels', 'hotels')
        assert (index.condition, index.partition, index.namespace, index.datastore_id) == ('', 'HASH(`a`)', None, None)

    def test_an_index_on_the_bucket(self) -> None:
        index = self._read(Index(bucket_name=BUCKET, name='#primary', is_primary=True))
        assert (index.scope_name, index.collection_name, index.keyspace) == (None, None, BUCKET)
        assert (index.type, index.state, index.condition, index.partition) == ('view', 'deferred', None, None)

    def test_a_state_the_sdk_does_not_know(self) -> None:
        assert self._read(Index(name='ix', state=cast(query_pb2.IndexState, 99))).state == 'unknown'


@pytest.fixture(scope='module')
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Certificates:
    made = make_certificates(tmp_path_factory.mktemp('pki'))
    if made is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    return made


@pytest.fixture
def gateway() -> FakeGateway:
    return new_gateway()


@pytest.fixture
def connect(gateway: FakeGateway, certificates: Certificates) -> Iterator[Callable[[], tuple[str, ClusterOptions]]]:
    with serving(gateway, certificates) as port:
        yield lambda: (f'couchbase2://localhost:{port}',
                       ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                      trust_store_path=str(certificates.ca)))


@pytest.fixture
def cluster(connect: Callable[[], tuple[str, ClusterOptions]]) -> Iterator[ClusterImpl]:
    impl = ClusterImpl(*connect())
    try:
        yield impl
    finally:
        impl.close_connection()


@pytest.fixture
def manager(cluster: ClusterImpl) -> QueryIndexMgmtImpl:
    return cluster.query_index_mgmt()


def _names(indexes: Any) -> list[str]:
    return sorted(index.name for index in indexes)


class QueryIndexMgmtImplUnitTests:

    def test_created_indexes_are_listed(self, manager: QueryIndexMgmtImpl) -> None:
        manager.create_primary_index(BUCKET)
        manager.create_index(BUCKET, 'ix', ['a', 'b'])
        indexes = {index.name: index for index in manager.get_all_indexes(BUCKET)}
        assert set(indexes) == {'#primary', 'ix'}
        assert indexes['#primary'].is_primary
        assert (indexes['ix'].index_key, indexes['ix'].state) == (['`a`', '`b`'], 'online')

    def test_drops(self, manager: QueryIndexMgmtImpl) -> None:
        manager.create_primary_index(BUCKET, index_name='p')
        manager.create_index(BUCKET, 'ix', ['a'])
        manager.drop_primary_index(BUCKET, index_name='p')
        manager.drop_index(BUCKET, 'ix')
        assert list(manager.get_all_indexes(BUCKET)) == []

    @pytest.mark.parametrize('call, exception', [
        (lambda m: m.create_index(BUCKET, 'ix', ['a']), QueryIndexAlreadyExistsException),
        (lambda m: m.drop_index(BUCKET, 'missing'), QueryIndexNotFoundException),
        (lambda m: m.drop_primary_index(BUCKET), QueryIndexNotFoundException),
    ])
    def test_the_gateways_errors(self, manager: QueryIndexMgmtImpl,
                                 call: Callable[[QueryIndexMgmtImpl], object], exception: type[Exception]) -> None:
        manager.create_index(BUCKET, 'ix', ['a'])
        with pytest.raises(exception):
            call(manager)

    def test_ignoring_the_errors(self, manager: QueryIndexMgmtImpl) -> None:
        manager.create_index(BUCKET, 'ix', ['a'])
        manager.create_index(BUCKET, 'ix', ['a'], ignore_if_exists=True)
        manager.drop_index(BUCKET, 'missing', ignore_if_not_exists=True)

    def test_a_collections_manager_sees_only_its_collection(self, cluster: ClusterImpl,
                                                            manager: QueryIndexMgmtImpl) -> None:
        manager.create_index(BUCKET, 'on_default', ['a'])
        collection = cluster.bucket(BUCKET).scope('inventory').collection('hotels').query_index_mgmt()
        collection.create_index(BUCKET, 'on_hotels', ['a'])
        assert _names(collection.get_all_indexes(BUCKET)) == ['on_hotels']
        assert _names(manager.get_all_indexes(BUCKET)) == ['on_default', 'on_hotels']

    def test_a_watch_waits_for_the_deferred_indexes_to_be_built(self, manager: QueryIndexMgmtImpl) -> None:
        manager.create_index(BUCKET, 'ix', ['a'], deferred=True)
        with pytest.raises(WatchQueryIndexTimeoutException):
            manager.watch_indexes(BUCKET, ['ix'], timeout=timedelta(milliseconds=300))
        manager.build_deferred_indexes(BUCKET)
        manager.watch_indexes(BUCKET, ['ix'], timeout=timedelta(seconds=5))
        assert [index.state for index in manager.get_all_indexes(BUCKET)] == ['online']

    def test_a_watched_index_that_does_not_exist(self, manager: QueryIndexMgmtImpl) -> None:
        with pytest.raises(QueryIndexNotFoundException):
            manager.watch_indexes(BUCKET, ['missing'], timeout=timedelta(seconds=5))

    def test_no_wait_outlives_the_watch(self, manager: QueryIndexMgmtImpl, gateway: FakeGateway) -> None:
        manager.create_index(BUCKET, 'ix', ['a'], deferred=True)
        watch_deadline = time.monotonic() + 0.5
        with pytest.raises(WatchQueryIndexTimeoutException):
            manager.watch_indexes(BUCKET, ['ix'], timeout=timedelta(milliseconds=500))
        waits = [call for call in gateway.calls if call.method == 'WaitForIndexOnline']
        assert len(waits) > 1
        assert all(call.deadline <= watch_deadline + 0.05 for call in waits)

    def test_a_watch_waits_on_each_index_in_turn(self, manager: QueryIndexMgmtImpl, gateway: FakeGateway) -> None:
        manager.create_primary_index(BUCKET)
        manager.create_index(BUCKET, 'ix', ['a'])
        manager.watch_indexes(BUCKET, ['ix'], timeout=timedelta(seconds=5), watch_primary=True)
        waited = [call.request.name for call in gateway.calls if call.method == 'WaitForIndexOnline']
        assert waited == ['ix', '#primary']
        assert not [call for call in gateway.calls if call.method == 'GetAllIndexes']

    def test_another_refused_wait_is_not_retried(self, manager: QueryIndexMgmtImpl, gateway: FakeGateway) -> None:
        manager.create_index(BUCKET, 'ix', ['a'])
        gateway.failures['WaitForIndexOnline'] = [Fail(grpc.StatusCode.FAILED_PRECONDITION, violation='OTHER')]
        with pytest.raises(CouchbaseException):
            manager.watch_indexes(BUCKET, ['ix'], timeout=timedelta(seconds=5))


class AsyncQueryIndexMgmtImplUnitTests:

    def test_a_create_a_watch_and_a_listing(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> list[QueryIndex]:
            impl = AsyncClusterImpl(*connect())
            try:
                manager = impl.query_index_mgmt()
                await manager.create_index(BUCKET, 'ix', ['a'], deferred=True)
                await manager.build_deferred_indexes(BUCKET)
                await manager.watch_indexes(BUCKET, ['ix'], timeout=timedelta(seconds=5))
                return await manager.get_all_indexes(BUCKET)
            finally:
                await impl.close_connection()

        [index] = run_on_own_loop(run())
        assert (index.name, index.state) == ('ix', 'online')

    def test_a_gateway_error(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                await impl.query_index_mgmt().drop_index(BUCKET, 'missing')
            finally:
                await impl.close_connection()

        with pytest.raises(QueryIndexNotFoundException):
            run_on_own_loop(run())
