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

"""The couchbase2 collection manager: its requests, its reading of the listing, and both impls end to end."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import pytest

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.exceptions import (BucketNotFoundException,
                                  CollectionAlreadyExistsException,
                                  CollectionNotFoundException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException)
from couchbase.management.collections import (CollectionSpec,
                                              CreateCollectionSettings,
                                              ScopeSpec,
                                              UpdateCollectionSettings)
from couchbase.management.options import CreateCollectionOptions
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._generated.admin.collection.v1 import collection_pb2
from couchbase.protocol.couchbase2._management._collection_mgmt_impl import CollectionMgmtImpl
from couchbase.protocol.couchbase2._management._collection_mgmt_req_builder import CollectionMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._collection_mgmt_types import history_unavailable, scope_specs
from tests.couchbase2_gateway import (CLOCK_TICK,
                                      Certificates,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

BUCKET = 'travel'

ListedCollection = collection_pb2.ListCollectionsResponse.Collection


def _fields(scope: ScopeSpec) -> list[tuple[str, str, timedelta | None, bool | None]]:
    return [(c.name, c.scope_name, c.max_expiry, c.history) for c in scope.collections]


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


@pytest.fixture
def builder(core: ClusterCore) -> CollectionMgmtRequestBuilder:
    return CollectionMgmtRequestBuilder(core, BUCKET)


class RequestUnitTests:

    def test_a_scope_is_named_in_its_bucket(self, builder: CollectionMgmtRequestBuilder) -> None:
        create = builder.build_create_scope_request('inventory', None).message
        drop = builder.build_drop_scope_request('inventory', None).message
        assert (create.bucket_name, create.scope_name) == (drop.bucket_name, drop.scope_name) == (BUCKET, 'inventory')

    def test_a_collection_is_named_in_its_scope(self, builder: CollectionMgmtRequestBuilder) -> None:
        create = builder.build_create_collection_request('inventory', 'hotels', obs_handler=None).message
        drop = builder.build_drop_collection_request('inventory', 'hotels', obs_handler=None).message
        for message in (create, drop):
            assert (message.bucket_name, message.scope_name, message.collection_name) == (
                BUCKET, 'inventory', 'hotels')

    def test_a_collection_spec_is_still_read_and_warned_of(self, builder: CollectionMgmtRequestBuilder) -> None:
        spec = CollectionSpec('hotels', 'inventory', max_expiry=timedelta(seconds=30))
        with pytest.warns(CouchbaseDeprecationWarning):
            message = builder.build_create_collection_request(spec, obs_handler=None).message
        assert (message.scope_name, message.collection_name, message.max_expiry_secs) == ('inventory', 'hotels', 30)

    @pytest.mark.parametrize('scope_name', [None, '', ' ', 7])
    def test_a_scope_name_that_is_not_a_name_is_refused(self, builder: CollectionMgmtRequestBuilder,
                                                        scope_name: Any) -> None:
        with pytest.raises(InvalidArgumentException, match='scope_name'):
            builder.build_create_scope_request(scope_name, None)

    def test_no_settings_leave_both_fields_out(self, builder: CollectionMgmtRequestBuilder) -> None:
        message = builder.build_create_collection_request('inventory', 'hotels', obs_handler=None).message
        assert not message.HasField('max_expiry_secs')
        assert not message.HasField('history_retention_enabled')

    @pytest.mark.parametrize('seconds, wire', [(0, None), (-1, 0), (1, 1), (2**31 - 1, 2**31 - 1)])
    def test_a_created_collections_max_expiry(self, builder: CollectionMgmtRequestBuilder,
                                              seconds: int, wire: int | None) -> None:
        settings = CreateCollectionSettings(max_expiry=timedelta(seconds=seconds))
        message = builder.build_create_collection_request('inventory', 'hotels', settings, obs_handler=None).message
        assert (message.max_expiry_secs if message.HasField('max_expiry_secs') else None) == wire

    @pytest.mark.parametrize('seconds', [-2, 2**31])
    def test_a_max_expiry_out_of_range_is_refused(self, builder: CollectionMgmtRequestBuilder, seconds: int) -> None:
        settings = CreateCollectionSettings(max_expiry=timedelta(seconds=seconds))
        with pytest.raises(InvalidArgumentException, match='maximum expiry'):
            builder.build_create_collection_request('inventory', 'hotels', settings, obs_handler=None)

    def test_history_is_carried(self, builder: CollectionMgmtRequestBuilder) -> None:
        settings = CreateCollectionSettings(history=False)
        message = builder.build_create_collection_request('inventory', 'hotels', settings, obs_handler=None).message
        assert message.HasField('history_retention_enabled') and not message.history_retention_enabled

    @pytest.mark.parametrize('seconds, wire', [(None, None), (-1, 0), (30, 30)])
    def test_an_updated_collections_max_expiry(self, builder: CollectionMgmtRequestBuilder,
                                               seconds: int | None, wire: int | None) -> None:
        max_expiry = None if seconds is None else timedelta(seconds=seconds)
        settings = UpdateCollectionSettings(max_expiry=max_expiry, history=True)
        message = builder.build_update_collection_request('inventory', 'hotels', settings, None).message
        assert (message.max_expiry_secs if message.HasField('max_expiry_secs') else None) == wire
        assert message.history_retention_enabled

    def test_updating_to_the_buckets_max_expiry_is_unavailable(self, builder: CollectionMgmtRequestBuilder) -> None:
        settings = UpdateCollectionSettings(max_expiry=timedelta(0))
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            builder.build_update_collection_request('inventory', 'hotels', settings, None)

    def test_the_timeout_defaults_to_seventy_five_seconds(self, builder: CollectionMgmtRequestBuilder) -> None:
        assert builder.build_get_all_scopes_request(None).timeout == 75.0

    def test_an_operations_timeout_wins(self, builder: CollectionMgmtRequestBuilder) -> None:
        options = CreateCollectionOptions(timeout=timedelta(seconds=5))
        assert builder.build_create_collection_request('inventory', 'hotels', None, options,
                                                       obs_handler=None).timeout == 5.0

    def test_the_clusters_management_timeout(self) -> None:
        core = ClusterCore('couchbase2://localhost',
                           ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                          timeout_options=ClusterTimeoutOptions(
                                              management_timeout=timedelta(seconds=9))))
        try:
            assert CollectionMgmtRequestBuilder(core, BUCKET).build_get_all_scopes_request(None).timeout == 9.0
        finally:
            core.close()


class ScopeSpecsUnitTests:

    def _read(self, collection: ListedCollection) -> CollectionSpec:
        response = collection_pb2.ListCollectionsResponse(
            scopes=[collection_pb2.ListCollectionsResponse.Scope(name='inventory', collections=[collection])])
        [scope] = scope_specs(response)
        assert scope.name == 'inventory'
        [spec] = scope.collections
        assert (spec.name, spec.scope_name) == (collection.name, 'inventory')
        return spec

    def test_no_max_expiry_is_the_buckets(self) -> None:
        assert self._read(ListedCollection(name='hotels')).max_expiry == timedelta(0)

    def test_a_zero_max_expiry_is_never_to_expire(self) -> None:
        assert self._read(ListedCollection(name='hotels', max_expiry_secs=0)).max_expiry == timedelta(seconds=-1)

    def test_a_max_expiry(self) -> None:
        assert self._read(ListedCollection(name='hotels', max_expiry_secs=30)).max_expiry == timedelta(seconds=30)

    def test_history_is_none_when_not_reported(self) -> None:
        assert self._read(ListedCollection(name='hotels')).history is None
        assert self._read(ListedCollection(name='hotels', history_retention_enabled=True)).history is True


class HistoryUnavailableUnitTests:

    def test_history_on_a_bucket_keeping_none_is_unavailable(self) -> None:
        with pytest.raises(FeatureUnavailableException, match='not allowed'):
            with history_unavailable():
                raise InvalidArgumentException(
                    message='invalid argument: HistoryEnabled - not allowed on this type of bucket')

    def test_any_other_invalid_argument_is_left_alone(self) -> None:
        with pytest.raises(InvalidArgumentException, match='scope_name'):
            with history_unavailable():
                raise InvalidArgumentException(message='scope_name is invalid')


@pytest.fixture(scope='module')
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Certificates:
    made = make_certificates(tmp_path_factory.mktemp('pki'))
    if made is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    return made


@pytest.fixture
def gateway() -> FakeGateway:
    gateway = new_gateway()
    gateway.manifest[BUCKET] = {'_default': {'_default': ListedCollection(name='_default')}}
    return gateway


@pytest.fixture
def connect(gateway: FakeGateway, certificates: Certificates) -> Iterator[Callable[[], tuple[str, ClusterOptions]]]:
    with serving(gateway, certificates) as port:
        yield lambda: (f'couchbase2://localhost:{port}',
                       ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                      trust_store_path=str(certificates.ca)))


@pytest.fixture
def manager(connect: Callable[[], tuple[str, ClusterOptions]]) -> Iterator[CollectionMgmtImpl]:
    impl = ClusterImpl(*connect())
    try:
        yield impl.bucket(BUCKET).collection_mgmt()
    finally:
        impl.close_connection()


class CollectionMgmtImplUnitTests:

    def test_a_created_collection_is_listed(self, manager: CollectionMgmtImpl) -> None:
        manager.create_scope('inventory')
        manager.create_collection('inventory', 'hotels', CreateCollectionSettings(max_expiry=timedelta(seconds=30)))
        scopes = {scope.name: scope for scope in manager.get_all_scopes()}
        assert set(scopes) == {'_default', 'inventory'}
        assert _fields(scopes['inventory']) == [('hotels', 'inventory', timedelta(seconds=30), None)]

    def test_an_update_and_a_drop(self, manager: CollectionMgmtImpl, gateway: FakeGateway) -> None:
        manager.create_scope('inventory')
        manager.create_collection('inventory', 'hotels')
        manager.update_collection('inventory', 'hotels', UpdateCollectionSettings(max_expiry=timedelta(seconds=-1)))
        assert gateway.manifest[BUCKET]['inventory']['hotels'].max_expiry_secs == 0
        manager.drop_collection('inventory', 'hotels')
        manager.drop_scope('inventory')
        assert [scope.name for scope in manager.get_all_scopes()] == ['_default']

    def test_the_timeout_defaults_to_seventy_five_seconds(self, manager: CollectionMgmtImpl,
                                                          gateway: FakeGateway) -> None:
        manager.get_all_scopes()
        assert 74.0 < gateway.calls[-1].remaining <= 75.0 + CLOCK_TICK

    @pytest.mark.parametrize('call, exception', [
        (lambda m: m.create_scope('_default'), ScopeAlreadyExistsException),
        (lambda m: m.drop_scope('missing'), ScopeNotFoundException),
        (lambda m: m.create_collection('_default', '_default'), CollectionAlreadyExistsException),
        (lambda m: m.drop_collection('_default', 'missing'), CollectionNotFoundException),
    ])
    def test_the_gateways_errors(self, manager: CollectionMgmtImpl,
                                 call: Callable[[CollectionMgmtImpl], object], exception: type[Exception]) -> None:
        with pytest.raises(exception):
            call(manager)

    def test_a_missing_bucket(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        impl = ClusterImpl(*connect())
        try:
            with pytest.raises(BucketNotFoundException):
                impl.bucket('missing').collection_mgmt().get_all_scopes()
        finally:
            impl.close_connection()


class AsyncCollectionMgmtImplUnitTests:

    def test_a_created_collection_is_listed(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> list[ScopeSpec]:
            impl = AsyncClusterImpl(*connect())
            try:
                manager = impl.bucket(BUCKET).collection_mgmt()
                await manager.create_scope('inventory')
                await manager.create_collection('inventory', 'hotels')
                return await manager.get_all_scopes()
            finally:
                await impl.close_connection()

        scopes = {scope.name: scope for scope in run_on_own_loop(run())}
        assert _fields(scopes['inventory']) == [('hotels', 'inventory', timedelta(0), None)]

    def test_a_gateway_error(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                await impl.bucket(BUCKET).collection_mgmt().drop_scope('missing')
            finally:
                await impl.close_connection()

        with pytest.raises(ScopeNotFoundException):
            run_on_own_loop(run())
