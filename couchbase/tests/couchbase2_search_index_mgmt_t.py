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

"""The couchbase2 search index manager: its requests, its reading of the definitions, and both impls end to end."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (FeatureUnavailableException,
                                  InvalidArgumentException,
                                  ParsingFailedException,
                                  QueryIndexAlreadyExistsException,
                                  SearchIndexNotFoundException)
from couchbase.management.search import SearchIndex
from couchbase.options import ClusterOptions
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2
from couchbase.protocol.couchbase2._management._search_index_mgmt_impl import SearchIndexMgmtImpl
from couchbase.protocol.couchbase2._management._search_index_mgmt_req_builder import SearchIndexMgmtRequestBuilder
from couchbase.protocol.couchbase2._management._search_index_mgmt_types import (analysis,
                                                                                found_index,
                                                                                search_index)
from tests.couchbase2_gateway import (Certificates,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

BUCKET = 'travel'
SCOPE = 'inventory'

PARAMS = {'mapping': {'default_mapping': {'enabled': True}}, 'store': {'indexType': 'scorch'}}


def _index(**fields: Any) -> SearchIndex:
    return SearchIndex(name='ix', source_name=BUCKET, **fields)


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


@pytest.fixture
def builder(core: ClusterCore) -> SearchIndexMgmtRequestBuilder:
    return SearchIndexMgmtRequestBuilder(core)


class RequestUnitTests:

    def test_the_clusters_manager_names_no_scope(self, builder: SearchIndexMgmtRequestBuilder) -> None:
        message = builder.build_get_all_indexes_request(None).message
        assert not message.HasField('bucket_name')
        assert not message.HasField('scope_name')

    def test_a_scopes_manager_addresses_its_scope(self, core: ClusterCore) -> None:
        builder = SearchIndexMgmtRequestBuilder(core, (BUCKET, SCOPE))
        drop = builder.build_drop_index_request('ix', None).message
        update = builder.build_upsert_index_request(_index(uuid='u1'), None).message
        assert (drop.bucket_name, drop.scope_name, drop.name) == (BUCKET, SCOPE, 'ix')
        assert (update.bucket_name, update.scope_name) == (BUCKET, SCOPE)

    def test_an_index_without_a_uuid_is_created(self, builder: SearchIndexMgmtRequestBuilder) -> None:
        message = builder.build_upsert_index_request(_index(params=PARAMS, plan_params={'numReplicas': 1}),
                                                     None).message
        assert isinstance(message, search_pb2.CreateIndexRequest)
        assert (message.name, message.type, message.source_type, message.source_name) == (
            'ix', 'fulltext-index', 'couchbase', BUCKET)
        assert {k: json.loads(v) for k, v in message.params.items()} == PARAMS
        assert dict(message.plan_params) == {'numReplicas': b'1'}
        assert not message.HasField('source_uuid') and not message.source_params

    def test_an_index_with_a_uuid_is_updated(self, builder: SearchIndexMgmtRequestBuilder) -> None:
        message = builder.build_upsert_index_request(_index(uuid='u1', source_uuid='s1'), None).message
        assert isinstance(message, search_pb2.UpdateIndexRequest)
        assert (message.index.name, message.index.uuid, message.index.source_uuid) == ('ix', 'u1', 's1')

    def test_a_document_is_sent_as_json(self, builder: SearchIndexMgmtRequestBuilder) -> None:
        message = builder.build_analyze_document_request('ix', {'field': 'text'}, None).message
        assert (message.name, json.loads(message.doc)) == ('ix', {'field': 'text'})

    @pytest.mark.parametrize('build, message', [
        (lambda b: b.build_get_index_request('', None), 'Expected non-empty index_name'),
        (lambda b: b.build_get_index_request(None, None), 'index_name must be provided'),
        (lambda b: b.build_upsert_index_request({'name': 'ix'}, None), 'Expected index to be a SearchIndex'),
        (lambda b: b.build_upsert_index_request(SearchIndex(name=''), None), 'Index must have name'),
        (lambda b: b.build_analyze_document_request('ix', {}, None), 'non-empty document'),
        (lambda b: b.build_analyze_document_request('ix', {'f': object()}, None), 'Cannot convert doc'),
    ])
    def test_the_arguments_refused(self, builder: SearchIndexMgmtRequestBuilder,
                                   build: Callable[[SearchIndexMgmtRequestBuilder], object], message: str) -> None:
        with pytest.raises(InvalidArgumentException, match=message):
            build(builder)

    def test_the_timeout_defaults_to_seventy_five_seconds(self, builder: SearchIndexMgmtRequestBuilder) -> None:
        assert builder.build_pause_ingest_request('ix', None).timeout == 75.0


class SearchIndexUnitTests:

    def test_a_definition_is_read_member_by_member(self) -> None:
        index = search_index(search_pb2.Index(name='ix', type='fulltext-index', source_type='gocbcore',
                                              source_name=BUCKET, uuid='u1',
                                              params={k: json.dumps(v).encode() for k, v in PARAMS.items()}))
        assert (index.name, index.idx_type, index.source_type, index.source_name, index.uuid) == (
            'ix', 'fulltext-index', 'gocbcore', BUCKET, 'u1')
        assert (index.params, index.plan_params, index.source_params, index.source_uuid) == (PARAMS, {}, {}, None)

    def test_a_member_that_is_not_json_fails_the_whole_definition(self) -> None:
        with pytest.raises(ParsingFailedException, match="'store'"):
            search_index(search_pb2.Index(name='ix', params={'mapping': b'{}', 'store': b'not json'}))

    def test_a_get_that_found_nothing(self) -> None:
        with pytest.raises(SearchIndexNotFoundException, match="'ix'"):
            found_index('ix', search_pb2.GetIndexResponse())

    def test_the_analysis_status_is_the_servers_or_ok(self) -> None:
        said = analysis(search_pb2.AnalyzeDocumentResponse(status='partial', analyzed=b'[]'))
        silent = analysis(search_pb2.AnalyzeDocumentResponse(analyzed=b'{"a": 1}'))
        assert (said, silent) == ({'analysis': [], 'status': 'partial'}, {'analysis': {'a': 1}, 'status': 'ok'})


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
def manager(cluster: ClusterImpl) -> SearchIndexMgmtImpl:
    return cluster.search_index_mgmt()


class SearchIndexMgmtImplUnitTests:

    def test_an_index_round_trips(self, manager: SearchIndexMgmtImpl) -> None:
        manager.upsert_index(_index(params=PARAMS))
        found = manager.get_index('ix')
        assert (found.name, found.source_name, found.params) == ('ix', BUCKET, PARAMS)
        assert found.uuid
        assert [index.name for index in manager.get_all_indexes()] == ['ix']

    def test_an_upsert_of_what_get_returned_is_an_update(self, manager: SearchIndexMgmtImpl,
                                                         gateway: FakeGateway) -> None:
        manager.upsert_index(_index())
        found = manager.get_index('ix')
        found.params = PARAMS
        manager.upsert_index(found)
        assert [call.method for call in gateway.calls if call.method.endswith('Index')] == [
            'CreateIndex', 'GetIndex', 'UpdateIndex']
        assert manager.get_index('ix').params == PARAMS

    @pytest.mark.parametrize('call, exception', [
        (lambda m: m.upsert_index(_index()), QueryIndexAlreadyExistsException),
        (lambda m: m.get_index('missing'), SearchIndexNotFoundException),
        (lambda m: m.drop_index('missing'), SearchIndexNotFoundException),
        (lambda m: m.pause_ingest('missing'), SearchIndexNotFoundException),
    ])
    def test_the_gateways_errors(self, manager: SearchIndexMgmtImpl,
                                 call: Callable[[SearchIndexMgmtImpl], object], exception: type[Exception]) -> None:
        manager.upsert_index(_index())
        with pytest.raises(exception):
            call(manager)

    def test_a_drop(self, manager: SearchIndexMgmtImpl) -> None:
        manager.upsert_index(_index())
        manager.drop_index('ix')
        assert list(manager.get_all_indexes()) == []

    def test_a_scopes_manager_sees_only_its_scope(self, cluster: ClusterImpl, manager: SearchIndexMgmtImpl) -> None:
        manager.upsert_index(SearchIndex(name='on_cluster', source_name=BUCKET))
        scoped = cluster.bucket(BUCKET).scope(SCOPE).search_index_mgmt()
        scoped.upsert_index(SearchIndex(name='on_scope', source_name=BUCKET))
        assert [index.name for index in scoped.get_all_indexes()] == ['on_scope']
        assert [index.name for index in manager.get_all_indexes()] == ['on_cluster']

    def test_the_controls(self, manager: SearchIndexMgmtImpl, gateway: FakeGateway) -> None:
        manager.upsert_index(_index())
        manager.pause_ingest('ix')
        manager.disallow_querying('ix')
        manager.freeze_plan('ix')
        assert gateway.search_admin.controls[('', '', 'ix')] == {'ingest': False, 'querying': False, 'plan': False}
        manager.resume_ingest('ix')
        manager.allow_querying('ix')
        manager.unfreeze_plan('ix')
        assert gateway.search_admin.controls[('', '', 'ix')] == {'ingest': True, 'querying': True, 'plan': True}

    def test_the_count_and_the_analysis(self, manager: SearchIndexMgmtImpl) -> None:
        manager.upsert_index(_index())
        assert manager.get_indexed_documents_count('ix') == 7
        assert manager.analyze_document('ix', {'field': 'text'}) == {'analysis': [{'doc': {'field': 'text'}}],
                                                                     'status': 'ok'}

    def test_a_definition_the_sdk_cannot_read_fails_the_listing(self, manager: SearchIndexMgmtImpl,
                                                                gateway: FakeGateway) -> None:
        manager.upsert_index(_index())
        gateway.search_admin.indexes[('', '', 'other')] = search_pb2.Index(name='other', params={'store': b'{'})
        with pytest.raises(ParsingFailedException):
            manager.get_all_indexes()

    @pytest.mark.parametrize('call', [
        lambda m: m.get_index_stats('ix'),
        lambda m: m.get_all_index_stats(),
    ])
    def test_there_are_no_stats(self, manager: SearchIndexMgmtImpl, call: Callable[[SearchIndexMgmtImpl], object]
                                ) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            call(manager)


class AsyncSearchIndexMgmtImplUnitTests:

    def test_an_upsert_and_a_get(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> SearchIndex:
            impl = AsyncClusterImpl(*connect())
            try:
                manager = impl.bucket(BUCKET).scope(SCOPE).search_index_mgmt()
                await manager.upsert_index(_index(params=PARAMS))
                await manager.pause_ingest('ix')
                return await manager.get_index('ix')
            finally:
                await impl.close_connection()

        found = run_on_own_loop(run())
        assert (found.name, found.params) == ('ix', PARAMS)

    def test_a_gateway_error(self, connect: Callable[[], tuple[str, ClusterOptions]]) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                await impl.search_index_mgmt().drop_index('missing')
            finally:
                await impl.close_connection()

        with pytest.raises(SearchIndexNotFoundException):
            run_on_own_loop(run())
