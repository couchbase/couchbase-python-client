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

"""The couchbase2 Twisted impls, end to end against an in-process gateway over TLS, on the reactor's thread."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any

import grpc
import pytest
from twisted.internet.defer import Deferred, inlineCallbacks

import couchbase.search as search
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException,
                                  QueryIndexNotFoundException)
from couchbase.options import ClusterOptions
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2
from couchbase.protocol.couchbase2._generated.search.v1 import search_pb2
from couchbase.result import QueryResult, SearchResult
from tests.couchbase2_gateway import (Certificates,
                                      Fail,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.environments.twisted_reactor import run_in_reactor_thread
from txcouchbase.protocol.couchbase2._cluster_impl import TxClusterImpl
from txcouchbase.protocol.couchbase2._collection_impl import TxCollectionImpl

MetaData = query_pb2.QueryResponse.MetaData
SearchResponse = search_pb2.SearchQueryResponse


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
def cluster(gateway: FakeGateway, certificates: Certificates) -> Iterator[TxClusterImpl]:
    with serving(gateway, certificates) as port:
        def construct() -> TxClusterImpl:
            return TxClusterImpl(f'couchbase2://localhost:{port}',
                                 ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                                trust_store_path=str(certificates.ca)))

        impl: TxClusterImpl = run_in_reactor_thread(construct)
        try:
            yield impl
        finally:
            run_in_reactor_thread(impl.close_connection_deferred)


def _collection(cluster: TxClusterImpl) -> TxCollectionImpl:
    return cluster.bucket('travel').scope('inventory').collection('hotels')


def _rows(*values: Any, meta: MetaData | None = None) -> query_pb2.QueryResponse:
    message = query_pb2.QueryResponse(rows=[json.dumps(v).encode() for v in values])
    if meta is not None:
        message.meta_data.CopyFrom(meta)
    return message


def _in_reactor(fn: Callable[[], Any]) -> Any:
    return run_in_reactor_thread(inlineCallbacks(fn))


class Couchbase2TwistedUnitTests:

    def test_an_upsert_reads_back(self, cluster: TxClusterImpl, gateway: FakeGateway) -> None:
        def run() -> Any:
            collection = _collection(cluster)
            mutation = yield collection.upsert_deferred('hotel_1', {'name': 'Ritz'})
            result = yield collection.get_deferred('hotel_1')
            return mutation.cas, result.cas, result.content_as[dict]

        upserted, read, content = _in_reactor(run)
        assert upserted == read and content == {'name': 'Ritz'}
        assert [call.method for call in gateway.calls] == ['Upsert', 'Get']

    def test_a_missing_document_fails_the_deferred(self, cluster: TxClusterImpl) -> None:
        with pytest.raises(DocumentNotFoundException):
            run_in_reactor_thread(lambda: _collection(cluster).get_deferred('hotel_1'))

    def test_an_invalid_argument_raises_at_the_call(self, cluster: TxClusterImpl, gateway: FakeGateway) -> None:
        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(lambda: _collection(cluster).get_deferred('hotel_1', timeout=5))
        assert gateway.calls == []

    def test_the_query_fires_once_its_rows_are_read(self, cluster: TxClusterImpl, gateway: FakeGateway) -> None:
        gateway.query_messages = [_rows(1, 2), _rows(3, meta=MetaData(request_id='r-1'))]

        def query() -> Deferred[QueryResult]:
            return cluster.bucket('travel').scope('inventory').query_deferred('SELECT 1')

        result: QueryResult = run_in_reactor_thread(query)
        assert result.execute() == [1, 2, 3]
        metadata = result.metadata()
        assert metadata is not None and metadata.request_id() == 'r-1'
        assert gateway.calls[0].request.scope_name == 'inventory'

    def test_a_query_error_fails_the_deferred(self, cluster: TxClusterImpl, gateway: FakeGateway) -> None:
        gateway.query_end = Fail(grpc.StatusCode.INVALID_ARGUMENT)
        with pytest.raises(InvalidArgumentException):
            run_in_reactor_thread(lambda: cluster.query_deferred('SELEC 1'))

    def test_a_search_fires_once_its_rows_are_read(self, cluster: TxClusterImpl, gateway: FakeGateway) -> None:
        gateway.search_messages = [SearchResponse(hits=[SearchResponse.SearchQueryRow(id='a'),
                                                        SearchResponse.SearchQueryRow(id='b')]),
                                   SearchResponse(meta_data=SearchResponse.MetaData(
                                       metrics=SearchResponse.SearchMetrics(total_rows=2)))]

        def run() -> Deferred[SearchResult]:
            return cluster.bucket('travel').scope('inventory').search_deferred(
                'ix', search.SearchRequest(search.MatchAllQuery()))

        result: SearchResult = run_in_reactor_thread(run)
        assert [row.id for row in result.rows()] == ['a', 'b']
        metadata = result.metadata()
        metrics = None if metadata is None else metadata.metrics()
        assert metrics is not None and metrics.total_rows() == 2
        assert gateway.calls[0].request.index_name == 'travel.inventory.ix'

    def test_a_search_on_a_missing_index_fails_the_deferred(self, cluster: TxClusterImpl,
                                                            gateway: FakeGateway) -> None:
        gateway.failures['SearchQuery'] = [Fail(grpc.StatusCode.NOT_FOUND, 'searchindex')]
        with pytest.raises(QueryIndexNotFoundException):
            run_in_reactor_thread(lambda: cluster.search_query_deferred('missing', search.MatchAllQuery()))

    def test_a_search_argument_is_refused_at_the_call(self, cluster: TxClusterImpl, gateway: FakeGateway) -> None:
        with pytest.raises(FeatureUnavailableException):
            run_in_reactor_thread(lambda: cluster.search_query_deferred('ix', search.RawQuery({'term': 'a'})))
        assert gateway.calls == []

    @pytest.mark.parametrize('call', [lambda c: c.ping_deferred(),
                                      lambda c: c.analytics_query_deferred('SELECT 1'),
                                      lambda c: c.bucket('b').ping_deferred(),
                                      lambda c: c.bucket('b').scope('s').analytics_query_deferred('SELECT 1'),
                                      lambda c: _collection(c).lookup_in_any_replica_deferred('k', ())])
    def test_every_other_operation_is_unavailable(self, cluster: TxClusterImpl,
                                                  call: Callable[[TxClusterImpl], object]) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            call(cluster)
