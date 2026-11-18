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

from collections.abc import Callable, Iterator
from typing import Any

import pytest
from twisted.internet.defer import inlineCallbacks

from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException)
from couchbase.options import ClusterOptions
from tests.couchbase2_gateway import (Certificates,
                                      FakeGateway,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.environments.twisted_reactor import run_in_reactor_thread
from txcouchbase.protocol.couchbase2._cluster_impl import TxClusterImpl
from txcouchbase.protocol.couchbase2._collection_impl import TxCollectionImpl


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

    @pytest.mark.parametrize('call', [lambda c: c.ping_deferred(),
                                      lambda c: c.analytics_query_deferred('SELECT 1'),
                                      lambda c: c.bucket('b').ping_deferred(),
                                      lambda c: c.bucket('b').scope('s').analytics_query_deferred('SELECT 1'),
                                      lambda c: _collection(c).lookup_in_any_replica_deferred('k', ())])
    def test_every_other_operation_is_unavailable(self, cluster: TxClusterImpl,
                                                  call: Callable[[TxClusterImpl], object]) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            call(cluster)
