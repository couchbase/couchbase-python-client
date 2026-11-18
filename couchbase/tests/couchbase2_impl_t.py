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

"""The couchbase2 sync and asyncio impls, end to end against an in-process gateway over TLS."""

from __future__ import annotations

import json
import logging
import warnings
from collections.abc import (Callable,
                             Coroutine,
                             Iterator)
from datetime import datetime, timedelta
from typing import Any

import grpc
import pytest

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from acouchbase.protocol.couchbase2._collection_impl import AsyncCollectionImpl
from acouchbase.protocol.couchbase2._streaming import buffered_rows
from couchbase import PYCBC_VERSION
from couchbase.auth import (CertificateAuthenticator,
                            JwtAuthenticator,
                            PasswordAuthenticator)
from couchbase.common.warnings import (CouchbaseApplicabilityWarning,
                                       CouchbaseDeprecationWarning,
                                       CouchbaseOptionWarning)
from couchbase.diagnostics import ClusterState, ServiceType
from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import (CouchbaseException,
                                  DocumentNotFoundException,
                                  FeatureUnavailableException,
                                  InvalidArgumentException,
                                  UnAmbiguousTimeoutException)
from couchbase.n1ql import QueryStatus
from couchbase.options import (ClusterOptions,
                               ClusterOrphanReportingOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               GetMultiOptions,
                               GetOptions,
                               QueryOptions,
                               TransactionConfig,
                               UpsertMultiOptions,
                               UpsertOptions,
                               WaitUntilReadyOptions)
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._collection_impl import CollectionImpl
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2
from couchbase.protocol.couchbase2._health import HealthCheckResponse
from couchbase.protocol.couchbase2._histogram import PercentileHistogram
from couchbase.protocol.couchbase2._streaming import QueryRows
from couchbase.result import QueryResult
from couchbase.transcoder import JSONTranscoder
from tests.couchbase2_gateway import (Certificates,
                                      Document,
                                      Fail,
                                      FakeGateway,
                                      dropping,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

KEY = ('travel', 'inventory', 'hotels', 'hotel_1')
JSON_FLAGS = JSONTranscoder().encode_value({})[1]
AUTHORIZATION = 'Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA=='

MetaData = query_pb2.QueryResponse.MetaData
NOT_SERVING = HealthCheckResponse.DESCRIPTOR.enum_types_by_name['ServingStatus'].values_by_name['NOT_SERVING'].number


@pytest.fixture(scope='module')
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Certificates:
    made = make_certificates(tmp_path_factory.mktemp('pki'))
    if made is None:
        pytest.skip('openssl is not on PATH, so no certificates can be made for a TLS server')
    return made


@pytest.fixture
def unreachable() -> Iterator[str]:
    """A connection string naming a gateway that drops every connection."""
    with dropping() as port:
        yield f'couchbase2://127.0.0.1:{port}'


@pytest.fixture
def gateway() -> FakeGateway:
    return new_gateway()


@pytest.fixture
def connect(gateway: FakeGateway, certificates: Certificates) -> Iterator[Callable[..., tuple[str, ClusterOptions]]]:
    """The arguments a cluster on the in-process gateway is constructed from, extra keywords added."""
    with serving(gateway, certificates) as port:
        def arguments(**kwargs: Any) -> tuple[str, ClusterOptions]:
            return (f'couchbase2://localhost:{port}',
                    ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                   trust_store_path=str(certificates.ca), **kwargs))
        yield arguments


@pytest.fixture
def cluster(connect: Callable[..., tuple[str, ClusterOptions]]) -> Iterator[ClusterImpl]:
    impl = ClusterImpl(*connect())
    try:
        yield impl
    finally:
        impl.close_connection()


def _collection(cluster: ClusterImpl) -> CollectionImpl:
    return cluster.bucket(KEY[0]).scope(KEY[1]).collection(KEY[2])


def _stored(gateway: FakeGateway, value: Any = None, expiry: int | None = None) -> None:
    content = json.dumps(value if value is not None else {'name': 'Ritz'}).encode()
    gateway.documents[KEY] = Document(content, JSON_FLAGS, 42, expiry)


def _rows(*values: Any, meta: MetaData | None = None) -> query_pb2.QueryResponse:
    message = query_pb2.QueryResponse(rows=[json.dumps(v).encode() for v in values])
    if meta is not None:
        message.meta_data.CopyFrom(meta)
    return message


def _remaining(gateway: FakeGateway) -> float:
    return gateway.calls[-1].remaining


class Couchbase2CollectionUnitTests:

    def test_an_upsert_reads_back(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        collection = _collection(cluster)
        mutation = collection.upsert(KEY[3], {'name': 'Ritz', 'stars': 5})
        result = collection.get(KEY[3])
        assert result.content_as[dict] == {'name': 'Ritz', 'stars': 5}
        assert result.cas == mutation.cas
        token = mutation.mutation_token()
        assert token is not None and token.as_tuple() == (7, 11, mutation.cas, 'travel')
        upsert = gateway.calls[0].request
        assert (upsert.bucket_name, upsert.scope_name, upsert.collection_name, upsert.key) == KEY
        assert upsert.content_flags == JSON_FLAGS
        assert [call.metadata['authorization'] for call in gateway.calls] == [AUTHORIZATION] * 2

    def test_a_missing_document(self, cluster: ClusterImpl) -> None:
        with pytest.raises(DocumentNotFoundException):
            _collection(cluster).get(KEY[3])

    def test_the_expiry_is_read_when_asked_for(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _stored(gateway, expiry=1_900_000_000)
        assert _collection(cluster).get(KEY[3]).expiry_time is None
        assert _collection(cluster).get(KEY[3], with_expiry=True).expiry_time == datetime.fromtimestamp(1_900_000_000)

    def test_a_projection_is_sent(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _stored(gateway)
        _collection(cluster).get(KEY[3], GetOptions(project=['name']))
        assert list(gateway.calls[0].request.project) == ['name']

    def test_the_kv_timeout_defaults_to_two_and_a_half_seconds(self, cluster: ClusterImpl,
                                                               gateway: FakeGateway) -> None:
        _stored(gateway)
        _collection(cluster).get(KEY[3])
        assert 2.0 < _remaining(gateway) <= 2.5

    def test_an_operations_timeout_wins(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _stored(gateway)
        _collection(cluster).get(KEY[3], timeout=timedelta(seconds=20))
        assert 19.0 < _remaining(gateway) <= 20.0

    def test_the_clusters_kv_timeout(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                     gateway: FakeGateway) -> None:
        _stored(gateway)
        impl = ClusterImpl(*connect(kv_timeout=timedelta(seconds=7)))
        try:
            _collection(impl).get(KEY[3])
        finally:
            impl.close_connection()
        assert 6.0 < _remaining(gateway) <= 7.0

    def test_a_durable_upsert_takes_the_durable_timeout(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _collection(cluster).upsert(KEY[3], {}, durability=ServerDurability(DurabilityLevel.MAJORITY))
        assert gateway.calls[0].request.durability_level == kv_pb2.DURABILITY_LEVEL_MAJORITY
        assert 9.0 < _remaining(gateway) <= 10.0

    def test_unavailable_is_retried(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _stored(gateway)
        gateway.failures['Get'] = [Fail(grpc.StatusCode.UNAVAILABLE)]
        assert _collection(cluster).get(KEY[3]).content_as[dict] == {'name': 'Ritz'}
        assert len(gateway.calls) == 2

    def test_an_unknown_option_is_reported(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _stored(gateway)
        with pytest.warns(CouchbaseOptionWarning, match='tiemout'):
            _collection(cluster).get(KEY[3], tiemout=timedelta(seconds=1))

    def test_a_timeout_that_is_not_a_timedelta_is_refused(self, cluster: ClusterImpl) -> None:
        with pytest.raises(InvalidArgumentException):
            _collection(cluster).get(KEY[3], timeout=5)

    @pytest.mark.parametrize('call', [lambda c: c.get_any_replica('k'),
                                      lambda c: c.get_all_replicas('k'),
                                      lambda c: c.lookup_in_any_replica('k', ()),
                                      lambda c: c.get_any_replica_multi(['k'])])
    def test_every_other_operation_is_unavailable(self, cluster: ClusterImpl,
                                                  call: Callable[[CollectionImpl], object]) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            call(_collection(cluster))


class Couchbase2MultiUnitTests:

    def test_each_key_is_its_own_operation(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        _stored(gateway)
        result = _collection(cluster).get_multi([KEY[3], 'hotel_2'])
        assert result.all_ok is False
        assert result.results[KEY[3]].content_as[dict] == {'name': 'Ritz'}
        assert isinstance(result.exceptions['hotel_2'], DocumentNotFoundException)
        assert [call.request.key for call in gateway.calls] == [KEY[3], 'hotel_2']

    def test_every_key_is_sent_before_the_first_error_is_raised(self, cluster: ClusterImpl,
                                                                gateway: FakeGateway) -> None:
        gateway.failures['Upsert'] = [Fail(grpc.StatusCode.NOT_FOUND, 'document')]
        with pytest.raises(DocumentNotFoundException):
            _collection(cluster).upsert_multi({'a': {}, 'b': {}, 'c': {}}, return_exceptions=False)
        assert [call.request.key for call in gateway.calls] == ['a', 'b', 'c']
        assert {key[3] for key in gateway.documents} == {'b', 'c'}

    def test_a_keys_options_lie_over_the_batchs(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        opts = UpsertMultiOptions(expiry=timedelta(seconds=10),
                                  per_key_options={'b': UpsertOptions(expiry=timedelta(seconds=20))})
        assert _collection(cluster).upsert_multi({'a': {}, 'b': {}}, opts).all_ok is True
        assert [call.request.expiry_secs for call in gateway.calls] == [10, 20]

    def test_an_invalid_argument_raises_with_nothing_sent(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        opts = UpsertMultiOptions(per_key_options={'b': UpsertOptions(
            durability=ClientDurability(replicate_to=ReplicateTo.ONE))})
        with pytest.raises(FeatureUnavailableException, match='ClientDurability'):
            _collection(cluster).upsert_multi({'a': {}, 'b': {}}, opts)
        assert gateway.calls == []

    @pytest.mark.parametrize('opts', [GetMultiOptions(project=['name']),
                                      GetMultiOptions(per_key_options={KEY[3]: GetOptions(project=['name'])})])
    def test_a_multi_get_cannot_project(self, cluster: ClusterImpl, gateway: FakeGateway,
                                        opts: GetMultiOptions) -> None:
        with pytest.raises(InvalidArgumentException, match='project'):
            _collection(cluster).get_multi([KEY[3]], opts)
        assert gateway.calls == []

    def test_a_multi_get_drops_with_expiry_with_a_deprecation(self, cluster: ClusterImpl,
                                                              gateway: FakeGateway) -> None:
        _stored(gateway, expiry=60)
        with (pytest.warns(CouchbaseApplicabilityWarning, match='with_expiry'),
              pytest.warns(CouchbaseDeprecationWarning, match='with_expiry')):
            result = _collection(cluster).get_multi([KEY[3]], GetMultiOptions(with_expiry=True))
        assert result.results[KEY[3]].expiry_time is None

    @pytest.mark.parametrize('call', [lambda c: c.get_multi('hotel_1'),
                                      lambda c: c.upsert_multi(['hotel_1']),
                                      lambda c: c.unlock_multi(['hotel_1'])])
    def test_keys_of_the_wrong_type_are_refused(self, cluster: ClusterImpl, gateway: FakeGateway,
                                                call: Callable[[CollectionImpl], object]) -> None:
        with pytest.raises(InvalidArgumentException):
            call(_collection(cluster))
        assert gateway.calls == []


class Couchbase2QueryUnitTests:

    def test_the_rows_stream_then_the_metadata(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        gateway.query_messages = [_rows({'a': 1}, {'a': 2}),
                                  _rows({'a': 3}, meta=MetaData(request_id='r-1', status=MetaData.STATUS_SUCCESS))]
        result = cluster.query('SELECT a FROM t WHERE x=$1', 'y')
        assert result.execute() == [{'a': 1}, {'a': 2}, {'a': 3}]
        metadata = result.metadata()
        assert metadata is not None
        assert (metadata.request_id(), metadata.status()) == ('r-1', QueryStatus.SUCCESS)
        request = gateway.calls[0].request
        assert request.statement == 'SELECT a FROM t WHERE x=$1'
        assert [json.loads(p) for p in request.positional_parameters] == ['y']
        assert not request.HasField('bucket_name')

    def test_nothing_is_sent_until_the_rows_are_read(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        result = cluster.query('SELECT 1')
        assert gateway.calls == []
        assert result.execute() == []
        assert len(gateway.calls) == 1

    def test_a_scope_query_runs_in_its_scope(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        cluster.bucket('travel').scope('inventory').query('SELECT 1').execute()
        request = gateway.calls[0].request
        assert (request.bucket_name, request.scope_name) == ('travel', 'inventory')

    def test_the_query_timeout_defaults_to_seventy_five_seconds(self, cluster: ClusterImpl,
                                                                gateway: FakeGateway) -> None:
        cluster.query('SELECT 1').execute()
        assert 74.0 < _remaining(gateway) <= 75.0

    def test_a_query_error_is_raised_from_the_rows(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        gateway.query_end = Fail(grpc.StatusCode.INVALID_ARGUMENT)
        result = cluster.query('SELEC 1')
        with pytest.raises(InvalidArgumentException):
            result.execute()

    def test_a_query_is_retried_before_its_first_message(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        gateway.failures['Query'] = [Fail(grpc.StatusCode.UNAVAILABLE)]
        gateway.query_messages = [_rows(1)]
        assert cluster.query('SELECT 1', QueryOptions(read_only=True)).execute() == [1]
        assert len(gateway.calls) == 2

    def test_closing_early_cancels_the_stream(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        gateway.query_messages = [_rows(1), _rows(2)]
        gateway.query_pause = 5.0
        rows = cluster.query('SELECT 1').rows()
        assert next(rows) == 1
        rows.close()
        assert gateway.query_cancelled.wait(5)

    def test_the_serializer_decodes_the_rows(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        class Upper:
            def serialize(self, value: Any) -> bytes:
                return json.dumps(value).encode()

            def deserialize(self, value: bytes) -> Any:
                return json.loads(value).upper()

        gateway.query_messages = [_rows('a', 'b')]
        assert cluster.query('SELECT 1', serializer=Upper()).execute() == ['A', 'B']


class Couchbase2ClusterUnitTests:

    def test_a_closed_cluster_refuses_operations(self, connect: Callable[..., tuple[str, ClusterOptions]]) -> None:
        impl = ClusterImpl(*connect())
        collection = _collection(impl)
        impl.close_connection()
        impl.close_connection()
        with pytest.raises(RuntimeError, match='closed cluster'):
            collection.get(KEY[3])
        with pytest.raises(RuntimeError, match='closed cluster'):
            impl.query('SELECT 1')

    @pytest.mark.parametrize('call', [lambda c: c.ping(),
                                      lambda c: c.diagnostics(),
                                      lambda c: c.transactions,
                                      lambda c: c.bucket_mgmt().bucket_describe('b'),
                                      lambda c: c.analytics_query('SELECT 1'),
                                      lambda c: c.bucket('b').ping(),
                                      lambda c: c.bucket('b').view_query('d', 'v'),
                                      lambda c: c.bucket('b').scope('s').analytics_query('SELECT 1')])
    def test_every_other_operation_is_unavailable(self, cluster: ClusterImpl,
                                                  call: Callable[[ClusterImpl], object]) -> None:
        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            call(cluster)

    @pytest.mark.parametrize('options, key', [
        ({'num_io_threads': 2}, 'num_io_threads'),
        ({'transaction_config': TransactionConfig()}, 'transaction_config'),
        ({'orphan_reporting_options': ClusterOrphanReportingOptions(sample_size=5)}, 'orphan_sample_size'),
        ({'timeout_options': ClusterTimeoutOptions(analytics_timeout=timedelta(seconds=5))}, 'analytics_timeout'),
    ])
    def test_an_option_the_gateway_cannot_act_on_is_reported(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                                             options: dict[str, Any], key: str) -> None:
        with pytest.warns(CouchbaseApplicabilityWarning, match=f'Option {key} '):
            ClusterImpl(*connect(**options)).close_connection()

    def test_the_options_couchbase2_reads_are_not_reported(self,
                                                           connect: Callable[..., tuple[str, ClusterOptions]]) -> None:
        options = {'user_agent_extra': 'app',
                   'timeout_options': ClusterTimeoutOptions(kv_timeout=timedelta(seconds=5),
                                                            search_timeout=timedelta(seconds=5)),
                   'tracing_options': ClusterTracingOptions(tracing_threshold_kv=timedelta(milliseconds=100))}
        with warnings.catch_warnings():
            warnings.simplefilter('error', CouchbaseApplicabilityWarning)
            ClusterImpl(*connect(**options)).close_connection()

    def test_a_connection_string_query_parameter_is_read(self, certificates: Certificates,
                                                         gateway: FakeGateway) -> None:
        with serving(gateway, certificates) as port:
            impl = ClusterImpl(f'couchbase2://localhost:{port}?trust_store_path={certificates.ca}',
                               ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
            try:
                with pytest.raises(DocumentNotFoundException):
                    _collection(impl).get(KEY[3])
            finally:
                impl.close_connection()

    def test_new_credentials_are_sent_on_the_next_call(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        collection = _collection(cluster)
        collection.upsert(KEY[3], {'name': 'Ritz'})
        cluster.update_credentials(PasswordAuthenticator('user', 'pass'))
        collection.get(KEY[3])
        assert [call.metadata['authorization'] for call in gateway.calls] == [AUTHORIZATION, 'Basic dXNlcjpwYXNz']

    @pytest.mark.parametrize('authenticator', [JwtAuthenticator('a.b.c'),
                                               CertificateAuthenticator(cert_path='cert', key_path='key')],
                             ids=['jwt', 'certificate'])
    def test_an_authenticator_of_another_type_is_refused(self, cluster: ClusterImpl, gateway: FakeGateway,
                                                         authenticator: Any) -> None:
        with pytest.raises(InvalidArgumentException):
            cluster.update_credentials(authenticator)
        _collection(cluster).upsert(KEY[3], {'name': 'Ritz'})
        assert gateway.calls[0].metadata['authorization'] == AUTHORIZATION

    def test_a_certificate_cannot_be_replaced(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                              certificates: Certificates) -> None:
        impl = ClusterImpl(connect()[0],
                           ClusterOptions(CertificateAuthenticator(cert_path=str(certificates.certificate),
                                                                   key_path=str(certificates.key)),
                                          trust_store_path=str(certificates.ca)))
        try:
            with pytest.raises(FeatureUnavailableException, match='couchbase2'):
                impl.update_credentials(CertificateAuthenticator(cert_path='cert', key_path='key'))
        finally:
            impl.close_connection()

    def test_an_unreachable_gateway_is_unavailable(self, certificates: Certificates, unreachable: str) -> None:
        impl = ClusterImpl(unreachable,
                           ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                          trust_store_path=str(certificates.ca)))
        try:
            with pytest.raises(UnAmbiguousTimeoutException) as ex:
                _collection(impl).get(KEY[3], timeout=timedelta(milliseconds=300))
        finally:
            impl.close_connection()
        assert ex.value.error_context.retry_reasons == {'service_not_available'}


class Couchbase2WaitUntilReadyUnitTests:

    def test_it_returns_once_the_gateway_reports_serving(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        gateway.health_statuses = [NOT_SERVING, NOT_SERVING]
        cluster.wait_until_ready(timedelta(seconds=5))
        assert [call.method for call in gateway.calls] == ['Check'] * 3

    def test_the_check_names_no_service_and_carries_no_credentials(self, cluster: ClusterImpl,
                                                                   gateway: FakeGateway) -> None:
        cluster.wait_until_ready(timedelta(seconds=5))
        assert gateway.calls[0].request.service == ''
        assert 'authorization' not in gateway.calls[0].metadata

    def test_it_times_out_with_the_last_status_it_was_given(self, cluster: ClusterImpl,
                                                            gateway: FakeGateway) -> None:
        gateway.health_statuses = [NOT_SERVING] * 1000
        with pytest.raises(UnAmbiguousTimeoutException) as ex:
            cluster.wait_until_ready(timedelta(milliseconds=300))
        assert ex.value.error_context.retry_reasons == {'service_not_available'}
        assert ex.value.error_context.last_exception == ('CouchbaseException(couchbase2 server reported serving '
                                                         'status NOT_SERVING), caused by gRPC status OK')

    def test_an_unreachable_gateway_times_out_with_why(self, certificates: Certificates, unreachable: str) -> None:
        impl = ClusterImpl(unreachable,
                           ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                          trust_store_path=str(certificates.ca)))
        try:
            with pytest.raises(UnAmbiguousTimeoutException) as ex:
                impl.wait_until_ready(timedelta(milliseconds=300))
        finally:
            impl.close_connection()
        last_exception = ex.value.error_context.last_exception
        assert last_exception is not None and last_exception.startswith('ServiceUnavailableException(')

    def test_the_desired_state_and_services_are_reported_as_inert(self, cluster: ClusterImpl,
                                                                  gateway: FakeGateway) -> None:
        options = WaitUntilReadyOptions(desired_state=ClusterState.Degraded, service_types=[ServiceType.Query])
        with pytest.warns(CouchbaseApplicabilityWarning) as record:
            cluster.wait_until_ready(timedelta(seconds=5), options)
        assert {str(w.message).split()[1] for w in record} == {'desired_state', 'service_types'}
        assert [call.method for call in gateway.calls] == ['Check']

    def test_a_timeout_that_is_not_a_timedelta_is_refused(self, cluster: ClusterImpl, gateway: FakeGateway) -> None:
        timeout: Any = 5
        with pytest.raises(InvalidArgumentException):
            cluster.wait_until_ready(timeout)
        assert gateway.calls == []


def _info(caplog: pytest.LogCaptureFixture, level: int = logging.INFO) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == 'couchbase.couchbase2' and r.levelno == level]


class Couchbase2LoggingUnitTests:

    def test_opening_logs_how_the_cluster_connects(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                                   caplog: pytest.LogCaptureFixture) -> None:
        connstr, options = connect()
        with caplog.at_level(logging.INFO, logger='couchbase.couchbase2'):
            impl = ClusterImpl(connstr, options)
        impl.close_connection()
        opened = _info(caplog)[0]
        assert opened.startswith(f'couchbase2: open cluster, SDK version: "{PYCBC_VERSION}", '
                                 f'grpcio version: "{grpc.__version__}", ')
        assert (f'gateway: "{connstr.removeprefix("couchbase2://")}", tls verification: "trust store", '
                'client certificate: false, authentication: "password", log redaction: ') in opened
        assert 'password"' not in opened.replace('authentication: "password"', '')

    def test_new_credentials_and_closing_are_logged(self, cluster: ClusterImpl,
                                                    caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger='couchbase.couchbase2'):
            cluster.update_credentials(PasswordAuthenticator('user', 'secret'))
            cluster.close_connection()
            cluster.close_connection()
        assert _info(caplog) == ["couchbase2: updated the cluster's password credentials",
                                 f'couchbase2: closed the cluster connected to '
                                 f'{cluster.core.channel_config.endpoint.target}']

    def test_an_unrecognized_value_warns_once_per_cluster(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                                          gateway: FakeGateway,
                                                          caplog: pytest.LogCaptureFixture) -> None:
        def get_twice() -> None:
            impl = ClusterImpl(*connect())
            try:
                for _ in range(2):
                    gateway.failures['Get'] = [Fail(grpc.StatusCode.NOT_FOUND, resource_type='vbucket')]
                    with pytest.raises(CouchbaseException):
                        _collection(impl).get(KEY[3])
            finally:
                impl.close_connection()

        with caplog.at_level(logging.WARNING, logger='couchbase.couchbase2'):
            get_twice()
            get_twice()
        assert _info(caplog, logging.WARNING) == [
            'couchbase2: the gateway sent NOT_FOUND resource type "vbucket", which this SDK version does not '
            'recognize, so the exception raised for it may not describe the failure.  A newer SDK may map it.'] * 2


def _run(coroutine: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    return run_on_own_loop(coroutine())


class Couchbase2AsyncUnitTests:

    def test_an_upsert_reads_back(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                  gateway: FakeGateway) -> None:
        async def run() -> Any:
            impl = AsyncClusterImpl(*connect())
            try:
                collection = impl.bucket(KEY[0]).scope(KEY[1]).collection(KEY[2])
                mutation = await collection.upsert(KEY[3], {'name': 'Ritz'})
                result = await collection.get(KEY[3])
                return mutation.cas, result.cas, result.content_as[dict]
            finally:
                await impl.close_connection()

        upserted, read, content = _run(run)
        assert upserted == read and content == {'name': 'Ritz'}
        assert [call.method for call in gateway.calls] == ['Upsert', 'Get']

    def test_a_missing_document(self, connect: Callable[..., tuple[str, ClusterOptions]]) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                await impl.bucket(KEY[0]).scope(KEY[1]).collection(KEY[2]).get(KEY[3])
            finally:
                await impl.close_connection()

        with pytest.raises(DocumentNotFoundException):
            _run(run)

    def test_the_rows_stream_then_the_metadata(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                               gateway: FakeGateway) -> None:
        gateway.query_messages = [_rows(1, 2), _rows(3, meta=MetaData(request_id='r-1'))]

        async def run() -> Any:
            impl = AsyncClusterImpl(*connect())
            try:
                result = impl.bucket('travel').scope('inventory').query('SELECT 1')
                rows = [row async for row in result]
                metadata = result.metadata()
                return rows, None if metadata is None else metadata.request_id()
            finally:
                await impl.close_connection()

        assert _run(run) == ([1, 2, 3], 'r-1')
        assert gateway.calls[0].request.scope_name == 'inventory'

    def test_a_query_error_is_raised_from_the_rows(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                                   gateway: FakeGateway) -> None:
        gateway.query_end = Fail(grpc.StatusCode.INVALID_ARGUMENT)

        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                [row async for row in impl.query('SELEC 1')]
            finally:
                await impl.close_connection()

        with pytest.raises(InvalidArgumentException):
            _run(run)

    def test_closing_early_cancels_the_stream(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                              gateway: FakeGateway) -> None:
        gateway.query_messages = [_rows(1), _rows(2)]
        gateway.query_pause = 5.0

        async def run() -> Any:
            impl = AsyncClusterImpl(*connect())
            try:
                rows = impl.query('SELECT 1').rows()
                first = await rows.__anext__()
                await rows.aclose()
                return first
            finally:
                await impl.close_connection()

        assert _run(run) == 1
        assert gateway.query_cancelled.wait(5)

    def test_the_whole_stream_is_read_for_twisted(self, connect: Callable[..., tuple[str, ClusterOptions]],
                                                  gateway: FakeGateway) -> None:
        gateway.query_messages = [_rows(1, 2), _rows(3, meta=MetaData(request_id='r-1'))]

        async def run() -> Any:
            impl = AsyncClusterImpl(*connect())
            try:
                prepared = impl.bucket('travel').scope('inventory').prepare_query('SELECT 1', (), {})
                return QueryResult(await buffered_rows(lambda: impl.open_query(prepared, None),
                                                       QueryRows(prepared.serializer)))
            finally:
                await impl.close_connection()

        result = _run(run)
        assert result.execute() == [1, 2, 3]
        metadata = result.metadata()
        assert metadata is not None and metadata.request_id() == 'r-1'

    def test_wait_until_ready_returns_once_the_gateway_reports_serving(
            self, connect: Callable[..., tuple[str, ClusterOptions]], gateway: FakeGateway) -> None:
        gateway.health_statuses = [NOT_SERVING]

        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            try:
                await impl.wait_until_ready(timedelta(seconds=5))
            finally:
                await impl.close_connection()

        _run(run)
        assert [call.method for call in gateway.calls] == ['Check'] * 2
        assert 'authorization' not in gateway.calls[0].metadata

    def test_every_other_operation_is_unavailable(self, connect: Callable[..., tuple[str, ClusterOptions]]) -> None:
        async def run() -> None:
            impl = AsyncClusterImpl(*connect())
            collection: AsyncCollectionImpl = impl.bucket(KEY[0]).scope(KEY[1]).collection(KEY[2])
            try:
                await collection.lookup_in_any_replica(KEY[3], ())
            finally:
                await impl.close_connection()

        with pytest.raises(FeatureUnavailableException, match='couchbase2'):
            _run(run)


class PercentileHistogramUnitTests:

    def _histogram(self) -> PercentileHistogram:
        return PercentileHistogram(lowest_discernible_value=1, highest_trackable_value=30_000_000,
                                   significant_figures=3)

    def test_empty(self) -> None:
        assert self._histogram().get_percentiles_and_reset([50.0, 100.0]) == {'total_count': 0,
                                                                              'percentiles': [0, 0]}

    def test_percentiles_read_as_the_highest_value_of_their_bucket(self) -> None:
        histogram = self._histogram()
        for value in range(1, 101):
            histogram.record_value(value)
        histogram.record_value(123_456)
        report = histogram.get_percentiles_and_reset([50.0, 99.0, 100.0])
        assert report == {'total_count': 101, 'percentiles': [51, 100, 123_999]}

    def test_reading_resets(self) -> None:
        histogram = self._histogram()
        histogram.record_value(5)
        histogram.get_percentiles_and_reset([50.0])
        assert histogram.get_percentiles_and_reset([50.0])['total_count'] == 0

    def test_values_outside_the_range_are_clamped(self) -> None:
        histogram = self._histogram()
        histogram.record_value(0)
        histogram.record_value(10**9)
        assert histogram.get_percentiles_and_reset([0.0, 100.0])['percentiles'] == [1, 30_000_000]
