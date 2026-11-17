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

import warnings
from collections.abc import (Callable,
                             Coroutine,
                             Iterator)
from datetime import timedelta
from typing import Any

import pytest

from acouchbase.protocol.couchbase2._cluster_impl import AsyncClusterImpl
from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.common.warnings import CouchbaseApplicabilityWarning
from couchbase.diagnostics import ClusterState, ServiceType
from couchbase.exceptions import (FeatureUnavailableException,
                                  InvalidArgumentException,
                                  UnAmbiguousTimeoutException)
from couchbase.options import (ClusterOptions,
                               ClusterOrphanReportingOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               TransactionConfig,
                               WaitUntilReadyOptions)
from couchbase.protocol.couchbase2._cluster_impl import ClusterImpl
from couchbase.protocol.couchbase2._health import HealthCheckResponse
from couchbase.protocol.couchbase2._histogram import PercentileHistogram
from tests.couchbase2_gateway import (Certificates,
                                      FakeGateway,
                                      dropping,
                                      make_certificates,
                                      new_gateway,
                                      serving)
from tests.event_loop import run_on_own_loop

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


class Couchbase2ClusterUnitTests:

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


def _run(coroutine: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    return run_on_own_loop(coroutine())


class Couchbase2AsyncUnitTests:

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
