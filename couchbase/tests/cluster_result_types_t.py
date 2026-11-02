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

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import pytest

from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.diagnostics import (ClusterState,
                                   EndpointState,
                                   PingState,
                                   ServiceType)
from couchbase.protocol.classic._result import (_cluster_info_from_core,
                                                _diagnostics_from_core,
                                                _ping_from_core)
from couchbase.result import (ClusterInfoResult,
                              DiagnosticsResult,
                              PingResult,
                              Result)
from tests.test_features import TestProtocol

# Each record here is built by classic's adapter from what its binding returns.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import (CppClusterInfo,
                                                                          CppClusterInfoNode,
                                                                          CppDiagnosticsResult,
                                                                          CppEndpointDiagInfo,
                                                                          CppEndpointPingInfo,
                                                                          CppPingResult)


def _diag_endpoint(service: str, i: int, state: str = 'connected') -> CppEndpointDiagInfo:
    return {'type': service, 'id': f'0x{i}', 'last_activity': 1500, 'remote': f'10.0.0.{i}:11210',
            'local': f'127.0.0.1:{5000 + i}', 'state': state, 'bucket': 'default', 'details': None}


def _ping_endpoint(service: str, i: int, state: str = 'ok') -> CppEndpointPingInfo:
    return {'type': service, 'id': f'0x{i}', 'latency': 120, 'remote': f'10.0.0.{i}:8093',
            'local': f'127.0.0.1:{6000 + i}', 'state': state, 'bucket': None, 'error': None}


def _diag_report(services: dict[str, list[CppEndpointDiagInfo]]) -> CppDiagnosticsResult:
    return {'id': 'report-1', 'sdk': 'python/4.6.0', 'version': 2, 'services': services}


def _ping_report(services: dict[str, list[CppEndpointPingInfo]]) -> CppPingResult:
    return {'id': 'report-1', 'sdk': 'python/4.6.0', 'version': 2, 'services': services}


def _diagnostics(states: list[str]) -> DiagnosticsResult:
    services = {'key_value': [_diag_endpoint('key_value', i, s) for i, s in enumerate(states)]}
    return DiagnosticsResult(_diagnostics_from_core(_diag_report(services)))


def _node(i: int, version: str) -> CppClusterInfoNode:
    return {'uuid': f'u{i}', 'otp_node': f'ns_1@10.0.0.{i}', 'status': 'healthy', 'hostname': f'10.0.0.{i}:8091',
            'os': 'x86_64-pc-linux-gnu', 'version': version, 'services': ['kv', 'n1ql']}


def _cluster_info(*versions: str) -> ClusterInfoResult:
    info: CppClusterInfo = {'nodes': [_node(i, v) for i, v in enumerate(versions)],
                            'buckets': [{'uuid': 'b1', 'name': 'default'}],
                            'services': {'key_value', 'query'}}
    return ClusterInfoResult(_cluster_info_from_core({'info': info}))


_VERSION_ACCESSORS = ('server_version', 'server_version_short', 'server_version_full', 'server_version_build',
                      'is_enterprise', 'is_community')


def _version_reads(info: ClusterInfoResult) -> tuple[Any, ...]:
    return tuple(getattr(info, name) for name in _VERSION_ACCESSORS)


class ClusterResultTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: each result is built from the dict the core would have
    # handed the classic adapter, so nothing here needs a cluster.

    @pytest.mark.parametrize('adapter, core', [
        (_diagnostics_from_core, _diag_report({'key_value': [_diag_endpoint('key_value', 1)]})),
        (_ping_from_core, _ping_report({'query': [_ping_endpoint('query', 1)]})),
        (_cluster_info_from_core, {'info': {'nodes': [_node(1, '7.6.2-3721-enterprise')], 'buckets': [],
                                            'services': {'key_value'}}}),
    ])
    def test_adapter_leaves_the_core_dict_as_the_core_sent_it(self, adapter: Callable[[Any], Any],
                                                              core: dict[str, Any]) -> None:
        sent = copy.deepcopy(core)

        adapter(core)

        assert core == sent

    @pytest.mark.parametrize('build', [
        lambda: DiagnosticsResult(_diagnostics_from_core(_diag_report({}))),
        lambda: PingResult(_ping_from_core(_ping_report({}))),
    ])
    def test_a_report_is_still_a_result(self, build: Callable[[], Any]) -> None:
        report = build()

        assert (report.id, report.sdk, report.version) == ('report-1', 'python/4.6.0', 2)
        assert isinstance(report, Result)

    @pytest.mark.parametrize('name, expected', [
        ('cas', 0), ('value', None), ('flags', 0), ('key', None), ('success', False),
    ])
    @pytest.mark.parametrize('build', [
        lambda: DiagnosticsResult(_diagnostics_from_core(_diag_report({}))),
        lambda: PingResult(_ping_from_core(_ping_report({}))),
    ])
    def test_document_fields_are_deprecated_and_keep_their_values(self, build: Callable[[], Any], name: str,
                                                                  expected: Any) -> None:
        report = build()

        with pytest.warns(CouchbaseDeprecationWarning, match=rf'{type(report).__name__}\.{name} is deprecated'):
            assert getattr(report, name) == expected

    def test_endpoints_are_grouped_by_service_in_the_order_the_core_sent_them(self) -> None:
        core = _ping_report({'query': [_ping_endpoint('query', 1)],
                             'key_value': [_ping_endpoint('key_value', 2), _ping_endpoint('key_value', 3, 'timeout')],
                             'view': []})

        endpoints = PingResult(_ping_from_core(core)).endpoints

        assert list(endpoints) == [ServiceType.Query, ServiceType.KeyValue, ServiceType.View]
        assert [e.id for e in endpoints[ServiceType.KeyValue]] == ['0x2', '0x3']
        assert [e.state for e in endpoints[ServiceType.KeyValue]] == [PingState.OK, PingState.TIMEOUT]
        assert endpoints[ServiceType.View] == []

    def test_a_report_without_services_has_no_endpoints(self) -> None:
        diag: CppDiagnosticsResult = {'id': 'report-1', 'sdk': 'python/4.6.0', 'version': 2}
        ping: CppPingResult = {'id': 'report-1', 'sdk': 'python/4.6.0', 'version': 2}

        assert DiagnosticsResult(_diagnostics_from_core(diag)).endpoints == {}
        assert PingResult(_ping_from_core(ping)).endpoints == {}

    @pytest.mark.parametrize('states, expected', [
        ([], ClusterState.Online),
        (['connected', 'connected'], ClusterState.Online),
        (['connected', 'connecting'], ClusterState.Degraded),
        (['disconnected', 'disconnecting'], ClusterState.Offline),
    ])
    def test_diagnostics_state_counts_connected_endpoints(self, states: list[str], expected: ClusterState) -> None:
        assert _diagnostics(states).state == expected

    def test_diagnostics_endpoint_reads_its_record(self) -> None:
        endpoint = _diagnostics(['connecting']).endpoints[ServiceType.KeyValue][0]

        assert endpoint.service_type == endpoint.type == ServiceType.KeyValue
        assert endpoint.state == EndpointState.Connecting
        expected = ('0x0', '10.0.0.0:11210', '127.0.0.1:5000', 'default')
        assert (endpoint.id, endpoint.remote, endpoint.local, endpoint.namespace) == expected

    def test_as_json_writes_each_service_by_its_value(self) -> None:
        result = DiagnosticsResult(_diagnostics_from_core(
            _diag_report({'key_value': [_diag_endpoint('key_value', 1)]})))

        assert json.loads(result.as_json()) == {
            'version': 2, 'id': 'report-1', 'sdk': 'python/4.6.0',
            'services': {'key_value': [{'type': 'key_value', 'id': '0x1', 'remote': '10.0.0.1:11210',
                                        'local': '127.0.0.1:5001', 'state': 'connected', 'bucket': 'default',
                                        'last_activity_us': 1500}]}}

    @pytest.mark.parametrize('version, expected', [
        ('7.6.2-3721-enterprise', ('7.6.2-3721', 7.6, '7.6.2-3721-enterprise', 3721, True, False)),
        ('7.6.2-3721-community', ('7.6.2-3721', 7.6, '7.6.2-3721-community', 3721, False, True)),
        # Each part is read on its own, so a two-digit part or a five-digit build reads whole.
        ('7.6.10-3721-enterprise', ('7.6.10-3721', 7.6, '7.6.10-3721-enterprise', 3721, True, False)),
        ('10.0.0-1000-enterprise', ('10.0.0-1000', 10.0, '10.0.0-1000-enterprise', 1000, True, False)),
        ('7.6.2-12345-enterprise', ('7.6.2-12345', 7.6, '7.6.2-12345-enterprise', 12345, True, False)),
        ('7.6.2', ('7.6.2', 7.6, '7.6.2', None, None, None)),
        ('', (None, None, '', None, None, None)),
    ])
    def test_server_version_is_read_by_its_parts(self, version: str, expected: tuple[Any, ...]) -> None:
        assert _version_reads(_cluster_info(version)) == expected

    def test_is_community_answers(self) -> None:
        assert _cluster_info('7.6.2-3721-community').is_community is True
        assert _cluster_info('7.6.2-3721-enterprise').is_community is False

    def test_nodes_on_one_version_report_it(self) -> None:
        info = _cluster_info('7.6.2-3721-enterprise', '7.6.2-3721-enterprise')

        assert info.server_version_full == '7.6.2-3721-enterprise'

    @pytest.mark.parametrize('build', [
        lambda: _cluster_info('7.6.2-3721-enterprise', '7.6.3-4000-enterprise'),
        lambda: _cluster_info(),
        lambda: ClusterInfoResult(_cluster_info_from_core({})),
    ])
    def test_a_cluster_with_no_single_version_reports_none(self, build: Callable[[], ClusterInfoResult]) -> None:
        # Mixed versions, no nodes, and a response with no `info` at all.
        assert _version_reads(build()) == (None,) * len(_VERSION_ACCESSORS)

    def test_nodes_are_the_core_shape(self) -> None:
        nodes = _cluster_info('7.6.2-3721-enterprise').nodes

        assert nodes == [_node(0, '7.6.2-3721-enterprise')]
        assert ClusterInfoResult(_cluster_info_from_core({})).nodes == []

    def test_a_version_given_as_parts_needs_no_string(self) -> None:
        # A protocol that sends the version as numbers builds the same record with no string.
        record = _cluster_info_from_core({'info': {'nodes': [_node(0, '8.0.1-2000-enterprise')]}})
        node = record.nodes[0]
        record = replace(record, nodes=(replace(node, version=replace(node.version, full=None)),))

        info = ClusterInfoResult(record)

        assert _version_reads(info) == ('8.0.1-2000', 8.0, None, 2000, True, False)

    @pytest.mark.parametrize('build, prefix', [
        (lambda: _diagnostics([]), 'DiagnosticsResult:'),
        (lambda: PingResult(_ping_from_core(_ping_report({}))), 'PingResult:'),
        (lambda: _cluster_info('7.6.2-3721-enterprise'), 'ClusterInfoResult:'),
    ])
    def test_repr_prints_the_record(self, build: Callable[[], Any], prefix: str) -> None:
        text = repr(build())

        assert text.startswith(prefix)
        assert 'pycbc_result' not in text
