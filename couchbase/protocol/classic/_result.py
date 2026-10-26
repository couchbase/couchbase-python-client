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

"""The classic adapters for key-value and cluster results, and for streaming rows.

A result is read into a record; a search or view row is read straight into the public row type,
which is itself protocol-neutral. Each dict is read and never written, so the core's response is
left as the core produced it. The top-level defaults are the ones the accessors used when they read
the dict directly, and the core sends every one of these keys, so a default only answers a dict
built by hand.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from couchbase.common.diagnostics import _EndpointDiagnosticsRecord, _EndpointPingRecord
from couchbase.common.result import (_ClusterInfoBucketRecord,
                                     _ClusterInfoNodeRecord,
                                     _ClusterInfoRecord,
                                     _DiagnosticsRecord,
                                     _PingRecord,
                                     _ServerVersionRecord)
from couchbase.common.views import ViewRow
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import (CppClusterDescribeResponse,
                                                                          CppClusterInfoBucket,
                                                                          CppClusterInfoNode,
                                                                          CppDiagnosticsResult,
                                                                          CppEndpointDiagInfo,
                                                                          CppEndpointPingInfo,
                                                                          CppPingResult,
                                                                          CppRangeScanItem,
                                                                          CppViewRow)

    class _CoreScanItem(TypedDict, total=False):
        """What the binding writes for one item of a range scan (``src/result.cxx``)."""
        scan_item: CppRangeScanItem


def _endpoint_diagnostics_from_core(raw: CppEndpointDiagInfo) -> _EndpointDiagnosticsRecord:
    return _EndpointDiagnosticsRecord(service_type=raw['type'],
                                      id=raw['id'],
                                      remote=raw['remote'],
                                      local=raw['local'],
                                      state=raw['state'],
                                      bucket=raw['bucket'],
                                      details=raw['details'],
                                      last_activity_us=raw['last_activity'])


def _endpoint_ping_from_core(raw: CppEndpointPingInfo) -> _EndpointPingRecord:
    return _EndpointPingRecord(service_type=raw['type'],
                               id=raw['id'],
                               remote=raw['remote'],
                               local=raw['local'],
                               state=raw['state'],
                               bucket=raw['bucket'],
                               error=raw['error'],
                               latency_us=raw['latency'])


def _diagnostics_from_core(raw: CppDiagnosticsResult) -> _DiagnosticsRecord:
    services = raw.get('services') or {}
    return _DiagnosticsRecord(id=raw['id'],
                              sdk=raw['sdk'],
                              version=raw['version'],
                              services=tuple((service, tuple(_endpoint_diagnostics_from_core(e) for e in endpoints))
                                             for service, endpoints in services.items()))


def _ping_from_core(raw: CppPingResult) -> _PingRecord:
    services = raw.get('services') or {}
    return _PingRecord(id=raw['id'],
                       sdk=raw['sdk'],
                       version=raw['version'],
                       services=tuple((service, tuple(_endpoint_ping_from_core(e) for e in endpoints))
                                      for service, endpoints in services.items()))


def _version_part(parts: list[str], index: int) -> int | None:
    if index < len(parts) and parts[index].isdecimal():
        return int(parts[index])
    return None


def _server_version_from_core(full: str) -> _ServerVersionRecord:
    """The server spells a version ``7.6.2-3721-enterprise``: release, build and edition."""
    tokens = full.split('-')
    release = tokens[0].split('.')
    return _ServerVersionRecord(full=full,
                                major=_version_part(release, 0),
                                minor=_version_part(release, 1),
                                patch=_version_part(release, 2),
                                build=_version_part(tokens, 1),
                                edition=tokens[2] if len(tokens) > 2 else None)


def _cluster_info_node_from_core(raw: CppClusterInfoNode) -> _ClusterInfoNodeRecord:
    return _ClusterInfoNodeRecord(uuid=raw['uuid'],
                                  otp_node=raw['otp_node'],
                                  status=raw['status'],
                                  hostname=raw['hostname'],
                                  os=raw['os'],
                                  version=_server_version_from_core(raw['version']),
                                  services=tuple(raw['services']))


def _cluster_info_bucket_from_core(raw: CppClusterInfoBucket) -> _ClusterInfoBucketRecord:
    return _ClusterInfoBucketRecord(uuid=raw['uuid'], name=raw['name'])


def _cluster_info_from_core(raw: CppClusterDescribeResponse) -> _ClusterInfoRecord:
    info = raw.get('info') or {}
    return _ClusterInfoRecord(nodes=tuple(_cluster_info_node_from_core(n) for n in info.get('nodes') or ()),
                              buckets=tuple(_cluster_info_bucket_from_core(b) for b in info.get('buckets') or ()),
                              services=frozenset(info.get('services') or ()))


def _view_row_from_core(raw: CppViewRow, serializer: Serializer) -> ViewRow:
    """Views sends a row's key and value as JSON text, and a reduced row has no id.

    Both are what the view's map function emitted, so the request's serializer reads them.
    """
    return ViewRow(key=serializer.deserialize(raw['key'].encode('utf-8')) if 'key' in raw else None,
                   id=raw.get('id'),
                   value=serializer.deserialize(raw['value'].encode('utf-8')) if 'value' in raw else None)
