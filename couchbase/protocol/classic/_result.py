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

import json
from typing import (TYPE_CHECKING,
                    Any,
                    TypedDict)

from couchbase.common._utils import is_null_or_empty
from couchbase.common.diagnostics import _EndpointDiagnosticsRecord, _EndpointPingRecord
from couchbase.common.result import (_ClusterInfoBucketRecord,
                                     _ClusterInfoNodeRecord,
                                     _ClusterInfoRecord,
                                     _CounterRecord,
                                     _DiagnosticsRecord,
                                     _ExistsRecord,
                                     _GetRecord,
                                     _GetReplicaRecord,
                                     _LookupInFieldRecord,
                                     _LookupInRecord,
                                     _LookupInReplicaRecord,
                                     _MutateInFieldRecord,
                                     _MutateInRecord,
                                     _MutationRecord,
                                     _MutationTokenRecord,
                                     _PingRecord,
                                     _ScanItemBodyRecord,
                                     _ScanItemRecord,
                                     _ServerVersionRecord)
from couchbase.common.search import (SearchRow,
                                     SearchRowFields,
                                     SearchRowLocation,
                                     SearchRowLocations)
from couchbase.common.views import ViewRow
from couchbase.protocol.classic._subdocument import _subdoc_error
from couchbase.serializer import Serializer

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import (CppClusterDescribeResponse,
                                                                          CppClusterInfoBucket,
                                                                          CppClusterInfoNode,
                                                                          CppDecrementResponse,
                                                                          CppDiagnosticsResult,
                                                                          CppEndpointDiagInfo,
                                                                          CppEndpointPingInfo,
                                                                          CppExistsResponse,
                                                                          CppGetAllReplicasResponseEntry,
                                                                          CppGetAnyReplicaResponse,
                                                                          CppGetProjectedResponse,
                                                                          CppIncrementResponse,
                                                                          CppLookupInAllReplicasResponseEntry,
                                                                          CppLookupInAnyReplicaResponse,
                                                                          CppLookupInResponse,
                                                                          CppLookupInResponseEntry,
                                                                          CppMutateInResponse,
                                                                          CppMutateInResponseEntry,
                                                                          CppMutationToken,
                                                                          CppPingResult,
                                                                          CppRangeScanItem,
                                                                          CppSearchLocation,
                                                                          CppSearchRow,
                                                                          CppUpsertResponse,
                                                                          CppViewRow)

    class _CoreScanItem(TypedDict, total=False):
        """What the binding writes for one item of a range scan (``src/result.cxx``)."""
        scan_item: CppRangeScanItem


def _mutation_token_from_core(raw: CppMutationToken | None) -> _MutationTokenRecord | None:
    if raw is None:
        return None
    return _MutationTokenRecord(partition_id=raw['partition_id'],
                                partition_uuid=raw['partition_uuid'],
                                sequence_number=raw['sequence_number'],
                                bucket_name=raw['bucket_name'])


def _lookup_in_field_from_core(raw: CppLookupInResponseEntry, key: str | None) -> _LookupInFieldRecord:
    return _LookupInFieldRecord(path=raw['path'],
                                value=raw['value'],
                                original_index=raw['original_index'],
                                exists=raw['exists'],
                                opcode=raw['opcode'],
                                status=raw['status'],
                                ec=raw['ec'],
                                error=_subdoc_error(raw['status'], raw['path'], key))


def _mutate_in_field_from_core(raw: CppMutateInResponseEntry, key: str | None) -> _MutateInFieldRecord:
    return _MutateInFieldRecord(path=raw['path'],
                                value=raw['value'],
                                original_index=raw['original_index'],
                                opcode=raw['opcode'],
                                status=raw['status'],
                                ec=raw['ec'],
                                error=_subdoc_error(raw['status'], raw['path'], key))


def _get_from_core(raw: CppGetProjectedResponse) -> _GetRecord:
    """``get``, ``get_and_lock``, ``get_and_touch`` and a projected ``get``.

    Only a projected ``get`` sends ``expiry``, and only when the caller asked for it, so its response
    holds every key the other three do.
    """
    return _GetRecord(cas=raw.get('cas', 0),
                      value=raw['value'],
                      flags=raw.get('flags', 0),
                      expiry=raw.get('expiry'))


def _get_replica_from_core(raw: CppGetAnyReplicaResponse | CppGetAllReplicasResponseEntry) -> _GetReplicaRecord:
    """``get_any_replica`` and each entry of ``get_all_replicas``."""
    return _GetReplicaRecord(cas=raw.get('cas', 0),
                             value=raw['value'],
                             flags=raw.get('flags', 0),
                             is_replica=raw.get('replica', False))


def _exists_from_core(raw: CppExistsResponse) -> _ExistsRecord:
    return _ExistsRecord(cas=raw.get('cas', 0),
                         exists=raw.get('document_exists', False),
                         deleted=raw.get('deleted'),
                         flags=raw.get('flags'),
                         expiry=raw.get('expiry'),
                         sequence_number=raw.get('sequence_number'),
                         datatype=raw.get('datatype'))


def _mutation_from_core(raw: CppUpsertResponse) -> _MutationRecord:
    """Every mutation but the counters and ``mutate_in``, and ``touch``, which sends no token.

    Each of them sends the keys an ``upsert`` does, or a subset of them.
    """
    return _MutationRecord(cas=raw.get('cas', 0),
                           mutation_token=_mutation_token_from_core(raw.get('token')))


def _counter_from_core(raw: CppIncrementResponse | CppDecrementResponse) -> _CounterRecord:
    return _CounterRecord(cas=raw.get('cas', 0),
                          content=raw.get('content'),
                          mutation_token=_mutation_token_from_core(raw.get('token')))


def _lookup_in_from_core(raw: CppLookupInResponse, key: str | None) -> _LookupInRecord:
    return _LookupInRecord(cas=raw.get('cas', 0),
                           fields=tuple(_lookup_in_field_from_core(f, key) for f in raw.get('fields') or ()),
                           deleted=raw.get('deleted', False))


def _lookup_in_replica_from_core(raw: CppLookupInAnyReplicaResponse | CppLookupInAllReplicasResponseEntry,
                                 key: str | None) -> _LookupInReplicaRecord:
    """``lookup_in_any_replica`` and each entry of ``lookup_in_all_replicas``."""
    return _LookupInReplicaRecord(cas=raw.get('cas', 0),
                                  fields=tuple(_lookup_in_field_from_core(f, key) for f in raw.get('fields') or ()),
                                  deleted=raw.get('deleted', False),
                                  is_replica=raw.get('is_replica', False))


def _mutate_in_from_core(raw: CppMutateInResponse, key: str | None) -> _MutateInRecord:
    return _MutateInRecord(cas=raw.get('cas', 0),
                           mutation_token=_mutation_token_from_core(raw.get('token')),
                           fields=tuple(_mutate_in_field_from_core(f, key) for f in raw.get('fields') or ()),
                           deleted=raw.get('deleted', False))


def _scan_item_from_core(raw: _CoreScanItem) -> _ScanItemRecord:
    """One item of a range scan. The core sends no ``body`` for a scan that asked for ids only."""
    item = raw.get('scan_item') or {}
    body = item.get('body')
    return _ScanItemRecord(id=item.get('key'),
                           body=None if body is None else _ScanItemBodyRecord(
                               cas=body['cas'],
                               value=body['value'],
                               flags=body['flags'],
                               expiry=body['expiry'],
                               sequence_number=body['sequence_number'],
                               datatype=body['datatype']))


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


def _search_location_from_core(raw: CppSearchLocation) -> SearchRowLocation:
    array_positions = raw.get('array_positions')
    return SearchRowLocation(field=raw['field'],
                             term=raw['term'],
                             position=raw['position'],
                             start=raw['start_offset'],
                             end=raw['end_offset'],
                             array_positions=None if array_positions is None else list(array_positions))


def _search_fields_from_core(text: str, serializer: Serializer) -> Any:
    if is_null_or_empty(text):
        return None
    value = serializer.deserialize(text.encode('utf-8'))
    return SearchRowFields(**value) if isinstance(value, dict) else value


def _search_row_from_core(raw: CppSearchRow, serializer: Serializer) -> SearchRow:
    """The core sends ``fields`` and ``explanation`` as JSON text, empty when the query did not ask.

    ``fields`` is the documents' own content, so the request's serializer reads it; a mapping comes
    back as :class:`SearchRowFields` and anything else as the serializer returned it. ``explanation``
    is the server's account of the score and is read as JSON.
    """
    explanation = raw['explanation']
    locations = raw['locations']
    return SearchRow(index=raw['index'],
                     id=raw['id'],
                     score=raw['score'],
                     fields=_search_fields_from_core(raw['fields'], serializer),
                     locations=SearchRowLocations(map(_search_location_from_core, locations)) if locations else None,
                     fragments={name: list(texts) for name, texts in raw['fragments'].items()},
                     explanation={} if is_null_or_empty(explanation) else json.loads(explanation))


def _view_row_from_core(raw: CppViewRow, serializer: Serializer) -> ViewRow:
    """Views sends a row's key and value as JSON text, and a reduced row has no id.

    Both are what the view's map function emitted, so the request's serializer reads them.
    """
    return ViewRow(key=serializer.deserialize(raw['key'].encode('utf-8')) if 'key' in raw else None,
                   id=raw.get('id'),
                   value=serializer.deserialize(raw['value'].encode('utf-8')) if 'value' in raw else None)
