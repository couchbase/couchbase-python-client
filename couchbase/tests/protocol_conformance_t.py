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

"""The contracts every protocol meets, asked of each protocol's adapters and encoders alike.

A query's metadata reaches the caller as one record whichever protocol carried it.  The record holds
the wire's own units and every field either protocol sends, and an adapter decides everything when it
builds the record.  A query's options each change what a protocol sends, unless the option spec
declares them inert for that protocol, which warns and sends nothing.

`tests/query_wire_shapes.json` holds what each protocol's adapters were handed for the same queries against
one cluster.  Where the protocols are known to differ, the difference is listed here, and asserted.
"""

from __future__ import annotations

import base64
import copy
import json
import pathlib
from collections.abc import Callable, Iterator
from dataclasses import asdict
from datetime import timedelta
from typing import Any

import grpc
import pytest
from google.protobuf import json_format
from google.rpc import status_pb2

import couchbase.exceptions as E
import tests
from couchbase.auth import PasswordAuthenticator
from couchbase.common._observability.no_op import NoOpSpan
from couchbase.common._option_spec import BLOCKS
from couchbase.common.exceptions import _ExceptionInfo
from couchbase.common.n1ql import QueryStatus, _QueryMetaDataRecord
from couchbase.common.result import _MutationRecord, _MutationTokenRecord
from couchbase.common.warnings import CouchbaseApplicabilityWarning
from couchbase.mutation_state import MutationState
from couchbase.n1ql import QueryProfile, QueryScanConsistency
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.protocol.classic._error_mapping import ErrorMapper
from couchbase.protocol.classic._metadata import _query_metadata_from_core
from couchbase.protocol.classic._n1ql import N1QLQuery
from couchbase.protocol.classic._pycbc_core import pycbc_exception
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._error_mapping import map_status
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2
from couchbase.protocol.couchbase2._metadata import query_metadata
from couchbase.protocol.couchbase2._query import create_query_request
from couchbase.result import MutationResult
from couchbase.serializer import DefaultJsonSerializer
from tests.test_features import TestProtocol

# The suite needs no cluster: it asks each protocol's adapter itself.
pytestmark = pytest.mark.covers_protocols(TestProtocol.CLASSIC, TestProtocol.COUCHBASE2)

MetaData = query_pb2.QueryResponse.MetaData

_CASES: list[dict[str, Any]] = json.loads(
    pathlib.Path(tests.__file__).with_name('query_wire_shapes.json').read_text())['cases']
_METADATA_CASES = [case for case in _CASES if 'metadata' in case['classic']]
_ERROR_CASES = [case for case in _CASES if 'error' in case['classic']]

PROTOCOLS = ('classic', 'couchbase2')


class _CoreError(pycbc_exception):
    """A core error holding the five values the classic mapper reads from one."""

    def __init__(self, err: int, category: str, strerror: str, context: dict[str, Any] | None,
                 info: _ExceptionInfo | None) -> None:
        super().__init__()
        self._readings = (err, category, strerror, context, info)

    def err(self) -> int:
        return self._readings[0]

    def err_category(self) -> str:
        return self._readings[1]

    def strerror(self) -> str:
        return self._readings[2]

    def error_context(self) -> dict[str, Any] | None:
        return self._readings[3]

    def error_info(self) -> _ExceptionInfo | None:
        return self._readings[4]


# What each protocol's adapter is handed, built from a recorded side, and the adapter itself.

def _classic_wire(side: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(side['metadata'])


def _couchbase2_wire(side: dict[str, Any]) -> MetaData:
    return json_format.ParseDict(side['metadata'], MetaData())


def _couchbase2_adapt(raw: MetaData) -> _QueryMetaDataRecord | None:
    return query_metadata(query_pb2.QueryResponse(meta_data=raw))


_WIRE: dict[str, Callable[[dict[str, Any]], Any]] = {'classic': _classic_wire, 'couchbase2': _couchbase2_wire}
_ADAPT: dict[str, Callable[[Any], _QueryMetaDataRecord | None]] = {'classic': _query_metadata_from_core,
                                                                   'couchbase2': _couchbase2_adapt}


def _record(protocol: str, case: dict[str, Any]) -> _QueryMetaDataRecord:
    record = _ADAPT[protocol](_WIRE[protocol](case[protocol]))
    assert record is not None
    return record


def _classic_raises(side: dict[str, Any]) -> type[E.CouchbaseException]:
    return type(ErrorMapper.build_exception(_CoreError(**side['error'])))


def _couchbase2_raises(side: dict[str, Any]) -> type[E.CouchbaseException]:
    error = side['error']
    rich = None if error['rich_status'] is None else status_pb2.Status.FromString(
        base64.b64decode(error['rich_status']))
    return map_status(grpc.StatusCode[error['code']], error['details'], rich,
                      readonly=error['readonly']).exception_type


_RAISES: dict[str, Callable[[dict[str, Any]], type[E.CouchbaseException]]] = {'classic': _classic_raises,
                                                                              'couchbase2': _couchbase2_raises}

# The values each run sets for itself, so two runs never share them.
_PER_REQUEST = ('request_id', 'client_context_id')
_PER_REQUEST_METRICS = ('elapsed_time_ns', 'execution_time_ns')


def _comparable(record: _QueryMetaDataRecord) -> dict[str, Any]:
    view = asdict(record)
    view.update(dict.fromkeys(_PER_REQUEST))
    if view['metrics'] is not None:
        view['metrics'].update(dict.fromkeys(_PER_REQUEST_METRICS))
    # A profile holds timings and the host that served the query, so only its keys compare.
    if isinstance(view['profile'], dict):
        view['profile'] = sorted(view['profile'])
    return view


# Where the protocols are known to differ: a case's name, mapped to what each protocol does.  A case
# listed here must differ in exactly this way, so an entry that stops being true fails.
_ERROR_DIFFERENCES: dict[str, dict[str, type[E.CouchbaseException]]] = {
    # The gateway reports a statement it cannot parse as an invalid argument.
    'syntax error': {'classic': E.ParsingFailedException, 'couchbase2': E.InvalidArgumentException},
}


class RecordedShapesUnitTests:

    def test_every_case_was_recorded_for_every_protocol(self) -> None:
        for case in _CASES:
            assert set(case) >= set(PROTOCOLS), case['name']
            # A query fails on both protocols or on neither.
            assert {('metadata' in case[p]) for p in PROTOCOLS} in ({True}, {False}), case['name']

    def test_every_listed_difference_names_a_case(self) -> None:
        assert set(_ERROR_DIFFERENCES) <= {case['name'] for case in _ERROR_CASES}


class QueryMetadataConformanceUnitTests:

    @pytest.mark.parametrize('case', _METADATA_CASES, ids=[c['name'] for c in _METADATA_CASES])
    def test_the_protocols_build_equal_records(self, case: dict[str, Any]) -> None:
        assert _comparable(_record('classic', case)) == _comparable(_record('couchbase2', case))

    @pytest.mark.parametrize('protocol', PROTOCOLS)
    @pytest.mark.parametrize('case', _METADATA_CASES, ids=[c['name'] for c in _METADATA_CASES])
    def test_durations_are_whole_nanoseconds(self, protocol: str, case: dict[str, Any]) -> None:
        metrics = _record(protocol, case).metrics
        if metrics is not None:
            assert type(metrics.elapsed_time_ns) is int and metrics.elapsed_time_ns > 0
            assert type(metrics.execution_time_ns) is int and metrics.execution_time_ns > 0

    @pytest.mark.parametrize('protocol', PROTOCOLS)
    @pytest.mark.parametrize('case', _METADATA_CASES, ids=[c['name'] for c in _METADATA_CASES])
    def test_an_adapter_leaves_its_input_unchanged_and_builds_the_same_record_twice(self,
                                                                                    protocol: str,
                                                                                    case: dict[str, Any]) -> None:
        raw = _WIRE[protocol](case[protocol])
        before = copy.deepcopy(raw)
        first, second = _ADAPT[protocol](raw), _ADAPT[protocol](raw)
        assert raw == before
        assert first == second

    @pytest.mark.parametrize('protocol', PROTOCOLS)
    def test_the_per_request_values_are_carried(self, protocol: str) -> None:
        case = next(c for c in _METADATA_CASES if 'client_context_id' in c['options'])
        record = _record(protocol, case)
        assert record.client_context_id == case['options']['client_context_id']
        assert record.request_id

    @pytest.mark.parametrize('protocol', PROTOCOLS)
    def test_metrics_the_wire_leaves_out_read_as_zero(self, protocol: str) -> None:
        # Proto3 drops a zero count from the wire, and classic sends it.
        case = next(c for c in _METADATA_CASES if c['name'] == 'warning, key type')
        metrics = _record(protocol, case).metrics
        assert metrics is not None
        assert (metrics.mutation_count, metrics.sort_count, metrics.error_count) == (0, 0, 0)


def _first_with_metrics() -> dict[str, Any]:
    return next(c for c in _METADATA_CASES if c['classic']['metadata']['metrics'])


def _classic_status(raw: dict[str, Any], value: Any) -> None:
    if value is None:
        del raw['status']
    else:
        raw['status'] = value


def _couchbase2_status(raw: MetaData, value: Any) -> None:
    if value is None:
        raw.ClearField('status')
    else:
        raw.status = value


# Each edge case, as the change each protocol's wire makes for it, and what the record holds.
_EDGES: list[tuple[str, Callable[[dict[str, Any]], None], Callable[[MetaData], None],
                   Callable[[_QueryMetaDataRecord], object], object]] = [
    ('a status the sdk cannot name',
     lambda raw: _classic_status(raw, 'queued'), lambda raw: _couchbase2_status(raw, 99),
     lambda record: record.status, QueryStatus.UNKNOWN),
    ('no signature',
     lambda raw: raw.update(signature=None), lambda raw: raw.ClearField('signature'),
     lambda record: record.signature, None),
    ('no profile',
     lambda raw: raw.update(profile=None), lambda raw: raw.ClearField('profile'),
     lambda record: record.profile, None),
    ('no metrics',
     lambda raw: raw.update(metrics=None), lambda raw: raw.ClearField('metrics'),
     lambda record: record.metrics, None),
    ('no warnings',
     lambda raw: raw.update(warnings=None), lambda raw: raw.ClearField('warnings'),
     lambda record: record.warnings, ()),
]

# The edges where the protocols cannot agree, with what each reads.  An unset proto3 enum is its
# zero, and the zero of the gateway's status is RUNNING.  The gateway always sends a status.
_EDGE_DIFFERENCES: dict[str, dict[str, object]] = {
    'no status': {'classic': QueryStatus.UNKNOWN, 'couchbase2': QueryStatus.RUNNING},
}


class QueryMetadataEdgeUnitTests:

    @pytest.mark.parametrize('name, classic, couchbase2, read, expected', _EDGES, ids=[e[0] for e in _EDGES])
    @pytest.mark.parametrize('protocol', PROTOCOLS)
    def test_edge(self,
                  protocol: str,
                  name: str,
                  classic: Callable[[dict[str, Any]], None],
                  couchbase2: Callable[[MetaData], None],
                  read: Callable[[_QueryMetaDataRecord], object],
                  expected: object) -> None:
        raw = _WIRE[protocol](_first_with_metrics()[protocol])
        change: Callable[[Any], None] = classic if protocol == 'classic' else couchbase2
        change(raw)
        record = _ADAPT[protocol](raw)
        assert record is not None
        assert read(record) == expected

    @pytest.mark.parametrize('protocol', PROTOCOLS)
    def test_no_status(self, protocol: str) -> None:
        raw = _WIRE[protocol](_first_with_metrics()[protocol])
        clear: Callable[[Any, Any], None] = _classic_status if protocol == 'classic' else _couchbase2_status
        clear(raw, None)
        record = _ADAPT[protocol](raw)
        assert record is not None
        assert record.status is _EDGE_DIFFERENCES['no status'][protocol]

    @pytest.mark.parametrize('protocol', PROTOCOLS)
    def test_no_metadata_builds_no_record(self, protocol: str) -> None:
        if protocol == 'classic':
            assert _query_metadata_from_core(None) is None
        else:
            assert query_metadata(query_pb2.QueryResponse(rows=[b'{}'])) is None


class QueryErrorConformanceUnitTests:

    @pytest.mark.parametrize('case', _ERROR_CASES, ids=[c['name'] for c in _ERROR_CASES])
    def test_a_failed_query_raises_the_same_exception_unless_listed(self, case: dict[str, Any]) -> None:
        raised = {protocol: _RAISES[protocol](case[protocol]) for protocol in PROTOCOLS}
        listed = _ERROR_DIFFERENCES.get(case['name'])
        if listed is None:
            assert raised['classic'] is raised['couchbase2']
        else:
            assert raised == listed
            assert raised['classic'] is not raised['couchbase2']

    @pytest.mark.parametrize('case', _ERROR_CASES, ids=[c['name'] for c in _ERROR_CASES])
    def test_the_recorded_exception_is_the_mapped_one(self, case: dict[str, Any]) -> None:
        for protocol in PROTOCOLS:
            assert _RAISES[protocol](case[protocol]).__name__ == case[protocol]['raised'], protocol


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


class _RowSerializer(DefaultJsonSerializer):
    pass


def _state() -> MutationState:
    state = MutationState()
    state.add_results(MutationResult(_MutationRecord(cas=1, mutation_token=_MutationTokenRecord(7, 11, 13, 'b'))))
    return state


# A value for every query option, other than what the option means when it is left out.
_OPTION_SAMPLES: dict[str, Callable[[], Any]] = {
    'adhoc': lambda: False,
    'client_context_id': lambda: 'a-context-id',
    'consistent_with': _state,
    'flex_index': lambda: True,
    'max_parallelism': lambda: 4,
    'metrics': lambda: True,
    'named_parameters': lambda: {'a': 1},
    'parent_span': lambda: NoOpSpan('parent'),
    'pipeline_batch': lambda: 8,
    'pipeline_cap': lambda: 16,
    'positional_parameters': lambda: [1],
    'preserve_expiry': lambda: True,
    'profile': lambda: QueryProfile.TIMINGS,
    'query_context': lambda: '`b`.`s`',
    'raw': lambda: {'x': 1},
    'read_only': lambda: True,
    'scan_cap': lambda: 32,
    'scan_consistency': lambda: QueryScanConsistency.REQUEST_PLUS,
    'scan_wait': lambda: timedelta(milliseconds=1500),
    'send_to_node': lambda: 'node1',
    'serializer': _RowSerializer,
    'span': lambda: NoOpSpan('span'),
    'timeout': lambda: timedelta(seconds=3),
    'use_replica': lambda: True,
}


def _classic_encode(_: ClusterCore, opts: dict[str, Any]) -> object:
    return N1QLQuery.create_query_object('SELECT 1', QueryOptions(**opts)).params


def _couchbase2_encode(core: ClusterCore, opts: dict[str, Any]) -> object:
    request = create_query_request(core, 'SELECT 1', QueryOptions(**opts))
    if 'client_context_id' not in opts:
        # One is generated for every request that sets none.
        request.message.ClearField('client_context_id')
    return request


# Each protocol's query encoder.  Classic judges its options before they reach this encoder, so only
# couchbase2's is expected to warn.
_ENCODERS: list[tuple[str, Callable[[ClusterCore, dict[str, Any]], object], bool]] = [
    ('classic', _classic_encode, False),
    ('couchbase2', _couchbase2_encode, True),
]


def _inert(protocol: str) -> set[str]:
    return {key for key, spec in BLOCKS['QueryOptions'].keys.items() if protocol in spec.inert}


class QueryOptionsConformanceUnitTests:

    def test_every_option_has_a_sample(self) -> None:
        assert set(_OPTION_SAMPLES) == set(BLOCKS['QueryOptions'].keys)

    # `span` is the deprecated spelling of `parent_span`.
    @pytest.mark.filterwarnings('ignore:Option span is deprecated')
    @pytest.mark.parametrize('name, encode, warns', _ENCODERS, ids=[e[0] for e in _ENCODERS])
    def test_every_option_is_sent_unless_inert(self,
                                               core: ClusterCore,
                                               name: str,
                                               encode: Callable[[ClusterCore, dict[str, Any]], object],
                                               warns: bool) -> None:
        inert = _inert(name)
        baseline = encode(core, {})
        dropped, sent_anyway = [], []
        for key, sample in _OPTION_SAMPLES.items():
            if key not in inert:
                if encode(core, {key: sample()}) == baseline:
                    dropped.append(key)
                continue
            if warns:
                with pytest.warns(CouchbaseApplicabilityWarning, match=key):
                    encoded = encode(core, {key: sample()})
            else:
                encoded = encode(core, {key: sample()})
            if encoded != baseline:
                sent_anyway.append(key)
        assert (dropped, sent_anyway) == ([], [])
