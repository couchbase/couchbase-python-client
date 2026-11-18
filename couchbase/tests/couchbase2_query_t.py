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

import json
import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.duration_pb2 import Duration

from couchbase.auth import PasswordAuthenticator
from couchbase.common._query_args import query_args
from couchbase.common.result import _MutationRecord, _MutationTokenRecord
from couchbase.common.warnings import CouchbaseApplicabilityWarning
from couchbase.exceptions import InvalidArgumentException
from couchbase.mutation_state import MutationState
from couchbase.n1ql import (QueryMetaData,
                            QueryProfile,
                            QueryScanConsistency,
                            QueryStatus)
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._generated.query.v1 import query_pb2
from couchbase.protocol.couchbase2._metadata import query_metadata
from couchbase.protocol.couchbase2._query import create_query_request, query_message
from couchbase.protocol.couchbase2._scope_req_builder import ScopeRequestBuilder
from couchbase.result import MutationResult

Request = query_pb2.QueryRequest
MetaData = query_pb2.QueryResponse.MetaData


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


_INERT = [{'raw': {'x': 1}}, {'use_replica': True}, {'send_to_node': 'node1'}]


def _request(*options: object, **kwargs: Any) -> query_pb2.QueryRequest:
    return query_message('SELECT 1', query_args(QueryOptions, options, kwargs))


def _scope_request(core: ClusterCore, *options: object, **kwargs: Any) -> query_pb2.QueryRequest:
    return ScopeRequestBuilder(core, 'travel', 'inventory').build_query_request('SELECT 1', *options, **kwargs).message


def _state(*tokens: tuple[int, int, int, str]) -> MutationState:
    state = MutationState()
    for partition_id, partition_uuid, sequence_number, bucket_name in tokens:
        record = _MutationRecord(cas=1, mutation_token=_MutationTokenRecord(partition_id, partition_uuid,
                                                                            sequence_number, bucket_name))
        state.add_results(MutationResult(record))
    return state


class QueryRequestUnitTests:

    def test_carries_the_statement_and_a_generated_client_context_id(self) -> None:
        request = _request()
        assert request.statement == 'SELECT 1'
        uuid.UUID(request.client_context_id)

    def test_carries_the_callers_client_context_id(self) -> None:
        assert _request(client_context_id='mine').client_context_id == 'mine'

    def test_a_cluster_level_query_names_no_keyspace(self) -> None:
        request = _request()
        assert not request.HasField('bucket_name') and not request.HasField('scope_name')

    @pytest.mark.parametrize('context', ['`travel`.`inventory`', 'default:`travel`.`inventory`', 'travel.inventory',
                                         'default:travel.inventory', 'travel.`inventory`', '`travel`.inventory'])
    def test_a_query_context_names_the_keyspace(self, context: str) -> None:
        request = _request(query_context=context)
        assert (request.bucket_name, request.scope_name) == ('travel', 'inventory')

    @pytest.mark.parametrize('context', ['travel.sample.inventory', '`travel.sample`.inventory'])
    def test_a_bare_context_splits_at_its_last_dot(self, context: str) -> None:
        request = _request(query_context=context)
        assert (request.bucket_name, request.scope_name) == ('travel.sample', 'inventory')

    @pytest.mark.parametrize('context', ['travel', '`travel`', '`a`.`b`.`c`', '`travel`.`inventory'])
    def test_a_query_context_that_is_not_a_bucket_and_scope_names_neither(self, context: str) -> None:
        request = _request(query_context=context)
        assert not request.HasField('bucket_name') and not request.HasField('scope_name')

    def test_flags_are_sent_only_when_given(self) -> None:
        request = _request()
        for name in ('read_only', 'flex_index', 'preserve_expiry', 'prepared'):
            assert not request.HasField(name), name
        request = _request(read_only=True, flex_index=True, preserve_expiry=False)
        assert (request.read_only, request.flex_index, request.preserve_expiry) == (True, True, False)

    def test_not_adhoc_asks_for_a_prepared_statement(self) -> None:
        assert _request(adhoc=False).prepared is True
        assert not _request(adhoc=True).HasField('prepared')

    @pytest.mark.parametrize('value, wire', [
        (QueryScanConsistency.NOT_BOUNDED, Request.SCAN_CONSISTENCY_NOT_BOUNDED),
        (QueryScanConsistency.REQUEST_PLUS, Request.SCAN_CONSISTENCY_REQUEST_PLUS),
        ('request_plus', Request.SCAN_CONSISTENCY_REQUEST_PLUS),
    ])
    def test_scan_consistency(self, value: object, wire: int) -> None:
        assert _request(scan_consistency=value).scan_consistency == wire

    @pytest.mark.parametrize('value', [QueryScanConsistency.AT_PLUS, 'eventually'])
    def test_a_scan_consistency_that_cannot_be_sent_is_refused(self, value: object) -> None:
        with pytest.raises(InvalidArgumentException):
            _request(scan_consistency=value)

    def test_consistent_with_sends_the_tokens(self) -> None:
        request = _request(consistent_with=_state((7, 11, 13, 'travel')))
        assert not request.HasField('scan_consistency')
        assert [(t.vbucket_id, t.vbucket_uuid, t.seq_no, t.bucket_name) for t in request.consistent_with] == [
            (7, 11, 13, 'travel')]

    @pytest.mark.parametrize('kwargs', [{'consistent_with': MutationState()},
                                        {'consistent_with': 'not a state'},
                                        {'consistent_with': _state((7, 11, 13, 'travel')),
                                         'scan_consistency': QueryScanConsistency.REQUEST_PLUS}])
    def test_a_consistent_with_that_cannot_be_sent_is_refused(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(InvalidArgumentException):
            _request(**kwargs)

    @pytest.mark.parametrize('value, wire', [(QueryProfile.OFF, Request.PROFILE_MODE_OFF),
                                             (QueryProfile.PHASES, Request.PROFILE_MODE_PHASES),
                                             ('timings', Request.PROFILE_MODE_TIMINGS)])
    def test_profile(self, value: object, wire: int) -> None:
        assert _request(profile=value).profile_mode == wire

    def test_parameters_are_sent_as_json(self) -> None:
        request = _request(QueryOptions(positional_parameters=['Paris', 3, None, {'a': [1]}]))
        assert [json.loads(p) for p in request.positional_parameters] == ['Paris', 3, None, {'a': [1]}]
        request = query_message('SELECT 1', query_args(QueryOptions, (), {'city': 'Paris', '$limit': 3}))
        assert {k: json.loads(v) for k, v in request.named_parameters.items()} == {'city': 'Paris', 'limit': 3}

    def test_metrics_are_off_unless_asked_for(self) -> None:
        assert _request().tuning_options.disable_metrics is True
        assert _request(metrics=True).tuning_options.disable_metrics is False

    def test_tuning(self) -> None:
        tuning = _request(max_parallelism=4, pipeline_batch=8, pipeline_cap=16, scan_cap=32,
                          scan_wait=timedelta(milliseconds=1500)).tuning_options
        assert (tuning.max_parallelism, tuning.pipeline_batch, tuning.pipeline_cap, tuning.scan_cap) == (4, 8, 16, 32)
        assert tuning.scan_wait == Duration(seconds=1, nanos=500_000_000)
        assert not _request().tuning_options.HasField('scan_wait')

    @pytest.mark.parametrize('kwargs', [{'max_parallelism': 2**32}, {'scan_cap': -1}, {'pipeline_cap': '4'},
                                        {'scan_wait': 1.5}])
    def test_tuning_that_does_not_fit_is_refused(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(InvalidArgumentException):
            _request(**kwargs)

    @pytest.mark.parametrize('kwargs', _INERT)
    def test_options_the_gateway_cannot_express_warn_and_are_not_sent(self,
                                                                      core: ClusterCore,
                                                                      kwargs: dict[str, Any]) -> None:
        with pytest.warns(CouchbaseApplicabilityWarning, match=next(iter(kwargs))):
            request = create_query_request(core, 'SELECT 1', QueryOptions(client_context_id='c', **kwargs))
        assert request.message == create_query_request(core, 'SELECT 1', QueryOptions(client_context_id='c')).message

    @pytest.mark.parametrize('kwargs', _INERT)
    def test_strict_validation_refuses_them(self, kwargs: dict[str, Any]) -> None:
        strict = ClusterCore('couchbase2://localhost',
                             ClusterOptions(PasswordAuthenticator('Administrator', 'password'),
                                            option_validation='strict'))
        try:
            with pytest.raises(InvalidArgumentException, match=next(iter(kwargs))):
                create_query_request(strict, 'SELECT 1', QueryOptions(**kwargs))
        finally:
            strict.close()


class ScopeRequestBuilderUnitTests:

    def test_a_query_runs_in_the_scope(self, core: ClusterCore) -> None:
        request = _scope_request(core)
        assert (request.bucket_name, request.scope_name) == ('travel', 'inventory')

    @pytest.mark.parametrize('as_option', [False, True])
    def test_the_callers_query_context_wins(self, core: ClusterCore, as_option: bool) -> None:
        context = '`other`.`scope`'
        if as_option:
            request = _scope_request(core, QueryOptions(query_context=context))
        else:
            request = _scope_request(core, query_context=context)
        assert (request.bucket_name, request.scope_name) == ('other', 'scope')

    def test_a_callers_context_that_is_not_a_bucket_and_scope_names_neither(self, core: ClusterCore) -> None:
        request = _scope_request(core, query_context='travel')
        assert not request.HasField('bucket_name') and not request.HasField('scope_name')


class QueryMetadataUnitTests:

    def test_a_message_without_metadata_has_none(self) -> None:
        assert query_metadata(query_pb2.QueryResponse(rows=[b'{}'])) is None

    def test_reads_as_query_metadata(self) -> None:
        raw = MetaData(request_id='r-1', client_context_id='c-1', status=MetaData.STATUS_SUCCESS,
                       signature=b'{"$1":"number"}', profile=b'{"phaseTimes":{}}',
                       warnings=[MetaData.Warning(code=1080, message='slow')],
                       metrics=MetaData.Metrics(elapsed_time=Duration(seconds=1, nanos=5),
                                                execution_time=Duration(nanos=900),
                                                result_count=1, result_size=2, mutation_count=3, sort_count=4,
                                                error_count=5, warning_count=6))
        record = query_metadata(query_pb2.QueryResponse(meta_data=raw))
        assert record is not None
        metadata = QueryMetaData(record)
        assert (metadata.request_id(), metadata.client_context_id(), metadata.status()) == (
            'r-1', 'c-1', QueryStatus.SUCCESS)
        assert metadata.signature() == {'$1': 'number'}
        assert metadata.profile() == {'phaseTimes': {}}
        assert [(w.code(), w.message()) for w in metadata.warnings()] == [(1080, 'slow')]
        assert metadata.errors() == []
        metrics = metadata.metrics()
        assert metrics is not None
        assert metrics.elapsed_time() == timedelta(seconds=1)
        assert record.metrics is not None and record.metrics.elapsed_time_ns == 1_000_000_005
        assert record.metrics.execution_time_ns == 900
        assert (metrics.result_count(), metrics.result_size(), metrics.mutation_count(), metrics.sort_count(),
                metrics.error_count(), metrics.warning_count()) == (1, 2, 3, 4, 5, 6)

    def test_what_the_gateway_leaves_out_reads_as_absent(self) -> None:
        record = query_metadata(query_pb2.QueryResponse(meta_data=MetaData(request_id='r-1')))
        assert record is not None
        assert (record.signature, record.profile, record.metrics, record.warnings) == (None, None, None, ())

    @pytest.mark.parametrize('status, expected', [(MetaData.STATUS_RUNNING, QueryStatus.RUNNING),
                                                  (MetaData.STATUS_TIMEOUT, QueryStatus.TIMEOUT),
                                                  (MetaData.STATUS_UNKNOWN, QueryStatus.UNKNOWN),
                                                  (99, QueryStatus.UNKNOWN)])
    def test_status(self, status: Any, expected: QueryStatus) -> None:
        record = query_metadata(query_pb2.QueryResponse(meta_data=MetaData(status=status)))
        assert record is not None and record.status is expected
