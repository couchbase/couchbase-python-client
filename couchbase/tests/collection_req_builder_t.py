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

from datetime import timedelta
from typing import Any, cast

import pytest

import couchbase.subdocument as SD
from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import (PrefixScan,
                                     RangeScan,
                                     SamplingScan,
                                     ScanTerm)
from couchbase.mutation_state import MutationState
from couchbase.options import (AppendMultiOptions,
                               AppendOptions,
                               DecrementMultiOptions,
                               DecrementOptions,
                               GetMultiOptions,
                               GetOptions,
                               IncrementMultiOptions,
                               IncrementOptions,
                               InsertMultiOptions,
                               InsertOptions,
                               LookupInAllReplicasOptions,
                               LookupInAnyReplicaOptions,
                               LookupInOptions,
                               MutateInOptions,
                               PrependMultiOptions,
                               PrependOptions,
                               RemoveMultiOptions,
                               RemoveOptions,
                               ReplaceMultiOptions,
                               ReplaceOptions,
                               UnlockMultiOptions,
                               UnlockOptions,
                               UpsertMultiOptions,
                               UpsertOptions)
from couchbase.protocol.classic._collection_multi_req_builder import CollectionMultiRequestBuilder
from couchbase.protocol.classic._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.classic._collection_types import CollectionDetails
from couchbase.protocol.classic._pycbc_core import pycbc_connection
from couchbase.protocol.classic._pycbc_core import pycbc_kv_request as PycbcCoreKeyValueRequest
from couchbase.protocol.classic._range_scan import RangeScanStream
from couchbase.result import MutationToken
from couchbase.serializer import DefaultJsonSerializer
from couchbase.transcoder import (JSONTranscoder,
                                  LegacyTranscoder,
                                  RawBinaryTranscoder,
                                  RawJSONTranscoder,
                                  RawStringTranscoder,
                                  Transcoder)
from tests.test_features import TestProtocol

# What is checked here is classic's request builder, which writes what its binding reads.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

# (the scan, the tagged dict the binding layer reads it as).
SCAN_TYPES = [
    (RangeScan(),
     {'scan_type': 'range_scan'}),
    (RangeScan(ScanTerm('start'), ScanTerm('end', True)),
     {'scan_type': 'range_scan',
      'from': {'term': 'start', 'exclusive': None},
      'to': {'term': 'end', 'exclusive': True}}),
    (PrefixScan('prefix'),
     {'scan_type': 'prefix_scan', 'prefix': 'prefix'}),
    (SamplingScan(10),
     {'scan_type': 'sampling_scan', 'limit': 10}),
    (SamplingScan(10, 5),
     {'scan_type': 'sampling_scan', 'limit': 10, 'seed': 5}),
]


def _request_builder(transcoder: Transcoder | None = None,
                     kv_durable_timeout: int | None = None) -> CollectionRequestBuilder:
    return CollectionRequestBuilder(
        CollectionDetails('default', 'test-scope', 'test-collection', transcoder or JSONTranscoder(),
                          kv_durable_timeout=kv_durable_timeout))


def _durable_request(builder: CollectionRequestBuilder, op: str, **options: Any) -> PycbcCoreKeyValueRequest:
    if op == 'upsert':
        return builder.build_upsert_request('key', {'a': 1}, None, UpsertOptions(**options))
    if op == 'insert':
        return builder.build_insert_request('key', {'a': 1}, None, InsertOptions(**options))
    if op == 'replace':
        return builder.build_replace_request('key', {'a': 1}, None, ReplaceOptions(**options))
    if op == 'remove':
        return builder.build_remove_request('key', None, RemoveOptions(**options))
    if op == 'append':
        return builder.build_append_request('key', b'a', None, AppendOptions(**options))
    if op == 'prepend':
        return builder.build_prepend_request('key', b'a', None, PrependOptions(**options))
    if op == 'mutate_in':
        req, _ = builder.build_mutate_in_request('key', [SD.upsert('a', 1)], None, MutateInOptions(**options))
        return req
    if op == 'increment':
        return builder.build_increment_request('key', None, IncrementOptions(**options))
    return builder.build_decrement_request('key', None, DecrementOptions(**options))


DURABLE_OPS = ['upsert', 'insert', 'replace', 'remove', 'append', 'prepend', 'mutate_in', 'increment', 'decrement']


def _durable_multi_requests(builder: CollectionMultiRequestBuilder,
                            op: str,
                            **options: Any) -> list[PycbcCoreKeyValueRequest]:
    docs = {'k1': {'a': 1}, 'k2': {'a': 2}}
    blobs = {'k1': b'a', 'k2': b'b'}
    keys = list(docs)
    if op == 'upsert':
        multi = builder.build_upsert_multi_request(docs, None, UpsertMultiOptions(**options))
    elif op == 'insert':
        multi = builder.build_insert_multi_request(docs, None, InsertMultiOptions(**options))
    elif op == 'replace':
        multi = builder.build_replace_multi_request(docs, None, ReplaceMultiOptions(**options))
    elif op == 'remove':
        multi = builder.build_remove_multi_request(keys, None, RemoveMultiOptions(**options))
    elif op == 'append':
        multi = builder.build_append_multi_request(blobs, None, AppendMultiOptions(**options))
    elif op == 'prepend':
        multi = builder.build_prepend_multi_request(blobs, None, PrependMultiOptions(**options))
    elif op == 'increment':
        multi = builder.build_increment_multi_request(keys, None, IncrementMultiOptions(**options))
    else:
        multi = builder.build_decrement_multi_request(keys, None, DecrementMultiOptions(**options))
    return multi.request_list


DURABLE_MULTI_OPS = ['upsert', 'insert', 'replace', 'remove', 'append', 'prepend', 'increment', 'decrement']
MAJORITY = ServerDurability(DurabilityLevel.MAJORITY)


class _TaggingSerializer(DefaultJsonSerializer):
    """JSON with a marker, so a test can tell this serializer's output from the default's."""

    def serialize(self, value: Any) -> bytes:
        return super().serialize({'tagged': value})


def _spec_values(req: PycbcCoreKeyValueRequest) -> list[bytes | None]:
    assert req.specs is not None
    return [spec.get('value') for spec in req.specs]


# Every transcoder the SDK ships.  None of them should change what a subdocument op sends, since
# a subdocument value is always JSON.
TRANSCODERS = [JSONTranscoder(), LegacyTranscoder(), RawJSONTranscoder(), RawStringTranscoder(),
               RawBinaryTranscoder()]


class CollectionRequestBuilderUnitTests:
    # No cb_env and no TEST_MANIFEST: every test builds a `CollectionRequestBuilder` directly
    # and reads the scan arguments it would hand the core.  Not generated either, since all
    # three trees tag a scan through this one builder.
    #
    # `kv_range_scan_t` drives real scans, which reaches the tagging only for the types a
    # server accepts and not at all on a build without the feature.  The rejections below have
    # no live form: `scan()` never gets as far as a request.

    @pytest.mark.parametrize('scan, tagged', SCAN_TYPES)
    def test_scan_type_tagged_dict(self, scan: Any, tagged: dict[str, Any]) -> None:
        assert _request_builder()._get_scan_type(scan) == tagged

    def test_scan_type_rejects_an_unknown_scan(self) -> None:
        # The string is the tag `_get_scan_type` emits for a RangeScan, so this is the near
        # miss rather than an arbitrary bad value.
        with pytest.raises(InvalidArgumentException):
            _request_builder()._get_scan_type('range_scan')  # type: ignore[arg-type]

    @pytest.mark.parametrize('limit', [0, -10])
    def test_sampling_scan_rejects_a_non_positive_limit(self, limit: int) -> None:
        # `SamplingScan(0)` constructs; the limit is checked here, on the way out.
        with pytest.raises(InvalidArgumentException):
            _request_builder()._get_scan_type(SamplingScan(limit))

    def test_range_scan_stream_carries_the_tagged_scan_type(self) -> None:
        # The stream stores the connection and nothing here reads it, so None stands in for
        # one.  A cast rather than an ignore, because that is the whole claim.
        scan_args = _request_builder().build_range_scan_args(PrefixScan('prefix'))
        stream = RangeScanStream(cast(pycbc_connection, None), scan_args)

        assert stream._scan_args['scan_type'] == {'scan_type': 'prefix_scan', 'prefix': 'prefix'}
        # `scan_config` is a `_core` key the Python builder has never set.
        assert 'scan_config' not in stream._scan_args


class DurableTimeoutUnitTests:
    # The binding builds each request itself, so the core's durable default never reaches it:
    # a server durability with no timeout of its own has to be given the cluster's here, or it
    # goes out under the plain key-value timeout.

    @pytest.mark.parametrize('op', DURABLE_OPS)
    def test_server_durability_takes_the_cluster_durable_timeout(self, op: str) -> None:
        req = _durable_request(_request_builder(kv_durable_timeout=30000), op, durability=MAJORITY)

        assert req.timeout == 30000

    @pytest.mark.parametrize('op', DURABLE_OPS)
    def test_server_durability_defaults_to_ten_seconds(self, op: str) -> None:
        req = _durable_request(_request_builder(), op, durability=MAJORITY)

        assert req.timeout == 10000

    @pytest.mark.parametrize('op', DURABLE_OPS)
    def test_a_cluster_durable_timeout_of_zero_is_kept(self, op: str) -> None:
        req = _durable_request(_request_builder(kv_durable_timeout=0), op, durability=MAJORITY)

        assert req.timeout == 0

    @pytest.mark.parametrize('op', DURABLE_OPS)
    def test_an_operation_timeout_wins(self, op: str) -> None:
        req = _durable_request(_request_builder(kv_durable_timeout=30000), op,
                               durability=MAJORITY, timeout=timedelta(seconds=3))

        assert req.timeout == 3000

    @pytest.mark.parametrize('op', DURABLE_OPS)
    def test_no_durable_timeout_without_server_durability(self, op: str) -> None:
        builder = _request_builder(kv_durable_timeout=30000)
        client = ClientDurability(ReplicateTo.ONE, PersistTo.ONE)

        # An unset attribute on the core request is absent rather than None.
        assert not hasattr(_durable_request(builder, op), 'timeout')
        assert not hasattr(_durable_request(builder, op, durability=client), 'timeout')


class MultiDurableTimeoutUnitTests:
    # Each key's request of a multi operation goes out alone, so each needs the durable timeout.

    @staticmethod
    def _builder(kv_durable_timeout: int | None = None) -> CollectionMultiRequestBuilder:
        return CollectionMultiRequestBuilder(
            CollectionDetails('default', 'test-scope', 'test-collection', JSONTranscoder(),
                              kv_durable_timeout=kv_durable_timeout))

    @pytest.mark.parametrize('op', DURABLE_MULTI_OPS)
    def test_server_durability_takes_the_cluster_durable_timeout(self, op: str) -> None:
        reqs = _durable_multi_requests(self._builder(kv_durable_timeout=30000), op, durability=MAJORITY)

        assert [req.timeout for req in reqs] == [30000, 30000]

    @pytest.mark.parametrize('op', DURABLE_MULTI_OPS)
    def test_server_durability_defaults_to_ten_seconds(self, op: str) -> None:
        reqs = _durable_multi_requests(self._builder(), op, durability=MAJORITY)

        assert [req.timeout for req in reqs] == [10000, 10000]

    @pytest.mark.parametrize('op', DURABLE_MULTI_OPS)
    def test_a_cluster_durable_timeout_of_zero_is_kept(self, op: str) -> None:
        reqs = _durable_multi_requests(self._builder(kv_durable_timeout=0), op, durability=MAJORITY)

        assert [req.timeout for req in reqs] == [0, 0]

    @pytest.mark.parametrize('op', DURABLE_MULTI_OPS)
    def test_an_operation_timeout_wins(self, op: str) -> None:
        reqs = _durable_multi_requests(self._builder(kv_durable_timeout=30000), op,
                                       durability=MAJORITY, timeout=timedelta(seconds=3))

        assert [req.timeout for req in reqs] == [3000, 3000]

    @pytest.mark.parametrize('op', DURABLE_MULTI_OPS)
    def test_no_durable_timeout_without_server_durability(self, op: str) -> None:
        builder = self._builder(kv_durable_timeout=30000)
        client = ClientDurability(ReplicateTo.ONE, PersistTo.ONE)

        for reqs in (_durable_multi_requests(builder, op), _durable_multi_requests(builder, op, durability=client)):
            assert not any(hasattr(req, 'timeout') for req in reqs)


class MultiOptionsRequestBuilderUnitTests:

    @staticmethod
    def _builder() -> CollectionMultiRequestBuilder:
        return CollectionMultiRequestBuilder(
            CollectionDetails('default', 'test-scope', 'test-collection', JSONTranscoder()))

    @pytest.mark.parametrize('opts', [GetMultiOptions(project=['a']),
                                      GetMultiOptions(per_key_options={'k2': GetOptions(project=['a'])})],
                             ids=['for-all', 'per-key'])
    def test_a_multi_get_refuses_a_projection(self, opts: GetMultiOptions) -> None:
        with pytest.raises(InvalidArgumentException):
            self._builder().build_get_multi_request(['k1', 'k2'], None, opts)

    @pytest.mark.parametrize('opts', [GetMultiOptions(with_expiry=True),
                                      GetMultiOptions(per_key_options={'k2': GetOptions(with_expiry=True)})],
                             ids=['for-all', 'per-key'])
    def test_a_multi_get_drops_with_expiry_with_a_deprecation(self, opts: GetMultiOptions) -> None:
        with pytest.warns(CouchbaseDeprecationWarning, match='with_expiry'):
            multi = self._builder().build_get_multi_request(['k1', 'k2'], None, opts)

        assert not any(hasattr(req, 'with_expiry') for req in multi.request_list)

    def test_unlock_keeps_the_per_key_options_and_takes_the_cas_from_the_keys(self) -> None:
        opts = UnlockMultiOptions(per_key_options={'k2': UnlockOptions(timeout=timedelta(seconds=3))})

        multi = self._builder().build_unlock_multi_request({'k1': 11, 'k2': 22}, None, opts)

        assert [req.cas for req in multi.request_list] == [11, 22]
        assert not hasattr(multi.request_list[0], 'timeout')
        assert multi.request_list[1].timeout == 3000

    def test_unlock_leaves_the_callers_per_key_options_alone(self) -> None:
        per_key = {'k1': UnlockOptions(timeout=timedelta(seconds=3))}

        self._builder().build_unlock_multi_request({'k1': 11}, None, UnlockMultiOptions(per_key_options=per_key))

        assert per_key == {'k1': {'timeout': timedelta(seconds=3)}}


class RangeScanRequestBuilderUnitTests:
    # Regression coverage for the consistent_with serialization bug (PYCBC-1778).  No cb_env:
    # nothing here talks to a cluster.

    @staticmethod
    def _mutation_state(count: int) -> MutationState:
        state = MutationState()
        for i in range(count):
            state.add_mutation_token(MutationToken({'partition_id': i,
                                                    'partition_uuid': 1000 + i,
                                                    'sequence_number': 1,
                                                    'bucket_name': 'default'}))
        return state

    def test_consistent_with_serialized_as_mutation_state(self) -> None:
        scan_args = _request_builder().build_range_scan_args(PrefixScan('prefix'),
                                                             consistent_with=self._mutation_state(3),
                                                             concurrency=1)

        # Must be a mutation_state dict ({'tokens': [...]}), not a bare list, or the C++
        # binding drops every token.
        consistent_with = scan_args['orchestrator_options']['consistent_with']
        assert isinstance(consistent_with, dict)
        assert list(consistent_with.keys()) == ['tokens']

        tokens = consistent_with['tokens']
        assert isinstance(tokens, list)
        assert len(tokens) == 3
        for token in tokens:
            assert set(token.keys()) == {'partition_id', 'partition_uuid',
                                         'sequence_number', 'bucket_name'}

    def test_empty_consistent_with_raises(self) -> None:
        with pytest.raises(InvalidArgumentException):
            _request_builder().build_range_scan_args(PrefixScan('prefix'), consistent_with=MutationState())

    def test_zero_concurrency_raises(self) -> None:
        opts = {'concurrency': 0}
        with pytest.raises(InvalidArgumentException):
            _request_builder()._process_scan_orchestrator_ops(opts)


class SubdocRequestBuilderUnitTests:
    # No cb_env: each test builds a subdocument request and reads the encoded spec values, or
    # the serializer handed back for the result, without sending anything.

    @pytest.mark.parametrize('transcoder', TRANSCODERS, ids=lambda t: type(t).__name__)
    def test_mutate_in_values_are_json_whatever_the_default_transcoder(self, transcoder: Transcoder) -> None:
        specs = (SD.upsert('a', 'text'),
                 SD.upsert('b', '123'),
                 SD.replace('c', {'x': [1, 2]}),
                 SD.increment('d', 5),
                 SD.array_append('e', 1, 'two'))
        req, _ = _request_builder(transcoder).build_mutate_in_request('k', specs, None)

        # A string is quoted, so '123' stays a string rather than becoming a number, and an
        # array op carries its values without the enclosing brackets.
        assert _spec_values(req) == [b'"text"', b'"123"', b'{"x": [1, 2]}', b'5', b'1, "two"']

    def test_mutate_in_uses_the_operation_serializer(self) -> None:
        specs = (SD.upsert('a', 'text'), SD.array_prepend('b', 1))
        options = MutateInOptions(serializer=_TaggingSerializer())
        req, _ = _request_builder().build_mutate_in_request('k', specs, None, options)

        # `{"tagged": [1]}` with its first and last byte dropped: what the serializer returns is
        # what is sent, brackets or not.
        assert _spec_values(req) == [b'{"tagged": "text"}', b'"tagged": [1]']

    def test_mutate_in_takes_its_specs_from_a_generator(self) -> None:
        specs = (spec for spec in (SD.upsert('a', 'text'), SD.replace('b', 1)))
        # preserve_expiry is what makes the builder read the specs twice, once to check it against them
        req, _ = _request_builder().build_mutate_in_request('k', specs, None, MutateInOptions(preserve_expiry=True))

        assert _spec_values(req) == [b'"text"', b'1']

    def test_mutate_in_drops_a_transcoder_it_does_not_declare(self) -> None:
        with pytest.warns(match='transcoder is not carried by MutateInOptions'):
            req, _ = _request_builder().build_mutate_in_request('k', (SD.upsert('a', 'text'),), None,
                                                                transcoder=LegacyTranscoder())

        assert _spec_values(req) == [b'"text"']

    def test_mutate_in_returns_the_serializer_its_result_decodes_with(self) -> None:
        builder = _request_builder(RawBinaryTranscoder())
        _, default = builder.build_mutate_in_request('k', (SD.increment('a', 1),), None)
        serializer = _TaggingSerializer()
        _, chosen = builder.build_mutate_in_request('k', (SD.increment('a', 1),), None,
                                                    MutateInOptions(serializer=serializer))

        assert type(default) is DefaultJsonSerializer
        assert chosen is serializer

    @pytest.mark.parametrize('build, options', [
        ('build_lookup_in_request', LookupInOptions),
        ('build_lookup_in_any_replica_request', LookupInAnyReplicaOptions),
        ('build_lookup_in_all_replicas_request', LookupInAllReplicasOptions),
    ])
    def test_lookup_in_returns_the_serializer_its_result_decodes_with(self, build: str, options: Any) -> None:
        builder = _request_builder(RawBinaryTranscoder())
        _, default = getattr(builder, build)('k', (SD.get('a'),), None)
        serializer = _TaggingSerializer()
        # The option is consumed here; it is not something the core request has an attribute for.
        _, chosen = getattr(builder, build)('k', (SD.get('a'),), None, options(serializer=serializer))

        assert type(default) is DefaultJsonSerializer
        assert chosen is serializer
