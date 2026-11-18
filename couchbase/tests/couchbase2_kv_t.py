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

from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from google.rpc import (code_pb2,
                        error_details_pb2,
                        status_pb2)

import couchbase.subdocument as SD
from couchbase.auth import PasswordAuthenticator
from couchbase.common._observability import KeyspaceDetails
from couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  ReplicateTo,
                                  ServerDurability)
from couchbase.exceptions import (FeatureUnavailableException,
                                  InternalServerFailureException,
                                  InvalidArgumentException,
                                  PathMismatchException,
                                  PathNotFoundException)
from couchbase.options import (ClusterOptions,
                               DeltaValue,
                               SignedInt64)
from couchbase.protocol.couchbase2._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.couchbase2._collection_types import (KvRequest,
                                                             LookupInRequest,
                                                             MutateInRequest,
                                                             counter_record,
                                                             exists_record,
                                                             get_record,
                                                             lookup_in_record,
                                                             mutate_in_record,
                                                             mutation_record,
                                                             xattrs_first)
from couchbase.protocol.couchbase2._core import ClusterCore
from couchbase.protocol.couchbase2._generated.kv.v1 import kv_pb2
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetResult,
                              LookupInResult,
                              MutateInResult,
                              MutationResult)
from couchbase.transcoder import JSONTranscoder

LOOKUP = kv_pb2.LookupInRequest.Spec
MUTATE = kv_pb2.MutateInRequest.Spec
STORE = kv_pb2.MutateInRequest

KEYSPACE: KeyspaceDetails = {'bucket_name': 'default', 'scope_name': 'inventory', 'collection_name': 'hotels'}


@pytest.fixture(scope='module')
def core() -> Iterator[ClusterCore]:
    built = ClusterCore('couchbase2://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    try:
        yield built
    finally:
        built.close()


def _get(core: ClusterCore, **kwargs: Any) -> kv_pb2.GetRequest:
    return CollectionRequestBuilder(core, KEYSPACE).build_get_request('hotel_1', None, **kwargs).message


def _upsert(core: ClusterCore, **kwargs: Any) -> kv_pb2.UpsertRequest:
    return CollectionRequestBuilder(core, KEYSPACE).build_upsert_request('hotel_1', {'a': 1}, None, **kwargs).message


def _builder(core: ClusterCore) -> CollectionRequestBuilder:
    return CollectionRequestBuilder(core, KEYSPACE)


def _lookup_in(core: ClusterCore, *specs: SD.Spec, **kwargs: Any) -> LookupInRequest:
    return CollectionRequestBuilder(core, KEYSPACE).build_lookup_in_request('hotel_1', specs, None, **kwargs)


def _mutate_in(core: ClusterCore, *specs: SD.Spec, **kwargs: Any) -> MutateInRequest:
    return CollectionRequestBuilder(core, KEYSPACE).build_mutate_in_request('hotel_1', specs, None, **kwargs)


def _not_found(path: str) -> status_pb2.Status:
    status = status_pb2.Status(code=code_pb2.NOT_FOUND, message=f'Subdocument path {path} was not found.')
    status.details.add().Pack(error_details_pb2.ResourceInfo(resource_type='path', resource_name=path))
    return status


class GetRequestUnitTests:

    def test_names_the_document(self, core: ClusterCore) -> None:
        request = _get(core)
        assert (request.bucket_name, request.scope_name, request.collection_name, request.key) == (
            'default', 'inventory', 'hotels', 'hotel_1')
        assert list(request.project) == []

    def test_does_not_ask_for_compression(self, core: ClusterCore) -> None:
        assert not _get(core).HasField('compression')

    def test_carries_the_projection(self, core: ClusterCore) -> None:
        assert list(_get(core, project=['name', 'geo.lat']).project) == ['name', 'geo.lat']

    @pytest.mark.parametrize('project', ['name', ['name', 1], ('name',)])
    def test_a_projection_that_is_not_a_list_of_strings_is_refused(self, core: ClusterCore, project: Any) -> None:
        with pytest.raises(InvalidArgumentException, match='Project must be a list of strings'):
            _get(core, project=project)


class GetRecordUnitTests:

    def test_reads_as_a_get_result(self) -> None:
        response = kv_pb2.GetResponse(content_uncompressed=b'{"name":"Ritz"}', content_flags=0x02000006, cas=42)
        result = GetResult(get_record(response), transcoder=JSONTranscoder(), key='hotel_1')
        assert result.content_as[dict] == {'name': 'Ritz'}
        assert (result.cas, result.flags, result.key) == (42, 0x02000006, 'hotel_1')

    def test_an_empty_document(self) -> None:
        response = kv_pb2.GetResponse(content_uncompressed=b'', cas=1)
        assert get_record(response).value == b''

    def test_the_expiry_is_carried_only_when_asked_for(self) -> None:
        response = kv_pb2.GetResponse(content_uncompressed=b'{}', cas=1, expiry=Timestamp(seconds=1_900_000_000))
        assert get_record(response).expiry is None
        assert get_record(response, with_expiry=True).expiry == 1_900_000_000
        assert GetResult(get_record(response, with_expiry=True)).expiry_time == datetime.fromtimestamp(1_900_000_000)

    def test_a_document_with_no_expiry(self) -> None:
        response = kv_pb2.GetResponse(content_uncompressed=b'{}', cas=1, expiry=Timestamp(seconds=0))
        assert GetResult(get_record(response, with_expiry=True)).expiry_time is None

    def test_a_null_document_stays_null(self) -> None:
        response = kv_pb2.GetResponse(content_uncompressed=b'null', content_flags=0x02000006, cas=1)
        assert GetResult(get_record(response), transcoder=JSONTranscoder()).value is None

    def test_no_content_is_a_server_failure(self) -> None:
        with pytest.raises(InternalServerFailureException):
            get_record(kv_pb2.GetResponse(cas=1))

    def test_compressed_content_is_a_server_failure(self) -> None:
        with pytest.raises(InternalServerFailureException):
            get_record(kv_pb2.GetResponse(content_compressed=b'\x00', cas=1))


class UpsertRequestUnitTests:

    def test_names_the_document_and_carries_its_content(self, core: ClusterCore) -> None:
        request = _upsert(core)
        assert (request.bucket_name, request.scope_name, request.collection_name, request.key) == (
            'default', 'inventory', 'hotels', 'hotel_1')
        assert request.WhichOneof('content') == 'content_uncompressed'
        assert (request.content_uncompressed, request.content_flags) == JSONTranscoder().encode_value({'a': 1})

    def test_no_expiry_is_an_explicit_zero(self, core: ClusterCore) -> None:
        request = _upsert(core)
        assert request.WhichOneof('expiry') == 'expiry_secs'
        assert request.expiry_secs == 0
        assert not request.HasField('preserve_expiry_on_existing')

    @pytest.mark.parametrize('expiry, seconds', [(timedelta(seconds=10), 10),
                                                 (timedelta(days=30), 2_592_000),
                                                 (timedelta(days=31), 2_678_400),
                                                 (timedelta(0), 0)])
    def test_an_expiry_is_always_a_duration(self, core: ClusterCore, expiry: timedelta, seconds: int) -> None:
        request = _upsert(core, expiry=expiry)
        assert request.WhichOneof('expiry') == 'expiry_secs'
        assert request.expiry_secs == seconds

    def test_preserving_with_no_expiry_leaves_the_expiry_out(self, core: ClusterCore) -> None:
        for expiry in (None, timedelta(0)):
            request = _upsert(core, expiry=expiry, preserve_expiry=True)
            assert request.WhichOneof('expiry') is None
            assert not request.HasField('preserve_expiry_on_existing')

    def test_preserving_with_an_expiry_applies_it_to_a_new_document(self, core: ClusterCore) -> None:
        request = _upsert(core, expiry=timedelta(minutes=5), preserve_expiry=True)
        assert request.expiry_secs == 300
        assert request.preserve_expiry_on_existing is True

    @pytest.mark.parametrize('expiry', [timedelta(seconds=-1), timedelta(seconds=2**32), 10])
    def test_an_expiry_out_of_range_or_not_a_timedelta_is_refused(self, core: ClusterCore, expiry: object) -> None:
        with pytest.raises(InvalidArgumentException):
            _upsert(core, expiry=expiry)

    @pytest.mark.parametrize('level, wire', [
        (DurabilityLevel.MAJORITY, kv_pb2.DURABILITY_LEVEL_MAJORITY),
        (DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE, kv_pb2.DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE),
        (DurabilityLevel.PERSIST_TO_MAJORITY, kv_pb2.DURABILITY_LEVEL_PERSIST_TO_MAJORITY),
    ])
    def test_server_durability(self, core: ClusterCore, level: DurabilityLevel, wire: int) -> None:
        assert _upsert(core, durability=ServerDurability(level)).durability_level == wire

    def test_no_durability_leaves_the_level_out(self, core: ClusterCore) -> None:
        assert not _upsert(core).HasField('durability_level')
        assert not _upsert(core, durability=ServerDurability(DurabilityLevel.NONE)).HasField('durability_level')

    def test_client_durability_is_unavailable(self, core: ClusterCore) -> None:
        with pytest.raises(FeatureUnavailableException, match='ServerDurability'):
            _upsert(core, durability=ClientDurability(replicate_to=ReplicateTo.ONE))

    def test_a_durability_that_is_neither_is_refused(self, core: ClusterCore) -> None:
        with pytest.raises(InvalidArgumentException):
            _upsert(core, durability=DurabilityLevel.MAJORITY)


class MutationRecordUnitTests:

    def test_reads_as_a_mutation_result_with_its_token(self) -> None:
        token = kv_pb2.MutationToken(bucket_name='default', vbucket_id=7, vbucket_uuid=123456789, seq_no=99)
        result = MutationResult(mutation_record(kv_pb2.UpsertResponse(cas=55, mutation_token=token)), key='hotel_1')
        assert result.cas == 55
        mutation_token = result.mutation_token()
        assert mutation_token is not None
        assert mutation_token.as_tuple() == (7, 123456789, 99, 'default')

    def test_no_token(self) -> None:
        assert MutationResult(mutation_record(kv_pb2.UpsertResponse(cas=55))).mutation_token() is None


# Each builder whose request carries a durability level, as a call taking the options.
_DURABLE: dict[str, Callable[..., KvRequest[Any]]] = {
    'insert': lambda core, **kw: _builder(core).build_insert_request('hotel_1', {'a': 1}, None, **kw),
    'upsert': lambda core, **kw: _builder(core).build_upsert_request('hotel_1', {'a': 1}, None, **kw),
    'replace': lambda core, **kw: _builder(core).build_replace_request('hotel_1', {'a': 1}, None, **kw),
    'remove': lambda core, **kw: _builder(core).build_remove_request('hotel_1', None, **kw),
    'increment': lambda core, **kw: _builder(core).build_increment_request('hotel_1', None, **kw),
    'decrement': lambda core, **kw: _builder(core).build_decrement_request('hotel_1', None, **kw),
    'append': lambda core, **kw: _builder(core).build_append_request('hotel_1', b'x', None, **kw),
    'prepend': lambda core, **kw: _builder(core).build_prepend_request('hotel_1', b'x', None, **kw),
}


class DurabilityUnitTests:

    @pytest.mark.parametrize('op', sorted(_DURABLE))
    def test_server_durability_and_its_timeout(self, core: ClusterCore, op: str) -> None:
        request = _DURABLE[op](core, durability=ServerDurability(DurabilityLevel.MAJORITY))
        assert request.message.durability_level == kv_pb2.DURABILITY_LEVEL_MAJORITY
        assert request.timeout == core.kv_durable_timeout

    @pytest.mark.parametrize('op', sorted(_DURABLE))
    def test_no_durability_leaves_the_level_out(self, core: ClusterCore, op: str) -> None:
        request = _DURABLE[op](core)
        assert not request.message.HasField('durability_level')
        assert request.timeout == core.kv_timeout

    @pytest.mark.parametrize('op', sorted(_DURABLE))
    def test_client_durability_is_unavailable(self, core: ClusterCore, op: str) -> None:
        with pytest.raises(FeatureUnavailableException, match='ServerDurability'):
            _DURABLE[op](core, durability=ClientDurability(replicate_to=ReplicateTo.ONE))


class InsertRequestUnitTests:

    def test_carries_the_content_and_an_explicit_zero_expiry(self, core: ClusterCore) -> None:
        request = _builder(core).build_insert_request('hotel_1', {'a': 1}, None).message
        assert request.key == 'hotel_1'
        assert (request.content_uncompressed, request.content_flags) == JSONTranscoder().encode_value({'a': 1})
        assert request.WhichOneof('expiry') == 'expiry_secs'
        assert request.expiry_secs == 0

    def test_an_expiry_is_a_duration(self, core: ClusterCore) -> None:
        request = _builder(core).build_insert_request('hotel_1', {}, None, expiry=timedelta(days=31)).message
        assert request.expiry_secs == 2_678_400


class ReplaceRequestUnitTests:

    def _replace(self, core: ClusterCore, **kwargs: Any) -> kv_pb2.ReplaceRequest:
        return _builder(core).build_replace_request('hotel_1', {'a': 1}, None, **kwargs).message

    def test_no_expiry_is_an_explicit_zero(self, core: ClusterCore) -> None:
        request = self._replace(core)
        assert request.WhichOneof('expiry') == 'expiry_secs'
        assert request.expiry_secs == 0

    def test_preserving_leaves_the_expiry_out(self, core: ClusterCore) -> None:
        assert self._replace(core, preserve_expiry=True).WhichOneof('expiry') is None

    def test_an_expiry_with_preserving_is_refused(self, core: ClusterCore) -> None:
        with pytest.raises(InvalidArgumentException, match='expiry and preserve_expiry'):
            self._replace(core, expiry=timedelta(seconds=10), preserve_expiry=True)

    def test_a_cas_of_zero_is_no_cas(self, core: ClusterCore) -> None:
        assert self._replace(core, cas=42).cas == 42
        assert not self._replace(core, cas=0).HasField('cas')


class RemoveRequestUnitTests:

    def test_names_the_document_and_carries_the_cas(self, core: ClusterCore) -> None:
        request = _builder(core).build_remove_request('hotel_1', None, cas=42).message
        assert (request.bucket_name, request.scope_name, request.collection_name, request.key, request.cas) == (
            'default', 'inventory', 'hotels', 'hotel_1', 42)
        assert not _builder(core).build_remove_request('hotel_1', None).message.HasField('cas')


class ExpiryRequestUnitTests:

    @pytest.mark.parametrize('expiry, seconds', [(timedelta(seconds=10), 10), (timedelta(days=31), 2_678_400),
                                                 (timedelta(0), 0)])
    def test_touch_and_get_and_touch_send_the_expiry_as_a_duration(self,
                                                                   core: ClusterCore,
                                                                   expiry: timedelta,
                                                                   seconds: int) -> None:
        assert _builder(core).build_touch_request('hotel_1', expiry, None).message.expiry_secs == seconds
        request = _builder(core).build_get_and_touch_request('hotel_1', expiry, None)
        assert request.message.expiry_secs == seconds
        assert not request.message.HasField('compression')

    @pytest.mark.parametrize('expiry', [timedelta(seconds=-1), 10])
    def test_an_expiry_out_of_range_or_not_a_timedelta_is_refused(self, core: ClusterCore, expiry: Any) -> None:
        with pytest.raises(InvalidArgumentException):
            _builder(core).build_touch_request('hotel_1', expiry, None)
        with pytest.raises(InvalidArgumentException):
            _builder(core).build_get_and_touch_request('hotel_1', expiry, None)


class LockRequestUnitTests:

    def test_get_and_lock_sends_the_lock_time_in_seconds(self, core: ClusterCore) -> None:
        request = _builder(core).build_get_and_lock_request('hotel_1', timedelta(seconds=15), None)
        assert request.message.lock_time_secs == 15
        assert not request.message.HasField('compression')

    @pytest.mark.parametrize('lock_time', [timedelta(seconds=-1), timedelta(seconds=2**32), 15])
    def test_a_lock_time_out_of_range_or_not_a_timedelta_is_refused(self, core: ClusterCore, lock_time: Any) -> None:
        with pytest.raises(InvalidArgumentException):
            _builder(core).build_get_and_lock_request('hotel_1', lock_time, None)

    def test_unlock_carries_the_cas(self, core: ClusterCore) -> None:
        assert _builder(core).build_unlock_request('hotel_1', 42, None).message.cas == 42
        not_an_int: Any = '42'
        with pytest.raises(InvalidArgumentException):
            _builder(core).build_unlock_request('hotel_1', not_an_int, None)


class CounterRequestUnitTests:

    @pytest.mark.parametrize('build', ['build_increment_request', 'build_decrement_request'])
    def test_a_delta_of_one_and_an_initial_zero_by_default(self, core: ClusterCore, build: str) -> None:
        request = getattr(_builder(core), build)('hotel_1', None).message
        assert (request.delta, request.initial, request.expiry_secs) == (1, 0, 0)
        assert request.WhichOneof('expiry') == 'expiry_secs'

    def test_carries_the_delta_initial_and_expiry(self, core: ClusterCore) -> None:
        request = _builder(core).build_increment_request('hotel_1',
                                                         None,
                                                         delta=DeltaValue(5),
                                                         initial=SignedInt64(10),
                                                         expiry=timedelta(seconds=30)).message
        assert (request.delta, request.initial, request.expiry_secs) == (5, 10, 30)

    def test_a_negative_initial_means_none(self, core: ClusterCore) -> None:
        request = _builder(core).build_increment_request('hotel_1', None, initial=SignedInt64(-1)).message
        assert not request.HasField('initial')

    @pytest.mark.parametrize('kwargs', [{'delta': 5}, {'initial': 5}])
    def test_a_delta_or_initial_of_the_wrong_type_is_refused(self, core: ClusterCore, kwargs: dict[str, Any]) -> None:
        with pytest.raises(InvalidArgumentException):
            _builder(core).build_increment_request('hotel_1', None, **kwargs)


class AppendRequestUnitTests:

    @pytest.mark.parametrize('value', ['x', b'x', bytearray(b'x')])
    def test_carries_the_value_as_bytes(self, core: ClusterCore, value: Any) -> None:
        assert _builder(core).build_append_request('hotel_1', value, None).message.content == b'x'
        assert _builder(core).build_prepend_request('hotel_1', value, None).message.content == b'x'

    def test_a_value_that_is_not_text_or_bytes_is_refused(self, core: ClusterCore) -> None:
        not_bytes: Any = 1
        with pytest.raises(ValueError):
            _builder(core).build_append_request('hotel_1', not_bytes, None)

    def test_a_cas_of_zero_is_no_cas(self, core: ClusterCore) -> None:
        assert _builder(core).build_prepend_request('hotel_1', b'x', None, cas=42).message.cas == 42
        assert not _builder(core).build_prepend_request('hotel_1', b'x', None, cas=0).message.HasField('cas')


class KvRecordUnitTests:

    def test_exists(self) -> None:
        result = ExistsResult(exists_record(kv_pb2.ExistsResponse(result=True, cas=42)), key='hotel_1')
        assert (result.exists, result.cas) == (True, 42)
        assert ExistsResult(exists_record(kv_pb2.ExistsResponse())).exists is False

    def test_a_counter_reads_its_value_and_token(self) -> None:
        token = kv_pb2.MutationToken(bucket_name='default', vbucket_id=7, vbucket_uuid=123456789, seq_no=99)
        result = CounterResult(counter_record(kv_pb2.IncrementResponse(cas=55, content=8, mutation_token=token)))
        assert result.content == 8
        mutation_token = result.mutation_token()
        assert mutation_token is not None
        assert mutation_token.as_tuple() == (7, 123456789, 99, 'default')
        assert CounterResult(counter_record(kv_pb2.DecrementResponse(cas=1))).content == 0

    def test_get_and_lock_and_get_and_touch_read_as_a_get(self) -> None:
        for response in (kv_pb2.GetAndLockResponse(content_uncompressed=b'{"a":1}', content_flags=0x02000006, cas=3),
                         kv_pb2.GetAndTouchResponse(content_uncompressed=b'{"a":1}', content_flags=0x02000006, cas=3)):
            result = GetResult(get_record(response), transcoder=JSONTranscoder(), key='hotel_1')
            assert (result.content_as[dict], result.cas) == ({'a': 1}, 3)


class XattrsFirstUnitTests:

    @pytest.mark.parametrize('xattr, order', [([], []),
                                              ([False, False], [0, 1]),
                                              ([True, False], [0, 1]),
                                              ([False, True], [1, 0]),
                                              ([False, True, False, True, False], [1, 3, 0, 2, 4])])
    def test_xattr_specs_first_each_in_the_order_written(self, xattr: list[bool], order: list[int]) -> None:
        assert xattrs_first(xattr) == order


class LookupInRequestUnitTests:

    def test_names_the_document(self, core: ClusterCore) -> None:
        message = _lookup_in(core, SD.get('name')).message
        assert (message.bucket_name, message.scope_name, message.collection_name, message.key) == (
            'default', 'inventory', 'hotels', 'hotel_1')
        assert not message.HasField('flags')

    def test_each_spec_as_the_gateway_names_it(self, core: ClusterCore) -> None:
        message = _lookup_in(core, SD.get('name'), SD.exists('geo'), SD.count('reviews'), SD.get_full()).message
        assert [(spec.operation, spec.path) for spec in message.specs] == [(LOOKUP.OPERATION_GET, 'name'),
                                                                           (LOOKUP.OPERATION_EXISTS, 'geo'),
                                                                           (LOOKUP.OPERATION_COUNT, 'reviews'),
                                                                           (LOOKUP.OPERATION_GET, '')]
        assert not any(spec.HasField('flags') for spec in message.specs)

    def test_xattr_specs_are_sent_first_and_remember_where_they_were_written(self, core: ClusterCore) -> None:
        request = _lookup_in(core, SD.get('name'), SD.get(SD.LookupInMacro.cas(), xattr=True), SD.exists('geo'))
        assert [(spec.path, spec.flags.xattr) for spec in request.message.specs] == [('$document.CAS', True),
                                                                                     ('name', False),
                                                                                     ('geo', False)]
        assert [(spec.path, spec.original_index) for spec in request.specs] == [('$document.CAS', 1),
                                                                                ('name', 0),
                                                                                ('geo', 2)]

    def test_access_deleted(self, core: ClusterCore) -> None:
        assert _lookup_in(core, SD.get('name'), access_deleted=True).message.flags.access_deleted is True

    def test_a_mutation_spec_is_refused(self, core: ClusterCore) -> None:
        with pytest.raises(InvalidArgumentException, match='DICT_UPSERT cannot be used in lookup_in'):
            _lookup_in(core, SD.upsert('name', 'Ritz'))


class LookupInRecordUnitTests:

    def test_answers_go_back_to_the_order_written(self, core: ClusterCore) -> None:
        request = _lookup_in(core, SD.get('name'), SD.get(SD.LookupInMacro.expiry_time(), xattr=True))
        response = kv_pb2.LookupInResponse(cas=7, specs=[kv_pb2.LookupInResponse.Spec(content=b'0'),
                                                         kv_pb2.LookupInResponse.Spec(content=b'"Ritz"')])
        result = LookupInResult(lookup_in_record(response, request.specs, 'hotel_1'), serializer=request.serializer)
        assert (result.content_as[str](0), result.content_as[int](1), result.cas) == ('Ritz', 0, 7)

    def test_exists_reads_the_content(self, core: ClusterCore) -> None:
        request = _lookup_in(core, SD.exists('name'), SD.exists('geo'))
        response = kv_pb2.LookupInResponse(cas=1, specs=[kv_pb2.LookupInResponse.Spec(content=b'true'),
                                                         kv_pb2.LookupInResponse.Spec(content=b'false')])
        result = LookupInResult(lookup_in_record(response, request.specs, 'hotel_1'), serializer=request.serializer)
        assert (result.exists(0), result.exists(1)) == (True, False)
        assert (result.content_as[bool](0), result.content_as[bool](1)) == (True, False)

    def test_a_failed_field_raises_what_its_status_maps_to(self, core: ClusterCore) -> None:
        request = _lookup_in(core, SD.get('name'), SD.get('geo'), SD.exists('geo'))
        response = kv_pb2.LookupInResponse(cas=1, specs=[kv_pb2.LookupInResponse.Spec(content=b'"Ritz"'),
                                                         kv_pb2.LookupInResponse.Spec(status=_not_found('geo')),
                                                         kv_pb2.LookupInResponse.Spec(status=_not_found('geo'))])
        result = LookupInResult(lookup_in_record(response, request.specs, 'hotel_1'), serializer=request.serializer)
        assert result.content_as[str](0) == 'Ritz'
        with pytest.raises(PathNotFoundException):
            result.content_as[dict](1)
        assert (result.exists(1), result.exists(2)) == (False, False)

    def test_a_precondition_failure(self, core: ClusterCore) -> None:
        request = _lookup_in(core, SD.get('name.first'))
        status = status_pb2.Status(code=code_pb2.FAILED_PRECONDITION, message='mismatch')
        status.details.add().Pack(error_details_pb2.PreconditionFailure(
            violations=[error_details_pb2.PreconditionFailure.Violation(type='PATH_MISMATCH')]))
        response = kv_pb2.LookupInResponse(cas=1, specs=[kv_pb2.LookupInResponse.Spec(status=status)])
        result = LookupInResult(lookup_in_record(response, request.specs, 'hotel_1'), serializer=request.serializer)
        with pytest.raises(PathMismatchException):
            result.content_as[str](0)

    def test_a_null_field(self, core: ClusterCore) -> None:
        request = _lookup_in(core, SD.get('empty'))
        response = kv_pb2.LookupInResponse(cas=1, specs=[kv_pb2.LookupInResponse.Spec(content=b'null')])
        result = LookupInResult(lookup_in_record(response, request.specs, 'hotel_1'), serializer=request.serializer)
        assert result.content_as[lambda x: x](0) is None
        assert result.exists(0) is True

    def test_an_answer_per_spec_or_a_server_failure(self, core: ClusterCore) -> None:
        request = _lookup_in(core, SD.get('name'), SD.get('geo'))
        with pytest.raises(InternalServerFailureException):
            lookup_in_record(kv_pb2.LookupInResponse(cas=1, specs=[kv_pb2.LookupInResponse.Spec()]),
                             request.specs,
                             'hotel_1')


class MutateInRequestUnitTests:

    def test_each_spec_as_the_gateway_names_it(self, core: ClusterCore) -> None:
        message = _mutate_in(core,
                             SD.insert('a', 1),
                             SD.upsert('b', 2),
                             SD.replace('c', 3),
                             SD.remove('d'),
                             SD.array_append('e', 4),
                             SD.array_prepend('f', 5),
                             SD.array_insert('g.[0]', 6),
                             SD.array_addunique('h', 7),
                             SD.increment('i', 8)).message
        assert [(spec.operation, spec.path, spec.content) for spec in message.specs] == [
            (MUTATE.OPERATION_INSERT, 'a', b'1'),
            (MUTATE.OPERATION_UPSERT, 'b', b'2'),
            (MUTATE.OPERATION_REPLACE, 'c', b'3'),
            (MUTATE.OPERATION_REMOVE, 'd', b''),
            (MUTATE.OPERATION_ARRAY_APPEND, 'e', b'4'),
            (MUTATE.OPERATION_ARRAY_PREPEND, 'f', b'5'),
            (MUTATE.OPERATION_ARRAY_INSERT, 'g.[0]', b'6'),
            (MUTATE.OPERATION_ARRAY_ADD_UNIQUE, 'h', b'7'),
            (MUTATE.OPERATION_COUNTER, 'i', b'8'),
        ]
        assert not any(spec.HasField('flags') for spec in message.specs)

    def test_the_whole_document_is_the_empty_path(self, core: ClusterCore) -> None:
        message = _mutate_in(core, SD.replace('', {'a': 1}), SD.remove('')).message
        assert [(spec.operation, spec.path) for spec in message.specs] == [(MUTATE.OPERATION_REPLACE, ''),
                                                                           (MUTATE.OPERATION_REMOVE, '')]

    def test_array_values_are_sent_without_brackets(self, core: ClusterCore) -> None:
        message = _mutate_in(core, SD.array_append('tags', 'a', 'b'), SD.array_addunique('tags', 'c')).message
        assert [spec.content for spec in message.specs] == [b'"a", "b"', b'"c"']

    def test_a_decrement_is_a_negative_counter(self, core: ClusterCore) -> None:
        assert _mutate_in(core, SD.decrement('n', 3)).message.specs[0].content == b'-3'

    def test_create_parents_and_xattr(self, core: ClusterCore) -> None:
        message = _mutate_in(core, SD.upsert('a.b', 1, create_parents=True), SD.upsert('meta', 2, xattr=True)).message
        assert [(spec.path, spec.flags.xattr, spec.flags.create_path) for spec in message.specs] == [
            ('meta', True, False), ('a.b', False, True)]

    def test_xattr_specs_are_sent_first_and_remember_where_they_were_written(self, core: ClusterCore) -> None:
        request = _mutate_in(core, SD.increment('n', 1), SD.upsert('meta', 2, xattr=True))
        assert [(spec.path, spec.original_index) for spec in request.specs] == [('meta', 1), ('n', 0)]

    def test_a_mutation_macro_is_unavailable(self, core: ClusterCore) -> None:
        with pytest.raises(FeatureUnavailableException, match='mutation macro'):
            _mutate_in(core, SD.upsert('meta.cas', SD.MutationMacro.cas()))
        with pytest.raises(FeatureUnavailableException, match='mutation macro'):
            _mutate_in(core, SD.insert('meta.cas', '${Mutation.CAS}', xattr=True, expand_macros=True))

    def test_a_lookup_spec_is_refused(self, core: ClusterCore) -> None:
        with pytest.raises(InvalidArgumentException, match='GET cannot be used in mutate_in'):
            _mutate_in(core, SD.get('name'))

    def test_no_expiry_is_an_explicit_zero(self, core: ClusterCore) -> None:
        message = _mutate_in(core, SD.upsert('a', 1)).message
        assert message.WhichOneof('expiry') == 'expiry_secs'
        assert message.expiry_secs == 0

    def test_an_expiry_is_a_duration(self, core: ClusterCore) -> None:
        assert _mutate_in(core, SD.upsert('a', 1), expiry=timedelta(days=31)).message.expiry_secs == 2_678_400

    def test_preserving_leaves_the_expiry_out(self, core: ClusterCore) -> None:
        for expiry in (None, timedelta(0)):
            assert _mutate_in(core, SD.upsert('a', 1), expiry=expiry, preserve_expiry=True).message.WhichOneof(
                'expiry') is None

    def test_preserving_with_an_expiry_is_unavailable(self, core: ClusterCore) -> None:
        with pytest.raises(FeatureUnavailableException, match='preserve_expiry'):
            _mutate_in(core, SD.upsert('a', 1), expiry=timedelta(minutes=5), preserve_expiry=True)

    def test_preserving_is_refused_where_classic_refuses_it(self, core: ClusterCore) -> None:
        with pytest.raises(InvalidArgumentException, match='insert operations'):
            _mutate_in(core, SD.insert('a', 1), preserve_expiry=True)
        with pytest.raises(InvalidArgumentException, match='replace operations'):
            _mutate_in(core, SD.replace('a', 1), expiry=timedelta(minutes=5), preserve_expiry=True)

    @pytest.mark.parametrize('kwargs, wire', [
        ({'store_semantics': SD.StoreSemantics.REPLACE}, STORE.STORE_SEMANTIC_REPLACE),
        ({'store_semantics': SD.StoreSemantics.UPSERT}, STORE.STORE_SEMANTIC_UPSERT),
        ({'store_semantics': SD.StoreSemantics.INSERT}, STORE.STORE_SEMANTIC_INSERT),
        ({'upsert_doc': True}, STORE.STORE_SEMANTIC_UPSERT),
        ({'insert_doc': True, 'store_semantics': SD.StoreSemantics.UPSERT}, STORE.STORE_SEMANTIC_INSERT),
    ])
    @pytest.mark.filterwarnings('ignore:Option .*_doc')
    def test_store_semantics(self, core: ClusterCore, kwargs: dict[str, Any], wire: int) -> None:
        assert _mutate_in(core, SD.upsert('a', 1), **kwargs).message.store_semantic == wire

    def test_no_store_semantics_leaves_it_out(self, core: ClusterCore) -> None:
        assert not _mutate_in(core, SD.upsert('a', 1)).message.HasField('store_semantic')

    @pytest.mark.filterwarnings('ignore:Option .*_doc')
    def test_more_than_one_store_semantics_keyword_is_refused(self, core: ClusterCore) -> None:
        with pytest.raises(InvalidArgumentException, match='multiple store semantics'):
            _mutate_in(core, SD.upsert('a', 1), insert_doc=True, upsert_doc=True)

    def test_cas_and_access_deleted(self, core: ClusterCore) -> None:
        message = _mutate_in(core, SD.upsert('a', 1), cas=99, access_deleted=True).message
        assert (message.cas, message.flags.access_deleted) == (99, True)
        message = _mutate_in(core, SD.upsert('a', 1)).message
        assert not message.HasField('cas')
        assert not message.HasField('flags')

    def test_server_durability(self, core: ClusterCore) -> None:
        request = _mutate_in(core, SD.upsert('a', 1), durability=ServerDurability(DurabilityLevel.MAJORITY))
        assert request.message.durability_level == kv_pb2.DURABILITY_LEVEL_MAJORITY
        assert request.timeout == core.kv_durable_timeout
        assert not _mutate_in(core, SD.upsert('a', 1)).message.HasField('durability_level')

    def test_client_durability_is_unavailable(self, core: ClusterCore) -> None:
        with pytest.raises(FeatureUnavailableException, match='ServerDurability'):
            _mutate_in(core, SD.upsert('a', 1), durability=ClientDurability(replicate_to=ReplicateTo.ONE))


class MutateInRecordUnitTests:

    def test_a_counter_reads_its_new_value_in_the_order_written(self, core: ClusterCore) -> None:
        request = _mutate_in(core, SD.increment('n', 3), SD.upsert('meta', 1, xattr=True))
        token = kv_pb2.MutationToken(bucket_name='default', vbucket_id=7, vbucket_uuid=123456789, seq_no=99)
        response = kv_pb2.MutateInResponse(cas=55,
                                           mutation_token=token,
                                           specs=[kv_pb2.MutateInResponse.Spec(),
                                                  kv_pb2.MutateInResponse.Spec(content=b'8')])
        result = MutateInResult(mutate_in_record(response, request.specs), serializer=request.serializer, key='h')
        assert result.content_as[int](0) == 8
        assert result.cas == 55
        mutation_token = result.mutation_token()
        assert mutation_token is not None
        assert mutation_token.as_tuple() == (7, 123456789, 99, 'default')

    def test_an_answer_per_spec_or_a_server_failure(self, core: ClusterCore) -> None:
        request = _mutate_in(core, SD.upsert('a', 1))
        with pytest.raises(InternalServerFailureException):
            mutate_in_record(kv_pb2.MutateInResponse(cas=1), request.specs)
