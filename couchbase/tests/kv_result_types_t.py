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
from collections.abc import (Callable,
                             Iterator,
                             Mapping)
from datetime import datetime, timedelta
from functools import partial
from types import SimpleNamespace
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

import pytest

from couchbase.common.result import _MutationTokenDict
from couchbase.exceptions import (DocumentNotFoundException,
                                  InvalidArgumentException,
                                  PathNotFoundException)
from couchbase.protocol.classic._collection_impl import CollectionImpl, decode_replicas
from couchbase.protocol.classic._pycbc_core import pycbc_exception, pycbc_result
from couchbase.protocol.classic._result import (_counter_from_core,
                                                _exists_from_core,
                                                _get_from_core,
                                                _get_replica_from_core,
                                                _lookup_in_from_core,
                                                _lookup_in_replica_from_core,
                                                _mutate_in_from_core,
                                                _mutation_from_core,
                                                _scan_item_from_core)
from couchbase.result import (CounterResult,
                              ExistsResult,
                              GetReplicaResult,
                              GetResult,
                              LookupInReplicaResult,
                              LookupInResult,
                              MultiCounterResult,
                              MultiExistsResult,
                              MultiGetReplicaResult,
                              MultiGetResult,
                              MultiMutationResult,
                              MutateInResult,
                              MutationResult,
                              MutationToken,
                              ScanResult)
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.transcoder import JSONTranscoder
from tests.test_features import TestProtocol

# Each record here is built by classic's adapter from what its binding returns.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import (CppExistsResponse,
                                                                          CppGetAnyReplicaResponse,
                                                                          CppGetProjectedResponse,
                                                                          CppIncrementResponse,
                                                                          CppLookupInAnyReplicaResponse,
                                                                          CppLookupInResponse,
                                                                          CppLookupInResponseEntry,
                                                                          CppMutateInResponse,
                                                                          CppMutationToken,
                                                                          CppRangeScanItemBody,
                                                                          CppUpsertResponse)
    from couchbase.protocol.classic._result import _CoreScanItem

JSON_FLAGS = 0x02000006
EXPIRY = 1893456000


def _token() -> _MutationTokenDict:
    return {'partition_id': 12, 'partition_uuid': 34, 'sequence_number': 56, 'bucket_name': 'default'}


def _core_token() -> CppMutationToken:
    return {'partition_id': 12, 'partition_uuid': 34, 'sequence_number': 56, 'bucket_name': 'default'}


def _lookup_field(path: str, value: bytes, index: int, exists: bool, status: int) -> CppLookupInResponseEntry:
    return {'path': path, 'value': value, 'original_index': index, 'exists': exists,
            'opcode': 197, 'status': status, 'ec': None}


# One dict per response the core hands the classic adapter, with the keys in the order the
# binding writes them.
def _core_get() -> CppGetProjectedResponse:
    return {'value': b'{"a": 1}', 'cas': 11, 'flags': JSON_FLAGS}


def _core_get_replica() -> CppGetAnyReplicaResponse:
    return {'value': b'{"a": 1}', 'cas': 11, 'flags': JSON_FLAGS, 'replica': True}


def _core_exists() -> CppExistsResponse:
    return {'deleted': False, 'cas': 11, 'flags': JSON_FLAGS, 'expiry': 0, 'sequence_number': 3,
            'datatype': 1, 'document_exists': True}


def _core_mutation() -> CppUpsertResponse:
    return {'cas': 11, 'token': _core_token()}


def _core_counter() -> CppIncrementResponse:
    return {'content': 7, 'cas': 11, 'token': _core_token()}


def _core_lookup_in() -> CppLookupInResponse:
    return {'cas': 11,
            'fields': [_lookup_field('a', b'{"x": 2}', 0, True, 0),
                       _lookup_field('b', b'', 1, False, 192)],
            'deleted': False}


def _core_lookup_in_replica() -> CppLookupInAnyReplicaResponse:
    return {'cas': 11,
            'fields': [_lookup_field('a', b'{"x": 2}', 0, True, 0),
                       _lookup_field('b', b'', 1, False, 192)],
            'deleted': False,
            'is_replica': True}


def _core_mutate_in() -> CppMutateInResponse:
    return {'cas': 11, 'token': _core_token(),
            'fields': [{'path': 'a', 'value': b'', 'original_index': 0, 'opcode': 200, 'status': 0, 'ec': None},
                       {'path': 'n', 'value': b'5', 'original_index': 1, 'opcode': 207, 'status': 0, 'ec': None}],
            'deleted': False}


def _core_scan_item(with_body: bool = True) -> _CoreScanItem:
    body: CppRangeScanItemBody = {'flags': JSON_FLAGS, 'expiry': 0, 'cas': 11, 'sequence_number': 3, 'datatype': 1,
                                  'value': b'{"a": 1}'}
    return {'scan_item': {'key': 'doc-1', 'body': body if with_body else None}}


def _core_result(raw: Mapping[str, Any]) -> pycbc_result[dict[str, Any]]:
    res: pycbc_result[dict[str, Any]] = pycbc_result()
    res.raw_result.update(raw)
    return res


class _NotFound(pycbc_exception):
    # The five accessors are what `ErrorMapper` reads off the core's exception.
    def err(self) -> int:
        return 101

    def err_category(self) -> str:
        return 'couchbase.key_value'

    def strerror(self) -> str:
        return 'document_not_found'

    def error_context(self) -> dict[str, Any] | None:
        return None

    def get_exception_info(self) -> dict[str, Any] | None:
        return None


class _CoreMulti:
    """The container the binding hands back for a multi operation: one entry per key, and the flag beside them.

    Stands in for `pycbc_result`, which cannot be subclassed and whose flag Python cannot set.
    """

    def __init__(self, values: dict[str, Any], all_okay: bool) -> None:
        self.raw_result = dict(values)
        self.multi_resp_all_okay = all_okay


def _core_multi(values: dict[str, Any], all_okay: bool = True) -> pycbc_result[dict[str, Any]]:
    return cast('pycbc_result[dict[str, Any]]', _CoreMulti(values, all_okay))


# The positional arguments after the keys, for the multi methods that take one.
_MULTI_EXTRA_ARGS: dict[str, tuple[Any, ...]] = {
    'get_and_lock_multi': (timedelta(seconds=1),),
    'touch_multi': (timedelta(seconds=1),),
}


def _run_multi(method: str, ret: pycbc_result[dict[str, Any]], keys: list[str]) -> Any:
    impl = CollectionImpl.__new__(CollectionImpl)
    # Each double answers only the one call a multi operation makes of it; the casts are what let a
    # partial double fill the attribute's declared type.
    impl._client_adapter = cast(Any, SimpleNamespace(execute_collection_request=lambda *_, **__: ret))
    req = SimpleNamespace(opcode=None, request_list=None, return_exceptions=True,
                          key_transcoders={k: JSONTranscoder() for k in keys})
    impl._multi_request_builder = cast(Any, SimpleNamespace(**{f'build_{method}_request': lambda *_, **__: req}))
    impl._scope_impl = cast(Any, SimpleNamespace(observability_instruments=SimpleNamespace(is_noop=True)))
    # The builder double ignores its arguments, so the keys stand in for a mapping of keys to values.
    return getattr(impl, method)(keys, *_MULTI_EXTRA_ARGS.get(method, ()))


# Each multi method, the core response for one successful key, and the per-key class it builds.
_MULTI_METHODS = [
    ('get_multi', _core_get, MultiGetResult, GetResult),
    ('get_and_lock_multi', _core_get, MultiGetResult, GetResult),
    ('get_any_replica_multi', _core_get_replica, MultiGetReplicaResult, GetReplicaResult),
    ('exists_multi', _core_exists, MultiExistsResult, ExistsResult),
    ('increment_multi', _core_counter, MultiCounterResult, CounterResult),
    ('decrement_multi', _core_counter, MultiCounterResult, CounterResult),
    ('append_multi', _core_mutation, MultiMutationResult, MutationResult),
    ('prepend_multi', _core_mutation, MultiMutationResult, MutationResult),
    ('insert_multi', _core_mutation, MultiMutationResult, MutationResult),
    ('upsert_multi', _core_mutation, MultiMutationResult, MutationResult),
    ('replace_multi', _core_mutation, MultiMutationResult, MutationResult),
    ('remove_multi', _core_mutation, MultiMutationResult, MutationResult),
    ('touch_multi', lambda: {'cas': 11}, MultiMutationResult, MutationResult),
]


_ADAPTERS: list[Callable[[], Any]] = [
    lambda: (_get_from_core, _core_get()),
    lambda: (_get_replica_from_core, _core_get_replica()),
    lambda: (_exists_from_core, _core_exists()),
    lambda: (_mutation_from_core, _core_mutation()),
    lambda: (_counter_from_core, _core_counter()),
    lambda: (partial(_lookup_in_from_core, key='doc-1'), _core_lookup_in()),
    lambda: (partial(_lookup_in_replica_from_core, key='doc-1'), _core_lookup_in_replica()),
    lambda: (partial(_mutate_in_from_core, key='doc-1'), _core_mutate_in()),
    lambda: (_scan_item_from_core, _core_scan_item()),
]


class KVResultTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: each result is built straight from the dict the core hands
    # the classic adapter, and `couchbase.common.result` is the only definition of any of them.
    # Not generated for that reason.  The KV suites read every one of these from a real server;
    # what they cannot reach is the core dict after the build, a response built twice, and the
    # responses a server rarely sends, such as a scan for ids only.

    @pytest.mark.parametrize('case', _ADAPTERS)
    def test_adapter_leaves_the_core_dict_unchanged(self, case: Callable[[], Any]) -> None:
        adapter, raw = case()
        before = copy.deepcopy(raw)

        adapter(raw)

        assert raw == before

    @pytest.mark.parametrize('case', _ADAPTERS)
    def test_one_core_dict_builds_two_equal_records(self, case: Callable[[], Any]) -> None:
        adapter, raw = case()

        assert adapter(raw) == adapter(raw)

    def test_get(self) -> None:
        res = GetResult(_get_from_core(_core_get()), transcoder=JSONTranscoder(), key='doc-1')

        assert res.value == {'a': 1}
        assert res.content_as[dict] == {'a': 1}
        assert (res.cas, res.flags, res.key, res.success) == (11, JSON_FLAGS, 'doc-1', True)
        assert res.expiry_time is None

    @pytest.mark.parametrize('expiry, expected', [(None, None), (0, None), (EXPIRY, datetime.fromtimestamp(EXPIRY))])
    def test_get_expiry(self, expiry: Any, expected: Any) -> None:
        raw = _core_get()
        if expiry is not None:
            raw['expiry'] = expiry

        res = GetResult(_get_from_core(raw), transcoder=JSONTranscoder())

        assert res.expiry_time == expected
        assert res.expiryTime == expected

    def test_get_replica(self) -> None:
        res = GetReplicaResult(_get_replica_from_core(_core_get_replica()), transcoder=JSONTranscoder())

        assert res.value == {'a': 1}
        assert (res.is_replica, res.is_active, res.flags) == (True, False, JSON_FLAGS)

    def test_exists(self) -> None:
        res = ExistsResult(_exists_from_core(_core_exists()), key='doc-1')

        assert (res.exists, res.cas, res.flags, res.value) == (True, 11, JSON_FLAGS, None)

    def test_mutation_token(self) -> None:
        res = MutationResult(_mutation_from_core(_core_mutation()), key='doc-1')

        token = res.mutation_token()
        assert token == MutationToken(_token())
        assert token is not None and token.as_tuple() == (12, 34, 56, 'default')
        assert res.flags == 0

    def test_a_changed_token_dict_changes_no_record(self) -> None:
        record = _mutation_from_core(_core_mutation())
        token = MutationResult(record).mutation_token()
        assert token is not None

        token.as_dict()['sequence_number'] = 0

        assert MutationResult(record).mutation_token() == MutationToken(_token())

    def test_touch_has_no_token(self) -> None:
        assert MutationResult(_mutation_from_core({'cas': 11})).mutation_token() is None

    def test_counter(self) -> None:
        res = CounterResult(_counter_from_core(_core_counter()), key='doc-1')

        assert (res.content, res.cas, res.key) == (7, 11, 'doc-1')
        assert res.mutation_token() == MutationToken(_token())

    def test_lookup_in_value_is_the_core_fields_with_each_value_decoded(self) -> None:
        res = LookupInResult(_lookup_in_from_core(_core_lookup_in(), 'doc-1'),
                             serializer=DefaultJsonSerializer(),
                             key='doc-1')
        fields = res.value
        assert fields is not None

        # A field whose value is empty keeps no value key, and a decoded value is written back
        # last, so the key order is part of what a caller printing the list sees.
        assert [list(f) for f in fields] == [
            ['path', 'original_index', 'exists', 'opcode', 'status', 'ec', 'value'],
            ['path', 'original_index', 'exists', 'opcode', 'status', 'ec'],
        ]
        assert fields[0]['value'] == {'x': 2}
        assert res.content_as[dict](0) == {'x': 2}
        assert res.exists(0) is True
        assert res.exists(1) is False
        with pytest.raises(PathNotFoundException):
            res.content_as[dict](1)

    def test_lookup_in_replica(self) -> None:
        res = LookupInReplicaResult(_lookup_in_replica_from_core(_core_lookup_in_replica(), None),
                                    serializer=DefaultJsonSerializer())

        assert res.is_replica is True
        assert res.content_as[dict](0) == {'x': 2}

    def test_mutate_in(self) -> None:
        res = MutateInResult(_mutate_in_from_core(_core_mutate_in(), 'doc-1'),
                             serializer=DefaultJsonSerializer(),
                             key='doc-1')

        assert res.content_as[int](1) == 5
        assert res.mutation_token() == MutationToken(_token())

    @pytest.mark.parametrize('build', [
        lambda serializer: LookupInResult(_lookup_in_from_core(_core_lookup_in(), 'doc-1'), serializer=serializer),
        lambda serializer: LookupInReplicaResult(_lookup_in_replica_from_core(_core_lookup_in_replica(), None),
                                                 serializer=serializer),
    ])
    def test_subdoc_values_decode_with_the_given_serializer(self, build: Callable[[Serializer], Any]) -> None:
        class _Wrapping(DefaultJsonSerializer):
            def deserialize(self, value: bytes) -> Any:
                return {'wrapped': super().deserialize(value)}

        res = build(_Wrapping())

        assert res.content_as[dict](0) == {'wrapped': {'x': 2}}
        assert res.value[0]['value'] == {'wrapped': {'x': 2}}

    def test_scan(self) -> None:
        res = ScanResult(_scan_item_from_core(_core_scan_item()), False, JSONTranscoder())

        assert (res.id, res.cas, res.expiry_time) == ('doc-1', 11, None)
        assert res.content_as[dict] == {'a': 1}

    def test_scan_flags_are_the_documents(self) -> None:
        res = ScanResult(_scan_item_from_core(_core_scan_item()), False, JSONTranscoder())

        assert res.flags == JSON_FLAGS

    def test_scan_for_ids_only(self) -> None:
        res = ScanResult(_scan_item_from_core(_core_scan_item(with_body=False)), True, JSONTranscoder())

        assert res.id == 'doc-1'
        assert res.value is None
        for read in (lambda: res.cas, lambda: res.expiry_time, lambda: res.content_as):
            with pytest.raises(InvalidArgumentException):
                read()

    @pytest.mark.parametrize('build, expected', [
        (lambda: GetResult(_get_from_core(_core_get())),
         "GetResult({'cas': 11, 'value': b'{\"a\": 1}', 'flags': 33554438, 'expiry': None})"),
        (lambda: MutationResult(_mutation_from_core({'cas': 11})),
         "MutationResult({'cas': 11, 'mutation_token': None})"),
        (lambda: CounterResult(_counter_from_core(_core_counter())),
         "CounterResult({'cas': 11, 'content': 7, 'mutation_token': {'partition_id': 12, 'partition_uuid': 34, "
         "'sequence_number': 56, 'bucket_name': 'default'}})"),
        (lambda: ScanResult(_scan_item_from_core(_core_scan_item(with_body=False)), True, None),
         "ScanResult({'id': 'doc-1', 'body': None})"),
    ])
    def test_repr_prints_the_record(self, build: Callable[[], Any], expected: str) -> None:
        assert repr(build()) == expected

    @pytest.mark.parametrize('method, core, multi_cls, result_cls', _MULTI_METHODS)
    def test_multi_builds_each_key_with_its_key(self, method: str, core: Callable[[], dict[str, Any]],
                                                multi_cls: Any, result_cls: Any) -> None:
        ret = _core_multi({'doc-1': _core_result(core()), 'doc-2': _core_result(core())})

        res = _run_multi(method, ret, ['doc-1', 'doc-2'])

        assert type(res) is multi_cls
        assert res.all_ok is True
        assert res.exceptions == {}
        assert list(res.results) == ['doc-1', 'doc-2']
        for key, result in res.results.items():
            assert type(result) is result_cls
            assert result.key == key
            assert result.cas == 11

    @pytest.mark.parametrize('method, core, multi_cls, result_cls', _MULTI_METHODS)
    def test_multi_leaves_the_core_container_unchanged(self, method: str, core: Callable[[], dict[str, Any]],
                                                       multi_cls: Any, result_cls: Any) -> None:
        ret = _core_multi({'doc-1': _core_result(core()), 'doc-2': _NotFound()}, all_okay=False)
        before = dict(ret.raw_result)

        res = _run_multi(method, ret, ['doc-1', 'doc-2'])

        assert ret.raw_result == before
        assert res.all_ok is False
        assert list(res.results) == ['doc-1']
        assert type(res.exceptions['doc-2']) is DocumentNotFoundException

    @pytest.mark.parametrize('method, core, multi_cls, result_cls', _MULTI_METHODS)
    def test_multi_keeps_a_document_keyed_all_okay(self, method: str, core: Callable[[], dict[str, Any]],
                                                   multi_cls: Any, result_cls: Any) -> None:
        ret = _core_multi({'all_okay': _core_result(core()), 'doc-2': _NotFound()}, all_okay=False)

        res = _run_multi(method, ret, ['all_okay', 'doc-2'])

        assert res.all_ok is False
        assert list(res.results) == ['all_okay']
        assert res.results['all_okay'].key == 'all_okay'
        assert list(res.exceptions) == ['doc-2']

    def test_multi_get_decodes_with_each_keys_transcoder(self) -> None:
        res = _run_multi('get_multi', _core_multi({'doc-1': _core_result(_core_get())}), ['doc-1'])

        assert res.results['doc-1'].content_as[dict] == {'a': 1}

    def test_get_all_replicas_multi(self) -> None:
        def stream(*entries: Mapping[str, Any]) -> Iterator[Any]:
            yield from (_core_result(e) for e in entries)
            yield None

        active = dict(_core_get_replica(), replica=False)
        ret = _core_multi({'doc-1': stream(active, _core_get_replica())})
        before = dict(ret.raw_result)

        res = _run_multi('get_all_replicas_multi', ret, ['doc-1'])

        assert ret.raw_result == before
        replicas = res.results['doc-1']
        assert [(r.key, r.is_replica, r.value) for r in replicas] == [('doc-1', False, {'a': 1}),
                                                                      ('doc-1', True, {'a': 1})]

    def test_get_all_replicas_multi_keeps_a_document_keyed_all_okay(self) -> None:
        def stream() -> Iterator[Any]:
            yield _core_result(_core_get_replica())
            yield None

        res = _run_multi('get_all_replicas_multi', _core_multi({'all_okay': stream()}), ['all_okay'])

        assert res.all_ok is True
        assert [r.key for r in res.results['all_okay']] == ['all_okay']

    def test_decode_replicas_stops_at_the_none(self) -> None:
        stream = [_core_result(_core_get_replica()), _core_result(_core_get_replica()), None,
                  _core_result(_core_get_replica())]

        replicas = list(decode_replicas(stream, lambda res: GetReplicaResult(_get_replica_from_core(res.raw_result))))

        assert [r.cas for r in replicas] == [11, 11]

    def test_decode_replicas_raises_an_error_entry_after_the_replicas_before_it(self) -> None:
        replicas = decode_replicas([_core_result(_core_get_replica()), _NotFound(), None],
                                   lambda res: GetReplicaResult(_get_replica_from_core(res.raw_result)))

        assert next(replicas).is_replica is True
        with pytest.raises(DocumentNotFoundException):
            next(replicas)
