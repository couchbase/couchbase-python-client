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

"""Every key a key-value operation accepts does something on classic, is declared `inert` there, or is refused.

Each key is set to two different values and the two builds are compared.  What differs in the
request the binding is handed is sent; what differs in what the builder hands back beside it, or
hands the request's observability handler, is held on the Python side.  A key that moves neither
is accepted and lost, unless the schema declares it `inert` on classic, and an `inert` key must
never move the request.  A range scan's options reach the binding as a dict, so there a key that
is sent also has to be one the binding reads.  A key the operation cannot carry out is refused
with an `InvalidArgumentException` rather than accepted and dropped.
"""

from __future__ import annotations

import functools
import pathlib
import re
import warnings
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

import couchbase
import couchbase.subdocument as SD
from couchbase.common import options
from couchbase.common._observability.no_op import NoOpSpan
from couchbase.common._option_spec import BLOCKS
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import InvalidArgumentException
from couchbase.kv_range_scan import PrefixScan
from couchbase.mutation_state import MutationState
from couchbase.options import DeltaValue, SignedInt64
from couchbase.protocol.classic._bindings import KeyValueBinding
from couchbase.protocol.classic._collection_multi_req_builder import CollectionMultiRequestBuilder
from couchbase.protocol.classic._collection_req_builder import CollectionRequestBuilder
from couchbase.protocol.classic._collection_types import CollectionDetails, CollectionOptionsBlock
from couchbase.protocol.classic._pycbc_core import pycbc_kv_request
from couchbase.replica_reads import (GetReplicaStrategy,
                                     ReadPreference,
                                     ReplicaIndex)
from couchbase.result import MutationToken
from couchbase.serializer import DefaultJsonSerializer
from couchbase.subdocument import StoreSemantics
from couchbase.transcoder import JSONTranscoder
from tests.test_features import TestProtocol

# What is checked here is classic's request builders and its binding.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

_REPO_ROOT = pathlib.Path(couchbase.__file__).resolve().parents[1]

_KEY = 'k1'
_DOC = {'a': 1}
_EXPIRY = timedelta(seconds=10)
_LOCK_TIME = timedelta(seconds=5)
_LOOKUP = [SD.get('a')]


class _FlaggedTranscoder(JSONTranscoder):
    """Encodes as JSON with its own flags, so two of them encode a document apart."""

    def __init__(self, flags: int) -> None:
        super().__init__()
        self._flags = flags

    def encode_value(self, value: Any) -> tuple[bytes, int]:
        return super().encode_value(value)[0], self._flags


def _state(sequence_number: int) -> MutationState:
    state = MutationState()
    state.add_mutation_token(MutationToken({'partition_id': 1, 'partition_uuid': 2,
                                            'sequence_number': sequence_number, 'bucket_name': 'default'}))
    return state


SAMPLES_BY_KEY: dict[str, tuple[Any, Any]] = {
    'access_deleted': (True, False),
    'batch_byte_limit': (7, 11),
    'batch_item_limit': (7, 11),
    'batch_time_limit': (timedelta(seconds=3), timedelta(seconds=5)),
    'cas': (7, 11),
    'concurrency': (2, 3),
    'consistent_with': (_state(3), _state(4)),
    'delta': (DeltaValue(3), DeltaValue(5)),
    'durability': (ServerDurability(DurabilityLevel.MAJORITY), ServerDurability(DurabilityLevel.PERSIST_TO_MAJORITY)),
    'expiry': (timedelta(seconds=30), timedelta(seconds=60)),
    'ids_only': (True, False),
    'initial': (SignedInt64(3), SignedInt64(5)),
    'lock_time': (timedelta(seconds=3), timedelta(seconds=5)),
    'parent_span': (NoOpSpan('first'), NoOpSpan('second')),
    'preserve_expiry': (True, False),
    'project': (['a'], ['b']),
    'read_preference': (ReadPreference.NO_PREFERENCE, ReadPreference.SELECTED_SERVER_GROUP),
    'return_exceptions': (True, False),
    'serializer': (DefaultJsonSerializer(), DefaultJsonSerializer()),
    'span': (NoOpSpan('first'), NoOpSpan('second')),
    'store_semantics': (StoreSemantics.UPSERT, StoreSemantics.INSERT),
    'timeout': (timedelta(seconds=3), timedelta(seconds=5)),
    'transcoder': (_FlaggedTranscoder(0x02000006), _FlaggedTranscoder(0x02000000)),
    'with_expiry': (True, False),
}


class _RecordingHandler:
    """An observability handler that keeps what the builder hands it."""

    is_legacy_tracer = False
    is_noop = False
    legacy_request_span = None
    wrapper_span_name = 'operation'
    with_metrics = False

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def create_kv_span(self, details: Any, parent_span: Any = None) -> None:
        self.calls.append(('create_kv_span', parent_span))

    def create_kv_multi_span(self, details: Any, parent_span: Any = None) -> None:
        self.calls.append(('create_kv_multi_span', parent_span))

    def add_kv_durability_attribute(self, level: Any) -> None:
        self.calls.append(('add_kv_durability_attribute', level))

    def maybe_create_encoding_span(self, encode: Callable[[], Any]) -> Any:
        return encode()

    def maybe_add_encoding_span(self, encode: Callable[[], Any]) -> Any:
        return encode()


def _details() -> CollectionDetails:
    return CollectionDetails('default', 'test-scope', 'test-collection', JSONTranscoder())


# block: build(builder, multi_builder, opts, handler, argument) -> what the builder returned.  The
# argument is the operation's own, for the operations that take one beside the key.
BUILDS: dict[str, Callable[[CollectionRequestBuilder, CollectionMultiRequestBuilder, Any, Any, Any], Any]] = {
    'AppendOptions': lambda b, m, o, h, a: b.build_append_request(_KEY, b'a', h, o),
    'DecrementOptions': lambda b, m, o, h, a: b.build_decrement_request(_KEY, h, o),
    'ExistsOptions': lambda b, m, o, h, a: b.build_exists_request(_KEY, h, o),
    'GetAllReplicasOptions': lambda b, m, o, h, a: b.build_get_all_replicas_request(_KEY, h, o),
    'GetAndLockOptions': lambda b, m, o, h, a: b.build_get_and_lock_request(_KEY, a, h, o),
    'GetAndTouchOptions': lambda b, m, o, h, a: b.build_get_and_touch_request(_KEY, a, h, o),
    'GetAnyReplicaOptions': lambda b, m, o, h, a: b.build_get_any_replica_request(_KEY, h, o),
    'GetOptions': lambda b, m, o, h, a: b.build_get_request(_KEY, h, o),
    'GetReplicaOptions': lambda b, m, o, h, a: b.build_get_replica_request(
        _KEY, GetReplicaStrategy.from_index(ReplicaIndex.FIRST), h, o),
    'IncrementOptions': lambda b, m, o, h, a: b.build_increment_request(_KEY, h, o),
    'InsertOptions': lambda b, m, o, h, a: b.build_insert_request(_KEY, _DOC, h, o),
    'LookupInAllReplicasOptions': lambda b, m, o, h, a: b.build_lookup_in_all_replicas_request(_KEY, _LOOKUP, h, o),
    'LookupInAnyReplicaOptions': lambda b, m, o, h, a: b.build_lookup_in_any_replica_request(_KEY, _LOOKUP, h, o),
    'LookupInOptions': lambda b, m, o, h, a: b.build_lookup_in_request(_KEY, _LOOKUP, h, o),
    'MutateInOptions': lambda b, m, o, h, a: b.build_mutate_in_request(_KEY, [SD.upsert('a', 1)], h, o),
    'PrependOptions': lambda b, m, o, h, a: b.build_prepend_request(_KEY, b'a', h, o),
    'RemoveOptions': lambda b, m, o, h, a: b.build_remove_request(_KEY, h, o),
    'ReplaceOptions': lambda b, m, o, h, a: b.build_replace_request(_KEY, _DOC, h, o),
    'ScanOptions': lambda b, m, o, h, a: b.build_range_scan_args(PrefixScan('p'), o),
    'TouchOptions': lambda b, m, o, h, a: b.build_touch_request(_KEY, a, h, o),
    'UnlockOptions': lambda b, m, o, h, a: b.build_unlock_request(_KEY, a, h, o),
    'UpsertOptions': lambda b, m, o, h, a: b.build_upsert_request(_KEY, _DOC, h, o),
    'AppendMultiOptions': lambda b, m, o, h, a: m.build_append_multi_request({_KEY: b'a'}, h, o),
    'DecrementMultiOptions': lambda b, m, o, h, a: m.build_decrement_multi_request([_KEY], h, o),
    'ExistsMultiOptions': lambda b, m, o, h, a: m.build_exists_multi_request([_KEY], h, o),
    'GetAllReplicasMultiOptions': lambda b, m, o, h, a: m.build_get_all_replicas_multi_request([_KEY], h, o),
    'GetAndLockMultiOptions': lambda b, m, o, h, a: m.build_get_and_lock_multi_request([_KEY], a, h, o),
    'GetAnyReplicaMultiOptions': lambda b, m, o, h, a: m.build_get_any_replica_multi_request([_KEY], h, o),
    'GetMultiOptions': lambda b, m, o, h, a: m.build_get_multi_request([_KEY], h, o),
    'IncrementMultiOptions': lambda b, m, o, h, a: m.build_increment_multi_request([_KEY], h, o),
    'InsertMultiOptions': lambda b, m, o, h, a: m.build_insert_multi_request({_KEY: _DOC}, h, o),
    'PrependMultiOptions': lambda b, m, o, h, a: m.build_prepend_multi_request({_KEY: b'a'}, h, o),
    'RemoveMultiOptions': lambda b, m, o, h, a: m.build_remove_multi_request([_KEY], h, o),
    'ReplaceMultiOptions': lambda b, m, o, h, a: m.build_replace_multi_request({_KEY: _DOC}, h, o),
    'TouchMultiOptions': lambda b, m, o, h, a: m.build_touch_multi_request([_KEY], a, h, o),
    'UnlockMultiOptions': lambda b, m, o, h, a: m.build_unlock_multi_request({_KEY: a}, h, o),
    'UpsertMultiOptions': lambda b, m, o, h, a: m.build_upsert_multi_request({_KEY: _DOC}, h, o),
}

# The keys a block carries only so the operation's own argument is converted with the options: no
# public options class declares them, and the argument always replaces one passed in the options.
# Each is checked through the argument, with the argument's value when the key is not varied.
ARGUMENT_KEYS: dict[tuple[str, str], Any] = {
    ('GetAndLockOptions', 'lock_time'): _LOCK_TIME,
    ('GetAndLockMultiOptions', 'lock_time'): _LOCK_TIME,
    ('GetAndTouchOptions', 'expiry'): _EXPIRY,
    ('TouchOptions', 'expiry'): _EXPIRY,
    ('TouchMultiOptions', 'expiry'): _EXPIRY,
    ('UnlockOptions', 'cas'): 1,
    ('UnlockMultiOptions', 'cas'): 1,
}

_REQUEST_FIELDS = tuple(name for name in dir(pycbc_kv_request)
                        if not name.startswith('_') and name not in ('callback', 'errback'))


def _source(name: str) -> str:
    source = _REPO_ROOT / 'src' / name
    if not source.exists():
        pytest.skip('the extension source is not beside the package')
    return source.read_text()


@functools.cache
def _request_reads() -> dict[str, frozenset[str]]:
    """The operations that read each request field, from the binding's `used by` comments.

    A field the comments give no operations for, such as the key or the parent span, is read by
    every operation and is not listed.
    """
    text = _source('pycbc_kv_request.hxx')
    body = text[text.index('struct pycbc_kv_request {'):text.index('/* observability */')]
    reads = {}
    for name, used in re.findall(r'PyObject\*\s*(\w+);\s*//[^\n]*?used by:((?:[^\n]*\n\s*//)*[^\n]*)', body):
        reads[name] = frozenset(op.strip() for op in used.replace('//', ' ').split(',') if op.strip())
    return reads


@functools.cache
def _scan_reads() -> frozenset[str]:
    """Every key the binding reads from a range scan's options dict."""
    text = _source('cpp_core_types_autogen.hxx')
    start = text.index('struct py_to_cbpp_t<couchbase::core::range_scan_orchestrator_options>')
    body = text[start:text.index('static inline PyObject* to_py', start)]
    return frozenset(re.findall(r'pycbc::keys::(\w+)', body))


def _token(value: Any, samples: tuple[Any, Any]) -> Any:
    # A sample is named by which one it is, so two instances of one class still compare apart.
    if value is samples[0]:
        return '<first sample>'
    if value is samples[1]:
        return '<second sample>'
    if isinstance(value, (str, int, float, bool, bytes, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return tuple(_token(item, samples) for item in value)
    if isinstance(value, dict):
        return {key: _token(item, samples) for key, item in value.items()}
    return type(value).__name__


def _operation(opcode: int) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', KeyValueBinding(opcode).name).lower()


def _request(req: pycbc_kv_request, samples: tuple[Any, Any], prefix: str = '') -> dict[str, Any]:
    # A field the builder never set reads as an AttributeError, and is left out.  The opcode is
    # named, so a field can be checked against the operation that carries it.
    sent = {f'{prefix}{name}': _token(getattr(req, name), samples) for name in _REQUEST_FIELDS if hasattr(req, name)}
    sent[f'{prefix}opcode'] = _operation(req.opcode)
    return sent


def _settled(block: str, key: str, value: Any, samples: tuple[Any, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """What the binding is handed, flattened, and what the builder keeps on the Python side."""
    argument = next((default for (b, _), default in ARGUMENT_KEYS.items() if b == block), None)
    if (block, key) in ARGUMENT_KEYS:
        argument, opts = value, None
    elif key == 'per_key_options':
        single = getattr(options, block.replace('Multi', ''))
        opts = getattr(options, block)(per_key_options={_KEY: single(**value)})
    else:
        opts = getattr(options, block)(**{key: value})
    handler = _RecordingHandler()
    with warnings.catch_warnings():
        # A deprecated key warns, which is option_diagnostics_t's to check.
        warnings.simplefilter('ignore')
        built = BUILDS[block](CollectionRequestBuilder(_details()), CollectionMultiRequestBuilder(_details()),
                              opts, handler, argument)
    held: dict[str, Any] = {'handler': _token(handler.calls, samples)}
    if isinstance(built, tuple):
        built, beside = built
        held['beside'] = _token(beside, samples)
    if isinstance(built, pycbc_kv_request):
        return _request(built, samples), held
    if isinstance(built, dict):
        scan = dict(built)
        held['transcoder'] = _token(scan.pop('transcoder'), samples)
        return {f'scan.{k}': _token(v, samples) for k, v in scan['orchestrator_options'].items()}, held
    sent: dict[str, Any] = {}
    for index, req in enumerate(built.request_list):
        sent.update(_request(req, samples, f'{index}.'))
    held['return_exceptions'] = built.return_exceptions
    held['key_transcoders'] = _token(getattr(built, 'key_transcoders', None), samples)
    return sent, held


def _differ(first: dict[str, Any], second: dict[str, Any]) -> list[str]:
    absent = object()
    return sorted(name for name in first.keys() | second.keys() if first.get(name, absent) != second.get(name, absent))


def _unread(sent: list[str], *builds: dict[str, Any]) -> list[str]:
    """The fields in `sent` that no operation carrying them reads."""
    unread = []
    for name in sent:
        prefix, _, field = name.rpartition('.')
        if prefix == 'scan':
            if field not in _scan_reads():
                unread.append(name)
            continue
        reads = _request_reads().get(field)
        ops = {build[f'{prefix}.opcode' if prefix else 'opcode'] for build in builds if name in build}
        if reads is not None and not ops & reads:
            unread.append(f'{name} on {", ".join(sorted(ops))}')
    return unread


def _samples(block: str, key: str) -> tuple[Any, Any] | None:
    if key == 'per_key_options':
        return {'timeout': timedelta(seconds=3)}, {'timeout': timedelta(seconds=5)}
    return SAMPLES_BY_KEY.get(key)


def _kv_blocks() -> list[str]:
    singles = [block.value for block in CollectionOptionsBlock]
    return singles + [name for name in BLOCKS if name.endswith('MultiOptions') and name.replace('Multi', '') in singles]


# Keys that do not yet do what they say.  Each case fails until it is fixed or declared inert, and
# strict, so the fix has to remove it from here.
GAPS: dict[tuple[str, str], str] = {}

# Keys the operation cannot carry out, so setting one raises.
REFUSED = {('GetMultiOptions', 'project')}


def _case(block: str, key: str) -> Any:
    gap = GAPS.get((block, key))
    marks = [pytest.mark.xfail(strict=True, reason=gap)] if gap else []
    return pytest.param(block, key, id=f'{block}.{key}', marks=marks)


def _cases() -> list[Any]:
    return [_case(block, key) for block in _kv_blocks() for key in BLOCKS[block].keys]


class KVOptionsCompletenessUnitTests:

    def test_every_block_has_a_build(self) -> None:
        assert sorted(_kv_blocks()) == sorted(BUILDS)

    def test_the_binding_reads_are_found(self) -> None:
        assert {'ids_only', 'concurrency', 'timeout'} <= _scan_reads()
        reads = _request_reads()
        assert reads['with_expiry'] == {'get_projected'}
        assert {'get', 'unlock', 'upsert_with_legacy_durability'} <= reads['timeout']

    @pytest.mark.parametrize('block, key', _cases())
    def test_every_key_reaches_something_or_is_inert(self, block: str, key: str) -> None:
        spec = BLOCKS[block].keys[key]
        samples = _samples(block, key)
        if samples is None:
            pytest.fail(f'no sample values for {block}.{key}; add a pair to SAMPLES_BY_KEY')
        if (block, key) in REFUSED:
            for value in samples:
                with pytest.raises(InvalidArgumentException):
                    _settled(block, key, value, samples)
            return
        sent_first, held_first = _settled(block, key, samples[0], samples)
        sent_second, held_second = _settled(block, key, samples[1], samples)
        sent, held = _differ(sent_first, sent_second), _differ(held_first, held_second)
        if 'classic' in spec.inert:
            assert sent == [], f'{block}.{key} is declared inert and is sent'
            return
        assert _unread(sent, sent_first, sent_second) == [], f'{block}.{key} reaches fields the binding never reads'
        assert sent or held, f'{block}.{key} is accepted and changes nothing'
