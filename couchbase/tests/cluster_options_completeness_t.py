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

"""Every key a cluster accepts either does something on classic or is declared `inert` there.

Each key is set to two different values and the two clusters are compared.  What differs in the
options the binding is handed has to be a key the binding reads; what differs anywhere else in
`ClusterSettings` is held on the Python side.  A key that moves neither is accepted and lost,
unless the schema declares it `inert` on classic, and an `inert` key must never move a key the
binding reads.
"""

from __future__ import annotations

import dataclasses
import functools
import pathlib
import re
import warnings
from datetime import timedelta
from typing import Any

import pytest

import couchbase
from couchbase import USER_AGENT_EXTRA
from couchbase.auth import PasswordAuthenticator
from couchbase.common._observability import NoOpMeter, NoOpTracer
from couchbase.common._option_spec import BLOCKS
from couchbase.common.option_enums import IpProtocol, TLSVerifyMode
from couchbase.options import (ClusterMetricsOptions,
                               ClusterOptions,
                               ClusterOrphanReportingOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               TransactionConfig)
from couchbase.protocol.classic._cluster_impl import ClusterImpl
from couchbase.protocol.classic._wire import wire_of
from couchbase.serializer import DefaultJsonSerializer
from couchbase.transcoder import JSONTranscoder
from tests.test_features import TestProtocol

# What is checked here is classic's binding.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

_REPO_ROOT = pathlib.Path(couchbase.__file__).resolve().parents[1]

NESTED: dict[str, type[Any]] = {
    'timeout_options': ClusterTimeoutOptions,
    'tracing_options': ClusterTracingOptions,
    'orphan_reporting_options': ClusterOrphanReportingOptions,
    'metrics_options': ClusterMetricsOptions,
}

SAMPLES_BY_TRANSFORM: dict[str, tuple[Any, Any]] = {
    'validate_bool': (True, False),
    'validate_int': (7, 11),
    'validate_str': ('first', 'second'),
    'timedelta_as_milliseconds': (timedelta(seconds=3), timedelta(seconds=5)),
    'tls_verify_as_str': (TLSVerifyMode.NONE, TLSVerifyMode.PEER),
    'ip_protocol_as_str': (IpProtocol.ForceIPv4, IpProtocol.ForceIPv6),
    'csv_or_list': (['PLAIN'], ['SCRAM-SHA512']),
    'passthrough': (object(), object()),
}

# Passthrough keys the parser type-checks.
SAMPLES_BY_KEY: dict[str, tuple[Any, Any]] = {
    'option_validation': ('warn', 'strict'),
    'serializer': (DefaultJsonSerializer(), DefaultJsonSerializer()),
    'transcoder': (JSONTranscoder(), JSONTranscoder()),
    'transaction_config': (TransactionConfig(), TransactionConfig()),
    'tracer': (NoOpTracer(), NoOpTracer()),
    'meter': (NoOpMeter(), NoOpMeter()),
}

# The fields `connect_options` builds the binding's options from; every other field is held in Python.
_PROJECTED_FIELDS = frozenset({'cluster_options', 'timeout_options', 'orphan_options'})


@functools.cache
def _binding_reads() -> frozenset[str]:
    """Every options key the binding reads at connect, dotted under the nested dict it is read from.

    `update_cluster_options_from_py` reads the options dict, and the client adapter reads the keys
    it constructs the connection with.
    """
    source = _REPO_ROOT / 'src' / 'utils.hxx'
    if not source.exists():
        pytest.skip('the extension source is not beside the package')
    text = source.read_text()
    start = text.index('update_cluster_options_from_py(')
    body = text[start:text.index('\ninline ', start)]
    prefixes = {'pyObj_options': ''}
    for var, parent, key in re.findall(r'PyObject\*\s+(\w+)\s*=\s*PyDict_GetItemString\((\w+),\s*"(\w+)"\)', body):
        if parent in prefixes:
            prefixes[var] = f'{prefixes[parent]}{key}.'
    reads = {prefixes[var] + key for var, key in re.findall(r'\(\s*(pyObj_\w+)\s*,\s*"(\w+)"', body) if var in prefixes}
    adapter = (_REPO_ROOT / 'couchbase' / 'protocol' / 'classic' / '_client_adapter.py').read_text()
    reads.update(re.findall(r"connect_req\.options\.get\('(\w+)'", adapter))
    return frozenset(reads)


def _token(value: Any, samples: tuple[Any, Any]) -> Any:
    # A sample is named by which one it is, so two instances of one class still compare apart.
    if value is samples[0]:
        return '<first sample>'
    if value is samples[1]:
        return '<second sample>'
    if isinstance(value, (str, int, float, bool, timedelta, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return tuple(_token(item, samples) for item in value)
    if isinstance(value, dict):
        return {key: _token(item, samples) for key, item in value.items()}
    return type(value).__name__


def _flatten(mapping: dict[str, Any], samples: tuple[Any, Any], prefix: str = '') -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in mapping.items():
        if isinstance(value, dict):
            flat.update(_flatten(value, samples, f'{prefix}{key}.'))
        else:
            flat[f'{prefix}{key}'] = _token(value, samples)
    return flat


def _settled(block: str, key: str, value: Any, samples: tuple[Any, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """What the binding is handed, flattened, and every field of `ClusterSettings` it is not built from."""
    authenticator = PasswordAuthenticator('Administrator', 'password')
    if block == 'ClusterOptions':
        options = ClusterOptions(authenticator, **{key: value})
    else:
        container = next(name for name, cls in NESTED.items() if cls.__name__ == block)
        options = ClusterOptions(authenticator, **{container: NESTED[container](**{key: value})})
    with warnings.catch_warnings():
        # A deprecated or inert key warns, which is option_diagnostics_t's to check.
        warnings.simplefilter('ignore')
        impl = ClusterImpl('couchbase://localhost', options, skip_connect='TEST_SKIP_CONNECT')
    settings = impl.cluster_settings
    held: dict[str, Any] = {}
    for field in dataclasses.fields(settings):
        if field.name in _PROJECTED_FIELDS:
            continue
        held_value = getattr(settings, field.name)
        if field.name == 'observability_instruments':
            held_value = (held_value.tracer.tracer, held_value.tracer.is_legacy, held_value.meter)
        held[field.name] = _token(held_value, samples)
    return _flatten(impl.connect_options, samples), held


def _differ(first: dict[str, Any], second: dict[str, Any]) -> list[str]:
    absent = object()
    return sorted(name for name in first.keys() | second.keys() if first.get(name, absent) != second.get(name, absent))


def _cases() -> list[Any]:
    cases = []
    for block in ('ClusterOptions', *(cls.__name__ for cls in NESTED.values())):
        for key in BLOCKS[block].keys:
            if block == 'ClusterOptions' and (key == 'authenticator' or key in NESTED):
                continue
            cases.append(pytest.param(block, key, id=f'{block}.{key}'))
    return cases


class ClusterOptionsCompletenessUnitTests:

    def test_the_binding_reads_are_found(self) -> None:
        reads = _binding_reads()
        assert {'enable_tls', 'timeout_options.key_value_timeout', 'num_io_threads'} <= reads

    @pytest.mark.parametrize('block, key', _cases())
    def test_every_key_reaches_something_or_is_inert(self, block: str, key: str) -> None:
        spec = BLOCKS[block].keys[key]
        transform = getattr(wire_of(block, key).transform, '__name__', '')
        samples = SAMPLES_BY_KEY.get(key) or SAMPLES_BY_TRANSFORM.get(transform)
        if samples is None:
            pytest.fail(f'no sample values for {block}.{key}; add a pair to SAMPLES_BY_KEY')
        sent_first, held_first = _settled(block, key, samples[0], samples)
        sent_second, held_second = _settled(block, key, samples[1], samples)
        sent, held = _differ(sent_first, sent_second), _differ(held_first, held_second)
        reads = _binding_reads()
        if 'classic' in spec.inert:
            assert [name for name in sent if name in reads] == [], f'{block}.{key} is declared inert and is read'
            return
        assert [name for name in sent if name not in reads] == [], f'{block}.{key} reaches keys the binding never reads'
        assert sent or held, f'{block}.{key} is accepted and changes nothing'

    def test_user_agent_extra_follows_the_sdks_own(self) -> None:
        impl = ClusterImpl('couchbase://localhost',
                           ClusterOptions(PasswordAuthenticator('Administrator',
                                          'password'), user_agent_extra='app/1.0'),
                           skip_connect='TEST_SKIP_CONNECT')
        assert impl.connect_options['user_agent_extra'] == ';'.join(filter(None, (USER_AGENT_EXTRA, 'app/1.0')))

    def test_user_agent_extra_is_the_sdks_alone_when_not_set(self) -> None:
        impl = ClusterImpl('couchbase://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')),
                           skip_connect='TEST_SKIP_CONNECT')
        assert impl.connect_options['user_agent_extra'] == USER_AGENT_EXTRA

    def test_sasl_mechanisms_reach_the_credentials_alone(self) -> None:
        impl = ClusterImpl('couchbase://localhost?allowed_sasl_mechanisms=PLAIN',
                           ClusterOptions(PasswordAuthenticator('Administrator', 'password')),
                           skip_connect='TEST_SKIP_CONNECT')
        assert 'allowed_sasl_mechanisms' not in impl.connect_options
        assert impl.cluster_settings.auth['allowed_sasl_mechanisms'] == ['PLAIN']
