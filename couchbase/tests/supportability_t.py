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

import contextlib
import importlib
import os
import subprocess
import sys
import warnings
from collections.abc import Mapping
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

import couchbase
from couchbase import subdocument
from couchbase.collection import Collection
from couchbase.common._capabilities import Capability
from couchbase.common._cluster_settings import build_tracing_and_orphan_options
from couchbase.common._deprecated_paths import PATHS
from couchbase.common._option_args import resolve_deprecated
from couchbase.common._option_spec import BLOCKS
from couchbase.common._supportability import DURABILITY_LEVEL_READ, Supportability
from couchbase.common._tracing import CouchbaseTracer
from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.diagnostics import EndpointDiagnosticsReport
from couchbase.exceptions import CouchbaseException, FeatureUnavailableException
from couchbase.management.buckets import BucketSettings
from couchbase.options import (ClusterMetricsOptions,
                               ClusterOptions,
                               ClusterOrphanReportingOptions,
                               ClusterTimeoutOptions,
                               ClusterTracingOptions,
                               TransactionConfig)
from couchbase.result import (CounterResult,
                              GetReplicaResult,
                              GetResult,
                              ValueResult)
from couchbase.scope import Scope
from couchbase.search import TermRangeQuery
from couchbase.transcoder import JSONTranscoder
from tests.test_features import TestProtocol


class FakeRequestSpan:
    """Satisfies `RequestSpanProtocol` and not `LegacySpanProtocol`, so passing it as ``span``
    reaches the deprecation and nothing else."""

    @property
    def name(self) -> str:
        return 'fake'

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_attributes(self, attributes: Mapping[str, Any]) -> None:
        pass

    def add_event(self, name: str, value: Any) -> None:
        pass

    def set_status(self, status: Any) -> None:
        pass

    def end(self, timestamp: int | None) -> None:
        pass


_PACKAGE_PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(couchbase.__file__)))


class RecordingCollectionImpl:
    """Stands in for a collection impl and its `legacy_datastructures`: records each call made on it and returns a
    sentinel."""

    def __init__(self, capabilities: frozenset[Capability]) -> None:
        self.capabilities = capabilities
        self.legacy_datastructures = self
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.returned = object()

    def __getattr__(self, name: str) -> Any:
        def record(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((name, args, kwargs))
            return self.returned
        return record


def collection_over(impl: RecordingCollectionImpl) -> Any:
    scope: Any = SimpleNamespace(_impl=SimpleNamespace(collection=lambda name: impl))
    return Collection(scope, 'test-collection')


LEGACY_DATASTRUCTURE_CALLS = [
    ('list_append', ('k', 'v'), {'create': True}, 'CouchbaseList.append', ('k', 'v', True)),
    ('list_prepend', ('k', 'v'), {}, 'CouchbaseList.prepend', ('k', 'v', False)),
    ('list_set', ('k', 1, 'v'), {}, 'CouchbaseList.set_at', ('k', 1, 'v')),
    ('list_get', ('k', 1), {}, 'CouchbaseList.get_at', ('k', 1)),
    ('list_remove', ('k', 1), {}, 'CouchbaseList.remove_at', ('k', 1)),
    ('list_size', ('k',), {}, 'CouchbaseList.size', ('k',)),
    ('map_add', ('k', 'mk', 'v'), {}, 'CouchbaseMap.add', ('k', 'mk', 'v', False)),
    ('map_get', ('k', 'mk'), {}, 'CouchbaseMap.get', ('k', 'mk')),
    ('map_remove', ('k', 'mk'), {}, 'CouchbaseMap.remove', ('k', 'mk')),
    ('map_size', ('k',), {}, 'CouchbaseMap.size', ('k',)),
    ('set_add', ('k', 'v'), {'create': True}, 'CouchbaseSet.add', ('k', 'v', True)),
    ('set_remove', ('k', 'v'), {}, 'CouchbaseSet.remove', ('k', 'v')),
    ('set_size', ('k',), {}, 'CouchbaseSet.size', ('k',)),
    ('set_contains', ('k', 'v'), {}, 'CouchbaseSet.contains', ('k', 'v')),
    ('queue_push', ('k', 'v'), {}, 'CouchbaseQueue.push', ('k', 'v', False)),
    ('queue_pop', ('k',), {}, 'CouchbaseQueue.pop', ('k',)),
    ('queue_size', ('k',), {}, 'CouchbaseQueue.size', ('k',)),
]


def deprecations(recorded: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    return [w for w in recorded if issubclass(w.category, CouchbaseDeprecationWarning)]


def next_line() -> int:
    return sys._getframe(1).f_lineno + 1


class SupportabilityUnitTests:
    # No cb_env and no TEST_MANIFEST: every warning here is raised by code that never reaches a cluster.

    def test_warning_points_at_the_caller(self) -> None:
        query = TermRangeQuery(min='a', max='b')

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            query.start_inclusive = True

        warned = deprecations(recorded)
        assert len(warned) == 1
        assert warned[0].filename == __file__
        assert warned[0].lineno == line

    def test_warning_points_at_the_caller_through_an_internal_chain(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            TermRangeQuery(field='f', start='a', end='b', start_inclusive=True)

        # three warnings from three lines of search_queries.py, two of them raised directly
        # by __init__ and the third through the start_inclusive setter it calls
        warned = deprecations(recorded)
        assert len(warned) == 3
        assert {w.lineno for w in warned} == {line}
        assert {w.filename for w in warned} == {__file__}

    def test_each_call_site_warns_once(self) -> None:
        query = TermRangeQuery(min='a', max='b')

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('default')
            for _ in range(5):
                query.start_inclusive = True
            query.start_inclusive = True
            query.start_inclusive = True

        # a loop is one call site and collapses to one warning; the other two are two lines
        assert len(deprecations(recorded)) == 3

    @pytest.mark.parametrize('use_instead, message, expected', [
        ('Thing.new', None,
         'Class property Thing.old is deprecated and will be removed in a future release. Use Thing.new instead.'),
        (None, 'It has no replacement.',
         'Class property Thing.old is deprecated and will be removed in a future release. It has no replacement.'),
        ('Thing.new', 'See the docs.',
         'Class property Thing.old is deprecated and will be removed in a future release. Use Thing.new instead. '
         'See the docs.'),
    ])
    def test_class_property_deprecated_message(self, use_instead: str | None, message: str | None,
                                               expected: str) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            Supportability.class_property_deprecated('Thing.old', use_instead=use_instead, message=message)

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [expected]
        assert warned[0].lineno == line

    # Each statement runs first in its own interpreter, since an import cycle only shows when
    # neither module is loaded yet.
    @pytest.mark.parametrize('statement', [
        'import couchbase.scope',
        'import couchbase.collection',
        'from couchbase.collection import Scope',
    ])
    def test_scope_and_collection_import_in_either_order(self, statement: str) -> None:
        env = dict(os.environ)
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [_PACKAGE_PARENT_DIR, os.environ.get('PYTHONPATH')]))
        proc = subprocess.run([sys.executable, '-c', statement], env=env, capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0, proc.stderr

    @pytest.mark.parametrize('module, name', [(m, n) for m, names in PATHS.items() for n in names])
    def test_a_deprecated_path_warns_where_it_is_imported(self, module: str, name: str) -> None:
        imported = importlib.import_module(module)
        origin, advice = PATHS[module][name]

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            served = getattr(imported, name)

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            f'Importing {name} from {module} is deprecated and will be removed in a future release. {advice}']
        assert warned[0].lineno == line
        assert served is getattr(importlib.import_module(origin), name)

    def test_a_deprecated_path_warns_on_every_import(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            from couchbase.collection import Scope as Again
            from couchbase.collection import Scope as DeprecatedScope

        assert len(deprecations(recorded)) == 2
        assert DeprecatedScope is Again is Scope

    def test_a_name_the_module_does_not_serve_is_an_attribute_error(self) -> None:
        import couchbase.cluster
        with pytest.raises(AttributeError, match="module 'couchbase.cluster' has no attribute 'Nope'"):
            getattr(couchbase.cluster, 'Nope')

    def test_http_result_warns_on_construction(self) -> None:
        from couchbase._legacy import HttpResult
        orig = object()

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            result = HttpResult(orig)

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            'SDK type HttpResult is deprecated and will be removed in a future release. '
            'Nothing in the SDK returns one.']
        assert warned[0].lineno == line
        assert result._orig is orig

    def test_async_scope_connection_warns(self) -> None:
        from acouchbase.scope import AsyncScope
        connection = object()
        bucket: Any = SimpleNamespace(_impl=SimpleNamespace(scope=lambda name: SimpleNamespace(connection=connection)))
        scope = AsyncScope(bucket, 'test-scope')

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            got = scope.connection

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            'Class property AsyncScope.connection is deprecated and will be removed in a future release.']
        assert warned[0].lineno == line
        assert got is connection

    @pytest.mark.parametrize('method, args, kwargs, use_instead, forwarded', LEGACY_DATASTRUCTURE_CALLS,
                             ids=[c[0] for c in LEGACY_DATASTRUCTURE_CALLS])
    def test_legacy_datastructure_method_warns_and_delegates(self,
                                                             method: str,
                                                             args: tuple[Any, ...],
                                                             kwargs: dict[str, Any],
                                                             use_instead: str,
                                                             forwarded: tuple[Any, ...]) -> None:
        impl = RecordingCollectionImpl(frozenset({Capability.LEGACY_DATASTRUCTURES}))
        collection = collection_over(impl)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            got = getattr(collection, method)(*args, **kwargs)

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            f'Method {method} is deprecated and will be removed in a future release. Use {use_instead} instead.']
        assert warned[0].filename == __file__
        assert warned[0].lineno == line
        assert impl.calls == [(method, forwarded, {})]
        assert got is impl.returned

    @pytest.mark.parametrize('method, args, kwargs, use_instead, forwarded', LEGACY_DATASTRUCTURE_CALLS,
                             ids=[c[0] for c in LEGACY_DATASTRUCTURE_CALLS])
    def test_legacy_datastructure_method_needs_the_capability(self,
                                                              method: str,
                                                              args: tuple[Any, ...],
                                                              kwargs: dict[str, Any],
                                                              use_instead: str,
                                                              forwarded: tuple[Any, ...]) -> None:
        impl = RecordingCollectionImpl(frozenset())
        collection = collection_over(impl)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            with pytest.raises(FeatureUnavailableException):
                getattr(collection, method)(*args, **kwargs)

        assert len(deprecations(recorded)) == 1
        assert impl.calls == []


@pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)
class ClassicSupportabilityUnitTests:
    # Its classic imports are inside the tests: at module level they would tie the shared tests above
    # to classic too.

    def test_warning_points_at_the_caller_through_a_request_builder(self) -> None:
        from couchbase.protocol.classic._collection_req_builder import CollectionRequestBuilder
        from couchbase.protocol.classic._collection_types import CollectionDetails

        details = CollectionDetails('test-bucket', 'test-scope', 'test-collection', JSONTranscoder())
        builder = CollectionRequestBuilder(details)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            builder.build_append_request('key', 'value', None, span=FakeRequestSpan())

        warned = deprecations(recorded)
        assert len(warned) == 1
        assert 'span' in str(warned[0].message)
        assert warned[0].filename == __file__
        assert warned[0].lineno == line

    def test_a_block_with_nothing_deprecated_maps_without_warning(self) -> None:
        from couchbase.protocol.classic._wire import to_wire

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            assert to_wire({'timeout': timedelta(seconds=1)}, 'GetOptions') == {'timeout': 1000}

        assert deprecations(recorded) == []


class ReachAnything:
    """Any attribute or call on it answers with itself, so a facade method handed it as ``self`` runs past its
    first line."""

    def __getattr__(self, name: str) -> Any:
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self


DEPRECATED_METHODS = [
    ('couchbase.durability', 'DurabilityLevel', 'from_server_str', (), DURABILITY_LEVEL_READ),
]

DEPRECATED_PROPERTIES = [
    (CounterResult, 'cas', SimpleNamespace(_record=SimpleNamespace(cas=7)), 7, None),
    (GetReplicaResult, 'is_active', SimpleNamespace(_record=SimpleNamespace(is_replica=True)), False, 'is_replica'),
    (GetResult, 'expiryTime', SimpleNamespace(_record=SimpleNamespace(expiry=None)), None, 'expiry_time'),
    (EndpointDiagnosticsReport, 'type', SimpleNamespace(service_type='kv'), 'kv', 'service_type'),
    (EndpointDiagnosticsReport, 'last_activity', SimpleNamespace(last_activity_us=3), 3, 'last_activity_us'),
    (CouchbaseException, 'context', SimpleNamespace(error_context='ctx'), 'ctx', 'error_context'),
    (BucketSettings, 'max_ttl', {'max_expiry': 9}, 9, 'max_expiry'),
]

# A value of each deprecated option's own type, where a string is not one.
OPTION_VALUES: dict[str, Any] = {'logging_meter_emit_interval': timedelta(seconds=1),
                                 'tracing_orphaned_queue_flush_interval': timedelta(seconds=1),
                                 'tracing_orphaned_queue_size': 1,
                                 'expiration_time': timedelta(seconds=1)}

DEPRECATED_OPTIONS = [(name, key, spec.deprecated) for name, entry in BLOCKS.items()
                      for key, spec in entry.keys.items() if spec.deprecated]


class DeprecationAlignmentUnitTests:
    """Everything documented as deprecated warns, once, at the caller's line, in its documentation's words.

    Except the methods a deprecated service is reached through, `view_query`, `view_indexes` and `search_query`:
    each is called once per request, so they are documented as deprecated and do not warn.
    """

    @pytest.mark.parametrize('module, cls, method, args, tail', DEPRECATED_METHODS,
                             ids=[f'{m}.{c}.{f}' for m, c, f, _, _ in DEPRECATED_METHODS])
    def test_deprecated_method_warns_before_it_does_anything(self, module: str, cls: str, method: str,
                                                             args: tuple[Any, ...], tail: str) -> None:
        unbound = getattr(getattr(importlib.import_module(module), cls), method)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            with contextlib.suppress(Exception):
                line = next_line()
                unbound(ReachAnything(), *args)

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            f'Method {method} is deprecated and will be removed in a future release. {tail}']
        assert warned[0].filename == __file__
        assert warned[0].lineno == line

    @pytest.mark.parametrize('owner, name, holder, expected, current', DEPRECATED_PROPERTIES,
                             ids=[f'{o.__name__}.{n}' for o, n, _, _, _ in DEPRECATED_PROPERTIES])
    def test_deprecated_property_warns_and_reads_the_current_field(self, owner: type, name: str, holder: Any,
                                                                   expected: Any, current: str | None) -> None:
        getter = getattr(owner, name).fget

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            got = getter(holder)

        warned = deprecations(recorded)
        message = f'Class property {owner.__name__}.{name} is deprecated and will be removed in a future release.'
        if current is not None:
            message += f' Use {owner.__name__}.{current} instead.'
        assert [str(w.message) for w in warned] == [message]
        assert warned[0].lineno == line
        assert got == expected

    def test_counter_result_success_reads_no_deprecated_property(self) -> None:
        holder = SimpleNamespace(_record=SimpleNamespace(cas=7))

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            assert getattr(CounterResult, 'success').fget(holder) is True

        assert deprecations(recorded) == []

    def test_subdocument_counter_and_value_result_warn(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            subdocument.counter('path', 1)
            ValueResult(1, None, 'value')

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            'Method counter is deprecated and will be removed in a future release. Use increment or decrement '
            'instead.',
            'SDK type ValueResult is deprecated and will be removed in a future release. ']
        assert {w.lineno for w in warned} == {line, line + 1}

    def test_legacy_tracer_warns_once_when_the_cluster_reads_it(self) -> None:
        class Tracer(CouchbaseTracer):
            def start_span(self, name: str, parent: Any = None) -> Any:
                return None

        tracer = Tracer(object())
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            build_tracing_and_orphan_options({'tracer': tracer})

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            'SDK type CouchbaseTracer is deprecated and will be removed in a future release. Use '
            'couchbase.observability.tracing.RequestTracer instead. CouchbaseSpan is deprecated with it; use '
            'couchbase.observability.tracing.RequestSpan instead.']

    @pytest.mark.parametrize('block, key, text', DEPRECATED_OPTIONS, ids=[f'{b}.{k}' for b, k, _ in DEPRECATED_OPTIONS])
    def test_deprecated_option_warns_where_it_is_resolved(self, block: str, key: str, text: str) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            resolve_deprecated(BLOCKS[block], {key: OPTION_VALUES.get(key, 'value')})

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            f'Option {key} is deprecated and will be removed in a future release. {text}']
        assert warned[0].lineno == line

    @pytest.mark.parametrize('cls', [ClusterMetricsOptions, ClusterOrphanReportingOptions, ClusterTimeoutOptions,
                                     ClusterTracingOptions])
    def test_keys_as_sent_are_deprecated_and_answer_the_keys(self, cls: Any) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            keys = cls.get_allowed_option_keys(use_transform_keys=True)

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            'Method get_allowed_option_keys(use_transform_keys=True) is deprecated and will be removed in a future '
            "release. Use get_allowed_option_keys() instead. What a key is sent as is the protocol's, so it answers "
            'with the same keys.']
        assert warned[0].lineno == line
        assert keys == cls.get_allowed_option_keys()

    def test_the_options_mapping_is_deprecated_and_answers_each_key_as_itself(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            line = next_line()
            mapping = ClusterOptions.get_valid_options()

        warned = deprecations(recorded)
        assert [str(w.message) for w in warned] == [
            'Method ClusterOptions.get_valid_options is deprecated and will be removed in a future release. '
            "What a key is sent as is the protocol's, so each key is answered under its own name and unconverted."]
        assert warned[0].lineno == line
        assert mapping['kv_timeout'] == ('kv_timeout', None)

    def test_a_deprecated_option_into_another_loses_to_it(self) -> None:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter('always')
            alone = dict(TransactionConfig(expiration_time=timedelta(seconds=5)))
            both = dict(TransactionConfig(expiration_time=timedelta(seconds=5), timeout=timedelta(seconds=9)))
            current = dict(TransactionConfig(timeout=timedelta(seconds=9)))

        assert (alone, both, current) == ({'timeout': timedelta(seconds=5)},
                                          {'timeout': timedelta(seconds=9)},
                                          {'timeout': timedelta(seconds=9)})
        assert len(deprecations(recorded)) == 2
