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

import inspect
from typing import Any, cast

import pytest

from couchbase.common._operations import Op, OpKind
from couchbase.protocol.classic import _bucket_types, _cluster_types
from couchbase.protocol.classic._binding_map import BindingMap
from couchbase.protocol.classic._bindings import KV_OPCODE
from couchbase.protocol.classic._management import (_analytics_mgmt_types,
                                                    _bucket_mgmt_types,
                                                    _collection_mgmt_types,
                                                    _eventing_function_mgmt_types,
                                                    _query_index_mgmt_types,
                                                    _search_index_mgmt_types,
                                                    _user_mgmt_types,
                                                    _view_index_mgmt_types)
from couchbase.protocol.classic._pycbc_core import pycbc_connection
from couchbase.protocol.classic._pycbc_core.binding_map_types import PycbcConnectionKVOps
from tests.test_features import TestProtocol

# What is checked here is classic's binding tables.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

# The operations classic carries out with a call not named after them.  The analytics link
# request builder picks the call by link type, and each search index control call is shared by
# an operation and its opposite, which it is told apart from by an argument.
_CALLS_BY_OP = {
    Op.AnalyticsLinkCreate: {'analytics_link_create_azure_blob_external_link',
                             'analytics_link_create_couchbase_remote_link',
                             'analytics_link_create_s3_external_link'},
    Op.AnalyticsLinkReplace: {'analytics_link_replace_azure_blob_external_link',
                              'analytics_link_replace_couchbase_remote_link',
                              'analytics_link_replace_s3_external_link'},
    Op.SearchIndexAllowQuerying: {'search_index_control_query'},
    Op.SearchIndexDisallowQuerying: {'search_index_control_query'},
    Op.SearchIndexFreezePlan: {'search_index_control_plan_freeze'},
    Op.SearchIndexUnfreezePlan: {'search_index_control_plan_freeze'},
    Op.SearchIndexPauseIngest: {'search_index_control_ingest'},
    Op.SearchIndexResumeIngest: {'search_index_control_ingest'},
    Op.RangeScan: {'kv_range_scan'},
}

# The key-value operation that picks a second call by its options: a get asking for a projection
# or for the expiry is a get_projected, which is called by its binding and not looked up.
_KV_CALLS_BY_OP = {Op.Get: {'get_projected'}}

# Composed in Python from other operations, each of which has a call of its own.  A
# datastructure operation is the same, and is told by its kind instead.
_COMPOSITES = frozenset({Op.QueryIndexWatchIndexes, Op.ViewIndexPublish, Op.WaitUntilReady})

_REQUEST_MODULES = (_analytics_mgmt_types, _bucket_mgmt_types, _bucket_types, _cluster_types,
                    _collection_mgmt_types, _eventing_function_mgmt_types,
                    _query_index_mgmt_types, _search_index_mgmt_types, _user_mgmt_types,
                    _view_index_mgmt_types)


class _NamedConnection:
    """Stands in for a connection, answering each binding call with its own name."""

    def __getattr__(self, name: str) -> str:
        return name


def _binding_map() -> BindingMap:
    return BindingMap(cast(pycbc_connection, _NamedConnection()))


def _kv_calls(op: Op) -> set[str]:
    looked_up = {PycbcConnectionKVOps._fields[opcode] for (kv_op, _), opcode in KV_OPCODE.items() if kv_op is op}
    return looked_up | _KV_CALLS_BY_OP.get(op, set())


def _calls(op: Op) -> set[str]:
    if op.kind in (OpKind.KeyValue, OpKind.KeyValueMulti):
        return _kv_calls(op)
    return _CALLS_BY_OP.get(op, {op.value})


def _binding_methods() -> list[tuple[str, str]]:
    found = []
    for module in _REQUEST_MODULES:
        for name, cls in inspect.getmembers(module, inspect.isclass):
            prop = cls.__dict__.get('binding_method')
            if cls.__module__ == module.__name__ and isinstance(prop, property) and prop.fget is not None:
                if not getattr(prop.fget, '__isabstractmethod__', False):
                    found.append((f'{module.__name__}.{name}', prop.fget(cast(Any, None))))
    return found


_CALLED = [op for op in Op if op.kind is not OpKind.Datastructure and op not in _COMPOSITES]


class ClassicBindingsUnitTests:
    # No cb_env and no TEST_MANIFEST: each test reads the tables that join an `Op` to the call
    # classic makes for it, and the connection type the calls are on.  Not generated, since
    # every API dispatches through the same tables.
    #
    # Classic reaches a key-value call by its position in `PycbcConnectionKVOps`, a streaming
    # call by calling it, and every other call by its name in `BindingMap.op_map`, so the three
    # are checked in their own ways.  A request class is what chooses a name, which is why the
    # last test reads them.

    @pytest.mark.parametrize('op', _CALLED, ids=lambda op: op.name)
    def test_every_operation_reaches_a_call_on_the_connection(self, op: Op) -> None:
        calls = _calls(op)
        assert calls, f'{op} has no binding call'
        for call in calls:
            assert hasattr(pycbc_connection, f'pycbc_{call}'), f'{op} names {call!r}, not a binding call'
        if op.kind not in (OpKind.KeyValue, OpKind.KeyValueMulti, OpKind.Streaming):
            assert calls <= set(_binding_map().op_map), f'{op} names a call op_map does not hold'

    @pytest.mark.parametrize('op', [op for op in Op if op.kind in (OpKind.KeyValue, OpKind.KeyValueMulti)],
                             ids=lambda op: op.name)
    def test_a_key_value_opcode_is_the_position_of_its_call(self, op: Op) -> None:
        # The opcode indexes `PycbcConnectionKVOps` and the generator renders the two from
        # separate templates, so they agree only while both keep the same order.
        assert PycbcConnectionKVOps._fields[KV_OPCODE[(op, False)]] == op.value
        legacy = KV_OPCODE.get((op, True))
        if legacy is None:
            return
        single = op.single or op
        suffix = '_multi' if op.single else ''
        assert PycbcConnectionKVOps._fields[legacy] == f'{single.value}_with_legacy_durability{suffix}'
        # A multi request makes each key's request as its single, with the same durability.
        assert (single, True) in KV_OPCODE

    def test_every_call_is_reached_by_an_operation(self) -> None:
        reached = set().union(*(_calls(op) for op in _CALLED))
        assert set(_binding_map().op_map) - reached == set()
        assert set(PycbcConnectionKVOps._fields) - reached == set()

    def test_op_map_calls_each_key_by_its_own_name(self) -> None:
        mismatched = {k: v for k, v in _binding_map().op_map.items() if v != f'pycbc_{k}'}
        assert mismatched == {}

    def test_every_request_class_names_a_call_in_op_map(self) -> None:
        found = _binding_methods()
        assert found
        op_map = set(_binding_map().op_map)
        assert [(cls, method) for cls, method in found if method not in op_map] == []
