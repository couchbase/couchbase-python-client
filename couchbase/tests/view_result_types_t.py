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
import json
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

import pytest

from couchbase.protocol.classic._metadata import _view_metadata_from_core
from couchbase.protocol.classic._result import _view_row_from_core
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.views import ViewMetaData, ViewRow
from tests.test_features import TestProtocol

# Each record here is built by classic's adapter from what its binding returns.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import CppViewMetadata, CppViewRow


def _core_metadata(**overrides: Any) -> CppViewMetadata:
    raw: dict[str, Any] = {'total_rows': 12, 'debug_info': '{"main_group": {}}'}
    raw.update(overrides)
    return cast('CppViewMetadata', raw)


def _metadata(**overrides: Any) -> ViewMetaData:
    record = _view_metadata_from_core(_core_metadata(**overrides))
    assert record is not None
    return ViewMetaData(record)


class ViewResultTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: the metadata is built straight from the dict the core hands
    # the classic adapter and the row from the dict the core would have handed it, and
    # `couchbase.common.views` is the only definition of either.  Not generated for that reason.
    # `views_t` counts rows from real queries, which is a stronger claim than any metadata field;
    # what it cannot reach is the count's type, the difference between an absent count and a
    # zero one, and a response built twice.

    def test_metadata_total_rows_is_a_plain_int(self) -> None:
        total_rows = _metadata().total_rows()

        assert total_rows == 12
        # Analytics wraps its counts in `UnsignedInt64`, which is neither orderable against an
        # int nor equal to one, so this line is specific to views.
        assert total_rows is not None and total_rows >= 12

    # The core sends both keys and fills what the server left out with None.
    def test_metadata_total_rows_absent(self) -> None:
        assert _metadata(total_rows=None).total_rows() is None

    def test_metadata_total_rows_zero(self) -> None:
        assert _metadata(total_rows=0).total_rows() == 0

    def test_metadata_debug_info(self) -> None:
        assert _metadata().debug_info() == '{"main_group": {}}'

    def test_metadata_debug_info_absent(self) -> None:
        assert _metadata(debug_info=None).debug_info() is None

    def test_adapter_leaves_the_core_dict_unchanged(self) -> None:
        raw = _core_metadata()
        before = copy.deepcopy(raw)

        _view_metadata_from_core(raw)

        assert raw == before

    def test_one_core_dict_builds_two_records(self) -> None:
        raw = _core_metadata()

        assert _view_metadata_from_core(raw) == _view_metadata_from_core(raw)

    @pytest.mark.parametrize('raw', [None, {}])
    def test_absent_metadata_builds_no_record(self, raw: Any) -> None:
        assert _view_metadata_from_core(raw) is None

    def test_metadata_repr(self) -> None:
        assert repr(_metadata()) == "ViewMetaData({'total_rows': 12, 'debug_info': '{\"main_group\": {}}'})"

    def test_row_from_json_decodes_key_and_value(self) -> None:
        # Views sends both as JSON text rather than as JSON values, so a compound key arrives
        # as a list and not as the string that carried it.
        row = ViewRow.from_json({
            'id': 'doc-1',
            'key': json.dumps(['compound', 1]),
            'value': json.dumps({'count': 2}),
        })

        assert row.id == 'doc-1'
        assert row.key == ['compound', 1]
        assert row.value == {'count': 2}

    def test_row_from_json_without_key_or_value(self) -> None:
        row = ViewRow.from_json({'id': 'doc-1'})

        assert row.id == 'doc-1'
        assert row.key is None
        assert row.value is None
        # Only a query asked to include the documents fills this in, after the row is decoded.
        assert row.document is None

    def test_adapter_row_decodes_key_and_value(self) -> None:
        raw = cast('CppViewRow', {'id': 'doc-1', 'key': json.dumps(['compound', 1]), 'value': json.dumps({'count': 2})})
        before = copy.deepcopy(raw)

        row = _view_row_from_core(raw, DefaultJsonSerializer())

        assert row == ViewRow(key=['compound', 1], id='doc-1', value={'count': 2})
        assert raw == before

    def test_adapter_row_of_a_reduced_query_has_no_id(self) -> None:
        row = _view_row_from_core(cast('CppViewRow', {'key': 'null', 'value': '42'}), DefaultJsonSerializer())

        assert row == ViewRow(key=None, id=None, value=42)

    def test_adapter_row_reads_as_from_json_does(self) -> None:
        # `from_json` stays public; the SDK builds its rows through the adapter.
        for raw in ({'id': 'doc-1', 'key': '"a"', 'value': 'null'}, {'id': 'doc-1'}):
            assert _view_row_from_core(cast('CppViewRow', raw), DefaultJsonSerializer()) == ViewRow.from_json(raw)

    def test_adapter_row_key_and_value_are_read_by_the_serializer(self) -> None:
        class _Tagging(Serializer):
            def serialize(self, value: Any) -> bytes:
                raise NotImplementedError()

            def deserialize(self, value: bytes) -> Any:
                return ('tagged', value)

        row = _view_row_from_core(cast('CppViewRow', {'id': 'doc-1', 'key': '"a"', 'value': '2'}), _Tagging())

        assert (row.key, row.value) == (('tagged', b'"a"'), ('tagged', b'2'))
