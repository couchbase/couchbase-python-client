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
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

import pytest

from couchbase.common.warnings import CouchbaseDeprecationWarning
from couchbase.exceptions import InvalidArgumentException
from couchbase.protocol.classic._result import _search_row_from_core
from couchbase.search import (SearchRow,
                              SearchRowFields,
                              SearchRowLocation,
                              SearchRowLocations)
from couchbase.serializer import DefaultJsonSerializer, Serializer
from tests.test_features import TestProtocol

# Each record here is built by classic's adapter from what its binding returns.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import CppSearchRow


def _location(field: str, term: str, position: int, start: int, end: int,
              array_positions: list[int] | None = None) -> dict[str, Any]:
    return {'field': field, 'term': term, 'position': position, 'start_offset': start, 'end_offset': end,
            'array_positions': array_positions}


_JSON = DefaultJsonSerializer()


class _TaggingSerializer(Serializer):
    # Wraps what it reads, so a test can tell it ran from what JSON alone would have produced.

    def serialize(self, value: Any) -> bytes:
        raise NotImplementedError()

    def deserialize(self, value: bytes) -> Any:
        return {'tagged': value.decode('utf-8')}


def _core_row(**overrides: Any) -> CppSearchRow:
    raw: dict[str, Any] = {'index': 'hotels_6c1a', 'id': 'hotel_1', 'score': 1.25, 'locations': [],
                           'fragments': {}, 'fields': '', 'explanation': ''}
    raw.update(overrides)
    return cast('CppSearchRow', raw)


# Two terms in one field and one term in two fields, so every method has something to tell apart.
_LOCATIONS = [_location('name', 'inn', 1, 0, 3),
              _location('name', 'hotel', 2, 4, 9, [0]),
              _location('city', 'inn', 1, 0, 3, [1, 2])]


def _locations() -> SearchRowLocations:
    locations = _search_row_from_core(_core_row(locations=copy.deepcopy(_LOCATIONS)), _JSON).locations
    assert locations is not None
    return locations


class SearchResultTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: each row is built from the dict the core hands the classic
    # adapter, and `couchbase.common.search` is the only definition of it.  Not generated for that
    # reason.  `search_t` reads rows from real queries; what it cannot reach is a row with a given
    # set of locations, or a field the query did not ask for.

    def test_adapter_leaves_the_core_row_as_the_core_sent_it(self) -> None:
        raw = _core_row(locations=copy.deepcopy(_LOCATIONS), fields='{"name": "Inn"}',
                        explanation='{"value": 1.2}', fragments={'name': ['<mark>Inn</mark>']})
        before = copy.deepcopy(raw)

        _search_row_from_core(raw, _JSON)

        assert raw == before

    def test_one_core_row_builds_two_equal_rows(self) -> None:
        raw = _core_row(locations=copy.deepcopy(_LOCATIONS), fields='{"name": "Inn"}')

        assert _search_row_from_core(raw, _JSON) == _search_row_from_core(raw, _JSON)

    def test_row_decodes_fields_and_explanation(self) -> None:
        row = _search_row_from_core(_core_row(fields='{"name": "Inn", "rating": 4}',
                                              explanation='{"value": 1.2, "message": "sum of:"}'), _JSON)

        assert isinstance(row.fields, SearchRowFields)
        assert row.fields == {'name': 'Inn', 'rating': 4}
        assert row.explanation == {'value': 1.2, 'message': 'sum of:'}
        assert (row.index, row.id, row.score) == ('hotels_6c1a', 'hotel_1', 1.25)

    @pytest.mark.parametrize('text', ['', '   '])
    def test_row_without_fields_or_explanation(self, text: str) -> None:
        # The core sends both as JSON text, and as an empty string when the query did not ask.
        row = _search_row_from_core(_core_row(fields=text, explanation=text), _JSON)

        assert row.fields is None
        assert row.explanation == {}

    def test_row_without_locations_has_none(self) -> None:
        assert _search_row_from_core(_core_row(), _JSON).locations is None

    def test_row_sort_is_deprecated_and_always_empty(self) -> None:
        row = _search_row_from_core(_core_row(), _JSON)

        with pytest.warns(CouchbaseDeprecationWarning, match=r'SearchRow\.sort is deprecated'):
            assert row.sort == []
        assert 'sort' not in repr(row)

    def test_locations_are_read_in_the_order_the_core_sent_them(self) -> None:
        assert _locations().get_all() == [
            SearchRowLocation(field='name', term='inn', position=1, start=0, end=3, array_positions=None),
            SearchRowLocation(field='name', term='hotel', position=2, start=4, end=9, array_positions=[0]),
            SearchRowLocation(field='city', term='inn', position=1, start=0, end=3, array_positions=[1, 2]),
        ]

    def test_locations_get_one_field_and_term(self) -> None:
        assert _locations().get('name', 'inn') == [
            SearchRowLocation(field='name', term='inn', position=1, start=0, end=3, array_positions=None)]
        assert [loc.field for loc in _locations().get('city', 'inn')] == ['city']

    @pytest.mark.parametrize('field, term, message', [
        ('rating', 'inn', 'Cannot find "rating" field'),
        ('city', 'hotel', 'Cannot find "hotel" within city'),
    ])
    def test_locations_get_what_is_not_there_raises(self, field: str, term: str, message: str) -> None:
        with pytest.raises(InvalidArgumentException, match=message):
            _locations().get(field, term)

    def test_locations_fields_terms_and_terms_for(self) -> None:
        locations = _locations()

        assert locations.fields() == ['name', 'city']
        assert locations.terms() == {'inn', 'hotel'}
        assert locations.terms_for('name') == ['inn', 'hotel']
        assert locations.terms_for('city') == ['inn']
        with pytest.raises(InvalidArgumentException, match='Cannot find "rating" field'):
            locations.terms_for('rating')

    def test_empty_locations(self) -> None:
        locations = SearchRowLocations()

        assert (locations.get_all(), locations.fields(), locations.terms()) == ([], [], set())
        assert repr(locations) == 'SearchRowLocations([])'

    def test_row_constructs_with_its_defaults(self) -> None:
        row = SearchRow(index='hotels_6c1a', id='hotel_1', score=1.25)

        assert row.locations == SearchRowLocations()
        assert (row.fields, row.fragments, row.explanation) == ({}, {}, {})

    def test_fields_are_read_by_the_serializer(self) -> None:
        row = _search_row_from_core(_core_row(fields='{"name": "Inn"}', explanation='{"value": 1.2}'),
                                    _TaggingSerializer())

        assert row.fields == {'tagged': '{"name": "Inn"}'}
        # The explanation is the server's, not the documents', so it stays JSON.
        assert row.explanation == {'value': 1.2}

    def test_fields_the_serializer_does_not_read_as_a_mapping_come_back_as_it_returned_them(self) -> None:
        class _Listing(Serializer):
            def serialize(self, value: Any) -> bytes:
                raise NotImplementedError()

            def deserialize(self, value: bytes) -> Any:
                return [value]

        row = _search_row_from_core(_core_row(fields='{"name": "Inn"}'), _Listing())

        assert row.fields == [b'{"name": "Inn"}']
