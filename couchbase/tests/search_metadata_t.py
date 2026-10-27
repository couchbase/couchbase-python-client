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
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    cast)

import pytest

from couchbase.protocol.classic._metadata import _search_facets_from_core, _search_metadata_from_core
from couchbase.search import (SearchDateRangeFacet,
                              SearchFacetResult,
                              SearchMetaData,
                              SearchNumericRangeFacet,
                              SearchTermFacet)
from tests.test_features import TestProtocol

# Each record here is built by classic's adapter from what its binding returns.
pytestmark = pytest.mark.unsupported_protocol(TestProtocol.COUCHBASE2)

if TYPE_CHECKING:
    from couchbase.protocol.classic._pycbc_core.binding_cpp_types import CppSearchFacet, CppSearchMetaData


def _core_metadata(**overrides: Any) -> CppSearchMetaData:
    raw: dict[str, Any] = {
        'client_context_id': 'a-context-id',
        'errors': {'pindex-1': 'an error'},
        'metrics': {'took': 123456789, 'total_rows': 5, 'max_score': 1.25,
                    'success_partition_count': 5, 'error_partition_count': 1},
    }
    raw.update(overrides)
    return cast('CppSearchMetaData', raw)


def _metadata(**overrides: Any) -> SearchMetaData:
    record = _search_metadata_from_core(_core_metadata(**overrides))
    assert record is not None
    return SearchMetaData(record)


def _core_facets() -> list[CppSearchFacet]:
    raw: list[dict[str, Any]] = [
        {'name': 'model', 'field': 'model', 'total': 9, 'missing': 1, 'other': 2,
         'terms': [{'term': 'auto', 'count': 4}, {'term': 'moto', 'count': 3}],
         'numeric_ranges': [], 'date_ranges': []},
        {'name': 'rating', 'field': 'rating', 'total': 6, 'missing': 0, 'other': 0, 'terms': [],
         'date_ranges': [],
         'numeric_ranges': [{'name': 'low', 'count': 2, 'min': None, 'max': 4},
                            {'name': 'high', 'count': 4, 'min': 4.5, 'max': None}]},
        {'name': 'updated', 'field': 'last_updated', 'total': 3, 'missing': 0, 'other': 0,
         'date_ranges': [{'name': 'early', 'count': 3, 'start': '2020-01-01T00:00:00Z',
                          'end': None}],
         'terms': [], 'numeric_ranges': []},
    ]
    return cast('list[CppSearchFacet]', raw)


class SearchMetaDataUnitTests:
    # No cb_env and no TEST_MANIFEST: the metadata and facets are built straight from what the
    # core hands the classic adapters, and `couchbase.common.search` is the only definition of
    # either.  `search_t` reads both off real queries; what it cannot reach is a response built
    # twice, or a response missing a key the server has always sent so far.

    def test_accessors_read_the_record(self) -> None:
        metadata = _metadata()

        assert metadata.client_context_id() == 'a-context-id'
        assert metadata.errors() == {'pindex-1': 'an error'}

    def test_adapter_leaves_the_core_dict_unchanged(self) -> None:
        raw = _core_metadata()
        before = copy.deepcopy(raw)

        _search_metadata_from_core(raw)

        assert raw == before

    def test_one_core_dict_builds_two_records(self) -> None:
        raw = _core_metadata()

        assert _search_metadata_from_core(raw) == _search_metadata_from_core(raw)

    @pytest.mark.parametrize('raw', [None, {}])
    def test_absent_metadata_builds_no_record(self, raw: Any) -> None:
        assert _search_metadata_from_core(raw) is None

    def test_absent_errors_read_as_empty(self) -> None:
        assert _metadata(errors=None).errors() == {}

    def test_errors_cannot_change_the_record(self) -> None:
        metadata = _metadata()

        metadata.errors().clear()

        assert metadata.errors() == {'pindex-1': 'an error'}

    def test_metadata_repr_names_the_class(self) -> None:
        assert repr(_metadata()).startswith('SearchMetaData:{')
        assert repr(_metadata().metrics()).startswith('SearchMetrics:{')

    def test_metrics(self) -> None:
        metrics = _metadata().metrics()

        assert metrics is not None
        assert metrics.total_rows() == 5
        assert metrics.max_score() == 1.25
        assert metrics.success_partition_count() == 5
        assert metrics.error_partition_count() == 1

    # The classic core sends the two partition counts and not their total, which the adapter sums.
    def test_total_partition_count_is_the_sum_of_the_counts(self) -> None:
        metrics = _metadata().metrics()

        assert metrics is not None
        assert metrics.total_partition_count() == 6

    # The core carries the duration in nanoseconds and the accessor divides into microseconds, so
    # the rounding is timedelta's.  123456789ns is 123456.789us and reads as 123457us.
    def test_took_rounds_in_the_accessor(self) -> None:
        metrics = _metadata().metrics()

        assert metrics is not None
        assert metrics.took() == timedelta(microseconds=123457)

    def test_absent_metric_keys_read_as_zero(self) -> None:
        metrics = _metadata(metrics={'total_rows': 1}).metrics()

        assert metrics is not None
        assert metrics.took() == timedelta(0)
        assert metrics.max_score() == 0.0
        assert metrics.total_partition_count() == 0

    @pytest.mark.parametrize('metrics', [None, {}])
    def test_absent_metrics_read_as_none(self, metrics: Any) -> None:
        assert _metadata(metrics=metrics).metrics() is None


class SearchFacetsUnitTests:

    def test_facets_are_keyed_by_name(self) -> None:
        facets = _search_facets_from_core(_core_facets())

        assert facets is not None
        assert list(facets) == ['model', 'rating', 'updated']
        assert all(isinstance(f, SearchFacetResult) for f in facets.values())

    def test_facet_counts(self) -> None:
        facets = _search_facets_from_core(_core_facets())

        assert facets is not None
        model = facets['model']
        assert (model.name, model.field, model.total, model.missing, model.other) == (
            'model', 'model', 9, 1, 2)

    def test_term_facet(self) -> None:
        facets = _search_facets_from_core(_core_facets())

        assert facets is not None
        assert facets['model'].terms == [SearchTermFacet('auto', 4), SearchTermFacet('moto', 3)]
        assert facets['model'].numeric_ranges is None
        assert facets['model'].date_ranges is None

    def test_numeric_range_bounds_keep_their_type(self) -> None:
        facets = _search_facets_from_core(_core_facets())

        assert facets is not None
        assert facets['rating'].numeric_ranges == [SearchNumericRangeFacet('low', 2, None, 4),
                                                   SearchNumericRangeFacet('high', 4, 4.5, None)]

    def test_date_range_bounds_are_the_server_string(self) -> None:
        facets = _search_facets_from_core(_core_facets())

        assert facets is not None
        assert facets['updated'].date_ranges == [
            SearchDateRangeFacet('early', 3, '2020-01-01T00:00:00Z', None)]

    def test_adapter_leaves_the_core_list_unchanged(self) -> None:
        raw = _core_facets()
        before = copy.deepcopy(raw)

        _search_facets_from_core(raw)

        assert raw == before

    def test_one_core_list_builds_two_results(self) -> None:
        raw = _core_facets()

        assert _search_facets_from_core(raw) == _search_facets_from_core(raw)

    @pytest.mark.parametrize('detail', [None, []])
    def test_facet_without_details_reads_as_none(self, detail: Any) -> None:
        raw = cast('list[CppSearchFacet]', [{'name': 'model', 'field': 'model', 'total': 9,
                                             'missing': 0, 'other': 0, 'terms': detail,
                                             'numeric_ranges': [], 'date_ranges': []}])
        facets = _search_facets_from_core(raw)

        assert facets is not None
        assert facets['model'].terms is None

    @pytest.mark.parametrize('raw', [None, []])
    def test_absent_facets_read_as_none(self, raw: Any) -> None:
        assert _search_facets_from_core(raw) is None
