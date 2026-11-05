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

__all__ = [
    'DateFacet',
    'Facet',
    'HighlightStyle',
    'MatchOperator',
    'NumericFacet',
    'SearchDateRangeFacet',
    'SearchFacetResult',
    'SearchMetaData',
    'SearchMetrics',
    'SearchNumericRangeFacet',
    'SearchRow',
    'SearchRowFields',
    'SearchRowLocation',
    'SearchRowLocations',
    'SearchScanConsistency',
    'SearchTermFacet',
    'Sort',
    'SortField',
    'SortGeoDistance',
    'SortID',
    'SortScore',
    'TermFacet',
]

from collections.abc import (Callable,
                             Iterable,
                             Sequence)
from dataclasses import (asdict,
                         dataclass,
                         field)
from datetime import timedelta
from enum import Enum
from typing import (Any,
                    Generic,
                    TypeVar,
                    cast,
                    overload)

from couchbase.common._supportability import Supportability
from couchbase.common._utils import JSONType
from couchbase.common.exceptions import InvalidArgumentException

# The records below hold one response's metadata in a shape no protocol owns. Each protocol fills
# them from its own wire format, and the public classes that follow read them without knowing
# which protocol did. Durations are integer nanoseconds, because protocols that each converted to
# timedelta would round differently. A record carries every field either protocol sends, whether
# or not an accessor reads it, and a protocol that does not send one fills it with None.


@dataclass(frozen=True)
class _SearchMetricsRecord:
    took_ns: int
    total_rows: int
    max_score: float
    total_partition_count: int
    success_partition_count: int
    error_partition_count: int


@dataclass(frozen=True)
class _SearchMetaDataRecord:
    client_context_id: str | None
    errors: dict[str, str]
    metrics: _SearchMetricsRecord | None


class SearchMetrics:
    """The metrics the search service reported for a search."""

    def __init__(self, record: _SearchMetricsRecord) -> None:
        self._record = record

    def success_partition_count(self) -> int:
        """Get the number of index partitions that answered the search.

        Returns:
            int: The number of partitions that answered.
        """
        return self._record.success_partition_count

    def error_partition_count(self) -> int:
        """Get the number of index partitions that failed to answer the search.

        Returns:
            int: The number of partitions that failed.
        """
        return self._record.error_partition_count

    def took(self) -> timedelta:
        """Get the time the search took.

        Returns:
            timedelta: The time the search took.
        """
        return timedelta(microseconds=self._record.took_ns / 1000)

    def total_partition_count(self) -> int:
        """Get the number of index partitions the search was sent to.

        Returns:
            int: The number of partitions.
        """
        return self._record.total_partition_count

    def max_score(self) -> float:
        """Get the highest score of any matching document.

        Returns:
            float: The highest score.
        """
        return self._record.max_score

    def total_rows(self) -> int:
        """Get the number of documents that matched, which can be more than the rows returned.

        Returns:
            int: The number of documents that matched.
        """
        return self._record.total_rows

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


class SearchMetaData:
    """The metadata the search service returned with a search's rows."""

    def __init__(self, record: _SearchMetaDataRecord) -> None:
        self._record = record

    def errors(self) -> dict[str, str]:
        """Get the errors the index partitions that failed reported.

        Returns:
            dict[str, str]: Each error, by the partition that reported it.
        """
        return dict(self._record.errors)

    def metrics(self) -> SearchMetrics | None:
        """Get the metrics the search service reported for the search.

        Returns:
            :class:`.SearchMetrics` | None: The search's metrics, if the service sent them.
        """
        if self._record.metrics is None:
            return None
        return SearchMetrics(self._record.metrics)

    def client_context_id(self) -> str | None:
        """Get the client context ID which is associated with the search.

        Returns:
            str | None: The client context ID, if the service sent it.
        """
        return self._record.client_context_id

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


@dataclass
class SearchTermFacet:
    """One term of a term facet's result.

    Attributes:
        term: The term.
        count: The number of matching documents with the term.
    """

    term: str
    count: int


@dataclass
class SearchNumericRangeFacet:
    """One range of a numeric range facet's result.

    Attributes:
        name: The range's name, as the facet named it.
        count: The number of matching documents in the range.
        min: The range's lower bound, if it has one.
        max: The range's upper bound, if it has one.
    """

    name: str
    count: int
    min: int | float | None = None
    max: int | float | None = None


@dataclass
class SearchDateRangeFacet:
    """One range of a date range facet's result.

    Attributes:
        name: The range's name, as the facet named it.
        count: The number of matching documents in the range.
        start: The range's start, if it has one.
        end: The range's end, if it has one.
    """

    name: str
    count: int
    start: str | None = None
    end: str | None = None


@dataclass
class SearchFacetResult:
    """The result of one facet of a search.

    Attributes:
        name: The facet's name, as the search named it.
        field: The field the facet counted.
        total: The number of values the facet counted.
        missing: The number of matching documents without the field.
        other: The number of values counted that fall in none of the returned terms or ranges.
        terms: The terms of a term facet.
        numeric_ranges: The ranges of a numeric range facet.
        date_ranges: The ranges of a date range facet.

    Each of the three lists is ``None`` for a facet of another kind, and for one the server returned
    without its details.
    """

    name: str
    field: str
    total: int
    missing: int
    other: int
    terms: list[SearchTermFacet] | None
    numeric_ranges: list[SearchNumericRangeFacet] | None
    date_ranges: list[SearchDateRangeFacet] | None


@dataclass(frozen=True)
class _SearchFinalMetadata:
    """What a search sends after its rows, its metadata and its facets, which arrive together."""
    metadata: SearchMetaData | None
    facets: dict[str, SearchFacetResult] | None


# The row types below are what the SDK hands back per hit. A protocol builds them from its own wire
# format; nothing in them knows which protocol did.


@dataclass
class SearchRowLocation:
    """One place a term of the search matched in a row.

    Attributes:
        field: The field the term was found in.
        term: The term.
        position: The term's position in the field, counted in terms.
        start: The offset of the term's first byte in the field.
        end: The offset of the byte after the term's last.
        array_positions: Where the field sits in the arrays that hold it, if any do.
    """

    field: str | None = None
    term: str | None = None
    position: int | None = None
    start: int | None = None
    end: int | None = None
    array_positions: list[int] | None = None

    @classmethod
    def from_server(cls, json_data: dict[str, Any]) -> SearchRowLocation:
        """**INTERNAL** Build a location from the service's JSON."""
        return cls(json_data.get('field'),
                   json_data.get('term'),
                   json_data.get('position'),
                   json_data.get('start_offset'),
                   json_data.get('end_offset'),
                   json_data.get('array_positions'))


class SearchRowLocations:
    """Every place a term of the query matched in one row, in the order the server listed them."""

    def __init__(self, locations: Iterable[SearchRowLocation] | None = None) -> None:
        self._locations = tuple(locations or ())

    def get_all(self) -> list[SearchRowLocation]:
        """Get every location, in any field, of any term.

        Returns:
            list[:class:`.SearchRowLocation`]: The locations.
        """
        return list(self._locations)

    def get(self, field: str, term: str) -> list[SearchRowLocation]:
        """Get the locations of a term in a field.

        Args:
            field: The field.
            term: The term.

        Returns:
            list[:class:`.SearchRowLocation`]: The locations.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the term has no location in the field.
        """
        in_field = [loc for loc in self._locations if loc.field == field]
        if not in_field:
            raise InvalidArgumentException(f'Cannot find "{field}" field in locations.')
        locations = [loc for loc in in_field if loc.term == term]
        if not locations:
            raise InvalidArgumentException(f'Cannot find "{term}" within {field}\'s locations.')
        return locations

    def fields(self) -> list[str]:
        """Get the fields with a location, in the order they first appear.

        Returns:
            list[str]: The fields.
        """
        return list(dict.fromkeys(loc.field for loc in self._locations if loc.field is not None))

    def terms(self) -> set[str]:
        """Get the terms with a location, in any field.

        Returns:
            set[str]: The terms.
        """
        return {loc.term for loc in self._locations if loc.term is not None}

    def terms_for(self, field: str) -> list[str]:
        """Get the terms with a location in a field, in the order they first appear.

        Args:
            field: The field.

        Returns:
            list[str]: The terms.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the field has no location.
        """
        terms = [loc.term for loc in self._locations if loc.field == field and loc.term is not None]
        if not terms:
            raise InvalidArgumentException(f'Cannot find "{field}" field in locations.')
        return list(dict.fromkeys(terms))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SearchRowLocations):
            return NotImplemented
        return self._locations == other._locations

    def __repr__(self) -> str:
        return f'{type(self).__name__}({list(self._locations)})'


class SearchRowFields(dict):
    """The stored fields a search returned for a row, by name."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


@dataclass
class SearchRow:
    """One document a search matched, which the server calls a hit.

    Attributes:
        index: The index partition that matched the document.
        id: The document's ID.
        score: How well the document matched.
        fields: The stored fields the search asked for.
        locations: Where the search's terms matched, if the search asked for them.
        fragments: Highlighted fragments of each field, if the search asked for them.
        explanation: How the score was computed, if the search asked for it.
    """

    index: str
    id: str
    score: float
    fields: Any = field(default_factory=SearchRowFields)
    locations: SearchRowLocations | None = field(default_factory=SearchRowLocations)
    fragments: dict = field(default_factory=dict)
    explanation: dict = field(default_factory=dict)

    @property
    def sort(self) -> list:
        """Always empty. A search row does not carry a hit's sort values.

        .. deprecated:: 4.7.0
        """
        Supportability.class_property_deprecated('SearchRow.sort')
        return []


# Shared by the sort and facet classes here and the query classes in `search_queries.py`.

_Body = dict[str, JSONType]
_Point = Sequence[float] | dict[str, float]
_T = TypeVar('_T')


def _point(value: _Point) -> list[float]:
    if isinstance(value, dict):
        if 'lon' in value and 'lat' in value:
            return [float(value['lon']), float(value['lat'])]
        raise InvalidArgumentException(message='Requires a dict with keys: lon, lat')
    if len(value) != 2 or not all(map(lambda pt: isinstance(pt, (int, float)), value)):
        raise InvalidArgumentException(message='Requires a tuple: (lon, lat)')
    return [float(value[0]), float(value[1])]


def _put(body: _Body, **fields: JSONType) -> _Body:
    body.update((k, v) for k, v in fields.items() if v is not None)
    return body


def _required(obj: object, prop: str, value: _T | None) -> _T:
    if value is None:
        raise ValueError(f'{type(obj).__name__} missing required property: {prop}')
    return value


def _as_is(value: _T) -> _T:
    return value


class _OptionalField(Generic[_T]):
    """An optional field: `None` until set, converted when set, and cleared by `None` or `del`."""

    def __init__(self, convert: Callable[[Any], _T], doc: str) -> None:
        self._convert = convert
        self.__doc__ = doc

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr = f'_{name}'

    @overload
    def __get__(self, obj: None, owner: type) -> _OptionalField[_T]: ...

    @overload
    def __get__(self, obj: object, owner: type) -> _T | None: ...

    def __get__(self, obj: object | None, owner: type) -> _OptionalField[_T] | _T | None:
        if obj is None:
            return self
        return cast(_T | None, getattr(obj, self._attr, None))

    def __set__(self, obj: object, value: _T | None) -> None:
        setattr(obj, self._attr, None if value is None else self._convert(value))

    def __delete__(self, obj: object) -> None:
        setattr(obj, self._attr, None)


def _range(name: str, n1: str, n2: str, r1: JSONType, r2: JSONType) -> _Body:
    """A facet's named range, keyed under ``n1`` and ``n2``, leaving out a bound that is ``None``.

    Raises if both are.
    """
    if r1 is None and r2 is None:
        raise InvalidArgumentException(message='Must specify at least one range boundary!')
    return _put({}, **{n1: r1, n2: r2, 'name': name})


class HighlightStyle(Enum):
    """How a search marks the matched terms in the fragments it returns: with ANSI escape codes, or HTML."""

    Ansi = 'ansi'
    Html = 'html'


class SearchScanConsistency(Enum):
    """How current the search index must be when a search reads it.

    ``NOT_BOUNDED`` reads the index as it is.  ``REQUEST_PLUS`` waits until the index holds every
    mutation made before the search.  ``AT_PLUS`` waits until it holds the mutations given as the
    ``consistent_with`` option.
    """

    NOT_BOUNDED = 'not_bounded'
    REQUEST_PLUS = 'request_plus'
    AT_PLUS = 'at_plus'


class MatchOperator(Enum):
    """How a match query combines the terms it analyzes its text into.

    **UNCOMMITTED** This API may change in the future.

    ``OR``, the default, matches a document with any of the terms; ``AND``, one with all of them.
    """

    OR = "or"
    AND = "and"


# Sorting


class Sort:
    """Sorts search results by ``by``, the Search service's name for what to sort on.

    You probably want one of the subclasses.

    .. seealso:: :class:`SortScore`, :class:`SortID`, :class:`SortField` and :class:`SortGeoDistance`.
    """

    desc: _OptionalField[bool] = _OptionalField(_as_is, """
        Sort in descending order
        """)

    def __init__(self,
                 by: str,
                 *,
                 desc: bool | None = None,
                 descending: bool | None = None) -> None:
        self._by = _required(self, 'by', by)
        self._props: _Body = {}
        self.desc = desc if descending is None else descending

    def set_prop(self, key: str, value: JSONType) -> None:
        """Sets ``key`` in the sort's JSON to ``value``, sent as given and without validation.

        The key is written over whatever the sort's own fields encode under it, and is not read
        back by any of the sort's properties.
        """
        self._props[key] = value

    @property
    def descending(self) -> bool | None:
        """The same as :attr:`desc`."""
        return self.desc

    @descending.setter
    def descending(self, value: bool | None) -> None:
        self.desc = value

    def as_encodable(self) -> JSONType:
        """Get the sort as the Search service's JSON.

        Returns:
            The sort, ready to serialize to JSON.  Each call builds a new object.
        """
        return _encode_sort(self)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.as_encodable()})'


class SortScore(Sort):
    """Sorts by the score of each match."""

    def __init__(self,
                 *,
                 desc: bool | None = None,
                 descending: bool | None = None) -> None:
        super().__init__('score', desc=desc, descending=descending)


class SortID(Sort):
    """Sorts lexically by the document ID of each match."""

    def __init__(self,
                 *,
                 desc: bool | None = None,
                 descending: bool | None = None) -> None:
        super().__init__('id', desc=desc, descending=descending)


class SortField(Sort):
    """Sorts by the value of a field."""

    type: _OptionalField[str] = _OptionalField(_as_is, """
        Coerce the field to this type: ``auto``, ``string``, ``number`` or ``date``
        """)
    mode: _OptionalField[str] = _OptionalField(_as_is, """
        Which value of a multi-valued field to sort by: ``default``, ``min`` or ``max``
        """)
    missing: _OptionalField[str] = _OptionalField(_as_is, """
        Where a match without the field sorts: ``first`` or ``last``
        """)

    def __init__(self,
                 field: str,
                 *,
                 type: str | None = None,
                 mode: str | None = None,
                 missing: str | None = None,
                 desc: bool | None = None,
                 descending: bool | None = None) -> None:
        super().__init__('field', desc=desc, descending=descending)
        self.field = field
        self.type = type
        self.mode = mode
        self.missing = missing

    @property
    def field(self) -> str:
        """The field to sort by."""
        return self._field

    @field.setter
    def field(self, value: str) -> None:
        self._field = _required(self, 'field', value)


class SortGeoDistance(Sort):
    """Sorts by each match's distance from a point."""

    unit: _OptionalField[str] = _OptionalField(_as_is, """
        Distance unit used for measuring, such as ``m``, ``km`` or ``mi``
        """)

    def __init__(self,
                 location: _Point,
                 field: str,
                 *,
                 unit: str | None = None,
                 desc: bool | None = None,
                 descending: bool | None = None) -> None:
        super().__init__('geo_distance', desc=desc, descending=descending)
        self.location = location
        self.field = field
        self.unit = unit

    @property
    def location(self) -> list[float]:
        """The point to measure from.

        Set as ``(lon, lat)`` or ``{'lon': lon, 'lat': lat}``, and read back as ``[lon, lat]``.
        """
        return self._location

    @location.setter
    def location(self, value: _Point) -> None:
        _required(self, 'location', value)
        self._location = _point(value)

    @property
    def field(self) -> str:
        """The field that holds each document's location."""
        return self._field

    @field.setter
    def field(self, value: str) -> None:
        self._field = _required(self, 'field', value)


# Facets


class Facet:
    """The base of the facets, each of which counts the values of one field across the matches."""

    limit: _OptionalField[int] = _OptionalField(_as_is, """
        Maximum number of facet results to return
        """)

    def __init__(self,
                 field: str,
                 limit: int | None = None) -> None:
        self._props: _Body = {}
        self.field = field
        self.limit = limit

    def set_prop(self, key: str, value: JSONType) -> None:
        """Sets ``key`` in the facet's JSON to ``value``, sent as given and without validation.

        The key is written over whatever the facet's own fields encode under it, and is not read
        back by any of the facet's properties.
        """
        self._props[key] = value

    @property
    def encodable(self) -> _Body:
        """The facet as the Search service's JSON, ready to serialize to JSON.

        Each read builds a new object.
        """
        return _encode_facet(self)

    @property
    def field(self) -> str:
        """The field the facet counts."""
        return self._field

    @field.setter
    def field(self, value: str) -> None:
        self._field = _required(self, 'field', value)

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.encodable})'


class TermFacet(Facet):
    """Counts the most frequent terms of a field across the matches."""


class DateFacet(Facet):
    """Counts the matches whose field falls in each of a set of date ranges.

    Add at least one range with :meth:`add_range` before the facet is used.
    """

    def __init__(self,
                 field: str,
                 limit: int | None = None) -> None:
        super().__init__(field, limit)
        self.date_ranges = []

    def add_range(self,
                  name: str,
                  start: str | None = None,
                  end: str | None = None) -> DateFacet:
        """Add a date range.

        Args:
            name: The name the range's count is returned under.
            start: The range's start, in RFC 3339 format.
            end: The range's end, in RFC 3339 format.

        Returns:
            This facet, so calls can be chained.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If neither ``start`` nor ``end`` is given.
        """
        self.date_ranges.append(_range(name, 'start', 'end', start, end))
        return self

    @property
    def date_ranges(self) -> list[_Body]:
        """The ranges, each a dict of ``name`` and at least one of ``start`` and ``end``."""
        return self._date_ranges

    @date_ranges.setter
    def date_ranges(self, value: list[_Body] | None) -> None:
        self._date_ranges = [] if value is None else value


class NumericFacet(Facet):
    """Counts the matches whose field falls in each of a set of numeric ranges.

    Add at least one range with :meth:`add_range` before the facet is used.
    """

    def __init__(self,
                 field: str,
                 limit: int | None = None) -> None:
        super().__init__(field, limit)
        self.numeric_ranges = []

    def add_range(self,
                  name: str,
                  min: int | float | None = None,
                  max: int | float | None = None) -> NumericFacet:
        """Add a numeric range.

        Args:
            name: The name the range's count is returned under.
            min: The range's lower bound.
            max: The range's upper bound.

        Returns:
            This facet, so calls can be chained.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If neither ``min`` nor ``max`` is given.
        """
        self.numeric_ranges.append(_range(name, 'min', 'max', min, max))
        return self

    @property
    def numeric_ranges(self) -> list[_Body]:
        """The ranges, each a dict of ``name`` and at least one of ``min`` and ``max``."""
        return self._numeric_ranges

    @numeric_ranges.setter
    def numeric_ranges(self, value: list[_Body] | None) -> None:
        self._numeric_ranges = [] if value is None else value


# The JSON encoding of sorts and facets, as for the queries in `search_queries.py`: each function
# returns the object's own fields under the Search service's names, leaving out a field that is
# not set, and the `set_prop` keys are written over them.

_SORT_JSON: dict[type, Callable[[Any], _Body]] = {
    Sort: lambda s: {'by': s._by},
    SortField: lambda s: _put({'by': s._by}, field=s.field, type=s.type, mode=s.mode, missing=s.missing),
    SortGeoDistance: lambda s: _put({'by': s._by}, location=s.location, field=s.field, unit=s.unit),
}


def _encode_sort(sort: Sort) -> JSONType:
    body = next(_SORT_JSON[k] for k in type(sort).__mro__ if k in _SORT_JSON)(sort)
    _put(body, desc=sort.desc)
    body.update(sort._props)
    return body


def _encode_facet(facet: Facet) -> _Body:
    body = _put({'field': facet.field}, size=facet.limit)
    if isinstance(facet, DateFacet):
        body['date_ranges'] = [dict(r) for r in facet.date_ranges]
    elif isinstance(facet, NumericFacet):
        body['numeric_ranges'] = [dict(r) for r in facet.numeric_ranges]
    body.update(facet._props)
    return body
