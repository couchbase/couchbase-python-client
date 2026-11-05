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
    'ViewErrorMode',
    'ViewMetaData',
    'ViewOrdering',
    'ViewRow',
    'ViewScanConsistency',
]

import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

from couchbase.common.exceptions import InvalidArgumentException

# The record below holds one response's metadata in a shape no protocol owns. A protocol fills it
# from its own wire format, and ViewMetaData reads it without knowing which protocol did.


@dataclass(frozen=True)
class _ViewMetaDataRecord:
    total_rows: int | None
    debug_info: str | None


class ViewMetaData:
    """The metadata the view service returned with a view query's rows."""

    def __init__(self, record: _ViewMetaDataRecord) -> None:
        self._record = record

    def debug_info(self) -> str | None:
        """Get the debug information the view service returned.

        Returns:
            str | None: The debug information, if the query asked for it.
        """
        return self._record.debug_info

    def total_rows(self) -> int | None:
        """Get the number of rows in the view's index, before the query's filters.

        Returns:
            int | None: The number of rows, if the service sent it.
        """
        return self._record.total_rows

    def __repr__(self) -> str:
        return f'{type(self).__name__}({asdict(self._record)})'


@dataclass
class ViewRow(object):
    """One row of a view query.

    Attributes:
        key: The key the view's map function emitted.
        id: The ID of the document that emitted the row, or ``None`` for a reduced row.
        value: The value the view's map function emitted, or the reduced value.
        document: The emitting document's content, when it was fetched with the row.
    """

    key: Any = None
    id: str | None = None
    value: Any = None
    document: Any = None

    @classmethod
    def from_json(cls, json_data: dict[str, Any]) -> ViewRow:
        """**INTERNAL** Build a row from the service's JSON."""
        output = cls(id=json_data.get('id', None))
        if 'key' in json_data:
            output.key = json.loads(json_data['key'])
        if 'value' in json_data:
            output.value = json.loads(json_data['value'])
        return output


class ViewScanConsistency(Enum):
    """How current a view's index must be when a view query reads it.

    ``NOT_BOUNDED`` reads the index as it is.  ``REQUEST_PLUS`` updates the index first, so the query
    sees every mutation made before it.  ``UPDATE_AFTER`` reads the index as it is and then updates it.
    """

    NOT_BOUNDED = 'ok'
    REQUEST_PLUS = 'false'
    UPDATE_AFTER = 'update_after'

    def to_str(self) -> str:
        """**INTERNAL** The member as the view request spells it."""
        if self.value == 'false':
            return 'request_plus'
        elif self.value == 'ok':
            return 'not_bounded'
        else:
            return self.value

    @classmethod
    def from_str(cls, value: str) -> ViewScanConsistency:
        """**INTERNAL** The member a string names."""
        if value in ['false', 'request_plus']:
            return cls.REQUEST_PLUS
        elif value in ['ok', 'not_bounded']:
            return cls.NOT_BOUNDED
        elif value == 'update_after':
            return cls.UPDATE_AFTER
        raise InvalidArgumentException(
            message=(f"{value} is not a valid ViewScanConsistency option. "
                     "Expected str representation of type ViewScanConsistency."))


class ViewOrdering(Enum):
    """The order, by key, a view query returns its rows in."""

    DESCENDING = 'true'
    ASCENDING = 'false'

    def to_str(self) -> str:
        """**INTERNAL** The member as the view request spells it."""
        if self.value == 'false':
            return 'ascending'
        else:
            return 'descending'

    @classmethod
    def from_str(cls, value: str) -> ViewOrdering:
        """**INTERNAL** The member a string names."""
        if value == 'descending':
            return cls.DESCENDING
        elif value == 'ascending':
            return cls.ASCENDING
        raise InvalidArgumentException(
            message=(f"{value} is not a valid ViewOrdering option. "
                     "Expected str representation of type ViewOrdering."))


class ViewErrorMode(Enum):
    """What a view query does when a node reports an error.

    ``CONTINUE`` returns the rows the other nodes sent; ``STOP`` ends the query.
    """

    CONTINUE = 'continue'
    STOP = 'stop'

    def to_str(self) -> str:
        """**INTERNAL** The member as the view request spells it."""
        return 'resume' if self.value == 'continue' else self.value

    @classmethod
    def from_str(cls, value: str) -> ViewErrorMode:
        """**INTERNAL** The member a string names."""
        if value in ['resume', 'continue']:
            return cls.CONTINUE
        elif value == 'stop':
            return cls.STOP
        raise InvalidArgumentException(
            message=(f"{value} is not a valid ViewErrorMode option. "
                     "Expected str representation of type ViewErrorMode."))
