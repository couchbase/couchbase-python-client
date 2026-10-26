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
    def __init__(self, record: _ViewMetaDataRecord) -> None:
        self._record = record

    def debug_info(self) -> str | None:
        return self._record.debug_info

    def total_rows(self) -> int | None:
        return self._record.total_rows

    def __repr__(self) -> str:
        return f'ViewMetaData({asdict(self._record)})'


@dataclass
class ViewRow(object):
    key: Any = None
    id: str | None = None
    value: Any = None
    document: Any = None

    @classmethod
    def from_json(cls, json_data: dict[str, Any]) -> ViewRow:
        output = cls(id=json_data.get('id', None))
        if 'key' in json_data:
            output.key = json.loads(json_data['key'])
        if 'value' in json_data:
            output.value = json.loads(json_data['value'])
        return output


class ViewScanConsistency(Enum):
    NOT_BOUNDED = 'ok'
    REQUEST_PLUS = 'false'
    UPDATE_AFTER = 'update_after'

    def to_str(self) -> str:
        if self.value == 'false':
            return 'request_plus'
        elif self.value == 'ok':
            return 'not_bounded'
        else:
            return self.value

    @classmethod
    def from_str(cls, value: str) -> ViewScanConsistency:
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
    DESCENDING = 'true'
    ASCENDING = 'false'

    def to_str(self) -> str:
        if self.value == 'false':
            return 'ascending'
        else:
            return 'descending'

    @classmethod
    def from_str(cls, value: str) -> ViewOrdering:
        if value == 'descending':
            return cls.DESCENDING
        else:
            return cls.ASCENDING


class ViewErrorMode(Enum):
    CONTINUE = 'continue'
    STOP = 'stop'

    def to_str(self) -> str:
        return 'resume' if self.value == 'continue' else self.value

    @classmethod
    def from_str(cls, value: str) -> ViewErrorMode:
        if value == 'resume':
            return cls.CONTINUE
        elif value == 'continue':
            return cls.CONTINUE
        else:
            return cls.STOP
