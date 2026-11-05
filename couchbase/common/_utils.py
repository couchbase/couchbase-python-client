#  Copyright 2016-2023. Couchbase, Inc.
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

from collections.abc import Mapping, Sequence
from typing import Any, Protocol
from urllib.parse import quote

JSONType = str | int | float | bool | None | Mapping[str, Any] | Sequence[Any]


class ScopeKeyspace(Protocol):
    """The bucket and scope names that identify a scope.

    Any of the three APIs' ``Scope`` classes satisfies it.
    """

    @property
    def bucket_name(self) -> str: ...

    @property
    def name(self) -> str: ...


# What a KV write accepts, which is a property of the collection's transcoder and not of the
# operation.  JSONType cannot state it: LegacyTranscoder pickles values outside that union by
# design, while JSONTranscoder rejects the bytes that union's Sequence[Any] arm admits.
DocumentValue = Any


def is_null_or_empty(value: str | None) -> bool:
    return not (value and not value.isspace())


def to_form_str(params: dict[str, Any]) -> str:
    encoded_params = []
    for k, v in params.items():
        encoded_params.append('{0}={1}'.format(quote(k), quote(str(v))))

    return '&'.join(encoded_params)


class _Redacted:
    def __repr__(self) -> str:
        return '<redacted>'


_REDACTED = _Redacted()


def redacted(value: object) -> object:
    """Stands in for a secret in a repr, keeping only whether it is set."""
    return None if value is None else _REDACTED


def fields_repr(obj: object, **fields: object) -> str:
    """``Name(key=value, ...)``, the name taken from the object's class so a subclass prints its own."""
    body = ', '.join(f'{k}={v!r}' for k, v in fields.items())
    return f'{type(obj).__name__}({body})'
