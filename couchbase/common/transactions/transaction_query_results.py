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
    'TransactionQueryResults',
]

from typing import TYPE_CHECKING, Any

from couchbase.common.transcoder import DefaultJsonSerializer

if TYPE_CHECKING:
    from couchbase.serializer import Serializer


class TransactionQueryResults:
    """The result of a query run inside a transaction."""

    def __init__(self, res: bytes, serializer: Serializer | None = None) -> None:
        self._res = (serializer or DefaultJsonSerializer()).deserialize(res)

    def rows(self) -> list[Any]:
        """Get the query's rows.

        Returns:
            list[Any]: The rows, all of them, already read.
        """
        return self._res.get("results")

    def execute(self) -> None:
        """Do nothing: the query ran, and its rows were read, before this result was returned."""
        return

    def __repr__(self) -> str:
        return f'{type(self).__name__}(res={self._res})'
