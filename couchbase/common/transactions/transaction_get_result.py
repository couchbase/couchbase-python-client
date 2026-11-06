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
    'TransactionGetResult',
]

import logging
from typing import Any, Protocol

from couchbase.common._utils import fields_repr
from couchbase.common.result import ContentProxy
from couchbase.common.transcoder import Transcoder

# Named for the public module, so configuring `couchbase.transactions` reaches it.
_log = logging.getLogger('couchbase.transactions.transaction_get_result')

# A decoded body can be falsy (b'', 0, {}), so "not decoded yet" needs a value of its own.
_UNDECODED = object()


class _GetResultSource(Protocol):
    """A protocol's result for one transactional read, holding `id`, `cas` and `value`."""

    def get(self, field_name: str, default: Any = ...) -> Any: ...


class TransactionGetResult:
    """A document read inside a transaction."""

    def __init__(self, res: _GetResultSource, transcoder: Transcoder) -> None:
        self._res = res
        self._transcoder = transcoder
        self._decoded_value: Any = _UNDECODED

    @property
    def id(self) -> str:
        """The document's ID."""
        return self._res.get("id")

    @property
    def cas(self) -> int:
        """The CAS of the document."""
        return self._res.get("cas")

    @property
    def content_as(self) -> ContentProxy:
        """The contents of the document.

        Get the value as a dict::

            res = ctx.get(collection, key)
            value = res.content_as[dict]
        """
        if self._decoded_value is _UNDECODED:
            val, flags = self._res.get('value')
            if val is None:
                _log.debug('Result is missing decoded value ')
                return ContentProxy('')
            self._decoded_value = self._transcoder.decode_value(val, flags)
        return ContentProxy(self._decoded_value)

    def __repr__(self) -> str:
        value, flags = self._res.get('value')
        return fields_repr(self, id=self.id, cas=self.cas, value=value, flags=flags)
