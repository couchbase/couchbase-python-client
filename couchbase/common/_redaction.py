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

"""Log redaction tags for the SDK's own log lines, in the format the C++ core writes them.

A tagged value renders as ``<ud>value</ud>`` (user data), ``<md>value</md>`` (metadata) or
``<sd>value</sd>`` (system data), so a redaction tool can find it, and as the plain value while
redaction is off.  The wrappers decide when the record is formatted, not when they are built, so
pass them as logging arguments (``log.debug('%s', user_data(key))``) rather than formatting
them into the message.

The flag is process-wide and is only ever set, never cleared, matching the core: once any cluster
asks for redaction, a log written for one cluster cannot be told apart from another's.
"""

from __future__ import annotations

from typing import Any

_TAG_SEQUENCES = ('<ud>', '</ud>', '<md>', '</md>', '<sd>', '</sd>')

_enabled = False


def enable() -> None:
    """Turns redaction on for the rest of the process."""
    global _enabled
    _enabled = True


def is_enabled() -> bool:
    return _enabled


def escape(value: str) -> str:
    """Replaces what would break the span around `value`.

    The consumer of these tags is line-oriented, so a newline or carriage return would leave the
    two tags on different lines and the value untouched.  A tag sequence inside the value would
    close the span early.  Only the ``<`` that begins a tag sequence is escaped, which is enough
    to break it, so other markup in a logged value stays readable.
    """
    if '\n' not in value and '\r' not in value and '<' not in value:
        return value
    out = []
    for index, char in enumerate(value):
        if char == '\n':
            out.append('\\n')
        elif char == '\r':
            out.append('\\r')
        elif char == '<' and value.startswith(_TAG_SEQUENCES, index):
            out.append('\\u003c')
        else:
            out.append(char)
    return ''.join(out)


class _Tagged:
    __slots__ = ('_tag', '_value')

    def __init__(self, tag: str, value: Any) -> None:
        self._tag = tag
        self._value = value

    def __str__(self) -> str:
        if not _enabled:
            return str(self._value)
        return f'<{self._tag}>{escape(str(self._value))}</{self._tag}>'


def user_data(value: Any) -> _Tagged:
    """Tags `value` as user data: document keys and values, query statements, usernames."""
    return _Tagged('ud', value)


def metadata(value: Any) -> _Tagged:
    """Tags `value` as metadata: bucket, scope, collection and index names."""
    return _Tagged('md', value)


def system_data(value: Any) -> _Tagged:
    """Tags `value` as system data: hostnames, IP addresses and ports."""
    return _Tagged('sd', value)
