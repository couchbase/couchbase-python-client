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
    'CouchbaseProtocol',
]

from enum import IntEnum


class CouchbaseProtocol(IntEnum):
    """**INTERNAL** The wire protocol a client adapter speaks.

    Set once, where the connection string is parsed, and read by everything below it through the adapter.
    """

    CLASSIC = 0
    COUCHBASE2 = 1

    @classmethod
    def from_connstr(cls, connstr: str) -> CouchbaseProtocol:
        """**INTERNAL** The protocol a connection string's scheme selects.

        Only couchbase2:// selects couchbase2. Every other scheme, and a connection string with none, is classic.
        """
        scheme, sep, _ = connstr.partition('://')
        if sep and scheme == 'couchbase2':
            return cls.COUCHBASE2
        return cls.CLASSIC
