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

"""Deprecated classes no public path binds.  Their old paths serve them, warning on each import."""

from couchbase.collection import Collection
from couchbase.common._supportability import Supportability


class HttpResult:
    """An HTTP response.

    .. deprecated:: 4.7.0
        Nothing in the SDK returns one.
    """

    def __init__(self, orig: object) -> None:
        Supportability.type_deprecated('HttpResult', message='Nothing in the SDK returns one.')
        self._orig = orig


@Supportability.class_deprecated('couchbase.collection.Collection')
class CBCollection(Collection):
    """**DEPRECATED** A collection.

    .. deprecated:: 4.0.0
        Use :class:`~couchbase.collection.Collection` instead.
    """
