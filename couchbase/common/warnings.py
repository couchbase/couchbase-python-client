#  Copyright 2016-2022. Couchbase, Inc.
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
    'CouchbaseApplicabilityWarning',
    'CouchbaseDeprecationWarning',
    'CouchbaseOptionWarning',
    'CouchbaseWarning',
]


class CouchbaseWarning(UserWarning):
    """Every warning the SDK emits, so that one filter can silence all of them.

    Rooted at `UserWarning` rather than at `DeprecationWarning` because CPython's default
    filter set carries `ignore::DeprecationWarning`, lifted only for the frame the warning is
    attributed to when that frame is `__main__`.  A diagnostic the default configuration hides
    is not a diagnostic, and a library has no way to unhide it: installing a filter of its own
    would invalidate every de-duplication registry in the process, including its users'.
    """


class CouchbaseDeprecationWarning(CouchbaseWarning):
    """Something the SDK still supports and will stop supporting."""


class CouchbaseOptionWarning(CouchbaseWarning):
    """A key the options class does not carry, with the closest one it does where there is one."""


class CouchbaseApplicabilityWarning(CouchbaseWarning):
    """A key the caller set that the protocol in use cannot honour.

    Separate from `CouchbaseOptionWarning` so that one configuration run against two protocols
    can silence what does not apply while still hearing about what is misspelled.  That is the
    off switch, and it is why the validation mode needs no fourth value for it.
    """
