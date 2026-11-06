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
    'CouchbaseApplicabilityWarning',
    'CouchbaseDeprecationWarning',
    'CouchbaseOptionWarning',
    'CouchbaseWarning',
]


class CouchbaseWarning(UserWarning):
    """The base of every warning the SDK emits, so that one filter covers all of them.

    It derives from ``UserWarning`` rather than ``DeprecationWarning`` so that Python's default
    filters show it: they hide a ``DeprecationWarning`` unless it is raised from ``__main__``.

    Example:
        Turn every SDK warning into an error, as a test suite might::

            import warnings
            from couchbase.warnings import CouchbaseWarning

            warnings.simplefilter('error', CouchbaseWarning)
    """


class CouchbaseDeprecationWarning(CouchbaseWarning):
    """Something the SDK still supports and will stop supporting."""


class CouchbaseOptionWarning(CouchbaseWarning):
    """An option key the options class does not carry, naming the closest key it does carry where there is one.

    Emitted when option validation is ``warn``; ``strict`` raises
    :class:`~couchbase.exceptions.InvalidArgumentException` instead.  See :envvar:`PYCBC_OPTION_VALIDATION`.
    """


class CouchbaseApplicabilityWarning(CouchbaseWarning):
    """An option key the protocol in use cannot act on, so that setting it changes nothing.

    Separate from :class:`~couchbase.warnings.CouchbaseOptionWarning` so that configuration shared
    between protocols can silence this one and still hear about a misspelled key.
    """
