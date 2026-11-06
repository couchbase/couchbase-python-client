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

"""The warning categories the SDK emits, for an application's warnings filters.

Every one derives from `CouchbaseWarning`, so a single filter on it covers all of them.
"""

__all__ = [
    'CouchbaseApplicabilityWarning',
    'CouchbaseDeprecationWarning',
    'CouchbaseOptionWarning',
    'CouchbaseWarning',
]

from couchbase.common.warnings import CouchbaseApplicabilityWarning  # noqa: F401
from couchbase.common.warnings import CouchbaseDeprecationWarning  # noqa: F401
from couchbase.common.warnings import CouchbaseOptionWarning  # noqa: F401
from couchbase.common.warnings import CouchbaseWarning  # noqa: F401
