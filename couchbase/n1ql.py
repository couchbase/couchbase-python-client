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

__all__ = [
    'QueryError',
    'QueryMetaData',
    'QueryMetrics',
    'QueryProfile',
    'QueryScanConsistency',
    'QueryStatus',
    'QueryWarning',
]

from typing import TYPE_CHECKING

from couchbase.common._supportability import Supportability
from couchbase.common.n1ql import QueryError  # noqa: F401
from couchbase.common.n1ql import QueryMetaData  # noqa: F401
from couchbase.common.n1ql import QueryMetrics  # noqa: F401
from couchbase.common.n1ql import QueryProfile  # noqa: F401
from couchbase.common.n1ql import QueryScanConsistency  # noqa: F401
from couchbase.common.n1ql import QueryStatus  # noqa: F401
from couchbase.common.n1ql import QueryWarning  # noqa: F401

if not TYPE_CHECKING:
    __getattr__ = Supportability.deprecated_paths(__name__)
