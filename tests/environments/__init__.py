#  Copyright 2016-2024. Couchbase, Inc.
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

from tests.skips import CouchbaseTestEnvironmentException

# Re-exported, not defined here.  The exception lives in tests/skips.py so that it sits
# next to the skip categories it carries: the taxonomy only works if the "did not run, and
# that is fine" path and the "environment is broken" path share one vocabulary.  This
# re-export keeps `from tests.environments import CouchbaseTestEnvironmentException`
# resolving.
__all__ = [
    'CouchbaseTestEnvironmentException',
]
