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

__all__ = [
    'AnalyticsError',
    'AnalyticsMetaData',
    'AnalyticsMetrics',
    'AnalyticsScanConsistency',
    'AnalyticsStatus',
    'AnalyticsWarning',
]

from couchbase.common import options
from couchbase.common._supportability import Supportability
from couchbase.common.analytics import AnalyticsError  # noqa: F401
from couchbase.common.analytics import AnalyticsMetaData  # noqa: F401
from couchbase.common.analytics import AnalyticsMetrics  # noqa: F401
from couchbase.common.analytics import AnalyticsScanConsistency  # noqa: F401
from couchbase.common.analytics import AnalyticsStatus  # noqa: F401
from couchbase.common.analytics import AnalyticsWarning  # noqa: F401
from couchbase.protocol.classic._analytics import AnalyticsQuery  # noqa: F401

"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead use:
    * All options should be imported from `couchbase.options`.

"""


@Supportability.import_deprecated('couchbase.analytics', 'couchbase.options')
class AnalyticsOptions(options.AnalyticsOptions):  # noqa: F811
    pass
