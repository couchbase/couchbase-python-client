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


from couchbase.logic.supportability import Supportability
from couchbase.observability.metrics import Meter, ValueRecorder


@Supportability.class_deprecated('couchbase.observability.metrics.Meter')
class CouchbaseMeter(Meter):
    """
    .. deprecated:: 4.7.0

        This class is deprecated and will be removed in a future release.  Use :class:`~couchbase.observability.metrics.Meter` instead.
    """  # noqa: E501
    pass


@Supportability.class_deprecated('couchbase.observability.metrics.ValueRecorder')
class CouchbaseValueRecorder(ValueRecorder):
    """
    .. deprecated:: 4.7.0

        This class is deprecated and will be removed in a future release.  Use :class:`~couchbase.observability.metrics.ValueRecorder` instead.
    """  # noqa: E501
    pass
