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

"""The `couchbase.observability.otel_metrics` import path.

The declarations live in `couchbase.common.observability.otel_metrics`, which every API shares, and
are re-exported here.
"""

__all__ = ['get_otel_meter']

from couchbase.common.observability.otel_metrics import get_otel_meter  # noqa: F401
from couchbase.common.observability.otel_metrics import _HAS_OTEL

if _HAS_OTEL:
    __all__ += ['OtelWrapperMeter', 'OtelWrapperValueRecorder']
    from couchbase.common.observability.otel_metrics import OtelWrapperMeter  # noqa: F401
    from couchbase.common.observability.otel_metrics import OtelWrapperValueRecorder  # noqa: F401
