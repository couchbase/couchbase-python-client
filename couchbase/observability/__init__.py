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

"""
Observability integrations for the Couchbase Python SDK.

This package provides integrations with observability platforms:
- OpenTelemetry tracing (otel_tracing)
- OpenTelemetry metrics (otel_metrics)
"""

__all__ = []

# OpenTelemetry integrations (optional - only available if otel installed)
try:
    from couchbase.observability.otel_tracing import get_otel_tracer  # noqa: F401
    __all__.append('get_otel_tracer')
except ImportError:
    pass  # OTel not installed

try:
    from couchbase.observability.otel_metrics import get_otel_meter  # noqa: F401
    __all__.append('get_otel_meter')
except ImportError:
    pass  # OTel not installed
