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

from couchbase.common._observability.logging_meter import LoggingMeter
from couchbase.common._observability.no_op import (NoOpMeter,
                                                   NoOpSpan,
                                                   NoOpTracer,
                                                   NoOpValueRecorder)
from couchbase.common._observability.threshold_logging import ThresholdLoggingTracer

from .handler import (CollectionDetails,
                      ObservableRequestHandler,
                      WrappedSpan)
from .observability_types import (DispatchAttributeName,
                                  LegacySpanProtocol,
                                  LegacyTracerProtocol,
                                  MeterProtocol,
                                  ObservabilityInstruments,
                                  OpAttributeName,
                                  OpName,
                                  RequestSpanProtocol,
                                  RequestTracerProtocol,
                                  ServiceType,
                                  SpanProtocol,
                                  TracerProtocol,
                                  WrappedTracer)

__all__ = [
    'CollectionDetails',
    'DispatchAttributeName',
    'LegacySpanProtocol',
    'LegacyTracerProtocol',
    'LoggingMeter',
    'MeterProtocol',
    'NoOpMeter',
    'NoOpSpan',
    'NoOpTracer',
    'NoOpValueRecorder',
    'ObservabilityInstruments',
    'ObservableRequestHandler',
    'OpAttributeName',
    'OpName',
    'RequestSpanProtocol',
    'RequestTracerProtocol',
    'ServiceType',
    'SpanProtocol',
    'ThresholdLoggingTracer',
    'TracerProtocol',
    'WrappedSpan',
    'WrappedTracer'
]
